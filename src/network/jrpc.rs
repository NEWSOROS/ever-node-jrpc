/*
* In-process JSON-RPC ("JRPC") server of the node - the "simple" API of
* broxus/everscale-jrpc (Apache-2.0) that nekoton based wallets speak, served
* straight from the node's own state:
*
*   getCapabilities, getStatus, getTimings, getLatestKeyBlock, getBlockchainConfig,
*   getContractState, getLibraryCell, sendMessage
*
* and, with a "history" section, getTransactionsList, getTransaction and getDstTransaction
* of the "full" mode for the accounts the node indexes (network/jrpc_history.rs), plus
* getHistoryStatus, a method of this server only. HTTP/1.1 and HTTP/2 without TLS
* (nekoton-transport's client speaks h2c from the first byte).
*
* Nothing is indexed and nothing is copied: every request reads the latest applied
* state the node already keeps (pinned only for the duration of the request, like the
* console's getaccountstate) or its last key block. sendMessage hands the message to the
* node exactly like the console's sendmessage - a successful answer means "taken for
* broadcast", not "executed"; clients follow the account state for the outcome.
*
* Disabled unless the node config has a "jrpc_server" section. It listens only on a
* loopback or private address (there is no authorization, and every request is served
* by the node process itself); a server that cannot start is logged and left off until
* the node is started again, the node runs without it.
*/

use crate::{
    engine::Engine, engine_traits::EngineOperations,
    network::jrpc_history::{self, History, HistoryConfig, TxHistory, MAX_LIST_LIMIT},
    shard_states_keeper::PinnedShardStateGuard,
};

use ever_block::{
    base64_decode, base64_encode, error, fail, read_single_root_boc, write_boc, Block, BlockIdExt, Cell,
    Deserializable, Message, MsgAddressInt, Result, Serializable, ShardAccount, SliceData,
    UInt256,
};
use hyper::{
    body::HttpBody, header, service::{make_service_fn, service_fn}, Body, Method, Request,
    Response, Server, StatusCode,
};
use serde_json::{json, Value};
use std::{convert::Infallible, net::{IpAddr, SocketAddr}, str::FromStr, sync::Arc, time::Duration};
use tokio::sync::{Mutex, Semaphore};

/// Largest request body accepted (an external message is a few KB).
pub const MAX_BODY_BYTES: usize = 1 << 20;
/// A request that takes longer than this is answered with "Not ready"; a request body
/// that takes longer to arrive is cut off.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(15);
/// How long a request may wait for a free slot before "Too many requests".
const QUEUE_TIMEOUT: Duration = Duration::from_secs(10);

pub const CAPABILITIES: [&str; 8] = [
    "getCapabilities", "getLatestKeyBlock", "getBlockchainConfig", "getStatus", "getTimings",
    "getContractState", "getLibraryCell", "sendMessage",
];
/// Added to CAPABILITIES when the history index is on. Not getAccountsByCodeHash: that
/// needs an index of every account.
pub const HISTORY_CAPABILITIES: [&str; 3] = ["getTransactionsList", "getTransaction", "getDstTransaction"];

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct JrpcServerConfig {
    pub listen_address: SocketAddr,
    #[serde(default = "JrpcServerConfig::default_max_concurrent_requests")]
    pub max_concurrent_requests: usize,
    /// Transaction history (network/jrpc_history.rs): the accounts listed in a file for
    /// good, every other account for a number of days.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub history: Option<HistoryConfig>,
}

impl JrpcServerConfig {
    fn default_max_concurrent_requests() -> usize { 32 }
}

/// Error codes and messages of broxus/everscale-jrpc (plus -32700/-32600/-32009).
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum QueryError {
    ParseError,
    InvalidRequest,
    MethodNotFound,
    InvalidParams,
    NotReady,
    NotSupported,
    ConnectionError,
    StorageError,
    FailedToSerialize,
    InvalidAccountState,
    InvalidMessage,
    Busy,
}

impl QueryError {
    pub fn info(&self) -> (i32, &'static str) {
        match self {
            Self::ParseError => (-32700, "Parse error"),
            Self::InvalidRequest => (-32600, "Invalid request"),
            Self::MethodNotFound => (-32601, "Method not found"),
            Self::InvalidParams => (-32602, "Invalid params"),
            Self::NotReady => (-32001, "Not ready"),
            Self::NotSupported => (-32002, "Not supported"),
            Self::ConnectionError => (-32003, "Connection error"),
            Self::StorageError => (-32004, "Storage error"),
            Self::FailedToSerialize => (-32005, "Failed to serialize"),
            Self::InvalidAccountState => (-32006, "Invalid account state"),
            Self::InvalidMessage => (-32007, "Invalid message"),
            Self::Busy => (-32009, "Too many requests"),
        }
    }
}

/// An error answer: the code, and a detail for the "data" field.
pub type Failure = (QueryError, Option<String>);
pub type QueryResult<T> = std::result::Result<T, Failure>;

fn fail_with(error: QueryError, detail: impl std::fmt::Display) -> Failure {
    (error, Some(detail.to_string()))
}

pub fn success(id: &Value, result: Value) -> Value {
    json!({"jsonrpc": "2.0", "result": result, "id": id})
}

pub fn failure(id: &Value, error: QueryError, detail: Option<String>) -> Value {
    let (code, message) = error.info();
    json!({"jsonrpc": "2.0", "error": {"code": code, "message": message, "data": detail}, "id": id})
}

// ---- what the server needs from the node -----------------------------------------

/// An account as the node's latest state holds it; the guard keeps that state pinned
/// while the answer is built.
pub struct ContractState {
    pub account: Option<ShardAccount>,
    pub gen_utime: u32,
    pub _guard: Option<PinnedShardStateGuard>,
}

pub struct KeyBlock {
    pub seqno: u32,
    pub block: Block,
    /// the block's BOC as stored by the node
    pub boc: Vec<u8>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Timings {
    pub last_mc_block_seqno: u32,
    pub last_shard_client_mc_block_seqno: u32,
    pub last_mc_utime: u32,
    pub mc_time_diff: i64,
    pub shard_client_time_diff: i64,
}

#[async_trait::async_trait]
pub trait JrpcBackend: Send + Sync {
    fn ready(&self) -> bool;
    fn timings(&self) -> Result<Timings>;
    async fn contract_state(&self, address: &MsgAddressInt) -> Result<ContractState>;
    /// The id of the latest key block, read from the masterchain state (cheap).
    async fn latest_key_block_id(&self) -> Result<BlockIdExt>;
    /// The key block itself, loaded from the node's block storage.
    async fn load_key_block(&self, id: &BlockIdExt) -> Result<KeyBlock>;
    async fn library_cell(&self, hash: &UInt256) -> Result<Option<Cell>>;
    async fn send_message(&self, data: &[u8], hash: UInt256) -> Result<()>;
}

pub struct EngineBackend {
    engine: Arc<dyn EngineOperations>,
}

#[async_trait::async_trait]
impl JrpcBackend for EngineBackend {
    fn ready(&self) -> bool {
        self.engine.get_sync_status() == Engine::SYNC_STATUS_FINISH_SYNC
    }

    fn timings(&self) -> Result<Timings> {
        let now = self.engine.now() as i64;
        let mc_id = self.engine.load_last_applied_mc_block_id()?
            .ok_or_else(|| error!("no last applied masterchain block yet"))?;
        let mc_utime = self.engine.load_block_handle(&mc_id)?
            .ok_or_else(|| error!("no handle for {}", mc_id))?.gen_utime()?;
        let sc_id = self.engine.load_shard_client_mc_block_id()?
            .ok_or_else(|| error!("no shard client masterchain block yet"))?;
        let sc_utime = self.engine.load_block_handle(&sc_id)?
            .ok_or_else(|| error!("no handle for {}", sc_id))?.gen_utime()?;
        Ok(Timings {
            last_mc_block_seqno: mc_id.seq_no(),
            last_shard_client_mc_block_seqno: sc_id.seq_no(),
            last_mc_utime: mc_utime,
            mc_time_diff: now - mc_utime as i64,
            shard_client_time_diff: now - sc_utime as i64,
        })
    }

    /// The same lookup as the console's getaccountstate (control.rs find_account).
    async fn contract_state(&self, addr: &MsgAddressInt) -> Result<ContractState> {
        let state = if addr.is_masterchain() {
            let mc_block_id = self.engine.load_last_applied_mc_block_id()?
                .ok_or_else(|| error!("no last applied masterchain block yet"))?;
            self.engine.load_and_pin_state(&mc_block_id).await?
        } else {
            let mc_block_id = self.engine.load_shard_client_mc_block_id()?
                .ok_or_else(|| error!("no shard client masterchain block yet"))?;
            let mc_state = self.engine.load_and_pin_state(&mc_block_id).await?;
            let mut shard_state = None;
            for id in mc_state.state().top_blocks(addr.workchain_id())? {
                if id.shard().contains_account(addr.address().clone())? {
                    shard_state = Some(self.engine.load_and_pin_state(&id).await?);
                    break;
                }
            }
            shard_state.ok_or_else(|| error!("no actual shard state for {}", addr))?
        };
        let account = state.state().shard_account(&addr.address())?;
        let gen_utime = state.state().state()?.gen_time();
        Ok(ContractState { account, gen_utime, _guard: Some(state) })
    }

    async fn latest_key_block_id(&self) -> Result<BlockIdExt> {
        let mc_state = self.engine.load_last_applied_mc_state().await?;
        let extra = mc_state.shard_state_extra()?;
        Ok(if extra.after_key_block {
            mc_state.block_id().clone()
        } else {
            extra.last_key_block.clone()
                .ok_or_else(|| error!("the masterchain state names no key block"))?
                .master_block_id().1
        })
    }

    async fn load_key_block(&self, id: &BlockIdExt) -> Result<KeyBlock> {
        let handle = self.engine.load_block_handle(id)?
            .ok_or_else(|| error!("no handle for key block {}", id))?;
        let block = self.engine.load_block(&handle).await?;
        Ok(KeyBlock { seqno: id.seq_no(), block: block.block()?.clone(), boc: block.data().to_vec() })
    }

    async fn library_cell(&self, hash: &UInt256) -> Result<Option<Cell>> {
        let mc_state = self.engine.load_last_applied_mc_state().await?;
        let found = mc_state.state()?.libraries().get(hash)?;
        Ok(found.map(|descr| descr.lib().clone()))
    }

    async fn send_message(&self, data: &[u8], hash: UInt256) -> Result<()> {
        self.engine.redirect_external_message(data, hash).await
    }
}

// ---- answers (pure, tested with real chain data) ----------------------------------

/// The account as nekoton reads it (serde_account_stuff): the Account cell without its
/// leading constructor bit, i.e. addr, storage_stat and storage exactly as stored.
/// None for account_none.
pub fn account_stuff_boc(account_cell: Cell) -> Result<Option<Vec<u8>>> {
    let mut slice = SliceData::load_cell(account_cell)?;
    if !slice.get_next_bit()? {
        return Ok(None);
    }
    Ok(Some(write_boc(&slice.into_cell())?))
}

fn timings_json(gen_lt: u64, gen_utime: u32) -> Value {
    json!({"genLt": gen_lt.to_string(), "genUtime": gen_utime})
}

/// getContractState: exists / notExists / unchanged (the client already has this lt).
pub fn contract_state_json(
    account: Option<&ShardAccount>, gen_utime: u32, known_lt: Option<u64>
) -> QueryResult<Value> {
    let Some(shard_account) = account else {
        return Ok(json!({"type": "notExists", "timings": timings_json(0, gen_utime)}));
    };
    let lt = shard_account.last_trans_lt();
    let stuff = account_stuff_boc(shard_account.account_cell())
        .map_err(|e| fail_with(QueryError::InvalidAccountState, e))?;
    let Some(boc) = stuff else {
        return Ok(json!({"type": "notExists", "timings": timings_json(0, gen_utime)}));
    };
    if known_lt.map_or(false, |known| lt <= known) {
        return Ok(json!({"type": "unchanged", "timings": timings_json(lt, gen_utime)}));
    }
    Ok(json!({
        "type": "exists",
        "account": base64_encode(&boc),
        "timings": timings_json(lt, gen_utime),
        "lastTransactionId": {
            "isExact": true,
            "lt": lt.to_string(),
            "hash": shard_account.last_trans_hash().to_hex_string(),
        },
    }))
}

pub fn key_block_json(key_block: &KeyBlock) -> Value {
    json!({"block": base64_encode(&key_block.boc)})
}

/// getBlockchainConfig: the config of the latest key block, as broxus/everscale-jrpc.
pub fn blockchain_config_json(key_block: &KeyBlock) -> QueryResult<Value> {
    let custom = key_block.block.read_extra()
        .and_then(|extra| extra.read_custom())
        .map_err(|e| fail_with(QueryError::NotReady, e))?
        .ok_or_else(|| fail_with(QueryError::NotReady, "key block without masterchain extra"))?;
    let config = custom.config()
        .ok_or_else(|| fail_with(QueryError::NotReady, "key block without config"))?;
    let cell = config.write_to_new_cell()
        .and_then(|builder| builder.into_cell())
        .map_err(|e| fail_with(QueryError::FailedToSerialize, e))?;
    let boc = write_boc(&cell).map_err(|e| fail_with(QueryError::FailedToSerialize, e))?;
    Ok(json!({
        "globalId": key_block.block.global_id(),
        "config": base64_encode(&boc),
        "seqno": key_block.seqno,
    }))
}

pub fn timings_response(t: &Timings, smallest_known_lt: Option<u64>) -> Value {
    // snake_case keys; smallest_known_lt is null without a history index (the "simple"
    // API) and a number with one (u64::MAX while it is empty, as jrpc.everwallet.net)
    json!({
        "last_mc_block_seqno": t.last_mc_block_seqno,
        "last_shard_client_mc_block_seqno": t.last_shard_client_mc_block_seqno,
        "last_mc_utime": t.last_mc_utime,
        "mc_time_diff": t.mc_time_diff,
        "shard_client_time_diff": t.shard_client_time_diff,
        "smallest_known_lt": smallest_known_lt,
    })
}

/// sendMessage accepts only a well-formed external inbound message; returns its hash.
pub fn check_external_message(data: &[u8]) -> QueryResult<UInt256> {
    let cell = read_single_root_boc(data).map_err(|e| fail_with(QueryError::InvalidMessage, e))?;
    let hash = cell.repr_hash();
    let message = Message::construct_from_cell(cell)
        .map_err(|e| fail_with(QueryError::InvalidMessage, e))?;
    if message.ext_in_header().is_none() {
        return Err(fail_with(QueryError::InvalidMessage, "not an external inbound message"));
    }
    Ok(hash)
}

// ---- parameters -------------------------------------------------------------------

fn string_or_number(value: &Value) -> Option<u64> {
    match value {
        Value::Number(n) => n.as_u64(),
        Value::String(s) => s.parse().ok(),
        _ => None,
    }
}

fn param_str<'a>(params: &'a Value, name: &str) -> QueryResult<&'a str> {
    params.get(name).and_then(|v| v.as_str())
        .ok_or_else(|| fail_with(QueryError::InvalidParams, format!("missing string param '{}'", name)))
}

/// An optional lt: absent or null means none; else a number or a decimal string.
fn param_optional_lt(params: &Value, name: &str) -> QueryResult<Option<u64>> {
    match params.get(name) {
        None | Some(Value::Null) => Ok(None),
        Some(value) => string_or_number(value).map(Some)
            .ok_or_else(|| fail_with(QueryError::InvalidParams, format!("{} must be a number", name))),
    }
}

/// "account" of the history methods: a standard 256-bit address.
fn param_account(params: &Value) -> QueryResult<(i8, UInt256)> {
    let (workchain, account) = jrpc_history::std_address(param_str(params, "account")?)
        .map_err(|e| fail_with(QueryError::InvalidParams, e))?;
    let workchain = i8::try_from(workchain)
        .map_err(|_| fail_with(QueryError::InvalidParams, "workchain out of range"))?;
    Ok((workchain, account))
}

/// "limit" as jrpc.everwallet.net takes it: a number (not a string); 0 asks for nothing,
/// more than 100 is cut to 100.
fn param_limit(params: &Value) -> QueryResult<usize> {
    let limit = params.get("limit").and_then(Value::as_u64).filter(|limit| *limit <= u64::from(u32::MAX))
        .ok_or_else(|| fail_with(QueryError::InvalidParams, "limit must be a number"))?;
    Ok((limit as usize).min(MAX_LIST_LIMIT))
}

fn storage_error(e: ever_block::Error) -> Failure {
    fail_with(QueryError::StorageError, e)
}

fn parse_hash(hex_str: &str) -> QueryResult<UInt256> {
    let bytes = hex::decode(hex_str).map_err(|e| fail_with(QueryError::InvalidParams, e))?;
    if bytes.len() != 32 {
        return Err(fail_with(QueryError::InvalidParams, "hash must be 32 bytes"));
    }
    Ok(UInt256::from_slice(&bytes))
}

// ---- the server -------------------------------------------------------------------

pub struct JrpcServer<B: JrpcBackend> {
    backend: B,
    key_block: Mutex<Option<(u32, Value, Value)>>,
    limiter: Semaphore,
    // the three time limits: constants in the node, shortened in the tests
    body_timeout: Duration,
    request_timeout: Duration,
    queue_timeout: Duration,
    history: Option<Arc<TxHistory>>,
}

impl<B: JrpcBackend> JrpcServer<B> {
    pub fn new(backend: B, max_concurrent_requests: usize) -> Self {
        Self {
            backend,
            key_block: Mutex::new(None),
            limiter: Semaphore::new(max_concurrent_requests.max(1)),
            body_timeout: REQUEST_TIMEOUT,
            request_timeout: REQUEST_TIMEOUT,
            queue_timeout: QUEUE_TIMEOUT,
            history: None,
        }
    }

    pub fn with_history(mut self, history: Arc<TxHistory>) -> Self {
        self.history = Some(history);
        self
    }

    /// One JSON-RPC 2.0 request body in, one response object out.
    pub async fn handle_body(&self, body: &[u8]) -> Value {
        let request: Value = match serde_json::from_slice(body) {
            Ok(value) => value,
            Err(e) => return failure(&Value::Null, QueryError::ParseError, Some(e.to_string())),
        };
        let id = request.get("id").cloned().unwrap_or(Value::Null);
        let Some(method) = request.get("method").and_then(|m| m.as_str()) else {
            return failure(&id, QueryError::InvalidRequest, Some("no method".to_string()));
        };
        let params = request.get("params").cloned().unwrap_or(Value::Null);
        match tokio::time::timeout(self.request_timeout, self.dispatch(method, &params)).await {
            Ok(Ok(result)) => success(&id, result),
            Ok(Err((error, detail))) => failure(&id, error, detail),
            Err(_) => failure(&id, QueryError::NotReady, Some("timed out".to_string())),
        }
    }

    async fn dispatch(&self, method: &str, params: &Value) -> QueryResult<Value> {
        match method {
            "getCapabilities" => Ok(self.capabilities()),
            "getStatus" => Ok(json!({"ready": self.backend.ready()})),
            "getTimings" => self.backend.timings()
                .map(|t| timings_response(&t, self.history.as_ref().map(|history| history.smallest_known_lt())))
                .map_err(|e| fail_with(QueryError::NotReady, e)),
            "getContractState" => self.get_contract_state(params).await,
            "sendMessage" => self.send_message(params).await,
            "getLatestKeyBlock" => self.key_block_answers().await.map(|(block, _)| block),
            "getBlockchainConfig" => self.key_block_answers().await.map(|(_, config)| config),
            "getLibraryCell" => self.get_library_cell(params).await,
            "getTransactionsList" => self.get_transactions_list(params),
            "getTransaction" => self.get_transaction(params),
            "getDstTransaction" => self.get_dst_transaction(params),
            "getHistoryStatus" => self.history()?.status().map_err(storage_error),
            _ => Err(fail_with(QueryError::MethodNotFound, format!("method `{}` not found", method))),
        }
    }

    fn capabilities(&self) -> Value {
        let mut all = CAPABILITIES.to_vec();
        if self.history.is_some() {
            all.extend(HISTORY_CAPABILITIES);
        }
        json!(all)
    }

    /// The history index, or "Not supported" as everscale-jrpc answers in simple mode.
    fn history(&self) -> QueryResult<&TxHistory> {
        self.history.as_deref()
            .ok_or_else(|| fail_with(QueryError::NotSupported, "this node keeps no transaction history"))
    }

    /// Newest first, lt <= lastTransactionLt; [] for an account the node does not index.
    fn get_transactions_list(&self, params: &Value) -> QueryResult<Value> {
        let (workchain, account) = param_account(params)?;
        let last_lt = param_optional_lt(params, "lastTransactionLt")?;
        let limit = param_limit(params)?;
        let history = self.history()?;
        if limit == 0 {
            return Ok(json!([]));
        }
        let found = history.list(workchain, &account, last_lt, limit).map_err(storage_error)?;
        Ok(json!(found.iter().map(|boc| base64_encode(boc)).collect::<Vec<_>>()))
    }

    fn get_transaction(&self, params: &Value) -> QueryResult<Value> {
        let hash = parse_hash(param_str(params, "id")?)?;
        let found = self.history()?.by_hash(&hash).map_err(storage_error)?;
        Ok(found.map_or(Value::Null, |boc| json!(base64_encode(&boc))))
    }

    /// The transaction that consumed the message with this hash (its inbound message).
    fn get_dst_transaction(&self, params: &Value) -> QueryResult<Value> {
        let hash = parse_hash(param_str(params, "messageHash")?)?;
        let found = self.history()?.by_in_msg(&hash).map_err(storage_error)?;
        Ok(found.map_or(Value::Null, |boc| json!(base64_encode(&boc))))
    }

    async fn get_contract_state(&self, params: &Value) -> QueryResult<Value> {
        let address = MsgAddressInt::from_str(param_str(params, "address")?)
            .map_err(|e| fail_with(QueryError::InvalidParams, e))?;
        let known_lt = param_optional_lt(params, "lastTransactionLt")?;
        let state = self.backend.contract_state(&address).await
            .map_err(|e| fail_with(QueryError::NotReady, e))?;
        // the state stays pinned (state._guard) until the answer is built
        contract_state_json(state.account.as_ref(), state.gen_utime, known_lt)
    }

    async fn send_message(&self, params: &Value) -> QueryResult<Value> {
        let data = base64_decode(param_str(params, "message")?)
            .map_err(|e| fail_with(QueryError::InvalidMessage, e))?;
        let hash = check_external_message(&data)?;
        self.backend.send_message(&data, hash).await
            .map_err(|e| fail_with(QueryError::ConnectionError, e))?;
        Ok(Value::Null)
    }

    async fn get_library_cell(&self, params: &Value) -> QueryResult<Value> {
        let hash = parse_hash(param_str(params, "hash")?)?;
        let cell = self.backend.library_cell(&hash).await
            .map_err(|e| fail_with(QueryError::NotReady, e))?;
        let boc = match cell {
            Some(cell) => Some(base64_encode(
                write_boc(&cell).map_err(|e| fail_with(QueryError::FailedToSerialize, e))?)),
            None => None,
        };
        Ok(json!({"cell": boc}))
    }

    /// Key block and config answers: the key block is loaded and the answers are built
    /// only when a new key block appears; otherwise the cached answers are returned.
    async fn key_block_answers(&self) -> QueryResult<(Value, Value)> {
        let id = self.backend.latest_key_block_id().await
            .map_err(|e| fail_with(QueryError::NotReady, e))?;
        let mut cached = self.key_block.lock().await;
        if let Some((seqno, block, config)) = cached.as_ref() {
            if *seqno == id.seq_no() {
                return Ok((block.clone(), config.clone()));
            }
        }
        let key_block = self.backend.load_key_block(&id).await
            .map_err(|e| fail_with(QueryError::NotReady, e))?;
        let answers = (key_block_json(&key_block), blockchain_config_json(&key_block)?);
        *cached = Some((key_block.seqno, answers.0.clone(), answers.1.clone()));
        Ok(answers)
    }

    async fn http(&self, request: Request<Body>) -> Response<Body> {
        if !matches!(request.uri().path(), "/" | "/rpc") {
            return plain(StatusCode::NOT_FOUND, "not found");
        }
        if request.method() != Method::POST {
            return plain(StatusCode::METHOD_NOT_ALLOWED, "use POST");
        }
        let reading = read_limited(request.into_body(), MAX_BODY_BYTES);
        let body = match tokio::time::timeout(self.body_timeout, reading).await {
            Ok(Ok(body)) => body,
            Ok(Err(status)) => return plain(status, "bad body"),
            Err(_) => return plain(StatusCode::REQUEST_TIMEOUT, "body too slow"),
        };
        // the slot is held until the answer is built; a request that got none is refused
        // before its body is looked at, so that answer carries no id
        let answer = match tokio::time::timeout(self.queue_timeout, self.limiter.acquire()).await {
            Ok(Ok(_permit)) => self.handle_body(&body).await,
            _ => failure(&Value::Null, QueryError::Busy, None),
        };
        let mut response = Response::new(Body::from(answer.to_string()));
        response.headers_mut().insert(header::CONTENT_TYPE, header::HeaderValue::from_static("application/json"));
        response
    }
}

fn plain(status: StatusCode, text: &'static str) -> Response<Body> {
    let mut response = Response::new(Body::from(text));
    *response.status_mut() = status;
    response
}

async fn read_limited(mut body: Body, limit: usize) -> std::result::Result<Vec<u8>, StatusCode> {
    let mut out = Vec::new();
    while let Some(chunk) = body.data().await {
        let chunk = chunk.map_err(|_| StatusCode::BAD_REQUEST)?;
        if out.len() + chunk.len() > limit {
            return Err(StatusCode::PAYLOAD_TOO_LARGE);
        }
        out.extend_from_slice(&chunk);
    }
    Ok(out)
}

/// Loopback, private networks (RFC 1918, fc00::/7) and the shared address space
/// 100.64.0.0/10 (RFC 6598) that overlay VPNs use: addresses that are not routed on the
/// internet. "Any address" (0.0.0.0, ::) would listen on the public interfaces too.
pub fn is_private_listen_address(ip: &IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => {
            let [a, b, _, _] = v4.octets();
            v4.is_loopback() || v4.is_private() || (a == 100 && (b & 0xc0) == 64)
        }
        IpAddr::V6(v6) => v6.is_loopback() || (v6.segments()[0] & 0xfe00) == 0xfc00,
    }
}

/// Starts the server on the node's runtime and opens the history index when configured -
/// returned for the indexer, which starts after the node's boot. Neither can stop the
/// node: what cannot start (a public or taken address, an unreadable accounts file) is
/// logged and left off, and the node keeps validating without it.
pub fn start(config: JrpcServerConfig, engine: Arc<dyn EngineOperations>) -> Option<History> {
    let history = config.history.as_ref().and_then(|history_config| {
        let opened = engine.db_root_dir().and_then(|root| History::open(history_config, root));
        opened.map_err(|e| log::error!("JRPC history is off: {}", e)).ok()
    });
    let index = history.as_ref().map(|history| history.index.clone());
    if let Err(e) = serve(config, EngineBackend { engine }, index) {
        log::error!("JRPC server is off: {}", e);
    }
    history
}

/// Binds the address and serves on the current runtime; returns the address it listens on.
pub fn serve<B: JrpcBackend + 'static>(
    config: JrpcServerConfig, backend: B, history: Option<Arc<TxHistory>>
) -> Result<SocketAddr> {
    if !is_private_listen_address(&config.listen_address.ip()) {
        fail!("JRPC server refuses to listen on {}: not a loopback or private address",
              config.listen_address);
    }
    let mut server = JrpcServer::new(backend, config.max_concurrent_requests);
    if let Some(history) = history {
        server = server.with_history(history);
    }
    let server = Arc::new(server);
    let make_service = make_service_fn(move |_connection| {
        let server = server.clone();
        async move {
            Ok::<_, Infallible>(service_fn(move |request| {
                let server = server.clone();
                async move { Ok::<_, Infallible>(server.http(request).await) }
            }))
        }
    });
    let wanted = config.listen_address;
    let incoming = hyper::server::conn::AddrIncoming::bind(&wanted)
        .map_err(|e| error!("JRPC server cannot listen on {}: {}", wanted, e))?;
    let address = incoming.local_addr();
    let http = Server::builder(incoming).serve(make_service);
    log::info!("JRPC server listening on {}", address);
    tokio::spawn(async move {
        if let Err(e) = http.await {
            log::error!("JRPC server stopped: {}", e);
        }
    });
    Ok(address)
}

#[cfg(test)]
#[path = "../tests/test_jrpc.rs"]
mod tests;
