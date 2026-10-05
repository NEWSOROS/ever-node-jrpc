/*
* Tests of the in-process JRPC server (network/jrpc.rs).
*
* The static/jrpc/golden_*.json files are answers of this server recorded on an Everscale
* mainnet node on 2026-10-02 (static/jrpc/README.md): getContractState of an ordinary
* wallet, getLatestKeyBlock and getBlockchainConfig. static/jrpc/wallet.account.boc is the
* same wallet's Account cell read another way at the same moment - the node console's
* getaccountstate saves the account cell of the ShardAccount - and static/jrpc/history has
* the wallet's transactions cut from the blocks themselves; the tests check that the three
* agree. The answers' shapes and the error behaviour were compared with
* broxus/everscale-jrpc's public endpoint while it was online (2026-09-24).
*/

use super::*;
use ever_block::{
    Account, BuilderData, ConfigParams, ExternalInboundMessageHeader, IBitstring,
    InternalMessageHeader, MsgAddressExt, Transaction, read_single_root_boc,
};
use std::sync::Mutex as StdMutex;

const STATIC: &str = "src/tests/static/jrpc";
/// The last transaction of the wallet in the fixtures - the newest one of
/// static/jrpc/history/wallet.json - and the state's generation time in the recorded answer.
const WALLET_LAST_LT: u64 = 76409127000006;
const WALLET_LAST_HASH: &str = "e6422c32acab9ec6c806f390edd08d8fb3bf1a1ecedac161f2119da64849e33b";
const WALLET_GEN_UTIME: u32 = 1790938993;
/// The key block of the fixtures.
const KEY_BLOCK_SEQNO: u32 = 62542934;

fn fixture(name: &str) -> Vec<u8> {
    std::fs::read(format!("{}/{}", STATIC, name)).unwrap()
}

fn golden(name: &str) -> Value {
    serde_json::from_slice(&fixture(&format!("golden_{}.json", name))).unwrap()
}

/// The ShardAccount as the node's state holds it: the account cell plus the id of the
/// account's last transaction.
fn wallet_account() -> ShardAccount {
    let account = read_single_root_boc(fixture("wallet.account.boc")).unwrap();
    ShardAccount::with_account_root(account, UInt256::from_str(WALLET_LAST_HASH).unwrap(), WALLET_LAST_LT)
}

fn boc_hash(b64: &str) -> UInt256 {
    read_single_root_boc(base64_decode(b64).unwrap()).unwrap().repr_hash()
}

#[test]
fn test_fixture_is_the_account_of_that_transaction() {
    // three ways to the same state: the transaction from the block names the account cell
    // the console saved as the state it left, and the recorded answer names that transaction
    let newest = &transactions("wallet")[0];
    let cell = read_single_root_boc(base64_decode(newest).unwrap()).unwrap();
    assert_eq!(cell.repr_hash().to_hex_string(), WALLET_LAST_HASH);
    let transaction = Transaction::construct_from_cell(cell).unwrap();
    assert_eq!(transaction.logical_time(), WALLET_LAST_LT);
    let shard_account = wallet_account();
    assert_eq!(transaction.read_state_update().unwrap().new_hash, shard_account.account_cell().repr_hash());

    // storage.last_trans_lt is the transaction's end lt: lt + 1 for a transaction without
    // outbound messages
    assert_eq!(transaction.msg_count(), 0);
    let account = shard_account.read_account().unwrap();
    assert_eq!(account.last_tr_time(), Some(WALLET_LAST_LT + 1));
    assert_eq!(account.get_addr().unwrap().to_string(), WALLET);
}

// ---- answers are what the server answered for the same state on a live node -----------

#[test]
fn test_contract_state_exists_matches_the_recorded_answer() {
    let recorded = &golden("state_wallet")["result"];
    let gen_utime = recorded["timings"]["genUtime"].as_u64().unwrap() as u32;
    assert_eq!(gen_utime, WALLET_GEN_UTIME);
    let ours = contract_state_json(Some(&wallet_account()), gen_utime, None).unwrap();
    assert_eq!(ours["type"], "exists");
    assert_eq!(ours["timings"], json!({"genLt": WALLET_LAST_LT.to_string(), "genUtime": WALLET_GEN_UTIME}));
    assert_eq!(ours["lastTransactionId"], json!({"isExact": true, "lt": WALLET_LAST_LT.to_string(), "hash": WALLET_LAST_HASH}));
    // the account read through the console gives the answer the live server gave
    assert_eq!(ours, *recorded);

    // what nekoton reads (AccountStuff): the Account cell without its leading constructor
    // bit. Putting the bit back gives the account cell itself
    let stuff = read_single_root_boc(base64_decode(ours["account"].as_str().unwrap()).unwrap()).unwrap();
    let account = wallet_account().account_cell();
    assert_eq!(stuff.bit_length() + 1, account.bit_length());
    let mut builder = BuilderData::new();
    builder.append_bit_one().unwrap();
    builder.checked_append_references_and_data(&SliceData::load_cell(stuff).unwrap()).unwrap();
    assert_eq!(builder.into_cell().unwrap().repr_hash(), account.repr_hash());
}

#[test]
fn test_contract_state_unchanged_and_not_exists() {
    let recorded = &golden("state_unchanged")["result"];
    let gen_utime = recorded["timings"]["genUtime"].as_u64().unwrap() as u32;
    let lt = wallet_account().last_trans_lt();
    assert_eq!(contract_state_json(Some(&wallet_account()), gen_utime, Some(lt)).unwrap(), *recorded);
    assert_eq!(recorded["type"], "unchanged");
    assert_eq!(contract_state_json(Some(&wallet_account()), gen_utime, Some(lt + 5)).unwrap()["type"], "unchanged");
    assert_eq!(contract_state_json(Some(&wallet_account()), gen_utime, Some(lt - 1)).unwrap()["type"], "exists");

    let none = &golden("state_none")["result"];
    let utime = none["timings"]["genUtime"].as_u64().unwrap() as u32;
    assert_eq!(contract_state_json(None, utime, None).unwrap(), *none);
    assert_eq!(none["type"], "notExists");
    assert_eq!(none["timings"]["genLt"], "0");
    // a record in the state that holds no account (account_none) is "notExists" as well
    let empty = ShardAccount::with_account_root(Account::default().serialize().unwrap(), UInt256::default(), 0);
    assert_eq!(contract_state_json(Some(&empty), utime, None).unwrap(), *none);
    assert_eq!(contract_state_json(Some(&empty), utime, Some(5)).unwrap(), *none);
}

#[test]
fn test_blockchain_config_matches_the_recorded_answer() {
    let recorded = &golden("config")["result"];
    let boc = base64_decode(golden("keyblock")["result"]["block"].as_str().unwrap()).unwrap();
    let block = Block::construct_from_bytes(&boc).unwrap();
    // the two recorded answers are of one key block
    let info = block.read_info().unwrap();
    assert!(info.key_block());
    assert_eq!(info.seq_no(), KEY_BLOCK_SEQNO);
    assert_eq!(recorded["seqno"], KEY_BLOCK_SEQNO);

    let key_block = KeyBlock { seqno: KEY_BLOCK_SEQNO, block, boc: boc.clone() };
    let ours = blockchain_config_json(&key_block).unwrap();
    assert_eq!(ours, *recorded);
    assert_eq!(ours["globalId"], 42);
    assert_eq!(key_block_json(&key_block)["block"].as_str().unwrap(), base64_encode(&boc));

    // what a client does with the answer: the config parses, it is the config contract's
    // and it names the validators
    let cell = read_single_root_boc(base64_decode(ours["config"].as_str().unwrap()).unwrap()).unwrap();
    let config = ConfigParams::construct_from_cell(cell).unwrap();
    assert_eq!(config.config_addr, UInt256::from([0x55; 32]));
    assert!(config.validator_set().unwrap().total() > 0);
}

#[test]
fn test_timings_keep_the_snake_case_keys() {
    let t = Timings { last_mc_block_seqno: 7, last_shard_client_mc_block_seqno: 6, last_mc_utime: 100,
                      mc_time_diff: 3, shard_client_time_diff: 5 };
    let ours = timings_response(&t, None);
    // everscale-jrpc's field names: snake_case here, unlike the camelCase of the other answers
    let mut keys: Vec<&str> = ours.as_object().unwrap().keys().map(String::as_str).collect();
    keys.sort();
    assert_eq!(keys, ["last_mc_block_seqno", "last_mc_utime", "last_shard_client_mc_block_seqno",
                      "mc_time_diff", "shard_client_time_diff", "smallest_known_lt"]);
    assert_eq!(ours, json!({"last_mc_block_seqno": 7, "last_shard_client_mc_block_seqno": 6, "last_mc_utime": 100,
        "mc_time_diff": 3, "shard_client_time_diff": 5, "smallest_known_lt": null}));
    assert_eq!(timings_response(&t, Some(9))["smallest_known_lt"], 9);
}

// ---- messages -------------------------------------------------------------------------

fn external_message() -> Vec<u8> {
    let dst = MsgAddressInt::from_str(WALLET).unwrap();
    let header = ExternalInboundMessageHeader::new(MsgAddressExt::default(), dst);
    let message = Message::with_ext_in_header(header);
    write_boc(&message.serialize().unwrap()).unwrap()
}

#[test]
fn test_only_external_inbound_messages_are_sent() {
    let data = external_message();
    let hash = check_external_message(&data).unwrap();
    assert_eq!(hash, read_single_root_boc(&data).unwrap().repr_hash());

    let src = MsgAddressInt::from_str("0:1111111111111111111111111111111111111111111111111111111111111111").unwrap();
    let dst = MsgAddressInt::from_str("0:2222222222222222222222222222222222222222222222222222222222222222").unwrap();
    let internal = Message::with_int_header(InternalMessageHeader::with_addresses(src, dst, Default::default()));
    let internal = write_boc(&internal.serialize().unwrap()).unwrap();
    assert_eq!(check_external_message(&internal).unwrap_err().0, QueryError::InvalidMessage);
    assert_eq!(check_external_message(b"not a boc").unwrap_err().0, QueryError::InvalidMessage);
}

// ---- the server against a fake node -----------------------------------------------------

#[derive(Default)]
struct FakeNode {
    sent: StdMutex<Vec<(Vec<u8>, UInt256)>>,
    refuse: bool,
    key_block_loads: StdMutex<u32>,
    /// the only library the masterchain state has
    library: Option<Cell>,
    /// the node has no applied state yet (it is still booting)
    booting: bool,
    /// getContractState does not come back until this is notified
    hold: Option<Arc<tokio::sync::Notify>>,
    /// key blocks that came after the one of the fixtures
    later_key_blocks: StdMutex<u32>,
}

impl FakeNode {
    fn state(&self) -> Result<()> {
        if self.booting {
            ever_block::fail!("no last applied masterchain block yet");
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl JrpcBackend for FakeNode {
    fn ready(&self) -> bool { !self.booting }
    fn timings(&self) -> Result<Timings> {
        self.state()?;
        Ok(Timings { last_mc_block_seqno: 1, ..Default::default() })
    }
    async fn contract_state(&self, address: &MsgAddressInt) -> Result<ContractState> {
        self.state()?;
        if let Some(hold) = &self.hold {
            hold.notified().await;
        }
        let account = if address.to_string() == WALLET { Some(wallet_account()) } else { None };
        Ok(ContractState { account, gen_utime: WALLET_GEN_UTIME, _guard: None })
    }
    async fn latest_key_block_id(&self) -> Result<BlockIdExt> {
        self.state()?;
        let mut id = BlockIdExt::default();
        id.seq_no = KEY_BLOCK_SEQNO + *self.later_key_blocks.lock().unwrap();
        Ok(id)
    }
    async fn load_key_block(&self, id: &BlockIdExt) -> Result<KeyBlock> {
        *self.key_block_loads.lock().unwrap() += 1;
        let boc = base64_decode(golden("keyblock")["result"]["block"].as_str().unwrap()).unwrap();
        Ok(KeyBlock { seqno: id.seq_no(), block: Block::construct_from_bytes(&boc)?, boc })
    }
    async fn library_cell(&self, hash: &UInt256) -> Result<Option<Cell>> {
        self.state()?;
        Ok(self.library.clone().filter(|cell| cell.repr_hash() == *hash))
    }
    async fn send_message(&self, data: &[u8], hash: UInt256) -> Result<()> {
        if self.refuse {
            ever_block::fail!("node is not synchronized");
        }
        self.sent.lock().unwrap().push((data.to_vec(), hash));
        Ok(())
    }
}

fn call(method: &str, params: Value) -> Vec<u8> {
    json!({"jsonrpc": "2.0", "id": 42, "method": method, "params": params}).to_string().into_bytes()
}

fn ask(server: &JrpcServer<FakeNode>, body: &[u8]) -> Value {
    tokio::runtime::Runtime::new().unwrap().block_on(server.handle_body(body))
}

#[test]
fn test_framing_and_errors() {
    let server = JrpcServer::new(FakeNode::default(), 4);
    let answer = ask(&server, &call("getCapabilities", json!({})));
    assert_eq!(answer["id"], 42);
    assert_eq!(answer["jsonrpc"], "2.0");
    // the method names, spelled out: they are the API
    assert_eq!(answer["result"], json!(["getCapabilities", "getLatestKeyBlock", "getBlockchainConfig",
        "getStatus", "getTimings", "getContractState", "getLibraryCell", "sendMessage"]));
    assert_eq!(answer["result"], json!(CAPABILITIES));

    let answer = ask(&server, &call("noSuchMethod", json!({})));
    assert_eq!(answer["error"]["code"], -32601);
    assert_eq!(answer["id"], 42);

    let answer = ask(&server, b"{broken json");
    assert_eq!(answer["error"]["code"], -32700);
    assert_eq!(answer["id"], Value::Null);

    // a request without a method; a batch (one request per body is all the server takes)
    let answer = ask(&server, br#"{"jsonrpc": "2.0", "id": 5, "params": {}}"#);
    assert_eq!(answer["error"]["code"], -32600);
    assert_eq!(answer["id"], 5);
    let batch = json!([{"jsonrpc": "2.0", "id": 1, "method": "getStatus", "params": {}}]).to_string();
    let answer = ask(&server, batch.as_bytes());
    assert_eq!(answer["error"]["code"], -32600);
    assert_eq!(answer["id"], Value::Null);

    let answer = ask(&server, &call("getContractState", json!({"address": "garbage"})));
    assert_eq!(answer["error"]["code"], -32602);
    let answer = ask(&server, &call("getContractState", json!({})));
    assert_eq!(answer["error"]["code"], -32602);

    assert_eq!(ask(&server, &call("getStatus", json!({})))["result"], json!({"ready": true}));
    assert_eq!(ask(&server, &call("getTimings", json!({})))["result"]["last_mc_block_seqno"], 1);
}

#[test]
fn test_contract_state_through_the_server() {
    let server = JrpcServer::new(FakeNode::default(), 4);
    let answer = ask(&server, &call("getContractState", json!({"address": WALLET})));
    // the whole answer, envelope aside, is the one recorded on the live node
    assert_eq!(answer["result"], golden("state_wallet")["result"]);
    let lt = answer["result"]["lastTransactionId"]["lt"].as_str().unwrap().to_string();
    // lastTransactionLt as a string (nekoton) and as a number
    let answer = ask(&server, &call("getContractState", json!({"address": WALLET, "lastTransactionLt": lt})));
    assert_eq!(answer["result"], golden("state_unchanged")["result"]);
    let answer = ask(&server, &call("getContractState",
        json!({"address": WALLET, "lastTransactionLt": lt.parse::<u64>().unwrap()})));
    assert_eq!(answer["result"]["type"], "unchanged");
    let answer = ask(&server, &call("getContractState", json!({"address": "0:".to_string() + &"2".repeat(64)})));
    assert_eq!(answer["result"]["type"], "notExists");
}

#[test]
fn test_library_cell() {
    // any cell will do as a library: here the wallet's account cell
    let library = wallet_account().account_cell();
    let hash = library.repr_hash().to_hex_string();
    let server = JrpcServer::new(FakeNode { library: Some(library.clone()), ..Default::default() }, 4);
    let answer = ask(&server, &call("getLibraryCell", json!({"hash": hash})));
    assert_eq!(boc_hash(answer["result"]["cell"].as_str().unwrap()), library.repr_hash(), "{}", answer);
    // a library the state does not have: null, not an error
    let answer = ask(&server, &call("getLibraryCell", json!({"hash": "0".repeat(64)})));
    assert_eq!(answer["result"], json!({"cell": null}));
    for params in [json!({"hash": "xyz"}), json!({"hash": "00"}), json!({})] {
        let answer = ask(&server, &call("getLibraryCell", params.clone()));
        assert_eq!(answer["error"]["code"], -32602, "{} -> {}", params, answer);
    }
}

#[test]
fn test_a_node_without_state_answers_not_ready() {
    let server = JrpcServer::new(FakeNode { booting: true, ..Default::default() }, 4);
    assert_eq!(ask(&server, &call("getStatus", json!({})))["result"], json!({"ready": false}));
    for (method, params) in [
        ("getTimings", json!({})),
        ("getContractState", json!({"address": WALLET})),
        ("getLatestKeyBlock", json!({})),
        ("getBlockchainConfig", json!({})),
        ("getLibraryCell", json!({"hash": "0".repeat(64)})),
    ] {
        let answer = ask(&server, &call(method, params));
        assert_eq!(answer["error"]["code"], -32001, "{} -> {}", method, answer);
        assert_eq!(answer["error"]["message"], "Not ready");
        assert!(answer["error"]["data"].as_str().unwrap().contains("no last applied masterchain block"), "{}", answer);
        assert_eq!(answer["id"], 42);
    }
    // nothing was cached from the failed attempts, and what needs no state still answers
    assert_eq!(*server.backend.key_block_loads.lock().unwrap(), 0);
    assert_eq!(ask(&server, &call("getCapabilities", json!({})))["result"], json!(CAPABILITIES));
}

#[test]
fn test_send_message_hands_the_message_to_the_node() {
    let server = JrpcServer::new(FakeNode::default(), 4);
    let data = external_message();
    let answer = ask(&server, &call("sendMessage", json!({"message": base64_encode(&data)})));
    assert_eq!(answer["result"], Value::Null, "{}", answer);
    let sent = server.backend.sent.lock().unwrap().clone();
    assert_eq!(sent, vec![(data.clone(), read_single_root_boc(&data).unwrap().repr_hash())]);

    let refusing = JrpcServer::new(FakeNode { refuse: true, ..Default::default() }, 4);
    let answer = ask(&refusing, &call("sendMessage", json!({"message": base64_encode(&data)})));
    assert_eq!(answer["error"]["code"], -32003);
    assert!(answer["error"]["data"].as_str().unwrap().contains("not synchronized"));

    let answer = ask(&server, &call("sendMessage", json!({"message": "!!!"})));
    assert_eq!(answer["error"]["code"], -32007);
}

#[test]
fn test_key_block_answers_are_cached_per_key_block() {
    let server = JrpcServer::new(FakeNode::default(), 4);
    let rt = tokio::runtime::Runtime::new().unwrap();
    let config = rt.block_on(server.handle_body(&call("getBlockchainConfig", json!({}))));
    assert_eq!(config["result"], golden("config")["result"]);
    let block = rt.block_on(server.handle_body(&call("getLatestKeyBlock", json!({}))));
    assert_eq!(block["result"], golden("keyblock")["result"]);
    // the key block is loaded from storage once; later calls reuse the answers
    assert_eq!(*server.backend.key_block_loads.lock().unwrap(), 1);
    assert_eq!(server.key_block.try_lock().unwrap().as_ref().unwrap().0, KEY_BLOCK_SEQNO);

    // a new key block: the answers are built again - once
    *server.backend.later_key_blocks.lock().unwrap() = 1;
    let config = rt.block_on(server.handle_body(&call("getBlockchainConfig", json!({}))));
    assert_eq!(config["result"]["seqno"], KEY_BLOCK_SEQNO + 1);
    rt.block_on(server.handle_body(&call("getLatestKeyBlock", json!({}))));
    rt.block_on(server.handle_body(&call("getBlockchainConfig", json!({}))));
    assert_eq!(*server.backend.key_block_loads.lock().unwrap(), 2);
    assert_eq!(server.key_block.try_lock().unwrap().as_ref().unwrap().0, KEY_BLOCK_SEQNO + 1);
}

fn post(body: Vec<u8>) -> Request<Body> {
    Request::builder().method(Method::POST).uri("/rpc").body(Body::from(body)).unwrap()
}

async fn answer_of(response: Response<Body>) -> Value {
    assert_eq!(response.status(), StatusCode::OK);
    serde_json::from_slice(&hyper::body::to_bytes(response.into_body()).await.unwrap()).unwrap()
}

#[test]
fn test_requests_above_the_limit_wait_for_a_slot_and_are_refused() {
    // one slot; the node does not finish the request that took it
    let hold = Arc::new(tokio::sync::Notify::new());
    let mut server = JrpcServer::new(FakeNode { hold: Some(hold.clone()), ..Default::default() }, 1);
    server.queue_timeout = Duration::from_millis(100);
    let server = Arc::new(server);
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let first = tokio::spawn({
            let server = server.clone();
            async move { server.http(post(call("getContractState", json!({"address": WALLET})))).await }
        });
        while server.limiter.available_permits() != 0 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        // the next request waits for the slot, then is refused - before its body is looked
        // at, so the refusal carries no id
        let started = std::time::Instant::now();
        let answer = answer_of(server.http(post(call("getStatus", json!({})))).await).await;
        assert!(started.elapsed() >= Duration::from_millis(100), "it waited for the slot first");
        assert_eq!(answer["error"]["code"], -32009);
        assert_eq!(answer["error"]["message"], "Too many requests");
        assert_eq!(answer["id"], Value::Null);
        assert_eq!(server.limiter.available_permits(), 0, "the first request still holds the slot");

        // the first request is answered: its slot is free again
        hold.notify_one();
        assert_eq!(answer_of(first.await.unwrap()).await["result"]["type"], "exists");
        assert_eq!(server.limiter.available_permits(), 1);
        let answer = answer_of(server.http(post(call("getStatus", json!({})))).await).await;
        assert_eq!(answer["result"], json!({"ready": true}));
    });
}

#[test]
fn test_a_request_the_node_does_not_finish_is_answered_not_ready() {
    let mut server = JrpcServer::new(FakeNode { hold: Some(Default::default()), ..Default::default() }, 4);
    server.request_timeout = Duration::from_millis(50);
    let answer = tokio::runtime::Runtime::new().unwrap().block_on(async {
        answer_of(server.http(post(call("getContractState", json!({"address": WALLET})))).await).await
    });
    assert_eq!(answer["error"]["code"], -32001);
    assert_eq!(answer["error"]["message"], "Not ready");
    assert_eq!(answer["error"]["data"], "timed out");
    assert_eq!(answer["id"], 42);
    // the request is given up, and its slot is free again
    assert_eq!(server.limiter.available_permits(), 4);
}

#[test]
fn test_http_layer() {
    let server = JrpcServer::new(FakeNode::default(), 4);
    let rt = tokio::runtime::Runtime::new().unwrap();
    let post = |path: &str, body: Vec<u8>| Request::builder()
        .method(Method::POST).uri(path).body(Body::from(body)).unwrap();

    let response = rt.block_on(server.http(post("/rpc", call("getCapabilities", json!({})))));
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()[header::CONTENT_TYPE], "application/json");
    let response = rt.block_on(server.http(post("/", call("getCapabilities", json!({})))));
    assert_eq!(response.status(), StatusCode::OK);
    // a JSON-RPC error is an answer too: HTTP 200 with the error object
    let response = rt.block_on(server.http(post("/rpc", call("noSuchMethod", json!({})))));
    assert_eq!(response.status(), StatusCode::OK);
    let body = rt.block_on(hyper::body::to_bytes(response.into_body())).unwrap();
    assert_eq!(serde_json::from_slice::<Value>(&body).unwrap()["error"]["code"], -32601);

    let response = rt.block_on(server.http(post("/other", vec![])));
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let get = Request::builder().method(Method::GET).uri("/rpc").body(Body::empty()).unwrap();
    assert_eq!(rt.block_on(server.http(get)).status(), StatusCode::METHOD_NOT_ALLOWED);
    // 1 MiB of body is taken (and found not to be JSON), one byte more is not
    assert_eq!(MAX_BODY_BYTES, 1024 * 1024);
    let response = rt.block_on(server.http(post("/rpc", vec![b' '; MAX_BODY_BYTES])));
    assert_eq!(response.status(), StatusCode::OK);
    let body = rt.block_on(hyper::body::to_bytes(response.into_body())).unwrap();
    assert_eq!(serde_json::from_slice::<Value>(&body).unwrap()["error"]["code"], -32700);
    let huge = vec![b' '; MAX_BODY_BYTES + 1];
    assert_eq!(rt.block_on(server.http(post("/rpc", huge))).status(), StatusCode::PAYLOAD_TOO_LARGE);
}

#[test]
fn test_a_body_that_never_ends_is_cut_off() {
    let mut server = JrpcServer::new(FakeNode::default(), 4);
    server.body_timeout = Duration::from_millis(50);
    let rt = tokio::runtime::Runtime::new().unwrap();
    // the client keeps the body open and sends nothing
    let (_client, body) = Body::channel();
    let request = Request::builder().method(Method::POST).uri("/rpc").body(body).unwrap();
    let response = rt.block_on(async {
        tokio::time::timeout(Duration::from_secs(5), server.http(request)).await
    }).expect("the server must not wait for the body forever");
    assert_eq!(response.status(), StatusCode::REQUEST_TIMEOUT);
}

#[test]
fn test_only_private_addresses_are_served() {
    let private = ["127.0.0.1", "127.8.0.1", "10.1.2.3", "172.16.0.1", "172.31.255.254",
                   "192.168.1.1", "100.64.0.1", "100.127.255.254", "::1", "fd12:3456:789a::1", "fc00::1"];
    let public = ["0.0.0.0", "::", "8.8.8.8", "203.0.113.7", "172.32.0.1", "100.128.0.1",
                  "100.63.255.255", "192.169.0.1", "2001:db8::1", "fe80::1"];
    for ip in private {
        assert!(is_private_listen_address(&ip.parse().unwrap()), "{}", ip);
    }
    for ip in public {
        assert!(!is_private_listen_address(&ip.parse().unwrap()), "{}", ip);
    }
}

#[test]
fn test_start_serves_over_tcp_and_refuses_a_taken_port() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let config = JrpcServerConfig {
            listen_address: "127.0.0.1:0".parse().unwrap(), max_concurrent_requests: 2, history: None,
        };
        let address = serve(config, FakeNode::default(), None).unwrap();
        assert_ne!(address.port(), 0);

        let body = String::from_utf8(call("getCapabilities", json!({}))).unwrap();
        let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
        let request = format!(
            "POST /rpc HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\n\
             Content-Length: {}\r\nConnection: close\r\n\r\n{}", body.len(), body);
        stream.write_all(request.as_bytes()).await.unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).await.unwrap();
        assert!(response.starts_with("HTTP/1.1 200"), "{}", response);
        let json: Value = serde_json::from_str(response.split("\r\n\r\n").nth(1).unwrap()).unwrap();
        assert_eq!(json["result"], json!(CAPABILITIES));

        // the same port again: an error for the caller, not a panic in the node
        let taken = JrpcServerConfig { listen_address: address, max_concurrent_requests: 2, history: None };
        assert!(serve(taken, FakeNode::default(), None).is_err());
        // every interface: refused before anything is bound
        let any = JrpcServerConfig { listen_address: "0.0.0.0:0".parse().unwrap(), max_concurrent_requests: 2, history: None };
        let error = serve(any, FakeNode::default(), None).unwrap_err().to_string();
        assert!(error.contains("not a loopback or private address"), "{}", error);
    });
}


// ---- history methods (network/jrpc_history.rs) ------------------------------------------

use crate::network::jrpc_history::{
    tests::{
        golden_index, hashes, mc_id, other_row_from_boc, temp_dir, transactions, CONFIG, ELECTOR,
        ELECTOR_NEWEST_IN_MSG, ELECTOR_NEWEST_LT, SMALLEST_LT, WALLET, WALLET_EXT_IN_MSG,
    },
    tx_key, Retention, TxHistory, TxRow,
};

fn with_history(index: TxHistory) -> JrpcServer<FakeNode> {
    JrpcServer::new(FakeNode::default(), 4).with_history(Arc::new(index))
}

fn result_list(answer: &Value) -> Vec<String> {
    answer["result"].as_array().unwrap_or_else(|| panic!("{}", answer))
        .iter().map(|v| v.as_str().unwrap().to_string()).collect()
}

#[test]
fn test_history_methods() {
    let (index, dir) = golden_index("server");
    let server = with_history(index);
    let elector = transactions("elector");
    let wallet = transactions("wallet");

    // getTransactionsList: newest first; lastTransactionLt as a string (nekoton), a number
    // (everscale-rpc-client) or null; inclusive
    let page = result_list(&ask(&server, &call("getTransactionsList", json!({"account": ELECTOR, "limit": 5}))));
    assert_eq!(hashes(&page), hashes(&elector[..5]));
    for lt in [json!(ELECTOR_NEWEST_LT.to_string()), json!(ELECTOR_NEWEST_LT), Value::Null] {
        let same = ask(&server, &call("getTransactionsList", json!({"account": ELECTOR, "lastTransactionLt": lt, "limit": 5})));
        assert_eq!(hashes(&result_list(&same)), hashes(&page), "{}", lt);
    }
    let older = ask(&server, &call("getTransactionsList",
        json!({"account": ELECTOR, "lastTransactionLt": (ELECTOR_NEWEST_LT - 1).to_string(), "limit": 3})));
    assert_eq!(hashes(&result_list(&older)), hashes(&elector[1..4]));
    // a full page of the masterchain account, and the workchain-0 account's whole history
    let full = ask(&server, &call("getTransactionsList", json!({"account": ELECTOR, "limit": 100})));
    assert_eq!(hashes(&result_list(&full)), hashes(&elector));
    let all = ask(&server, &call("getTransactionsList", json!({"account": WALLET, "limit": 100})));
    assert_eq!(hashes(&result_list(&all)), hashes(&wallet));

    // limit as the public endpoint jrpc.everwallet.net took it: 0 -> [], a string or nothing -> -32602
    assert_eq!(ask(&server, &call("getTransactionsList", json!({"account": ELECTOR, "limit": 0})))["result"], json!([]));
    for params in [json!({"account": ELECTOR, "limit": "5"}), json!({"account": ELECTOR}), Value::Null,
                   json!({"account": "garbage", "limit": 5}), json!({"account": ELECTOR, "limit": -1}),
                   json!({"account": ELECTOR, "limit": 5, "lastTransactionLt": "12x"})] {
        let answer = ask(&server, &call("getTransactionsList", params.clone()));
        assert_eq!(answer["error"]["code"], -32602, "{} -> {}", params, answer);
    }
    // an account this node does not index: no transactions (not an error - a wallet would fail)
    assert_eq!(ask(&server, &call("getTransactionsList", json!({"account": CONFIG, "limit": 5})))["result"], json!([]));

    // getTransaction / getDstTransaction: the transaction, or null
    let latest = elector[0].clone();
    let latest_hash = hashes(&[latest.clone()])[0].to_hex_string();
    let answer = ask(&server, &call("getTransaction", json!({"id": latest_hash})));
    assert_eq!(hashes(&[answer["result"].as_str().unwrap().to_string()]), hashes(&[latest.clone()]));
    let answer = ask(&server, &call("getTransaction", json!({"id": latest_hash.to_uppercase()})));
    assert!(answer["result"].is_string(), "hex in either case");
    assert_eq!(ask(&server, &call("getTransaction", json!({"id": "0".repeat(64)})))["result"], Value::Null);
    assert_eq!(ask(&server, &call("getTransaction", json!({"id": "XYZ"})))["error"]["code"], -32602);
    // the wallet transaction that consumed an external message (what a wallet looks for after
    // sendMessage), the elector one that consumed an internal message
    for (message, expected) in [(WALLET_EXT_IN_MSG, &wallet[1]), (ELECTOR_NEWEST_IN_MSG, &elector[0])] {
        let answer = ask(&server, &call("getDstTransaction", json!({"messageHash": message})));
        assert_eq!(hashes(&[answer["result"].as_str().unwrap().to_string()]), hashes(&[expected.clone()]), "{}", message);
    }
    assert_eq!(ask(&server, &call("getDstTransaction", json!({"messageHash": "0".repeat(64)})))["result"], Value::Null);
    assert_eq!(ask(&server, &call("getDstTransaction", json!({})))["error"]["code"], -32602);

    // capabilities: the full-mode history methods except getAccountsByCodeHash; getHistoryStatus
    // is served but not listed - it is not a method of everscale-jrpc
    let capabilities = ask(&server, &call("getCapabilities", Value::Null))["result"].clone();
    assert_eq!(capabilities, json!(["getCapabilities", "getLatestKeyBlock", "getBlockchainConfig", "getStatus",
        "getTimings", "getContractState", "getLibraryCell", "sendMessage",
        "getTransactionsList", "getTransaction", "getDstTransaction"]));
    // smallest_known_lt: a number with a history index
    let timings = ask(&server, &call("getTimings", Value::Null));
    assert_eq!(timings["result"]["smallest_known_lt"], json!(SMALLEST_LT), "{}", timings);
    let status = ask(&server, &call("getHistoryStatus", json!({})));
    assert_eq!(status["result"]["lastMcSeqno"], 1, "{}", status);
    assert_eq!(status["result"]["smallestKnownLt"], SMALLEST_LT.to_string());
    // this index keeps the listed accounts only
    assert_eq!(status["result"]["otherAccountsDays"], Value::Null, "{}", status);
    assert_eq!(status["result"]["otherTransactions"], 0);
    assert_eq!(status["result"]["otherBytes"], 0);

    drop(server);
    std::fs::remove_dir_all(dir).ok();
}

#[test]
fn test_history_of_an_account_that_is_not_listed() {
    // the node keeps every account's transactions for a while: the answers are the same as
    // for a listed account, and getHistoryStatus says how much there is of them
    let dir = temp_dir("server-others");
    let index = TxHistory::open(&dir, Some(Retention { keep_sec: 30 * 86_400, max_bytes: u64::MAX })).unwrap();
    let wallet = transactions("wallet");
    let rows: Vec<TxRow> = wallet.iter().map(|boc| other_row_from_boc(0, boc)).collect();
    let bytes: usize = rows.iter().map(|row| row.boc.len()).sum();
    index.commit(&rows, &mc_id(1), None).unwrap();
    let server = with_history(index);

    let all = ask(&server, &call("getTransactionsList", json!({"account": WALLET, "limit": 100})));
    assert_eq!(hashes(&result_list(&all)), hashes(&wallet));
    let answer = ask(&server, &call("getDstTransaction", json!({"messageHash": WALLET_EXT_IN_MSG})));
    assert_eq!(hashes(&[answer["result"].as_str().unwrap().to_string()]), hashes(&[wallet[1].clone()]));
    let newest = hashes(&[wallet[0].clone()])[0].to_hex_string();
    let answer = ask(&server, &call("getTransaction", json!({"id": newest})));
    assert_eq!(hashes(&[answer["result"].as_str().unwrap().to_string()]), hashes(&[wallet[0].clone()]));
    // an account without transactions in that time: none, not an error
    assert_eq!(ask(&server, &call("getTransactionsList", json!({"account": CONFIG, "limit": 5})))["result"], json!([]));

    let status = ask(&server, &call("getHistoryStatus", json!({})))["result"].clone();
    assert_eq!(status["accounts"], 0, "{}", status);
    assert_eq!(status["otherAccountsDays"], 30, "{}", status);
    assert_eq!(status["otherTransactions"], 5, "{}", status);
    assert_eq!(status["otherBytes"], bytes, "{}", status);
    assert_eq!(status["smallestKnownLt"], SMALLEST_LT.to_string());
    let timings = ask(&server, &call("getTimings", Value::Null));
    assert_eq!(timings["result"]["smallest_known_lt"], json!(SMALLEST_LT), "{}", timings);

    drop(server);
    std::fs::remove_dir_all(dir).ok();
}

#[test]
fn test_limit_is_cut_to_100() {
    // as jrpc.everwallet.net did: a larger limit is not an error
    let dir = temp_dir("limit");
    let index = TxHistory::open(&dir, None).unwrap();
    let account = UInt256::from([0x42; 32]);
    let rows: Vec<TxRow> = (1..=105u64).map(|lt| TxRow {
        key: tx_key(0, &account, lt), hash: UInt256::from([lt as u8; 32]), in_msg_hash: None, boc: vec![lt as u8],
        utime: 0, listed: true,
    }).collect();
    index.commit(&rows, &mc_id(1), None).unwrap();
    let server = with_history(index);
    let address = format!("0:{}", "42".repeat(32));
    for limit in [101u64, 256, 1000] {
        let answer = ask(&server, &call("getTransactionsList", json!({"account": address, "limit": limit})));
        let got = result_list(&answer);
        assert_eq!(got.len(), 100, "limit {}", limit);
        assert_eq!(base64_decode(&got[0]).unwrap(), vec![105], "newest first");
        assert_eq!(base64_decode(&got[99]).unwrap(), vec![6]);
    }
    drop(server);
    std::fs::remove_dir_all(dir).ok();
}

#[test]
fn test_history_methods_without_history() {
    // as everscale-jrpc in simple mode: "Not supported", after the params are checked
    let server = JrpcServer::new(FakeNode::default(), 4);
    for (method, params) in [
        ("getTransactionsList", json!({"account": WALLET, "limit": 5})),
        ("getTransaction", json!({"id": "0".repeat(64)})),
        ("getDstTransaction", json!({"messageHash": "0".repeat(64)})),
        ("getHistoryStatus", json!({})),
    ] {
        assert_eq!(ask(&server, &call(method, params))["error"]["code"], -32002, "{}", method);
    }
    assert_eq!(ask(&server, &call("getTransactionsList", json!({"account": WALLET})))["error"]["code"], -32602);
    assert_eq!(ask(&server, &call("getCapabilities", json!({})))["result"], json!(CAPABILITIES));
    assert_eq!(ask(&server, &call("getTimings", json!({})))["result"]["smallest_known_lt"], Value::Null);
}

#[test]
fn test_http2_without_tls_from_the_first_byte() {
    // nekoton-transport's JrpcClient speaks h2c with prior knowledge
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let config = JrpcServerConfig {
            listen_address: "127.0.0.1:0".parse().unwrap(), max_concurrent_requests: 2, history: None,
        };
        let address = serve(config, FakeNode::default(), None).unwrap();
        let client = hyper::Client::builder().http2_only(true).build_http::<Body>();
        let request = Request::builder().method(Method::POST).uri(format!("http://{}/rpc", address))
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(call("getCapabilities", Value::Null))).unwrap();
        let response = client.request(request).await.expect("an HTTP/2 answer");
        assert_eq!(response.version(), hyper::Version::HTTP_2);
        let body = hyper::body::to_bytes(response.into_body()).await.unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["result"], json!(CAPABILITIES));
    });
}
