/*
* Transaction history for the in-process JRPC server (network/jrpc.rs): getTransactionsList,
* getTransaction and getDstTransaction of broxus/everscale-jrpc's "full" mode, answered from
* an index of ONLY the accounts listed in a file (an operator's own wallets and contracts,
* for example) - not of the whole chain.
*
* The indexer follows applied masterchain blocks the way the external db worker does: for
* each one it takes the masterchain block itself and the shard blocks between it and the
* previous masterchain block (behind the shard client), keeps the transactions of the listed
* accounts in its own RocksDB (<node db>/jrpc_history) and commits the id of the processed
* masterchain block in the same write batch - a restart resumes exactly there, and
* processing a block twice is harmless.
*
* The node keeps blocks only from its cold boot on, and archive GC drops older ones:
* history starts where the index was enabled, or at `start_from_mc_seqno` when the node
* still stores that block. A block the node no longer has - never stored in its database,
* or stored in an archive slice the GC collected since - becomes a recorded gap instead of
* a stuck indexer, and so does everything between the index and a node database that was
* built anew from a later block (a resync). Indexed transactions stay after the node drops
* their blocks.
*/

use crate::{block::BlockStuff, engine::Engine, engine_traits::EngineOperations};
use storage::block_handle_db::BlockHandle;

use ever_block::{
    error, fail, write_boc, Block, BlockIdExt, Deserializable, HashmapAugType, MsgAddressInt,
    Result, ShardIdent, Transaction, UInt256,
};
use rocksdb::{ColumnFamily, ColumnFamilyDescriptor, Options, ReadOptions, WriteBatch, DB};
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, HashSet},
    future::Future,
    path::{Path, PathBuf},
    str::FromStr,
    sync::{atomic::{AtomicU64, AtomicUsize, Ordering}, Arc},
    time::{Duration, Instant, SystemTime},
};

/// Most transactions one getTransactionsList answer carries (as jrpc.everwallet.net).
pub const MAX_LIST_LIMIT: usize = 100;

const CF_TRANSACTIONS: &str = "transactions";
const CF_BY_HASH: &str = "transactions_by_hash";
const CF_BY_IN_MSG: &str = "transactions_by_in_msg";
const META_LAST_MC_BLOCK: &[u8] = b"last_mc_block";
const META_START_MC_SEQNO: &[u8] = b"start_mc_seqno";
const META_GAPS: &[u8] = b"gaps";

/// How often the accounts file is checked for changes.
const ACCOUNTS_CHECK_PERIOD: Duration = Duration::from_secs(10);
/// A masterchain block older than this means the indexer is catching up.
const CATCH_UP_LAG_SEC: u32 = 30;
/// The longest single sleep of a pause: the stop flag is looked at this often.
const PAUSE_STEP: Duration = Duration::from_millis(100);
/// A wait for the next masterchain block that failed sooner than this did not run out of
/// time - it failed, and is not repeated at once.
const QUICK_FAILURE: Duration = Duration::from_millis(500);

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct HistoryConfig {
    /// The accounts to index, one "wc:hex" address per line ('#' starts a comment). The
    /// file is re-read when it changes; an added account is indexed from then on.
    pub accounts_file: String,
    /// Where the index lives; default <node db>/jrpc_history.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub db_path: Option<String>,
    /// First masterchain block to index while the index is still empty - a backfill from
    /// what the node still stores; default: from the last applied block on.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start_from_mc_seqno: Option<u32>,
    /// Masterchain blocks per second at most while catching up (a backfill, or after the
    /// node was down), so block application keeps its share of the machine.
    #[serde(default = "HistoryConfig::default_catch_up_rate")]
    pub catch_up_mc_blocks_per_sec: u32,
    /// Seconds after the node's boot before the indexer processes anything: the node's
    /// startup - the validator sessions coming up - goes first. Where history starts is
    /// fixed at boot, so nothing is skipped; the indexer catches up afterwards.
    #[serde(default = "HistoryConfig::default_start_delay")]
    pub start_delay_sec: u32,
}

impl HistoryConfig {
    fn default_catch_up_rate() -> u32 { 10 }
    fn default_start_delay() -> u32 { 600 }
}

// ---- which accounts -------------------------------------------------------------------

/// "wc:hex" of a standard 256-bit address.
pub fn std_address(text: &str) -> Result<(i32, UInt256)> {
    match MsgAddressInt::from_str(text)? {
        MsgAddressInt::AddrStd(std) if std.anycast.is_none() && std.address.remaining_bits() == 256 =>
            Ok((std.workchain_id as i32, UInt256::from_slice(&std.address.get_bytestring(0)))),
        _ => fail!("{} is not a standard 256-bit address", text),
    }
}

/// The accounts to index, per workchain.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Watched {
    by_workchain: BTreeMap<i32, Vec<UInt256>>,
}

impl Watched {
    /// One address per line; '#' starts a comment; a bad line is an error naming it.
    pub fn parse(text: &str) -> Result<Self> {
        let mut seen = HashSet::new();
        let mut by_workchain: BTreeMap<i32, Vec<UInt256>> = BTreeMap::new();
        for (n, line) in text.lines().enumerate() {
            let line = line.split('#').next().unwrap_or("").trim();
            if line.is_empty() {
                continue;
            }
            let (workchain, account) = std_address(line).map_err(|e| error!("line {}: {}", n + 1, e))?;
            if seen.insert((workchain, account.clone())) {
                by_workchain.entry(workchain).or_default().push(account);
            }
        }
        Ok(Self { by_workchain })
    }

    pub fn len(&self) -> usize {
        self.by_workchain.values().map(Vec::len).sum()
    }

    pub fn in_workchain(&self, workchain: i32) -> &[UInt256] {
        self.by_workchain.get(&workchain).map_or(&[], Vec::as_slice)
    }
}

fn read_accounts(path: &Path) -> Result<Watched> {
    let text = std::fs::read_to_string(path).map_err(|e| error!("cannot read {}: {}", path.display(), e))?;
    Watched::parse(&text).map_err(|e| error!("{}: {}", path.display(), e))
}

// ---- what a block contributes ---------------------------------------------------------

/// Key of a transaction: workchain (as u8), account, lt big-endian - an account's
/// transactions are adjacent and ordered by lt.
pub const KEY_LEN: usize = 41;
pub type TxKey = [u8; KEY_LEN];

pub fn tx_key(workchain: i8, account: &UInt256, lt: u64) -> TxKey {
    let mut key = [0u8; KEY_LEN];
    key[0] = workchain as u8;
    key[1..33].copy_from_slice(account.as_slice());
    key[33..].copy_from_slice(&lt.to_be_bytes());
    key
}

fn key_lt(key: &[u8]) -> u64 {
    let mut lt = [0u8; 8];
    lt.copy_from_slice(&key[33..KEY_LEN]);
    u64::from_be_bytes(lt)
}

/// One transaction of an indexed account, ready to store.
#[derive(Clone, Debug, PartialEq)]
pub struct TxRow {
    pub key: TxKey,
    pub hash: UInt256,
    pub in_msg_hash: Option<UInt256>,
    pub boc: Vec<u8>,
}

/// The transactions of the watched accounts in one block of `workchain` (a masterchain
/// block holds only -1 accounts, a shard block only its workchain's). The account block
/// dictionary is keyed by the 256-bit address alone, so only that workchain's watched
/// accounts are looked up.
pub fn block_rows(block: &Block, workchain: i32, watched: &Watched) -> Result<Vec<TxRow>> {
    let accounts = watched.in_workchain(workchain);
    let Ok(wc) = i8::try_from(workchain) else { return Ok(Vec::new()) };
    if accounts.is_empty() {
        return Ok(Vec::new());
    }
    let account_blocks = block.read_extra()?.read_account_blocks()?;
    let mut rows = Vec::new();
    for account in accounts {
        let Some(account_block) = account_blocks.get(account)? else { continue };
        account_block.transaction_iterate_full(|lt, cell, _fees| {
            let transaction = Transaction::construct_from_cell(cell.clone())?;
            rows.push(TxRow {
                key: tx_key(wc, account, lt),
                hash: cell.repr_hash(),
                in_msg_hash: transaction.in_msg_cell().map(|msg| msg.repr_hash()),
                boc: write_boc(&cell)?,
            });
            Ok(true)
        })?;
    }
    Ok(rows)
}

// ---- the index ------------------------------------------------------------------------

pub struct TxHistory {
    db: DB,
    smallest_lt: AtomicU64,
    accounts: AtomicUsize,
}

/// Adds `from..=to` to the gaps (ascending): joined with the last one when they touch or
/// overlap.
fn add_gap(gaps: &mut Vec<(u32, u32)>, from: u32, to: u32) {
    match gaps.last_mut() {
        Some(last) if last.1 + 1 >= from => last.1 = last.1.max(to),
        _ => gaps.push((from, to)),
    }
}

fn encode_mc_block(id: &BlockIdExt) -> Vec<u8> {
    let mut data = id.seq_no().to_be_bytes().to_vec();
    data.extend_from_slice(id.root_hash().as_slice());
    data.extend_from_slice(id.file_hash().as_slice());
    data
}

fn decode_mc_block(data: &[u8]) -> Result<BlockIdExt> {
    if data.len() != 68 {
        fail!("bad masterchain block id in the history index ({} bytes)", data.len());
    }
    let mut seqno = [0u8; 4];
    seqno.copy_from_slice(&data[..4]);
    Ok(BlockIdExt::with_params(
        ShardIdent::masterchain(),
        u32::from_be_bytes(seqno),
        UInt256::from_slice(&data[4..36]),
        UInt256::from_slice(&data[36..68]),
    ))
}

impl TxHistory {
    pub fn open(path: &Path) -> Result<Self> {
        let mut options = Options::default();
        options.create_if_missing(true);
        options.create_missing_column_families(true);
        options.set_max_open_files(64);
        options.set_keep_log_file_num(2);
        options.set_max_total_wal_size(16 << 20);
        let families = [CF_TRANSACTIONS, CF_BY_HASH, CF_BY_IN_MSG]
            .map(|name| ColumnFamilyDescriptor::new(name, Options::default()));
        let db = DB::open_cf_descriptors(&options, path, families)
            .map_err(|e| error!("cannot open {}: {}", path.display(), e))?;
        let history = Self { db, smallest_lt: AtomicU64::new(u64::MAX), accounts: AtomicUsize::new(0) };
        history.smallest_lt.store(history.scan_smallest_lt()?, Ordering::Relaxed);
        Ok(history)
    }

    fn cf(&self, name: &str) -> Result<&ColumnFamily> {
        self.db.cf_handle(name).ok_or_else(|| error!("no column family {} in the history index", name))
    }

    /// Stores the rows of one masterchain block and moves the progress marker to that
    /// block, in one write: both happen or neither does. A gap - masterchain seqnos
    /// `from..=to` whose transactions are missing or incomplete - goes into that write too:
    /// the marker never moves past blocks the node no longer had without the gap being on
    /// record.
    pub fn commit(&self, rows: &[TxRow], mc_block: &BlockIdExt, gap: Option<(u32, u32)>) -> Result<()> {
        let (transactions, by_hash, by_in_msg) =
            (self.cf(CF_TRANSACTIONS)?, self.cf(CF_BY_HASH)?, self.cf(CF_BY_IN_MSG)?);
        let mut batch = WriteBatch::default();
        let mut smallest = u64::MAX;
        for row in rows {
            batch.put_cf(transactions, row.key, &row.boc);
            batch.put_cf(by_hash, row.hash.as_slice(), row.key);
            if let Some(in_msg) = &row.in_msg_hash {
                batch.put_cf(by_in_msg, in_msg.as_slice(), row.key);
            }
            smallest = smallest.min(key_lt(&row.key));
        }
        if let Some((from, to)) = gap {
            let mut gaps = self.gaps()?;
            add_gap(&mut gaps, from, to);
            batch.put(META_GAPS, serde_json::to_vec(&gaps)?);
        }
        batch.put(META_LAST_MC_BLOCK, encode_mc_block(mc_block));
        self.db.write(batch)?;
        self.smallest_lt.fetch_min(smallest, Ordering::Relaxed);
        Ok(())
    }

    /// The first commit of an empty index: history starts after `base`.
    fn begin(&self, base: &BlockIdExt) -> Result<()> {
        let mut batch = WriteBatch::default();
        batch.put(META_START_MC_SEQNO, (base.seq_no() + 1).to_be_bytes());
        batch.put(META_LAST_MC_BLOCK, encode_mc_block(base));
        self.db.write(batch)?;
        Ok(())
    }

    /// The last masterchain block whose transactions are all stored.
    pub fn last_mc_block(&self) -> Result<Option<BlockIdExt>> {
        self.db.get(META_LAST_MC_BLOCK)?.map(|data| decode_mc_block(&data)).transpose()
    }

    pub fn start_mc_seqno(&self) -> Result<Option<u32>> {
        match self.db.get(META_START_MC_SEQNO)? {
            Some(data) if data.len() == 4 => Ok(Some(u32::from_be_bytes([data[0], data[1], data[2], data[3]]))),
            Some(_) => fail!("bad start seqno in the history index"),
            None => Ok(None),
        }
    }

    /// Masterchain seqno ranges whose transactions are (partly) missing because the node
    /// no longer had the blocks.
    pub fn gaps(&self) -> Result<Vec<(u32, u32)>> {
        match self.db.get(META_GAPS)? {
            Some(data) => Ok(serde_json::from_slice(&data)?),
            None => Ok(Vec::new()),
        }
    }

    /// The account's transactions with lt <= `last_lt` (all when None), newest first.
    pub fn list(&self, workchain: i8, account: &UInt256, last_lt: Option<u64>, limit: usize) -> Result<Vec<Vec<u8>>> {
        let mut options = ReadOptions::default();
        options.set_iterate_lower_bound(tx_key(workchain, account, 0).to_vec());
        let mut iter = self.db.raw_iterator_cf_opt(self.cf(CF_TRANSACTIONS)?, options);
        iter.seek_for_prev(tx_key(workchain, account, last_lt.unwrap_or(u64::MAX)));
        let mut found = Vec::new();
        while found.len() < limit {
            let Some(value) = iter.value() else { break };
            found.push(value.to_vec());
            iter.prev();
        }
        iter.status()?;
        Ok(found)
    }

    pub fn by_hash(&self, hash: &UInt256) -> Result<Option<Vec<u8>>> {
        self.by_index(CF_BY_HASH, hash)
    }

    /// The transaction that consumed the message with this (inbound message cell) hash.
    pub fn by_in_msg(&self, hash: &UInt256) -> Result<Option<Vec<u8>>> {
        self.by_index(CF_BY_IN_MSG, hash)
    }

    fn by_index(&self, family: &str, hash: &UInt256) -> Result<Option<Vec<u8>>> {
        match self.db.get_cf(self.cf(family)?, hash.as_slice())? {
            Some(key) => Ok(self.db.get_cf(self.cf(CF_TRANSACTIONS)?, key)?),
            None => Ok(None),
        }
    }

    /// The smallest lt in the index, u64::MAX while it is empty (as jrpc.everwallet.net).
    pub fn smallest_known_lt(&self) -> u64 {
        self.smallest_lt.load(Ordering::Relaxed)
    }

    fn scan_smallest_lt(&self) -> Result<u64> {
        // a few accounts: a key scan at start is cheap
        let mut smallest = u64::MAX;
        let mut iter = self.db.raw_iterator_cf(self.cf(CF_TRANSACTIONS)?);
        iter.seek_to_first();
        while let Some(key) = iter.key() {
            if key.len() == KEY_LEN {
                smallest = smallest.min(key_lt(key));
            }
            iter.next();
        }
        iter.status()?;
        Ok(smallest)
    }

    /// For getHistoryStatus (not part of the everscale-jrpc API).
    pub fn status(&self) -> Result<Value> {
        let transactions = self.db.property_int_value_cf(self.cf(CF_TRANSACTIONS)?, "rocksdb.estimate-num-keys")?;
        let smallest = self.smallest_known_lt();
        Ok(json!({
            "accounts": self.accounts.load(Ordering::Relaxed),
            "startMcSeqno": self.start_mc_seqno()?,
            "lastMcSeqno": self.last_mc_block()?.map(|id| id.seq_no()),
            "gaps": self.gaps()?,
            "transactions": transactions,
            "smallestKnownLt": if smallest == u64::MAX { Value::Null } else { json!(smallest.to_string()) },
        }))
    }
}

// ---- walking the chain ------------------------------------------------------------------

/// A block the indexer asked for.
pub enum Loaded<T> {
    Block(T),
    /// the node no longer stores it (its history limit)
    Gone,
    /// the node is stopping
    Stopping,
}

pub enum Walk<T> {
    /// the chain, newest first; `complete` is false when an older block was gone
    Done { chain: Vec<T>, complete: bool },
    Stopping,
}

/// The shard blocks a masterchain block adds for one shard: from its top block back to the
/// previous masterchain block's top block of that shard (exclusive). After a merge the walk
/// stops at the merge block - both its parents were committed before; after a split both
/// children walk back through the parent's blocks (stored twice, harmlessly). `load`
/// returns a block with its (prev1, prev2).
pub async fn walk_shard_chain<T, F, Fut>(top: BlockIdExt, prev_tops: &[BlockIdExt], mut load: F) -> Result<Walk<T>>
where
    F: FnMut(BlockIdExt) -> Fut,
    Fut: Future<Output = Result<Loaded<(T, (BlockIdExt, Option<BlockIdExt>))>>>,
{
    let mut chain = Vec::new();
    if top.seq_no() == 0 {
        return Ok(Walk::Done { chain, complete: true });
    }
    let stop_at = prev_tops.iter()
        .find(|prev| prev.shard().intersect_with(top.shard()))
        .ok_or_else(|| error!("no previous top block for {}", top))?
        .clone();
    let mut id = top;
    while id != stop_at && id.seq_no() != 0 {
        match load(id.clone()).await? {
            Loaded::Stopping => return Ok(Walk::Stopping),
            Loaded::Gone => return Ok(Walk::Done { chain, complete: false }),
            Loaded::Block((block, (prev1, prev2))) => {
                chain.push(block);
                if prev2.is_some() {
                    break;
                }
                id = prev1;
            }
        }
    }
    Ok(Walk::Done { chain, complete: true })
}

/// The first stored masterchain block in `from..=last`, by binary search: a node stores a
/// contiguous range of blocks that ends at its last applied one. `load` gives the block
/// with that seqno, or None when the node does not store it. None: `last` itself is not
/// stored (yet). A block that survived alone below the range - a key block lives on in the
/// key block archive - may be found instead of the start of the range; indexing from it
/// runs into the next missing block, and the search is repeated from there.
pub async fn first_stored<T, F, Fut>(from: u32, last: u32, mut load: F) -> Result<Option<(u32, T)>>
where
    F: FnMut(u32) -> Fut,
    Fut: Future<Output = Result<Option<T>>>,
{
    if from > last {
        return Ok(None);
    }
    let Some(mut found) = load(last).await? else { return Ok(None) };
    let (mut lo, mut hi) = (from, last);
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        match load(mid).await? {
            Some(block) => {
                found = block;
                hi = mid;
            }
            None => lo = mid + 1,
        }
    }
    Ok(Some((hi, found)))
}

/// What to do when the block the index was committed at cannot be loaded.
#[derive(Debug, PartialEq)]
pub enum Missing<T> {
    /// the node has not got there (again) yet, or has nothing later to continue from
    Wait,
    /// history misses the masterchain seqnos `from..=to`; the indexer continues after
    /// `block`, the one with seqno `to` (its own transactions can be stored, its shard part
    /// cannot be walked without the block before it)
    AfterGap { from: u32, to: u32, block: T },
}

/// Where the indexer continues when the masterchain block after `committed` - or the
/// committed block itself - cannot be loaded. What tells the cases apart is the node's last
/// applied masterchain block:
/// - it is not past the committed one: the node is behind the index (it restarted after a
///   crash, or runs on an older copy of its database) and will apply that block again -
///   wait;
/// - it is past it: the blocks in between were applied and are gone - the archive GC
///   collected them, or the node's database was built anew from a later block (a resync)
///   and never had them. They will not come back: history continues after a gap, from
///   the first block the node stores.
/// `load` is as in `first_stored`.
pub async fn after_missing<T, F, Fut>(committed: u32, last_applied: Option<u32>, load: F) -> Result<Missing<T>>
where
    F: FnMut(u32) -> Fut,
    Fut: Future<Output = Result<Option<T>>>,
{
    let Some(last) = last_applied else { return Ok(Missing::Wait) };
    // a node that is not past the committed block: the range is empty, nothing is found
    Ok(match first_stored(committed + 1, last, load).await? {
        Some((to, block)) => Missing::AfterGap { from: committed + 1, to, block },
        None => Missing::Wait,
    })
}

/// One sleep of a pause that has `left` to go.
fn pause_step(left: Duration) -> Duration {
    left.min(PAUSE_STEP)
}

/// The pause after each masterchain block while catching up, for a pace of at most `rate`
/// blocks per second; none above 1000.
fn catch_up_pause(rate: u32) -> Duration {
    Duration::from_millis(1000 / u64::from(rate.max(1)))
}

// ---- the indexer ----------------------------------------------------------------------

/// What the JRPC server answers from, and what the indexer fills.
pub struct History {
    pub index: Arc<TxHistory>,
    pub config: HistoryConfig,
}

impl History {
    /// Opens (or creates) the index. The accounts file must be readable now: a typo shows
    /// at start instead of as a silently empty history.
    pub fn open(config: &HistoryConfig, db_root: &str) -> Result<Self> {
        let watched = read_accounts(Path::new(&config.accounts_file))?;
        let path = match &config.db_path {
            Some(path) => PathBuf::from(path),
            None => Path::new(db_root).join("jrpc_history"),
        };
        let index = Arc::new(TxHistory::open(&path)?);
        index.accounts.store(watched.len(), Ordering::Relaxed);
        log::info!("JRPC history: {} accounts from {}, index {}", watched.len(), config.accounts_file, path.display());
        Ok(Self { index, config: config.clone() })
    }
}

struct AccountsFile {
    path: PathBuf,
    modified: Option<SystemTime>,
    checked: Instant,
    watched: Arc<Watched>,
}

impl AccountsFile {
    fn load(path: &Path, index: &TxHistory) -> Result<Self> {
        let modified = std::fs::metadata(path).and_then(|m| m.modified()).ok();
        let watched = Arc::new(read_accounts(path)?);
        index.accounts.store(watched.len(), Ordering::Relaxed);
        Ok(Self { path: path.to_path_buf(), modified, checked: Instant::now(), watched })
    }

    /// Picks up an edited file; a file that does not parse keeps the previous list.
    fn refresh(&mut self, index: &TxHistory) {
        if self.checked.elapsed() < ACCOUNTS_CHECK_PERIOD {
            return;
        }
        self.checked = Instant::now();
        let modified = std::fs::metadata(&self.path).and_then(|m| m.modified()).ok();
        if modified == self.modified {
            return;
        }
        match read_accounts(&self.path) {
            Ok(watched) => {
                log::info!("JRPC history: accounts file changed, {} -> {} accounts", self.watched.len(), watched.len());
                index.accounts.store(watched.len(), Ordering::Relaxed);
                self.watched = Arc::new(watched);
                self.modified = modified;
            }
            Err(e) => log::error!("JRPC history: keeping the previous {} accounts: {}", self.watched.len(), e),
        }
    }
}

/// Runs the indexer on the node's runtime until the node stops. Errors are logged and the
/// indexer resumes from the last committed masterchain block.
pub fn start_indexer(engine: Arc<dyn EngineOperations>, history: History) {
    tokio::spawn(async move {
        engine.acquire_stop(Engine::MASK_SERVICE_JRPC_HISTORY);
        let indexer = Indexer { engine: engine.clone(), index: history.index, config: history.config };
        // history starts where the node starts; the work waits until its startup is over
        if let Err(e) = indexer.begin_if_empty().await {
            log::error!("JRPC history: cannot fix where history starts: {}", e);
        }
        let delay = Duration::from_secs(u64::from(indexer.config.start_delay_sec));
        log::info!("JRPC history indexer: starts in {} s, after the node's startup", delay.as_secs());
        let stopping = indexer.pause(delay).await;
        while !stopping {
            match indexer.run().await {
                Ok(()) => break,
                Err(e) => {
                    log::error!("JRPC history indexer: {} - resuming from the last committed block", e);
                    if indexer.pause(Duration::from_secs(5)).await {
                        break;
                    }
                }
            }
        }
        log::info!("JRPC history indexer stopped");
        engine.release_stop(Engine::MASK_SERVICE_JRPC_HISTORY);
    });
}

struct Indexer {
    engine: Arc<dyn EngineOperations>,
    index: Arc<TxHistory>,
    config: HistoryConfig,
}

type McBlock = (Arc<BlockHandle>, BlockStuff);

impl Indexer {
    /// Sleeps in steps of at most 100 ms; true when the node is stopping.
    async fn pause(&self, total: Duration) -> bool {
        let until = Instant::now() + total;
        loop {
            if self.engine.check_stop() {
                return true;
            }
            let left = until.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return false;
            }
            tokio::time::sleep(pause_step(left)).await;
        }
    }

    async fn run(&self) -> Result<()> {
        let mut accounts = AccountsFile::load(Path::new(&self.config.accounts_file), &self.index)?;
        let Some((mut prev_handle, mut prev_block)) = self.resume(&accounts.watched).await? else { return Ok(()) };
        log::info!("JRPC history indexer: {} accounts, continuing after masterchain block {}",
            accounts.watched.len(), prev_handle.id().seq_no());
        loop {
            if self.engine.check_stop() {
                return Ok(());
            }
            accounts.refresh(&self.index);
            let asked = Instant::now();
            let (handle, block) = match self.engine.wait_next_applied_mc_block(&prev_handle, Some(1000)).await {
                Ok(next) => next,
                Err(_) => {
                    // not applied yet (the wait ran out of time) - or applied, but the node
                    // no longer stores it
                    if let Some(after_gap) = self.skip_gone(&prev_handle, &accounts.watched).await? {
                        (prev_handle, prev_block) = after_gap;
                    } else if asked.elapsed() < QUICK_FAILURE && self.pause(Duration::from_secs(1)).await {
                        return Ok(());
                    }
                    continue;
                }
            };
            let Some(complete) = self.index_mc_block(&prev_block, &block, &accounts.watched).await? else {
                return Ok(());
            };
            if !complete {
                log::warn!("JRPC history: masterchain block {} - the node no longer has some of its shard blocks", handle.id().seq_no());
            }
            if block.gen_utime()? + CATCH_UP_LAG_SEC < self.engine.now()
                && self.pause(catch_up_pause(self.config.catch_up_mc_blocks_per_sec)).await
            {
                return Ok(());
            }
            (prev_handle, prev_block) = (handle, block);
        }
    }

    /// Stores the transactions of one masterchain block and of the shard blocks it adds,
    /// then moves the marker to it; when some of its shard blocks were gone, the block's
    /// seqno goes on record as a gap in the same write. None when the node is stopping;
    /// Some(false) when some shard blocks were gone.
    async fn index_mc_block(&self, prev_mc: &BlockStuff, mc: &BlockStuff, watched: &Arc<Watched>) -> Result<Option<bool>> {
        let prev_tops = prev_mc.top_blocks_all()?;
        let mut blocks = vec![mc.clone()];
        let mut complete = true;
        for top in mc.top_blocks_all()? {
            match walk_shard_chain(top, &prev_tops, |id| self.applied_block(id)).await? {
                Walk::Stopping => return Ok(None),
                Walk::Done { chain, complete: whole } => {
                    blocks.extend(chain);
                    complete &= whole;
                }
            }
        }
        let seqno = mc.id().seq_no();
        self.store(blocks, mc.id().clone(), watched.clone(), (!complete).then_some((seqno, seqno))).await?;
        Ok(Some(complete))
    }

    /// Filters and commits off the async runtime (block parsing, RocksDB write).
    async fn store(
        &self, blocks: Vec<BlockStuff>, mc_block: BlockIdExt, watched: Arc<Watched>, gap: Option<(u32, u32)>
    ) -> Result<()> {
        let index = self.index.clone();
        tokio::task::spawn_blocking(move || -> Result<()> {
            let mut rows = Vec::new();
            for block in &blocks {
                if block.is_usual_block() {
                    rows.extend(block_rows(block.block()?, block.id().shard().workchain_id(), &watched)?);
                }
            }
            index.commit(&rows, &mc_block, gap)
        }).await.map_err(|e| error!("history indexer task: {}", e))?
    }

    /// The block's data, or None when the node no longer stores it. A block that never got
    /// its data has `has_data` off. A block whose archive slice the archive GC collected
    /// still says "has data" and "archived" in its handle - only loading it fails.
    async fn stored_block(&self, handle: &BlockHandle) -> Result<Option<BlockStuff>> {
        if !handle.has_data() {
            return Ok(None);
        }
        match self.engine.load_block(handle).await {
            Ok(block) => Ok(Some(block)),
            Err(e) if handle.is_archived() && !self.engine.check_stop() => {
                log::debug!("JRPC history: block {} is no longer stored: {}", handle.id(), e);
                Ok(None)
            }
            Err(e) => Err(e),
        }
    }

    /// The applied masterchain block with this seqno, when the node stores it.
    async fn stored_mc_block(&self, seqno: u32) -> Result<Option<McBlock>> {
        let handle = match self.engine.find_mc_block_by_seq_no(seqno).await {
            Ok(handle) => handle,
            Err(e) if self.engine.check_stop() => return Err(e),
            // no such block in this node's database: a seqno before its cold boot
            Err(_) => return Ok(None),
        };
        if !handle.is_applied() {
            return Ok(None);
        }
        Ok(self.stored_block(&handle).await?.map(|block| (handle, block)))
    }

    /// An applied block with its previous ids; waits for the shard client in 1 s steps.
    async fn applied_block(&self, id: BlockIdExt) -> Result<Loaded<(BlockStuff, (BlockIdExt, Option<BlockIdExt>))>> {
        let mut waiting_since = Instant::now();
        loop {
            if self.engine.check_stop() {
                return Ok(Loaded::Stopping);
            }
            if let Ok(handle) = self.engine.wait_applied_block(&id, Some(1000)).await {
                let Some(block) = self.stored_block(&handle).await? else { return Ok(Loaded::Gone) };
                let prev = block.construct_prev_id()?;
                return Ok(Loaded::Block((block, prev)));
            }
            if waiting_since.elapsed() > Duration::from_secs(60) {
                log::warn!("JRPC history: still waiting for block {} to be applied", id);
                waiting_since = Instant::now();
            }
        }
    }

    /// An empty index gets its start point now (the node just booted): history starts
    /// after the last applied masterchain block, or at start_from_mc_seqno.
    async fn begin_if_empty(&self) -> Result<()> {
        if self.index.last_mc_block()?.is_none() {
            let base = self.start_base().await?;
            self.index.begin(&base)?;
            log::info!("JRPC history: new index, history starts at masterchain block {}", base.seq_no() + 1);
        }
        Ok(())
    }

    /// Where to continue: the committed block, or (empty index) the start point.
    async fn resume(&self, watched: &Arc<Watched>) -> Result<Option<McBlock>> {
        let base = match self.index.last_mc_block()? {
            Some(id) => id,
            None => {
                let base = self.start_base().await?;
                self.index.begin(&base)?;
                log::info!("JRPC history: new index, history starts at masterchain block {}", base.seq_no() + 1);
                base
            }
        };
        loop {
            if self.engine.check_stop() {
                return Ok(None);
            }
            if let Some(handle) = self.engine.load_block_handle(&base)?.filter(|handle| handle.is_applied()) {
                if let Some(block) = self.stored_block(&handle).await? {
                    return Ok(Some((handle, block)));
                }
            }
            // the committed block cannot be loaded: the node dropped it while the indexer was
            // not running, or it is not in this database - not yet, or never again
            if let Some(after_gap) = self.continue_after_gap(base.seq_no(), watched).await? {
                return Ok(Some(after_gap));
            }
            log::warn!("JRPC history: the node has neither the committed block {} nor a later one \
                to continue from yet - waiting", base);
            if self.pause(Duration::from_secs(5)).await {
                return Ok(None);
            }
        }
    }

    /// The block history starts after: the last applied one, or the block before
    /// `start_from_mc_seqno` - moved forward to the oldest one the node still stores.
    async fn start_base(&self) -> Result<BlockIdExt> {
        let last = self.engine.load_last_applied_mc_block_id()?
            .ok_or_else(|| error!("no applied masterchain block yet"))?;
        let Some(start) = self.config.start_from_mc_seqno.filter(|s| *s > 0 && *s <= last.seq_no()) else {
            return Ok((*last).clone());
        };
        match first_stored(start - 1, last.seq_no(), |seqno| self.stored_mc_block(seqno)).await? {
            Some((seqno, (handle, _block))) => {
                if seqno != start - 1 {
                    log::warn!("JRPC history: asked to start at masterchain block {}, the node keeps blocks from {} on",
                        start, seqno);
                }
                Ok(handle.id().clone())
            }
            None => Ok((*last).clone()),
        }
    }

    /// The next masterchain block did not come. None while it is not applied yet - and when
    /// it can be loaded after all. When it is applied but the node no longer stores it, the
    /// indexer continues after a gap.
    async fn skip_gone(&self, prev: &BlockHandle, watched: &Arc<Watched>) -> Result<Option<McBlock>> {
        if !prev.has_next1() {
            return Ok(None);
        }
        let next = self.engine.load_block_next1(prev.id())?;
        match self.engine.load_block_handle(&next)? {
            Some(handle) if handle.is_applied() => {
                if self.stored_block(&handle).await?.is_some() {
                    return Ok(None);
                }
            }
            _ => return Ok(None), // not applied yet: keep waiting
        }
        self.continue_after_gap(prev.id().seq_no(), watched).await
    }

    /// The masterchain block after `committed` cannot be loaded (see `after_missing`): when
    /// the node is past it, finds the first later block it still stores and commits that
    /// block's own transactions together with the gap - the skipped seqnos and that block's
    /// shard part, which cannot be walked without the block before it. The indexer continues
    /// after the returned block; None when there is nothing to continue from yet.
    async fn continue_after_gap(&self, committed: u32, watched: &Arc<Watched>) -> Result<Option<McBlock>> {
        let last = self.engine.load_last_applied_mc_block_id()?.map(|id| id.seq_no());
        let found = after_missing(committed, last, |seqno| self.stored_mc_block(seqno)).await?;
        let Missing::AfterGap { from, to, block: (handle, block) } = found else { return Ok(None) };
        if self.engine.check_stop() {
            return Ok(None);
        }
        self.store(vec![block.clone()], handle.id().clone(), watched.clone(), Some((from, to))).await?;
        log::error!("JRPC history: masterchain blocks {}..{} are a gap in the history - \
            the node no longer stores their blocks", from, to);
        Ok(Some((handle, block)))
    }
}

#[cfg(test)]
#[path = "../tests/test_jrpc_history.rs"]
pub(crate) mod tests;
