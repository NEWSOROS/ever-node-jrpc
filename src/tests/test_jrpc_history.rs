/*
* Tests of the transaction history index (network/jrpc_history.rs).
*
* The files in static/jrpc/history hold real transactions of Everscale mainnet, cut from
* blocks of 2026-10-02 (static/jrpc/README.md): 100 consecutive transactions of the elector
* (masterchain: a tick-tock and the block's fee message in every block), the 5 newest of an
* ordinary wallet (workchain 0: external and internal inbound messages) and 3 of the config
* contract. Each file is a JSON array of transaction BOCs, newest first - what
* getTransactionsList answers with.
*/

use super::*;
use ever_block::{
    base64_decode, read_single_root_boc, BlockExtra, BlockInfo, Cell, InMsg, MerkleUpdate,
    ShardAccountBlocks, ValueFlow,
};

/// The elector: a system contract of the masterchain that transacts twice in every block.
pub(crate) const ELECTOR: &str = "-1:3333333333333333333333333333333333333333333333333333333333333333";
/// The config contract: in the same masterchain blocks; the tests leave it out of the index.
pub(crate) const CONFIG: &str = "-1:5555555555555555555555555555555555555555555555555555555555555555";
/// An ordinary wallet of workchain 0.
pub(crate) const WALLET: &str = "0:a25af8eabe4bbe2bb0ef14eafad3393fa564a67e1222607777e6472c47de7677";

/// The elector's newest transaction in the fixtures, and the internal message it consumed
/// (the fee message of masterchain block 62558433).
pub(crate) const ELECTOR_NEWEST_LT: u64 = 76411530000002;
pub(crate) const ELECTOR_NEWEST_IN_MSG: &str = "23e9c8d30fc76105e2e58f25dc95f31e1e4644eb90a7d69dc4a2f94a69a8079b";
/// The external message the wallet's second newest transaction consumed.
pub(crate) const WALLET_EXT_IN_MSG: &str = "9c1bccd4c754ba8465c22b805e2f8a8979feaf336196461055932384d93335a2";
/// The smallest lt in the fixtures of the elector and the wallet: the wallet's oldest one.
pub(crate) const SMALLEST_LT: u64 = 76409065000006;

/// The transactions of a fixture file, newest first.
pub(crate) fn transactions(name: &str) -> Vec<String> {
    let text = std::fs::read_to_string(format!("src/tests/static/jrpc/history/{}.json", name)).unwrap();
    serde_json::from_str(&text).unwrap()
}

pub(crate) fn boc_cell(boc_b64: &str) -> Cell {
    read_single_root_boc(base64_decode(boc_b64).unwrap()).unwrap()
}

pub(crate) fn transaction(boc_b64: &str) -> Transaction {
    Transaction::construct_from_cell(boc_cell(boc_b64)).unwrap()
}

/// An index row as the indexer makes it for a listed account, from a transaction BOC of
/// the given workchain.
pub(crate) fn row_from_boc(workchain: i8, boc_b64: &str) -> TxRow {
    let cell = boc_cell(boc_b64);
    let transaction = Transaction::construct_from_cell(cell.clone()).unwrap();
    let account = UInt256::from_slice(&transaction.account_id().get_bytestring(0));
    TxRow {
        key: tx_key(workchain, &account, transaction.logical_time()),
        hash: cell.repr_hash(),
        in_msg_hash: transaction.in_msg_cell().map(|msg| msg.repr_hash()),
        boc: write_boc(&cell).unwrap(),
        utime: transaction.now(),
        listed: true,
    }
}

/// The same row for an account that is not listed.
pub(crate) fn other_row_from_boc(workchain: i8, boc_b64: &str) -> TxRow {
    TxRow { listed: false, ..row_from_boc(workchain, boc_b64) }
}

pub(crate) fn hashes(bocs: &[String]) -> Vec<UInt256> {
    bocs.iter().map(|boc| boc_cell(boc).repr_hash()).collect()
}

/// A fresh directory under the system temp dir (removed by the caller).
pub(crate) fn temp_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_nanos();
    let dir = std::env::temp_dir().join(format!("jrpc-history-{}-{}-{}", name, std::process::id(), nanos));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

pub(crate) fn mc_id(seqno: u32) -> BlockIdExt {
    BlockIdExt::with_params(ShardIdent::masterchain(), seqno, UInt256::from([seqno as u8; 32]), UInt256::from([7; 32]))
}

/// An index with the elector's history (100 transactions, masterchain) and the wallet's
/// (5, workchain 0).
pub(crate) fn golden_index(name: &str) -> (TxHistory, PathBuf) {
    let dir = temp_dir(name);
    let index = TxHistory::open(&dir, None).unwrap();
    let mut rows: Vec<TxRow> = transactions("elector").iter().map(|boc| row_from_boc(-1, boc)).collect();
    rows.extend(transactions("wallet").iter().map(|boc| row_from_boc(0, boc)));
    index.commit(&rows, &mc_id(1), None).unwrap();
    (index, dir)
}

fn account(address: &str) -> UInt256 {
    std_address(address).unwrap().1
}

// ---- the fixtures ---------------------------------------------------------------------

#[test]
fn test_fixtures_are_consecutive_transactions_of_their_accounts() {
    // each file: one account's transactions, newest first, every one naming the next as its
    // previous transaction (lt and hash) - nothing in between is missing
    for (name, address, count) in [("elector", ELECTOR, 100), ("wallet", WALLET, 5), ("config", CONFIG, 3)] {
        let list = transactions(name);
        assert_eq!(list.len(), count, "{}", name);
        for boc in &list {
            let id = UInt256::from_slice(&transaction(boc).account_id().get_bytestring(0));
            assert_eq!(id, account(address), "{}", name);
        }
        for (boc, older) in list.iter().zip(list.iter().skip(1)) {
            let newer = transaction(boc);
            assert_eq!(newer.prev_trans_lt(), transaction(older).logical_time(), "{}", name);
            assert_eq!(*newer.prev_trans_hash(), boc_cell(older).repr_hash(), "{}", name);
        }
    }
    // the elector, twice per masterchain block: a tick-tock (no inbound message) and the
    // block's fees arriving as an internal message
    let elector = transactions("elector");
    assert_eq!(transaction(&elector[0]).logical_time(), ELECTOR_NEWEST_LT);
    assert_eq!(elector.iter().filter(|boc| transaction(boc).in_msg_cell().is_some()).count(), 50);
    // the wallet: its second newest transaction was started by an external message
    let wallet = transactions("wallet");
    let message = transaction(&wallet[1]).read_in_msg().unwrap().expect("an inbound message");
    assert!(message.get_std().unwrap().is_inbound_external());
    assert_eq!(transaction(&wallet[4]).logical_time(), SMALLEST_LT);
}

// ---- which accounts -------------------------------------------------------------------

#[test]
fn test_history_config_defaults() {
    // nothing is required: every account's transactions of the last 30 days, 8 GB of them
    // at most, and no account kept for good. The indexer waits 10 min after boot by
    // default, so the node's startup (the validator sessions coming up) is exactly as
    // without it
    let config: HistoryConfig = serde_json::from_str("{}").unwrap();
    assert_eq!(config.accounts_file, None);
    assert_eq!(config.other_accounts_days, 30);
    assert_eq!(config.other_accounts_max_mb, 8192);
    assert_eq!(config.retention(), Some(Retention { keep_sec: 30 * 86_400, max_bytes: 8192 << 20 }));
    assert_eq!(config.start_delay_sec, 600);
    assert_eq!(config.catch_up_mc_blocks_per_sec, 10);
    assert_eq!(config.start_from_mc_seqno, None);
    assert_eq!(config.db_path, None);
    // a section written for the version before this one: the listed accounts, and now the
    // other ones too
    let listed: HistoryConfig = serde_json::from_str(r#"{"accounts_file": "/a", "start_delay_sec": 0}"#).unwrap();
    assert_eq!(listed.accounts_file.as_deref(), Some("/a"));
    assert_eq!(listed.start_delay_sec, 0);
    assert_eq!(listed.other_accounts_days, 30);
    // only the listed accounts, as that version indexed
    let only: HistoryConfig = serde_json::from_str(r#"{"accounts_file": "/a", "other_accounts_days": 0}"#).unwrap();
    assert_eq!(only.retention(), None);
    // days and megabytes as written; 0 megabytes is no limit
    let tuned: HistoryConfig =
        serde_json::from_str(r#"{"other_accounts_days": 7, "other_accounts_max_mb": 0}"#).unwrap();
    assert_eq!(tuned.retention(), Some(Retention { keep_sec: 7 * 86_400, max_bytes: u64::MAX }));
    let small: HistoryConfig =
        serde_json::from_str(r#"{"other_accounts_days": 1, "other_accounts_max_mb": 3}"#).unwrap();
    assert_eq!(small.retention(), Some(Retention { keep_sec: 86_400, max_bytes: 3 * 1024 * 1024 }));
}

#[test]
fn test_history_needs_something_to_index() {
    let dir = temp_dir("nothing");
    let root = dir.to_str().unwrap();
    // neither listed accounts nor the other ones: the history stays off, with the reason
    let nothing: HistoryConfig = serde_json::from_str(r#"{"other_accounts_days": 0}"#).unwrap();
    let error = History::open(&nothing, root).err().expect("nothing to index").to_string();
    assert!(error.contains("nothing to index"), "{}", error);
    assert!(!dir.join("jrpc_history").exists(), "no index is made for it");
    // an accounts file that cannot be read is an error whatever else is indexed
    let missing: HistoryConfig = serde_json::from_str(r#"{"accounts_file": "/no/such/file"}"#).unwrap();
    assert!(History::open(&missing, root).is_err());
    // every account, none listed: the index is in the node's database directory
    let all: HistoryConfig = serde_json::from_str("{}").unwrap();
    let history = History::open(&all, root).unwrap();
    assert_eq!(history.index.status().unwrap()["accounts"], 0);
    assert_eq!(history.index.status().unwrap()["otherAccountsDays"], 30);
    assert!(dir.join("jrpc_history").exists());
    drop(history);
    std::fs::remove_dir_all(dir).ok();
}

#[test]
fn test_accounts_file_is_parsed_strictly() {
    let text = format!("# the accounts to index\n{}\n\n  {}  # a wallet\n{}\n{}\n", ELECTOR, WALLET, ELECTOR, CONFIG);
    let watched = Watched::parse(&text).unwrap();
    assert_eq!(watched.len(), 3, "the duplicate elector counts once");
    assert_eq!(watched.in_workchain(-1), &[account(ELECTOR), account(CONFIG)]);
    assert_eq!(watched.in_workchain(0), &[account(WALLET)]);
    assert!(watched.in_workchain(5).is_empty());
    // an account is listed in its workchain only
    assert!(watched.contains(-1, &account(ELECTOR)) && watched.contains(0, &account(WALLET)));
    assert!(!watched.contains(0, &account(ELECTOR)) && !watched.contains(-1, &account(WALLET)));
    assert!(!Watched::default().contains(-1, &account(ELECTOR)));

    let bad = Watched::parse(&format!("{}\nnot an address\n", WALLET)).unwrap_err().to_string();
    assert!(bad.contains("line 2"), "{}", bad);
    assert!(Watched::parse("0:1234\n").is_err(), "a short address is not an account");
    assert_eq!(Watched::parse("\n# nothing\n").unwrap().len(), 0);
}

// ---- what a block contributes ---------------------------------------------------------

/// A block whose account blocks hold the given transactions (oldest first per account, as
/// consecutive transactions chain their state hashes).
fn block_with(transactions: &[&str]) -> Block {
    let mut account_blocks = ShardAccountBlocks::default();
    for boc in transactions {
        let cell = boc_cell(boc);
        let transaction = Transaction::construct_from_cell(cell.clone()).unwrap();
        account_blocks.add_serialized_transaction(&transaction, &cell).unwrap();
    }
    let mut extra = BlockExtra::default();
    extra.write_account_blocks(&account_blocks).unwrap();
    Block::with_params(42, BlockInfo::default(), ValueFlow::default(), MerkleUpdate::default(), extra).unwrap()
}

#[test]
fn test_block_rows_take_only_the_watched_accounts() {
    let elector = transactions("elector");
    let config = transactions("config");
    let wallet = transactions("wallet");
    // a masterchain block with the elector's and the config contract's transactions, a
    // workchain-0 block with the wallet's; the elector and the wallet are watched
    let mc_block = block_with(&[&elector[2], &elector[1], &elector[0], &config[0]]);
    let shard_block = block_with(&[&wallet[1], &wallet[0]]);
    let watched = Watched::parse(&format!("{}\n{}\n", ELECTOR, WALLET)).unwrap();

    let rows = block_rows(&mc_block, -1, &watched, false).unwrap();
    let expected: Vec<TxRow> = [&elector[2], &elector[1], &elector[0]].iter().map(|boc| row_from_boc(-1, boc)).collect();
    assert_eq!(rows.len(), 3, "the config contract is not watched and stays out");
    for row in &expected {
        let found = rows.iter().find(|r| r.key == row.key).expect("watched transaction indexed");
        assert_eq!(found, row, "hashes, the BOC, the transaction's own time, kept for good");
        assert_eq!(boc_cell(&ever_block::base64_encode(&found.boc)).repr_hash(), row.hash);
        assert_eq!(found.utime, transaction(&ever_block::base64_encode(&found.boc)).now());
        assert!(found.listed);
    }
    // the elector's newest transaction and its inbound message, as the chain names them
    let newest = rows.iter().find(|r| r.hash == boc_cell(&elector[0]).repr_hash()).unwrap();
    assert_eq!(key_lt(&newest.key), ELECTOR_NEWEST_LT);
    assert_eq!(newest.in_msg_hash.as_ref().unwrap().to_hex_string(), ELECTOR_NEWEST_IN_MSG);
    assert_eq!(newest.key[0], 0xff, "masterchain rows carry workchain -1");
    // a tick-tock transaction has no inbound message
    let tick_tock = rows.iter().find(|r| r.hash == boc_cell(&elector[1]).repr_hash()).unwrap();
    assert_eq!(tick_tock.in_msg_hash, None);

    let rows = block_rows(&shard_block, 0, &watched, false).unwrap();
    assert_eq!(rows.len(), 2);
    let external = rows.iter().find(|r| r.hash == boc_cell(&wallet[1]).repr_hash()).unwrap();
    assert_eq!(external.in_msg_hash.as_ref().unwrap().to_hex_string(), WALLET_EXT_IN_MSG);
    assert_eq!(external.key[0], 0);

    // a block read as another workchain, or with nothing watched in it: nothing
    assert!(block_rows(&mc_block, 0, &watched, false).unwrap().is_empty());
    assert!(block_rows(&shard_block, -1, &watched, false).unwrap().is_empty());
    assert!(block_rows(&mc_block, -1, &Watched::parse(WALLET).unwrap(), false).unwrap().is_empty());
    assert!(block_rows(&mc_block, -1, &Watched::default(), false).unwrap().is_empty());
}

#[test]
fn test_block_rows_take_every_account_when_the_other_ones_are_indexed() {
    let elector = transactions("elector");
    let config = transactions("config");
    let wallet = transactions("wallet");
    let mc_block = block_with(&[&elector[2], &elector[1], &elector[0], &config[1], &config[0]]);
    let shard_block = block_with(&[&wallet[1], &wallet[0]]);
    let watched = Watched::parse(ELECTOR).unwrap();

    // the elector is listed, the config contract is not: both are taken, each marked
    let rows = block_rows(&mc_block, -1, &watched, true).unwrap();
    assert_eq!(rows.len(), 5);
    for boc in &elector[..3] {
        assert!(rows.contains(&row_from_boc(-1, boc)), "the listed account's row, kept for good");
    }
    for boc in &config[..2] {
        assert!(rows.contains(&other_row_from_boc(-1, boc)), "the other account's row, to be swept later");
    }
    // nobody is listed: every row is one to sweep
    let rows = block_rows(&mc_block, -1, &Watched::default(), true).unwrap();
    assert_eq!(rows.len(), 5);
    assert!(rows.iter().all(|row| !row.listed));
    // an account is listed in its own workchain: the wallet of workchain 0 is not the
    // account with the same id in the masterchain
    let rows = block_rows(&shard_block, 0, &watched, true).unwrap();
    assert_eq!(rows, vec![other_row_from_boc(0, &wallet[1]), other_row_from_boc(0, &wallet[0])]);
    let same_id_elsewhere = Watched::parse(&WALLET.replacen("0:", "-1:", 1)).unwrap();
    assert!(block_rows(&shard_block, 0, &same_id_elsewhere, true).unwrap().iter().all(|row| !row.listed));
    let rows = block_rows(&shard_block, 0, &Watched::parse(WALLET).unwrap(), true).unwrap();
    assert!(rows.iter().all(|row| row.listed));
    // a workchain id that does not fit a row key: nothing, as before
    assert!(block_rows(&shard_block, 1000, &watched, true).unwrap().is_empty());
}

#[test]
fn test_block_rows_of_a_real_masterchain_block() {
    // the key block of the server fixtures: every masterchain block has the elector's
    // transactions
    let text = std::fs::read_to_string("src/tests/static/jrpc/golden_keyblock.json").unwrap();
    let boc = serde_json::from_str::<serde_json::Value>(&text).unwrap()["result"]["block"].as_str().unwrap().to_string();
    let block = Block::construct_from_bytes(&base64_decode(&boc).unwrap()).unwrap();
    let rows = block_rows(&block, -1, &Watched::parse(ELECTOR).unwrap(), false).unwrap();
    assert_eq!(rows.len(), 2, "the elector transacts twice in every masterchain block");
    for row in &rows {
        assert_eq!(row.key[0], 0xff, "masterchain rows carry workchain -1");
        assert_eq!(&row.key[1..33], account(ELECTOR).as_slice());
        let cell = read_single_root_boc(&row.boc).unwrap();
        assert_eq!(cell.repr_hash(), row.hash);
        let transaction = Transaction::construct_from_cell(cell).unwrap();
        assert_eq!(transaction.logical_time(), key_lt(&row.key));
    }
    // the hash of an inbound message is the chain's own name for it: the block lists the
    // messages it imported under these hashes (InMsgDescr), and the one the elector consumed
    // is the block's fee message (recover_create_msg)
    let extra = block.read_extra().unwrap();
    let mut imported = Vec::new();
    extra.read_in_msg_descr().unwrap().iterate_with_keys(|hash: UInt256, _in_msg: InMsg| {
        imported.push(hash);
        Ok(true)
    }).unwrap();
    let consumed: Vec<UInt256> = rows.iter().filter_map(|row| row.in_msg_hash.clone()).collect();
    assert_eq!(consumed.len(), 1, "one of the two is a tick-tock, without an inbound message");
    assert!(imported.contains(&consumed[0]));
    let fees = extra.read_custom().unwrap().expect("a masterchain block")
        .read_recover_create_msg().unwrap().expect("the block's fees go to the elector");
    assert_eq!(fees.message_cell().unwrap().repr_hash(), consumed[0]);
    assert!(index_finds(&rows, &consumed[0]));

    let unused = format!("-1:{}", "4".repeat(64));
    assert!(block_rows(&block, -1, &Watched::parse(&unused).unwrap(), false).unwrap().is_empty());

    // every account of the block: the elector's two rows are among them, the same rows but
    // for the mark; each row is a transaction of the block, under its own account and lt
    let all = block_rows(&block, -1, &Watched::default(), true).unwrap();
    assert!(all.len() > rows.len(), "the elector is not the only account of a masterchain block");
    for row in &rows {
        assert!(all.contains(&TxRow { listed: false, ..row.clone() }));
    }
    let mut keys = HashSet::new();
    for row in &all {
        let cell = read_single_root_boc(&row.boc).unwrap();
        let transaction = Transaction::construct_from_cell(cell.clone()).unwrap();
        assert_eq!(cell.repr_hash(), row.hash);
        assert_eq!(&row.key[1..33], transaction.account_id().get_bytestring(0).as_slice());
        assert_eq!(key_lt(&row.key), transaction.logical_time());
        assert_eq!(row.utime, transaction.now());
        assert!(keys.insert(row.key), "each transaction once");
    }
    let listed = block_rows(&block, -1, &Watched::parse(ELECTOR).unwrap(), true).unwrap();
    assert_eq!(listed.iter().filter(|row| row.listed).cloned().collect::<Vec<_>>(), rows);
    assert_eq!(listed.len(), all.len());
}

/// The rows, committed to a fresh index, are found by that inbound message hash.
fn index_finds(rows: &[TxRow], in_msg: &UInt256) -> bool {
    let dir = temp_dir("finds");
    let index = TxHistory::open(&dir, None).unwrap();
    index.commit(rows, &mc_id(1), None).unwrap();
    let found = index.by_in_msg(in_msg).unwrap().is_some();
    drop(index);
    std::fs::remove_dir_all(dir).ok();
    found
}

// ---- the index ------------------------------------------------------------------------

#[test]
fn test_list_is_newest_first_inclusive_and_pages_like_nekoton() {
    let (index, dir) = golden_index("list");
    let elector = account(ELECTOR);
    let all = transactions("elector");

    let page = index.list(-1, &elector, None, 5).unwrap();
    let as_b64: Vec<String> = page.iter().map(|boc| ever_block::base64_encode(boc)).collect();
    assert_eq!(hashes(&as_b64), hashes(&all[..5]));
    // lastTransactionLt is inclusive: the newest transaction's own lt gives the same page
    assert_eq!(index.list(-1, &elector, Some(ELECTOR_NEWEST_LT), 5).unwrap(), page);
    let older: Vec<String> = index.list(-1, &elector, Some(ELECTOR_NEWEST_LT - 1), 3).unwrap()
        .iter().map(|boc| ever_block::base64_encode(boc)).collect();
    assert_eq!(hashes(&older), hashes(&all[1..4]));

    // nekoton pages with lastTransactionLt = prev_trans_lt of the oldest one it got
    let mut collected = Vec::new();
    let mut last_lt = None;
    loop {
        let page = index.list(-1, &elector, last_lt, 7).unwrap();
        if page.is_empty() {
            break;
        }
        let oldest = Transaction::construct_from_cell(read_single_root_boc(page.last().unwrap()).unwrap()).unwrap();
        collected.extend(page.iter().map(|boc| ever_block::base64_encode(boc)));
        if oldest.prev_trans_lt() == 0 {
            break;
        }
        last_lt = Some(oldest.prev_trans_lt());
        if collected.len() >= all.len() {
            // the index starts where the fixture ends: the next page is empty
            assert!(index.list(-1, &elector, last_lt, 7).unwrap().is_empty());
            break;
        }
    }
    assert_eq!(hashes(&collected), hashes(&all), "paging walks the whole history once, in order");

    // the same account id in another workchain is another account
    assert!(index.list(0, &elector, None, 5).unwrap().is_empty());
    assert_eq!(index.list(0, &account(WALLET), None, 100).unwrap().len(), 5);
    drop(index);
    std::fs::remove_dir_all(dir).ok();
}

#[test]
fn test_list_stays_inside_the_account() {
    let dir = temp_dir("boundary");
    let index = TxHistory::open(&dir, None).unwrap();
    let a = UInt256::from([0x55; 32]);
    let mut b_bytes = [0x55; 32];
    b_bytes[31] = 0x56;
    let b = UInt256::from(b_bytes);
    let row = |workchain: i8, account: &UInt256, lt: u64| TxRow {
        key: tx_key(workchain, account, lt), hash: UInt256::from([lt as u8; 32]), in_msg_hash: None, boc: vec![lt as u8],
        utime: 0, listed: true,
    };
    index.commit(&[row(0, &a, 10), row(0, &a, 20), row(0, &b, 5), row(0, &b, 30), row(-1, &a, 15)], &mc_id(1), None).unwrap();
    assert_eq!(index.list(0, &a, None, 10).unwrap(), vec![vec![20], vec![10]]);
    assert_eq!(index.list(0, &b, None, 10).unwrap(), vec![vec![30], vec![5]]);
    assert_eq!(index.list(0, &b, Some(29), 10).unwrap(), vec![vec![5]], "not a's 20 below b's keys");
    assert_eq!(index.list(-1, &a, None, 10).unwrap(), vec![vec![15]], "the workchain is part of the key");
    assert_eq!(index.list(0, &a, Some(9), 10).unwrap(), Vec::<Vec<u8>>::new());
    assert_eq!(index.list(0, &UInt256::from([0x54; 32]), None, 10).unwrap(), Vec::<Vec<u8>>::new());
    assert_eq!(index.smallest_known_lt(), 5);
    drop(index);
    std::fs::remove_dir_all(dir).ok();
}

#[test]
fn test_lookups_by_hash_and_by_inbound_message() {
    let (index, dir) = golden_index("lookups");
    let elector = transactions("elector");
    let wallet = transactions("wallet");
    let hash = boc_cell(&elector[0]).repr_hash();
    let found = index.by_hash(&hash).unwrap().expect("by hash");
    assert_eq!(read_single_root_boc(&found).unwrap().repr_hash(), hash);

    // the elector transaction that consumed an internal message, the wallet one that
    // consumed an external message (what a wallet looks for after sendMessage)
    for (message, expected) in [(ELECTOR_NEWEST_IN_MSG, &elector[0]), (WALLET_EXT_IN_MSG, &wallet[1])] {
        let found = index.by_in_msg(&UInt256::from_str(message).unwrap()).unwrap().expect(message);
        assert_eq!(read_single_root_boc(&found).unwrap().repr_hash(), boc_cell(expected).repr_hash(), "{}", message);
    }
    assert!(index.by_hash(&UInt256::default()).unwrap().is_none());
    assert!(index.by_in_msg(&UInt256::default()).unwrap().is_none());
    // a transaction hash is not a message hash
    assert!(index.by_in_msg(&hash).unwrap().is_none());
    drop(index);
    std::fs::remove_dir_all(dir).ok();
}

#[test]
fn test_commit_moves_the_marker_with_the_rows_and_survives_a_reopen() {
    let dir = temp_dir("reopen");
    {
        let index = TxHistory::open(&dir, None).unwrap();
        assert_eq!(index.last_mc_block().unwrap(), None);
        assert_eq!(index.smallest_known_lt(), u64::MAX, "empty: u64::MAX, as jrpc.everwallet.net");
        index.begin(&mc_id(99)).unwrap();
        assert_eq!(index.start_mc_seqno().unwrap(), Some(100));
        let rows: Vec<TxRow> = transactions("wallet").iter().map(|boc| row_from_boc(0, boc)).collect();
        index.commit(&rows, &mc_id(100), None).unwrap();
        // the same block again (a restart before the marker moved): nothing doubles
        index.commit(&rows, &mc_id(100), None).unwrap();
    }
    let index = TxHistory::open(&dir, None).unwrap();
    assert_eq!(index.last_mc_block().unwrap(), Some(mc_id(100)));
    assert_eq!(index.start_mc_seqno().unwrap(), Some(100));
    assert_eq!(index.list(0, &account(WALLET), None, 100).unwrap().len(), 5);
    assert_eq!(index.smallest_known_lt(), SMALLEST_LT, "recomputed from the stored keys");
    drop(index);
    std::fs::remove_dir_all(dir).ok();
}

#[test]
fn test_gaps_are_merged_and_reported() {
    let dir = temp_dir("gaps");
    let index = TxHistory::open(&dir, None).unwrap();
    index.commit(&[], &mc_id(12), Some((10, 12))).unwrap();
    index.commit(&[], &mc_id(15), Some((13, 15))).unwrap();
    index.commit(&[], &mc_id(20), Some((20, 20))).unwrap();
    assert_eq!(index.gaps().unwrap(), vec![(10, 15), (20, 20)]);
    index.accounts.store(3, Ordering::Relaxed);
    let status = index.status().unwrap();
    assert_eq!(status["gaps"], json!([[10, 15], [20, 20]]));
    assert_eq!(status["accounts"], 3);
    assert_eq!(status["smallestKnownLt"], Value::Null);
    drop(index);
    std::fs::remove_dir_all(dir).ok();
}

#[test]
fn test_a_gap_is_committed_with_the_marker() {
    let dir = temp_dir("gap-commit");
    {
        let index = TxHistory::open(&dir, None).unwrap();
        index.begin(&mc_id(40)).unwrap();
        // the node no longer had blocks 41..=49: block 50's own transactions, the marker and
        // the gap (through 50 - its shard part is missing too) go in one write
        let rows: Vec<TxRow> = transactions("wallet").iter().map(|boc| row_from_boc(0, boc)).collect();
        index.commit(&rows, &mc_id(50), Some((41, 50))).unwrap();
        assert_eq!(index.gaps().unwrap(), vec![(41, 50)]);
        assert_eq!(index.last_mc_block().unwrap(), Some(mc_id(50)));
        // a block some of whose shard blocks were gone: right after that gap, then apart from it
        index.commit(&[], &mc_id(51), Some((51, 51))).unwrap();
        index.commit(&[], &mc_id(60), Some((60, 60))).unwrap();
        // no gap: the record stays as it is
        index.commit(&[], &mc_id(61), None).unwrap();
    }
    let index = TxHistory::open(&dir, None).unwrap();
    assert_eq!(index.gaps().unwrap(), vec![(41, 51), (60, 60)]);
    assert_eq!(index.last_mc_block().unwrap(), Some(mc_id(61)));
    assert_eq!(index.list(0, &account(WALLET), None, 100).unwrap().len(), 5);
    let status = index.status().unwrap();
    assert_eq!(status["gaps"], json!([[41, 51], [60, 60]]));
    assert_eq!(status["startMcSeqno"], 41);
    assert_eq!(status["lastMcSeqno"], 61);
    assert_eq!(status["smallestKnownLt"], SMALLEST_LT.to_string());
    drop(index);
    std::fs::remove_dir_all(dir).ok();
}

// ---- the accounts that are not listed ------------------------------------------------------

const DAY: u32 = 86_400;
/// When the made-up rows of these tests are made.
const T: u32 = 1_790_000_000;

/// Every account that is not listed is kept for so many days, without a size limit.
fn kept_for(days: u32) -> Option<Retention> {
    Some(Retention { keep_sec: u64::from(days) * 86_400, max_bytes: u64::MAX })
}

fn other_account(n: u8) -> UInt256 {
    UInt256::from([n; 32])
}

/// A made-up row of workchain 0 of an account that is not listed: a transaction made at
/// `utime`, of `size` bytes, started by a message of its own.
fn other_row(account: &UInt256, lt: u64, utime: u32, size: usize) -> TxRow {
    let mut hash = [account.as_slice()[0]; 32];
    hash[..8].copy_from_slice(&lt.to_be_bytes());
    let mut in_msg = hash;
    in_msg[31] ^= 0xff;
    TxRow {
        key: tx_key(0, account, lt), hash: UInt256::from(hash), in_msg_hash: Some(UInt256::from(in_msg)),
        boc: vec![lt as u8; size], utime, listed: false,
    }
}

/// The lts of the account's made-up rows in the index, newest first.
fn lts(index: &TxHistory, account: &UInt256) -> Vec<u8> {
    index.list(0, account, None, 100).unwrap().iter().map(|boc| boc[0]).collect()
}

/// How many rows of accounts that are not listed the index says it has, and their bytes.
fn others(index: &TxHistory) -> (u64, u64) {
    let status = index.status().unwrap();
    (status["otherTransactions"].as_u64().unwrap(), status["otherBytes"].as_u64().unwrap())
}

/// What is in the database: rows, their names by hash, their names by inbound message, and
/// the records of the rows of the accounts that are not listed. A row that is swept must
/// take all of its entries along - one that stayed would not be found by any lookup, and
/// would stay for ever.
fn entries(index: &TxHistory) -> (usize, usize, usize, usize) {
    let count = |family: &str| {
        index.db.iterator_cf(index.cf(family).unwrap(), rocksdb::IteratorMode::Start).map(|entry| entry.unwrap()).count()
    };
    let records = index.db.iterator(rocksdb::IteratorMode::Start)
        .map(|entry| entry.unwrap()).filter(|(key, _)| key[0] == OTHER_PREFIX).count();
    (count(CF_TRANSACTIONS), count(CF_BY_HASH), count(CF_BY_IN_MSG), records)
}

#[test]
fn test_rows_of_other_accounts_are_stored_and_found_like_the_listed_ones() {
    let dir = temp_dir("others");
    let index = TxHistory::open(&dir, kept_for(30)).unwrap();
    let (elector, wallet) = (transactions("elector"), transactions("wallet"));
    // the elector is listed, the wallet is not; its shard block was walked twice (as after a
    // split), so its rows come twice in one go
    let wallet_rows: Vec<TxRow> = wallet.iter().map(|boc| other_row_from_boc(0, boc)).collect();
    let mut rows: Vec<TxRow> = elector[..3].iter().map(|boc| row_from_boc(-1, boc)).collect();
    rows.extend(wallet_rows.iter().chain(wallet_rows.iter()).cloned());
    index.commit(&rows, &mc_id(1), None).unwrap();

    let found: Vec<String> = index.list(0, &account(WALLET), None, 100).unwrap()
        .iter().map(|boc| ever_block::base64_encode(boc)).collect();
    assert_eq!(hashes(&found), hashes(&wallet));
    let by_message = index.by_in_msg(&UInt256::from_str(WALLET_EXT_IN_MSG).unwrap()).unwrap().expect("by message");
    assert_eq!(read_single_root_boc(&by_message).unwrap().repr_hash(), boc_cell(&wallet[1]).repr_hash());
    let hash = boc_cell(&wallet[0]).repr_hash();
    assert_eq!(read_single_root_boc(&index.by_hash(&hash).unwrap().expect("by hash")).unwrap().repr_hash(), hash);

    // what the index has of the other accounts: the wallet's 5 rows, each once, and the bytes
    // of their BOCs - the elector's rows are not among them
    let bytes: u64 = wallet_rows.iter().map(|row| row.boc.len() as u64).sum();
    assert_eq!(others(&index), (5, bytes));
    assert_eq!(index.status().unwrap()["otherAccountsDays"], 30);
    assert_eq!(index.smallest_known_lt(), SMALLEST_LT, "the wallet's oldest row is the oldest of all");
    // the same block again (a restart before the marker moved): nothing doubles
    index.commit(&rows, &mc_id(1), None).unwrap();
    assert_eq!(others(&index), (5, bytes));
    assert_eq!(index.list(0, &account(WALLET), None, 100).unwrap().len(), 5);
    drop(index);

    // the numbers are in the index: there after a restart, without its keys being read
    let index = TxHistory::open(&dir, kept_for(30)).unwrap();
    assert_eq!(others(&index), (5, bytes));
    assert_eq!(index.smallest_known_lt(), SMALLEST_LT);
    drop(index);
    // an index that keeps no other accounts says so
    let index = TxHistory::open(&dir, None).unwrap();
    assert_eq!(index.status().unwrap()["otherAccountsDays"], Value::Null);
    drop(index);
    std::fs::remove_dir_all(dir).ok();
}

#[test]
fn test_old_rows_of_other_accounts_are_swept() {
    let dir = temp_dir("sweep");
    let index = TxHistory::open(&dir, kept_for(10)).unwrap();
    let (a, b, listed) = (other_account(0xa1), other_account(0xb2), other_account(0xc3));
    let nobody = Watched::default();
    // rows of two accounts that are not listed, and one of a listed account older than all
    let rows = [
        other_row(&a, 10, T, 100), other_row(&b, 15, T + 2 * DAY, 30),
        other_row(&a, 20, T + DAY, 100), other_row(&a, 30, T + 5 * DAY, 100),
        TxRow { listed: true, ..other_row(&listed, 5, T - 100 * DAY, 9) },
    ];
    index.commit(&rows, &mc_id(1), None).unwrap();
    assert_eq!(others(&index), (4, 330));
    assert_eq!(index.smallest_known_lt(), 5);
    assert_eq!(entries(&index), (5, 5, 5, 4), "five rows, each with its two names; four of them to sweep");

    // 10 days are kept: a row made exactly 10 days ago stays, a second later it goes
    assert_eq!(index.sweep(T + 10 * DAY, &nobody).unwrap(), Swept::default());
    assert_eq!(lts(&index, &a), vec![30, 20, 10]);
    assert_eq!(index.sweep(T + 10 * DAY + 1, &nobody).unwrap(), Swept { dropped: 1, kept: 0 });
    assert_eq!(lts(&index, &a), vec![30, 20]);
    // ... with what named it, and nothing else
    assert!(index.by_hash(&rows[0].hash).unwrap().is_none());
    assert!(index.by_in_msg(rows[0].in_msg_hash.as_ref().unwrap()).unwrap().is_none());
    assert!(index.by_hash(&rows[2].hash).unwrap().is_some());
    assert!(index.by_in_msg(rows[2].in_msg_hash.as_ref().unwrap()).unwrap().is_some());
    assert_eq!(others(&index), (3, 230));
    assert_eq!(entries(&index), (4, 4, 4, 3), "the row took its names and its record along");
    assert_eq!(index.sweep(T + 10 * DAY + 1, &nobody).unwrap(), Swept::default(), "nothing more by then");

    // two days on: the rows of both accounts made until then. The listed account's row, the
    // oldest of all, is never swept
    assert_eq!(index.sweep(T + 12 * DAY + 1, &nobody).unwrap(), Swept { dropped: 2, kept: 0 });
    assert_eq!(lts(&index, &a), vec![30]);
    assert!(lts(&index, &b).is_empty());
    assert_eq!(lts(&index, &listed), vec![5]);
    assert_eq!(others(&index), (1, 100));
    assert_eq!(entries(&index), (2, 2, 2, 1));
    assert_eq!(index.smallest_known_lt(), 5);
    drop(index);

    // after a restart the sweep goes on
    let index = TxHistory::open(&dir, kept_for(10)).unwrap();
    assert_eq!(others(&index), (1, 100));
    assert_eq!(index.sweep(T + 15 * DAY, &nobody).unwrap(), Swept::default());
    assert_eq!(index.sweep(T + 15 * DAY + 1, &nobody).unwrap(), Swept { dropped: 1, kept: 0 });
    assert_eq!(others(&index), (0, 0));
    assert!(lts(&index, &a).is_empty());
    assert_eq!(lts(&index, &listed), vec![5]);
    assert_eq!(entries(&index), (1, 1, 1, 0), "the listed account's row and its names are all that is left");
    drop(index);
    std::fs::remove_dir_all(dir).ok();
}

#[test]
fn test_rows_are_swept_in_the_order_of_their_lt() {
    // the chain's lt and time grow together; rows are looked at in the order of their lt, and
    // the first one that is not old ends the sweep
    let dir = temp_dir("order");
    let index = TxHistory::open(&dir, kept_for(1)).unwrap();
    let (a, b) = (other_account(0xa1), other_account(0xb2));
    let nobody = Watched::default();
    index.commit(&[other_row(&b, 20, T, 10), other_row(&a, 10, T, 10), other_row(&a, 30, T + 3 * DAY, 10)], &mc_id(1), None).unwrap();
    assert_eq!(index.smallest_known_lt(), 10);
    assert_eq!(index.sweep(T + 2 * DAY, &nobody).unwrap(), Swept { dropped: 2, kept: 0 });
    assert_eq!((lts(&index, &a), lts(&index, &b)), (vec![30], vec![]));
    assert_eq!(index.smallest_known_lt(), 30, "the oldest row that is left");
    // a row with a greater lt made before it waits for it
    index.commit(&[other_row(&b, 40, T, 10)], &mc_id(2), None).unwrap();
    assert_eq!(index.sweep(T + 2 * DAY, &nobody).unwrap(), Swept::default());
    assert_eq!(index.sweep(T + 4 * DAY + 1, &nobody).unwrap(), Swept { dropped: 2, kept: 0 });
    assert_eq!(others(&index), (0, 0));
    assert_eq!(index.smallest_known_lt(), u64::MAX, "no row, no lt");
    drop(index);
    std::fs::remove_dir_all(dir).ok();
}

#[test]
fn test_the_size_limit_takes_the_oldest_rows_first() {
    let dir = temp_dir("limit-bytes");
    // rows of any age are kept - 250 bytes of them at most
    let index = TxHistory::open(&dir, Some(Retention { keep_sec: 1000 * 86_400, max_bytes: 250 })).unwrap();
    let a = other_account(0xa1);
    let nobody = Watched::default();
    let rows: Vec<TxRow> = (1..=5u64).map(|lt| other_row(&a, lt, T, 100)).collect();
    index.commit(&rows[..2], &mc_id(1), None).unwrap();
    assert_eq!(index.sweep(T, &nobody).unwrap(), Swept::default(), "200 bytes are within the limit");
    index.commit(&rows[2..], &mc_id(2), None).unwrap();
    assert_eq!(others(&index), (5, 500));
    // 500 bytes: the oldest rows go until no more than 250 are left
    assert_eq!(index.sweep(T, &nobody).unwrap(), Swept { dropped: 3, kept: 0 });
    assert_eq!(lts(&index, &a), vec![5, 4]);
    assert_eq!(others(&index), (2, 200));
    assert_eq!(index.smallest_known_lt(), 4);
    // exactly the limit is within it; one byte more is not
    index.commit(&[other_row(&a, 6, T, 50)], &mc_id(3), None).unwrap();
    assert_eq!(index.sweep(T, &nobody).unwrap(), Swept::default());
    assert_eq!(others(&index), (3, 250));
    index.commit(&[other_row(&a, 7, T, 1)], &mc_id(4), None).unwrap();
    assert_eq!(index.sweep(T, &nobody).unwrap(), Swept { dropped: 1, kept: 0 });
    assert_eq!(lts(&index, &a), vec![7, 6, 5]);
    assert_eq!(others(&index), (3, 151));
    drop(index);
    std::fs::remove_dir_all(dir).ok();
}

#[test]
fn test_a_sweep_takes_a_bounded_number_of_rows() {
    let dir = temp_dir("sweep-some");
    let index = TxHistory::open(&dir, kept_for(1)).unwrap();
    let a = other_account(0xa1);
    let nobody = Watched::default();
    let rows: Vec<TxRow> = (1..=5u64).map(|lt| other_row(&a, lt, T, 10)).collect();
    index.commit(&rows, &mc_id(1), None).unwrap();
    // all five are old; two at a time, each sweep from where the one before it stopped
    let now = T + 2 * DAY;
    assert_eq!(index.sweep_rows(now, &nobody, 2).unwrap(), Swept { dropped: 2, kept: 0 });
    assert_eq!(lts(&index, &a), vec![5, 4, 3]);
    assert_eq!(others(&index), (3, 30));
    assert_eq!(index.smallest_known_lt(), 3);
    assert_eq!(index.sweep_rows(now, &nobody, 2).unwrap(), Swept { dropped: 2, kept: 0 });
    assert_eq!(index.sweep_rows(now, &nobody, 2).unwrap(), Swept { dropped: 1, kept: 0 });
    assert_eq!(index.sweep_rows(now, &nobody, 2).unwrap(), Swept::default());
    assert_eq!(others(&index), (0, 0));
    assert!(lts(&index, &a).is_empty());
    drop(index);
    std::fs::remove_dir_all(dir).ok();
}

#[test]
fn test_rows_of_an_account_listed_meanwhile_are_kept_for_good() {
    let dir = temp_dir("listed-meanwhile");
    let index = TxHistory::open(&dir, kept_for(1)).unwrap();
    let (a, b, c) = (other_account(0xa1), other_account(0xb2), other_account(0xc3));
    let rows = [other_row(&a, 10, T, 10), other_row(&b, 15, T, 20), other_row(&a, 20, T, 10)];
    index.commit(&rows, &mc_id(1), None).unwrap();
    assert_eq!(others(&index), (3, 40));
    // `a` was put on the list before its rows got old: they stay - `b`'s go
    let with_a = Watched::parse(&format!("0:{}\n", a.to_hex_string())).unwrap();
    assert_eq!(index.sweep(T + 2 * DAY, &with_a).unwrap(), Swept { dropped: 1, kept: 2 });
    assert_eq!(lts(&index, &a), vec![20, 10]);
    assert!(lts(&index, &b).is_empty());
    assert!(index.by_hash(&rows[0].hash).unwrap().is_some());
    assert!(index.by_in_msg(rows[2].in_msg_hash.as_ref().unwrap()).unwrap().is_some());
    assert_eq!(others(&index), (0, 0), "they are a listed account's rows now");
    assert_eq!(entries(&index), (2, 2, 2, 0), "with their names, and no record to sweep them by");
    assert_eq!(index.smallest_known_lt(), 10);
    // ... for good: whatever the list says later, and after a restart
    assert_eq!(index.sweep(T + 100 * DAY, &Watched::default()).unwrap(), Swept::default());
    drop(index);
    let index = TxHistory::open(&dir, kept_for(1)).unwrap();
    assert_eq!(index.smallest_known_lt(), 10);
    assert_eq!(index.sweep(T + 100 * DAY, &Watched::default()).unwrap(), Swept::default());
    assert_eq!(lts(&index, &a), vec![20, 10]);

    // the account with the same id in another workchain is another account
    index.commit(&[other_row(&c, 40, T, 5)], &mc_id(2), None).unwrap();
    let elsewhere = Watched::parse(&format!("-1:{}\n", c.to_hex_string())).unwrap();
    assert_eq!(index.sweep(T + 2 * DAY, &elsewhere).unwrap(), Swept { dropped: 1, kept: 0 });
    assert!(lts(&index, &c).is_empty());
    drop(index);
    std::fs::remove_dir_all(dir).ok();
}

#[test]
fn test_rows_of_other_accounts_go_when_they_are_not_indexed_any_more() {
    let dir = temp_dir("off");
    let (a, listed) = (other_account(0xa1), other_account(0xc3));
    {
        let index = TxHistory::open(&dir, kept_for(30)).unwrap();
        let rows = [
            other_row(&a, 10, T, 10), other_row(&a, 20, T + DAY, 10),
            TxRow { listed: true, ..other_row(&listed, 30, T, 10) },
        ];
        index.commit(&rows, &mc_id(1), None).unwrap();
        assert_eq!(index.sweep(T + DAY, &Watched::default()).unwrap(), Swept::default());
    }
    // other_accounts_days was set to 0: their rows are not wanted, whatever their age - not
    // even one made after the block the sweep counts from
    let index = TxHistory::open(&dir, None).unwrap();
    assert_eq!(others(&index), (2, 20));
    assert_eq!(index.smallest_known_lt(), 10);
    assert_eq!(index.sweep(T, &Watched::default()).unwrap(), Swept { dropped: 2, kept: 0 });
    assert!(lts(&index, &a).is_empty());
    assert_eq!(lts(&index, &listed), vec![30]);
    assert_eq!(others(&index), (0, 0));
    assert_eq!(entries(&index), (1, 1, 1, 0));
    // the smallest lt is the listed account's now: what was read at the start told the two
    // kinds of rows apart
    assert_eq!(index.smallest_known_lt(), 30);
    drop(index);
    std::fs::remove_dir_all(dir).ok();
}

#[test]
fn test_a_swept_row_takes_only_its_own_names_along() {
    // an account can take the same external message twice: the message names the later
    // transaction, and still does when the earlier one is swept
    let dir = temp_dir("twice");
    let index = TxHistory::open(&dir, kept_for(1)).unwrap();
    let a = other_account(0xa1);
    let first = other_row(&a, 10, T, 10);
    let second = TxRow { in_msg_hash: first.in_msg_hash.clone(), ..other_row(&a, 20, T + 5 * DAY, 10) };
    index.commit(&[first.clone()], &mc_id(1), None).unwrap();
    index.commit(&[second.clone()], &mc_id(2), None).unwrap();
    let message = first.in_msg_hash.clone().unwrap();
    assert_eq!(index.by_in_msg(&message).unwrap(), Some(second.boc.clone()));
    assert_eq!(entries(&index), (2, 2, 1, 2), "two rows under one message");
    assert_eq!(index.sweep(T + 2 * DAY, &Watched::default()).unwrap(), Swept { dropped: 1, kept: 0 });
    assert_eq!(index.by_in_msg(&message).unwrap(), Some(second.boc.clone()));
    assert!(index.by_hash(&first.hash).unwrap().is_none());
    assert!(index.by_hash(&second.hash).unwrap().is_some());
    assert_eq!(entries(&index), (1, 1, 1, 1));
    // the later one, when its time comes, takes the name along
    assert_eq!(index.sweep(T + 7 * DAY, &Watched::default()).unwrap(), Swept { dropped: 1, kept: 0 });
    assert!(index.by_in_msg(&message).unwrap().is_none());
    assert_eq!(entries(&index), (0, 0, 0, 0), "nothing is left behind");
    drop(index);
    std::fs::remove_dir_all(dir).ok();
}

#[test]
fn test_a_row_older_than_the_swept_ones_is_not_forgotten() {
    let dir = temp_dir("late");
    let index = TxHistory::open(&dir, kept_for(1)).unwrap();
    let a = other_account(0xa1);
    let nobody = Watched::default();
    let start = vec![OTHER_PREFIX];
    assert_eq!(*index.sweep_from.lock().unwrap(), start);
    index.commit(&[other_row(&a, 10, T, 10), other_row(&a, 30, T, 10)], &mc_id(1), None).unwrap();
    assert_eq!(index.sweep(T + 2 * DAY, &nobody).unwrap(), Swept { dropped: 2, kept: 0 });
    // the next sweep seeks past the records this one deleted instead of through them
    let swept_to = OtherRow::record_key(&tx_key(0, &a, 30)).to_vec();
    assert_eq!(*index.sweep_from.lock().unwrap(), swept_to);
    // a row after that place leaves it where it is
    index.commit(&[other_row(&a, 40, T + 5 * DAY, 10)], &mc_id(2), None).unwrap();
    assert_eq!(*index.sweep_from.lock().unwrap(), swept_to);
    // a shard block that comes with a later masterchain block can hold a transaction with a
    // smaller lt than rows swept already: its record is before that place - the sweeps
    // start over, or it would stay for ever
    index.commit(&[other_row(&a, 20, T, 10)], &mc_id(3), None).unwrap();
    assert_eq!(*index.sweep_from.lock().unwrap(), start);
    assert_eq!(index.smallest_known_lt(), 20);
    assert_eq!(index.sweep(T + 2 * DAY, &nobody).unwrap(), Swept { dropped: 1, kept: 0 });
    assert_eq!(lts(&index, &a), vec![40]);
    assert_eq!(others(&index), (1, 10));
    assert_eq!(index.smallest_known_lt(), 40);
    drop(index);
    std::fs::remove_dir_all(dir).ok();
}

#[test]
fn test_an_index_made_before_other_accounts_were_indexed_opens_as_it_is() {
    let dir = temp_dir("before");
    {
        let index = TxHistory::open(&dir, None).unwrap();
        let mut rows: Vec<TxRow> = transactions("elector").iter().map(|boc| row_from_boc(-1, boc)).collect();
        rows.extend(transactions("wallet").iter().map(|boc| row_from_boc(0, boc)));
        index.commit(&rows, &mc_id(7), None).unwrap();
        // what the version before this one did not write
        for name in [META_LISTED_SMALLEST_LT, META_OTHER_COUNT, META_OTHER_BYTES] {
            index.db.delete(name).unwrap();
        }
    }
    // its rows are all listed accounts' rows: the smallest lt is found by reading their keys
    // through, once; nothing is there to sweep
    let index = TxHistory::open(&dir, kept_for(30)).unwrap();
    assert_eq!(index.smallest_known_lt(), SMALLEST_LT);
    assert_eq!(index.last_mc_block().unwrap(), Some(mc_id(7)));
    assert_eq!(others(&index), (0, 0));
    assert_eq!(index.sweep(u32::MAX, &Watched::default()).unwrap(), Swept::default());
    assert_eq!(index.list(-1, &account(ELECTOR), None, 100).unwrap().len(), 100);
    index.commit(&[other_row(&other_account(0xa1), u64::MAX - 1, T, 10)], &mc_id(8), None).unwrap();
    drop(index);
    // from the first write on that lt is in the index: the keys are not read again
    let index = TxHistory::open(&dir, kept_for(30)).unwrap();
    assert_eq!(index.number(META_LISTED_SMALLEST_LT).unwrap(), Some(SMALLEST_LT));
    assert_eq!(index.smallest_known_lt(), SMALLEST_LT);
    assert_eq!(others(&index), (1, 10));
    drop(index);
    // and the way back: the database keeps the layout that version opens - the same column
    // families, the records of the other accounts' rows next to the marker
    let mut families = DB::list_cf(&Options::default(), &dir).unwrap();
    families.sort();
    assert_eq!(families, ["default", "transactions", "transactions_by_hash", "transactions_by_in_msg"]);
    std::fs::remove_dir_all(dir).ok();
}

#[test]
fn test_a_damaged_record_is_an_error_not_a_guess() {
    let dir = temp_dir("damaged");
    let index = TxHistory::open(&dir, kept_for(1)).unwrap();
    index.commit(&[other_row(&other_account(0xa1), 10, T, 10)], &mc_id(1), None).unwrap();
    // a record with a value that is none: too short, or of a length no record has
    let record = OtherRow::record_key(&tx_key(0, &other_account(0xa1), 5));
    for value in [vec![b'?'], vec![0u8; 41], vec![0u8; 73]] {
        index.db.put(record, &value).unwrap();
        let error = index.sweep(T + 2 * DAY, &Watched::default()).unwrap_err().to_string();
        assert!(error.contains("bad record"), "{} bytes: {}", value.len(), error);
        assert_eq!(others(&index), (1, 10), "nothing was taken off on the way");
        assert_eq!(lts(&index, &other_account(0xa1)), vec![10]);
    }
    // a record with a key that is none: the index does not open as if nothing was wrong
    index.db.put([OTHER_PREFIX, 0], b"?").unwrap();
    drop(index);
    let error = TxHistory::open(&dir, kept_for(1)).err().expect("a damaged index").to_string();
    assert!(error.contains("bad record"), "{}", error);
    std::fs::remove_dir_all(dir).ok();
}

// ---- blocks the node no longer stores -----------------------------------------------------

/// A node's masterchain blocks as a table: the `load` of first_stored and after_missing,
/// the block with a stored seqno being the seqno itself.
fn stored(seqnos: impl IntoIterator<Item = u32>) -> impl FnMut(u32) -> std::future::Ready<Result<Option<u32>>> {
    let stored: std::collections::BTreeSet<u32> = seqnos.into_iter().collect();
    move |seqno| std::future::ready(Ok(stored.contains(&seqno).then_some(seqno)))
}

#[tokio::test]
async fn test_first_stored_block_is_found_by_binary_search() {
    // the node stores 50..=100, its last applied block is 100
    assert_eq!(first_stored(10, 100, stored(50..=100)).await.unwrap(), Some((50, 50)));
    assert_eq!(first_stored(50, 100, stored(50..=100)).await.unwrap(), Some((50, 50)));
    assert_eq!(first_stored(73, 100, stored(50..=100)).await.unwrap(), Some((73, 73)));
    assert_eq!(first_stored(100, 100, stored(50..=100)).await.unwrap(), Some((100, 100)));
    assert_eq!(first_stored(0, 100, stored([100])).await.unwrap(), Some((100, 100)));
    // everything is stored: the first one asked for
    assert_eq!(first_stored(1, 100, stored(0..=100)).await.unwrap(), Some((1, 1)));
    // the last applied block itself is not stored (yet), or nothing is asked for: nothing
    assert_eq!(first_stored(10, 100, stored(50..=99)).await.unwrap(), None);
    assert_eq!(first_stored(101, 100, stored(50..=100)).await.unwrap(), None);
    // the whole range of seqnos
    assert_eq!(first_stored(0, u32::MAX, stored([u32::MAX])).await.unwrap(), Some((u32::MAX, u32::MAX)));

    // a logarithmic number of loads, not one per block
    let mut loads = 0;
    let found = first_stored(0, 1_000_000, |seqno| {
        loads += 1;
        std::future::ready(Ok((seqno >= 777_777).then_some(seqno)))
    }).await.unwrap();
    assert_eq!(found, Some((777_777, 777_777)));
    assert!(loads <= 22, "{} loads", loads);

    // a load that fails is an error, not "not stored"
    let failed = first_stored(0, 100, |_| std::future::ready(Err::<Option<u32>, _>(error!("no disk")))).await;
    assert!(failed.unwrap_err().to_string().contains("no disk"));
}

#[tokio::test]
async fn test_where_the_indexer_continues_without_the_committed_block() {
    // the index was committed at block 40; the node's last applied block is 100 and it stores
    // 50..=100: the archive GC collected what was before, or the database was built anew
    // from block 50 (a resync) - block 41 will not come back either way
    let found = after_missing(40, Some(100), stored(50..=100)).await.unwrap();
    assert_eq!(found, Missing::AfterGap { from: 41, to: 50, block: 50 });
    // only block 40 itself is gone: the shard part of 41 cannot be walked without it
    let found = after_missing(40, Some(100), stored(41..=100)).await.unwrap();
    assert_eq!(found, Missing::AfterGap { from: 41, to: 41, block: 41 });

    // the node is behind the index - it restarted after a crash, or runs on an older copy of
    // its database - and will apply block 40 again: wait, nothing goes on record, whatever
    // it stores
    assert_eq!(after_missing(40, Some(35), stored(0..=35)).await.unwrap(), Missing::Wait);
    assert_eq!(after_missing(40, Some(40), stored(0..=40)).await.unwrap(), Missing::Wait);
    assert_eq!(after_missing(40, Some(40), stored(0..=100)).await.unwrap(), Missing::Wait);
    // nothing applied yet (the node is booting); the last applied block not stored yet
    assert_eq!(after_missing(40, None, stored(0..=100)).await.unwrap(), Missing::Wait);
    assert_eq!(after_missing(40, Some(100), stored(0..=40)).await.unwrap(), Missing::Wait);

    // a key block lives on in the key block archive after its neighbours were collected: the
    // search can come out at it (33) instead of the start of the stored range (50) ...
    let blocks = || stored((33..=33).chain(50..=100));
    let found = after_missing(10, Some(100), blocks()).await.unwrap();
    assert_eq!(found, Missing::AfterGap { from: 11, to: 33, block: 33 });
    // ... the block after it is missing again, and the next search reaches the range; the two
    // gaps join in the record
    let found = after_missing(33, Some(100), blocks()).await.unwrap();
    assert_eq!(found, Missing::AfterGap { from: 34, to: 50, block: 50 });
    let mut gaps = Vec::new();
    add_gap(&mut gaps, 11, 33);
    add_gap(&mut gaps, 34, 50);
    assert_eq!(gaps, vec![(11, 50)]);
}

#[test]
fn test_catch_up_pace_and_pause_steps() {
    // 10 masterchain blocks per second by default: a pause of 100 ms after each one
    assert_eq!(catch_up_pause(HistoryConfig::default_catch_up_rate()), Duration::from_millis(100));
    assert_eq!(catch_up_pause(50), Duration::from_millis(20));
    assert_eq!(catch_up_pause(1), Duration::from_secs(1));
    assert_eq!(catch_up_pause(0), Duration::from_secs(1), "0 is taken as 1");
    assert_eq!(catch_up_pause(1000), Duration::from_millis(1));
    assert_eq!(catch_up_pause(5000), Duration::ZERO, "above 1000: no pause");
    // a pause sleeps what is left of it, 100 ms at most at a time - the stop flag is looked
    // at in between - so a pause of 20 ms does not take 100
    assert_eq!(pause_step(Duration::from_millis(20)), Duration::from_millis(20));
    assert_eq!(pause_step(Duration::from_secs(600)), Duration::from_millis(100));
    assert_eq!(pause_step(Duration::ZERO), Duration::ZERO);
}

// ---- the indexer against a fake node ------------------------------------------------------

use crate::collator_test_bundle::create_block_handle_storage;
use ever_block::{BinTree, BlkPrevInfo, ExtBlkRef, InRefValue, McBlockExtra, ShardDescr};
use std::{collections::HashMap, sync::{atomic::{AtomicBool, AtomicU32}, Mutex as StdMutex}};
use storage::{block_handle_db::BlockHandleStorage, types::BlockMeta};

/// The time on the fake node; its blocks are generated at this very moment, so the indexer
/// never thinks it is catching up.
const NOW: u32 = 1_790_000_000;

/// A node as the indexer sees it: applied blocks - masterchain blocks by seqno, and the
/// shard blocks they refer to - some of them collected by the archive GC: the handle of
/// such a block still says "has data" and "archived", and loading it fails.
struct FakeChain {
    handles: BlockHandleStorage,
    blocks: StdMutex<HashMap<BlockIdExt, (Arc<BlockHandle>, BlockStuff)>>,
    collected: StdMutex<HashSet<BlockIdExt>>,
    last_applied: AtomicU32,
    stopping: AtomicBool,
}

impl FakeChain {
    fn new() -> Arc<Self> {
        Arc::new(FakeChain {
            handles: create_block_handle_storage().unwrap(),
            blocks: StdMutex::new(HashMap::new()),
            collected: StdMutex::new(HashSet::new()),
            last_applied: AtomicU32::new(0),
            stopping: AtomicBool::new(false),
        })
    }

    /// The node applies a block: its handle says the data is stored, in an archive.
    fn apply(&self, block: BlockStuff) {
        let id = block.id().clone();
        let handle = self.handles.create_handle(id.clone(), BlockMeta::with_data(0, NOW, 0, id.seq_no(), 0), None)
            .unwrap().expect("a new handle");
        handle.set_data();
        handle.set_block_applied();
        handle.set_archived();
        if id.shard().is_masterchain() {
            if let Some(prev) = self.mc_handle(id.seq_no() - 1) {
                prev.set_next1();
            }
            self.last_applied.fetch_max(id.seq_no(), Ordering::Relaxed);
        }
        self.blocks.lock().unwrap().insert(id, (handle, block));
    }

    /// A node that has applied the masterchain blocks `seqnos`, without shards; the block
    /// `n` holds the elector's transaction `elector[newest - n]`, so the newest block has
    /// the newest one.
    fn with_blocks(seqnos: std::ops::RangeInclusive<u32>) -> Arc<Self> {
        let chain = Self::new();
        let elector = transactions("elector");
        for seqno in seqnos.clone() {
            chain.apply(mc_block(seqno, &elector[(*seqnos.end() - seqno) as usize], None));
        }
        chain
    }

    /// The archive GC collects these masterchain blocks.
    fn collect(&self, seqnos: std::ops::RangeInclusive<u32>) {
        self.collected.lock().unwrap().extend(seqnos.map(mc_id));
    }

    fn handle(&self, id: &BlockIdExt) -> Option<Arc<BlockHandle>> {
        self.blocks.lock().unwrap().get(id).map(|(handle, _)| handle.clone())
    }

    fn mc_handle(&self, seqno: u32) -> Option<Arc<BlockHandle>> {
        self.handle(&mc_id(seqno))
    }

    /// An indexer of the elector and the wallet on this node - of the listed accounts
    /// only - with its index in a fresh directory.
    fn indexer(self: &Arc<Self>, name: &str, start_from_mc_seqno: Option<u32>) -> (Indexer, PathBuf) {
        self.indexer_of(name, start_from_mc_seqno, &[ELECTOR, WALLET], 0)
    }

    /// An indexer that keeps the `listed` accounts for good and, with `other_days` above
    /// 0, every other account for that many days.
    fn indexer_of(
        self: &Arc<Self>, name: &str, start_from_mc_seqno: Option<u32>, listed: &[&str], other_days: u32
    ) -> (Indexer, PathBuf) {
        let dir = temp_dir(name);
        let accounts_file = dir.join("accounts.txt");
        std::fs::write(&accounts_file, listed.iter().map(|address| format!("{}\n", address)).collect::<String>()).unwrap();
        let config = HistoryConfig {
            accounts_file: Some(accounts_file.to_str().unwrap().to_string()), db_path: None, start_from_mc_seqno,
            catch_up_mc_blocks_per_sec: 10, start_delay_sec: 0,
            other_accounts_days: other_days, other_accounts_max_mb: 0,
        };
        let index = Arc::new(TxHistory::open(&dir.join("index"), config.retention()).unwrap());
        (Indexer { engine: self.clone() as Arc<dyn EngineOperations>, index, config }, dir)
    }
}

fn one_transaction(transaction_boc: &str) -> BlockExtra {
    let cell = boc_cell(transaction_boc);
    let mut account_blocks = ShardAccountBlocks::default();
    account_blocks.add_serialized_transaction(&Transaction::construct_from_cell(cell.clone()).unwrap(), &cell).unwrap();
    let mut extra = BlockExtra::default();
    extra.write_account_blocks(&account_blocks).unwrap();
    extra
}

/// A masterchain block generated NOW that holds one transaction. `shard_top`: the top block
/// of workchain 0 (one shard) it refers to; None - a masterchain without shards.
fn mc_block(seqno: u32, transaction_boc: &str, shard_top: Option<&BlockIdExt>) -> BlockStuff {
    mc_block_at(seqno, transaction_boc, shard_top, NOW)
}

/// The same block, generated at `utime`.
fn mc_block_at(seqno: u32, transaction_boc: &str, shard_top: Option<&BlockIdExt>, utime: u32) -> BlockStuff {
    let mut custom = McBlockExtra::default();
    if let Some(top) = shard_top {
        let descr = ShardDescr {
            seq_no: top.seq_no(), root_hash: top.root_hash().clone(), file_hash: top.file_hash().clone(),
            ..ShardDescr::default()
        };
        custom.shards_mut().set(&0, &InRefValue(BinTree::with_item(&descr).unwrap())).unwrap();
    }
    let mut extra = one_transaction(transaction_boc);
    extra.write_custom(Some(&custom)).unwrap();
    let mut info = BlockInfo::default();
    info.set_shard(ShardIdent::masterchain());
    info.set_seq_no(seqno).unwrap();
    info.set_gen_utime(utime.into());
    let block = Block::with_params(42, info, ValueFlow::default(), MerkleUpdate::default(), extra).unwrap();
    BlockStuff::fake_with_block(mc_id(seqno), block)
}

/// The only shard of workchain 0 on the fake node.
fn shard() -> ShardIdent {
    ShardIdent::with_tagged_prefix(0, 0x8000_0000_0000_0000).unwrap()
}

/// The shard block `seqno` of workchain 0: it follows the block before it and holds one
/// transaction.
fn wc0_block(seqno: u32, transaction_boc: &str) -> BlockStuff {
    let (id, prev) = (shard_block(&shard(), seqno), shard_block(&shard(), seqno - 1));
    let mut info = BlockInfo::default();
    info.set_shard(shard());
    info.set_seq_no(seqno).unwrap();
    info.set_gen_utime(NOW.into());
    let prev = ExtBlkRef {
        end_lt: 0, seq_no: prev.seq_no(), root_hash: prev.root_hash().clone(), file_hash: prev.file_hash().clone(),
    };
    info.set_prev_stuff(false, &BlkPrevInfo::Block { prev }).unwrap();
    let extra = one_transaction(transaction_boc);
    let block = Block::with_params(42, info, ValueFlow::default(), MerkleUpdate::default(), extra).unwrap();
    BlockStuff::fake_with_block(id, block)
}

#[async_trait::async_trait]
impl EngineOperations for FakeChain {
    fn check_stop(&self) -> bool { self.stopping.load(Ordering::Relaxed) }
    fn acquire_stop(&self, _mask: u32) {}
    fn release_stop(&self, _mask: u32) {}
    fn now(&self) -> u32 { NOW }

    fn load_block_handle(&self, id: &BlockIdExt) -> Result<Option<Arc<BlockHandle>>> {
        Ok(self.handle(id))
    }
    fn load_block_next1(&self, id: &BlockIdExt) -> Result<BlockIdExt> {
        self.mc_handle(id.seq_no() + 1).map(|handle| handle.id().clone()).ok_or_else(|| error!("no next block"))
    }
    fn load_last_applied_mc_block_id(&self) -> Result<Option<Arc<BlockIdExt>>> {
        Ok(self.mc_handle(self.last_applied.load(Ordering::Relaxed)).map(|handle| Arc::new(handle.id().clone())))
    }
    async fn find_mc_block_by_seq_no(&self, seqno: u32) -> Result<Arc<BlockHandle>> {
        self.mc_handle(seqno).ok_or_else(|| error!("Cannot load handle for master block {}", seqno))
    }
    async fn load_block(&self, handle: &BlockHandle) -> Result<BlockStuff> {
        if self.collected.lock().unwrap().contains(handle.id()) {
            fail!("{} is archived, but archive was not found", handle.id());
        }
        Ok(self.blocks.lock().unwrap().get(handle.id()).ok_or_else(|| error!("no block {}", handle.id()))?.1.clone())
    }
    async fn wait_applied_block(&self, id: &BlockIdExt, _timeout_ms: Option<u64>) -> Result<Arc<BlockHandle>> {
        self.handle(id).ok_or_else(|| error!("{} is not applied", id))
    }
    /// The next applied block - loading it fails at once when its data is gone, as in the
    /// node. With no next block the node "stops": the indexer has done all there was to do.
    async fn wait_next_applied_mc_block(
        &self, prev: &BlockHandle, _timeout_ms: Option<u64>
    ) -> Result<(Arc<BlockHandle>, BlockStuff)> {
        let Some(handle) = self.mc_handle(prev.id().seq_no() + 1) else {
            self.stopping.store(true, Ordering::Relaxed);
            fail!("no next block yet");
        };
        let block = self.load_block(&handle).await?;
        Ok((handle, block))
    }
}

/// The wallet's transactions in the index, newest first, as hashes.
fn indexed_wallet(indexer: &Indexer) -> Vec<UInt256> {
    let bocs = indexer.index.list(0, &account(WALLET), None, 100).unwrap();
    bocs.iter().map(|boc| read_single_root_boc(boc).unwrap().repr_hash()).collect()
}

/// A node with shard blocks: masterchain blocks 40..=43 refer to the shard blocks 10, 12,
/// 12 and 13 of workchain 0 as their top ones; the shard blocks 11, 12 and 13 hold the
/// wallet's transactions 2, 1 and 0, the masterchain blocks the elector's.
fn chain_with_shard_blocks() -> Arc<FakeChain> {
    let chain = FakeChain::new();
    let (elector, wallet) = (transactions("elector"), transactions("wallet"));
    chain.apply(wc0_block(10, &wallet[3]));
    for (seqno, boc) in [(11, &wallet[2]), (12, &wallet[1]), (13, &wallet[0])] {
        chain.apply(wc0_block(seqno, boc));
    }
    for (seqno, top) in [(40, 10), (41, 12), (42, 12), (43, 13)] {
        chain.apply(mc_block(seqno, &elector[(43 - seqno) as usize], Some(&shard_block(&shard(), top))));
    }
    chain
}

#[tokio::test]
async fn test_indexer_takes_the_shard_blocks_a_masterchain_block_adds() {
    let node = chain_with_shard_blocks();
    let (indexer, dir) = node.indexer("shards", None);
    indexer.index.begin(&mc_id(40)).unwrap();

    indexer.run().await.unwrap();
    assert_eq!(indexer.index.last_mc_block().unwrap(), Some(mc_id(43)));
    assert_eq!(indexer.index.gaps().unwrap(), vec![]);
    // the wallet's transactions of the shard blocks 11, 12 (both added by masterchain block
    // 41) and 13 - not the one of block 10, which masterchain block 40 had added before
    let wallet = transactions("wallet");
    assert_eq!(indexed_wallet(&indexer), hashes(&wallet[..3]));
    assert_eq!(indexed(&indexer), hashes(&transactions("elector")[..3]));
    drop(indexer);
    std::fs::remove_dir_all(dir).ok();

    // the shard block 11 was collected: masterchain block 41 is indexed as far as its shard
    // blocks can be walked (12), and goes on record as a gap in the same write
    let node = chain_with_shard_blocks();
    node.collected.lock().unwrap().insert(shard_block(&shard(), 11));
    let (indexer, dir) = node.indexer("shards-gc", None);
    indexer.index.begin(&mc_id(40)).unwrap();
    indexer.run().await.unwrap();
    assert_eq!(indexer.index.last_mc_block().unwrap(), Some(mc_id(43)));
    assert_eq!(indexer.index.gaps().unwrap(), vec![(41, 41)]);
    assert_eq!(indexed_wallet(&indexer), hashes(&wallet[..2]));
    assert_eq!(indexed(&indexer), hashes(&transactions("elector")[..3]));
    drop(indexer);
    std::fs::remove_dir_all(dir).ok();
}

/// The elector's transactions in the index, newest first, as hashes.
fn indexed(indexer: &Indexer) -> Vec<UInt256> {
    let bocs = indexer.index.list(-1, &account(ELECTOR), None, 100).unwrap();
    bocs.iter().map(|boc| read_single_root_boc(boc).unwrap().repr_hash()).collect()
}

#[tokio::test]
async fn test_indexer_follows_the_applied_blocks() {
    let node = FakeChain::with_blocks(40..=44);
    let (indexer, dir) = node.indexer("follows", None);
    // a new index starts after the last applied block; a block before that, when asked for
    indexer.begin_if_empty().await.unwrap();
    assert_eq!(indexer.index.start_mc_seqno().unwrap(), Some(45));
    indexer.index.begin(&mc_id(40)).unwrap();

    indexer.run().await.unwrap();
    assert_eq!(indexer.index.last_mc_block().unwrap(), Some(mc_id(44)));
    // blocks 41..=44 hold the elector's four newest transactions; block 40 is where it began
    assert_eq!(indexed(&indexer), hashes(&transactions("elector")[..4]));
    assert_eq!(indexer.index.gaps().unwrap(), vec![]);
    drop(indexer);
    std::fs::remove_dir_all(dir).ok();
}

#[tokio::test]
async fn test_indexer_keeps_every_account_when_the_other_ones_are_indexed() {
    // only the elector is listed: the wallet's transactions of the shard blocks are kept as
    // an other account's, for 30 days
    let node = chain_with_shard_blocks();
    let (indexer, dir) = node.indexer_of("others", None, &[ELECTOR], 30);
    indexer.index.begin(&mc_id(40)).unwrap();
    indexer.run().await.unwrap();
    assert_eq!(indexer.index.last_mc_block().unwrap(), Some(mc_id(43)));
    assert_eq!(indexed_wallet(&indexer), hashes(&transactions("wallet")[..3]));
    assert_eq!(indexed(&indexer), hashes(&transactions("elector")[..3]));
    let status = indexer.index.status().unwrap();
    assert_eq!(status["accounts"], 1, "{}", status);
    assert_eq!(status["otherTransactions"], 3, "{}", status);
    assert_eq!(status["otherAccountsDays"], 30);
    drop(indexer);
    std::fs::remove_dir_all(dir).ok();

    // nobody is listed: every account is an other one - the elector too
    let node = chain_with_shard_blocks();
    let (indexer, dir) = node.indexer_of("nobody", None, &[], 30);
    indexer.index.begin(&mc_id(40)).unwrap();
    indexer.run().await.unwrap();
    assert_eq!(indexed_wallet(&indexer).len(), 3);
    assert_eq!(indexed(&indexer).len(), 3);
    let status = indexer.index.status().unwrap();
    assert_eq!((status["accounts"].as_u64(), status["otherTransactions"].as_u64()), (Some(0), Some(6)), "{}", status);
    drop(indexer);
    std::fs::remove_dir_all(dir).ok();

    // ... and so it is without an accounts file at all
    let node = chain_with_shard_blocks();
    let (mut indexer, dir) = node.indexer_of("no-file", None, &[], 30);
    indexer.config.accounts_file = None;
    indexer.index.begin(&mc_id(40)).unwrap();
    indexer.run().await.unwrap();
    assert_eq!(indexer.index.last_mc_block().unwrap(), Some(mc_id(43)));
    assert_eq!((indexed_wallet(&indexer).len(), indexed(&indexer).len()), (3, 3));
    assert_eq!(indexer.index.status().unwrap()["accounts"], 0);
    drop(indexer);
    std::fs::remove_dir_all(dir).ok();

    // the other accounts are not indexed: the listed elector only, as before
    let node = chain_with_shard_blocks();
    let (indexer, dir) = node.indexer_of("listed-only", None, &[ELECTOR], 0);
    indexer.index.begin(&mc_id(40)).unwrap();
    indexer.run().await.unwrap();
    assert!(indexed_wallet(&indexer).is_empty());
    assert_eq!(indexed(&indexer), hashes(&transactions("elector")[..3]));
    assert_eq!(indexer.index.status().unwrap()["otherTransactions"], 0);
    // ... and nothing of the other accounts was stored on the way, to be swept at once: no
    // sweep ever had a row to take
    assert_eq!(*indexer.index.sweep_from.lock().unwrap(), vec![OTHER_PREFIX]);
    drop(indexer);
    std::fs::remove_dir_all(dir).ok();
}

/// How many rows of the config contract the indexer's index has.
fn indexed_config(indexer: &Indexer) -> Vec<UInt256> {
    let bocs = indexer.index.list(-1, &account(CONFIG), None, 100).unwrap();
    bocs.iter().map(|boc| read_single_root_boc(boc).unwrap().repr_hash()).collect()
}

#[tokio::test]
async fn test_indexer_sweeps_by_the_time_of_the_chain() {
    let (elector, config) = (transactions("elector"), transactions("config"));
    // when the config contract's transactions of the fixtures were made
    let (oldest, newest) = (transaction(&config[2]).now(), transaction(&config[1]).now());
    assert!(oldest <= newest);
    let node = FakeChain::new();
    node.apply(mc_block_at(40, &elector[3], None, oldest));
    node.apply(mc_block_at(41, &config[2], None, oldest));
    // this block is generated exactly a day after the older one: not older than a day yet
    node.apply(mc_block_at(42, &config[1], None, oldest + DAY));
    // the elector is listed, every other account is kept for a day
    let (indexer, dir) = node.indexer_of("chain-time", None, &[ELECTOR], 1);
    indexer.index.begin(&mc_id(40)).unwrap();
    indexer.run().await.unwrap();
    assert_eq!(indexed_config(&indexer), hashes(&config[1..3]));

    // a block generated more than a day after both: they are swept as it is stored - by
    // the block's time, the node's clock says an earlier day. The listed elector's row of
    // the same age stays
    assert!(node.now() < oldest);
    node.apply(mc_block_at(43, &elector[2], None, newest + DAY + 1));
    node.stopping.store(false, Ordering::Relaxed);
    indexer.run().await.unwrap();
    assert_eq!(indexer.index.last_mc_block().unwrap(), Some(mc_id(43)));
    assert!(indexed_config(&indexer).is_empty());
    assert_eq!(indexed(&indexer), hashes(&[elector[2].clone()]));
    assert_eq!(indexer.index.status().unwrap()["otherTransactions"], 0);
    drop(indexer);
    std::fs::remove_dir_all(dir).ok();
}

#[tokio::test]
async fn test_indexer_keeps_what_it_has_of_an_account_put_on_the_list() {
    let (elector, config) = (transactions("elector"), transactions("config"));
    let made = transaction(&config[1]).now();
    let node = FakeChain::new();
    node.apply(mc_block_at(40, &elector[3], None, made));
    node.apply(mc_block_at(41, &config[1], None, made));
    let (indexer, dir) = node.indexer_of("listed-later", None, &[ELECTOR], 1);
    indexer.index.begin(&mc_id(40)).unwrap();
    indexer.run().await.unwrap();
    assert_eq!(indexed_config(&indexer), hashes(&config[1..2]));
    assert_eq!(indexer.index.status().unwrap()["otherTransactions"], 1);

    // the config contract is put on the list while the index still has its row: when the
    // row gets old it is not swept, and the next one is a listed account's from the start
    std::fs::write(dir.join("accounts.txt"), format!("{}\n{}\n", ELECTOR, CONFIG)).unwrap();
    node.apply(mc_block_at(42, &config[0], None, made + 2 * DAY));
    node.stopping.store(false, Ordering::Relaxed);
    indexer.run().await.unwrap();
    assert_eq!(indexed_config(&indexer), hashes(&config[..2]));
    let status = indexer.index.status().unwrap();
    assert_eq!((status["accounts"].as_u64(), status["otherTransactions"].as_u64()), (Some(2), Some(0)), "{}", status);
    // ... and off the list again it keeps them: they are a listed account's rows
    std::fs::write(dir.join("accounts.txt"), format!("{}\n", ELECTOR)).unwrap();
    node.apply(mc_block_at(43, &elector[2], None, made + 10 * DAY));
    node.stopping.store(false, Ordering::Relaxed);
    indexer.run().await.unwrap();
    assert_eq!(indexer.index.last_mc_block().unwrap(), Some(mc_id(43)));
    assert_eq!(indexed_config(&indexer), hashes(&config[..2]));
    drop(indexer);
    std::fs::remove_dir_all(dir).ok();
}

#[tokio::test]
async fn test_indexer_starts_where_it_is_told_if_the_node_still_has_the_block() {
    let node = FakeChain::with_blocks(50..=53);
    for (asked, starts_at) in [
        (None, 54),      // not told: after the last applied block
        (Some(52), 52),  // the block before 52 is stored
        (Some(51), 51),
        (Some(45), 51),  // not stored: right after the oldest block the node has
        (Some(0), 54), (Some(99), 54), // nonsense is ignored
    ] {
        let (indexer, dir) = node.indexer("starts", asked);
        indexer.begin_if_empty().await.unwrap();
        assert_eq!(indexer.index.start_mc_seqno().unwrap(), Some(starts_at), "{:?}", asked);
        drop(indexer);
        std::fs::remove_dir_all(dir).ok();
    }
    // blocks the archive GC collected are not stored, whatever their handles say
    node.collect(50..=51);
    let (indexer, dir) = node.indexer("starts-gc", Some(45));
    indexer.begin_if_empty().await.unwrap();
    assert_eq!(indexer.index.start_mc_seqno().unwrap(), Some(53));
    drop(indexer);
    std::fs::remove_dir_all(dir).ok();
}

#[tokio::test]
async fn test_indexer_records_a_gap_for_blocks_the_archive_gc_collected() {
    // the indexer is at block 40 and far behind; meanwhile the GC collected 43 and 44
    let node = FakeChain::with_blocks(40..=46);
    node.collect(43..=44);
    let (indexer, dir) = node.indexer("gc", None);
    indexer.index.begin(&mc_id(40)).unwrap();

    indexer.run().await.unwrap();
    assert_eq!(indexer.index.last_mc_block().unwrap(), Some(mc_id(46)));
    // 43 and 44 are gone, and the shard part of 45 cannot be walked without 44
    assert_eq!(indexer.index.gaps().unwrap(), vec![(43, 45)]);
    // the transactions of 46, 45 (its own), 42 and 41 - not those of 44 and 43
    let elector = transactions("elector");
    assert_eq!(indexed(&indexer), hashes(&[elector[0].clone(), elector[1].clone(), elector[4].clone(), elector[5].clone()]));
    drop(indexer);
    std::fs::remove_dir_all(dir).ok();
}

#[tokio::test]
async fn test_indexer_continues_after_a_gap_when_the_node_was_resynced() {
    // the index, kept outside the node's database, was committed at block 40; the database
    // was built anew from block 50: block 40 is not in it and never will be
    let node = FakeChain::with_blocks(50..=53);
    let (indexer, dir) = node.indexer("resync", None);
    indexer.index.begin(&mc_id(40)).unwrap();

    indexer.run().await.unwrap();
    assert_eq!(indexer.index.last_mc_block().unwrap(), Some(mc_id(53)));
    assert_eq!(indexer.index.gaps().unwrap(), vec![(41, 50)]);
    assert_eq!(indexed(&indexer), hashes(&transactions("elector")[..4]), "the transactions of 50..=53");
    drop(indexer);
    std::fs::remove_dir_all(dir).ok();

    // the same when the committed block is there but collected: the indexer was off for long
    let node = FakeChain::with_blocks(40..=53);
    node.collect(40..=49);
    let (indexer, dir) = node.indexer("long-off", None);
    indexer.index.begin(&mc_id(40)).unwrap();
    indexer.run().await.unwrap();
    assert_eq!(indexer.index.gaps().unwrap(), vec![(41, 50)]);
    assert_eq!(indexed(&indexer), hashes(&transactions("elector")[..4]));
    drop(indexer);
    std::fs::remove_dir_all(dir).ok();
}

#[tokio::test]
async fn test_indexer_waits_for_a_node_that_is_behind_its_index() {
    // the node restarted behind the index (its last applied block is 53, the index is at 60):
    // it will apply block 60 again - nothing is skipped and nothing goes on record
    let node = FakeChain::with_blocks(50..=53);
    let (indexer, dir) = node.indexer("behind", None);
    indexer.index.begin(&mc_id(60)).unwrap();
    let stop = {
        let node = node.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(300)).await;
            node.stopping.store(true, Ordering::Relaxed);
        })
    };
    indexer.run().await.unwrap();
    stop.await.unwrap();
    assert_eq!(indexer.index.last_mc_block().unwrap(), Some(mc_id(60)));
    assert_eq!(indexer.index.gaps().unwrap(), vec![]);
    assert!(indexed(&indexer).is_empty());
    drop(indexer);
    std::fs::remove_dir_all(dir).ok();
}

// ---- walking the chain ------------------------------------------------------------------

fn shard_block(shard: &ShardIdent, seqno: u32) -> BlockIdExt {
    let mut root = [0u8; 32];
    root[..8].copy_from_slice(&shard.shard_prefix_with_tag().to_be_bytes());
    root[8..12].copy_from_slice(&seqno.to_be_bytes());
    BlockIdExt::with_params(shard.clone(), seqno, UInt256::from(root), UInt256::from([1; 32]))
}

type Prevs = std::collections::HashMap<BlockIdExt, (BlockIdExt, Option<BlockIdExt>)>;

/// Walks with a table of previous ids; a block missing from the table is "gone".
async fn walk(top: BlockIdExt, prev_tops: &[BlockIdExt], prevs: &Prevs) -> Walk<BlockIdExt> {
    walk_shard_chain(top, prev_tops, |id| {
        let entry = prevs.get(&id).cloned();
        async move {
            Ok::<_, ever_block::Error>(match entry {
                Some(prev) => Loaded::Block((id, prev)),
                None => Loaded::Gone,
            })
        }
    }).await.unwrap()
}

fn done(walk: Walk<BlockIdExt>) -> (Vec<u32>, bool) {
    match walk {
        Walk::Done { chain, complete } => (chain.iter().map(|id| id.seq_no()).collect(), complete),
        Walk::Stopping => panic!("not stopping"),
    }
}

#[tokio::test]
async fn test_shard_chain_walks_back_to_the_previous_top_block() {
    let full = ShardIdent::with_tagged_prefix(0, 0x8000_0000_0000_0000).unwrap();
    let mut prevs = Prevs::new();
    for seqno in 6..=8 {
        prevs.insert(shard_block(&full, seqno), (shard_block(&full, seqno - 1), None));
    }
    // three new shard blocks since the previous masterchain block
    assert_eq!(done(walk(shard_block(&full, 8), &[shard_block(&full, 5)], &prevs).await), (vec![8, 7, 6], true));
    // none new
    assert_eq!(done(walk(shard_block(&full, 5), &[shard_block(&full, 5)], &prevs).await), (vec![], true));
    // an older one the node no longer has: what was walked is kept, the chain is incomplete
    prevs.remove(&shard_block(&full, 6));
    assert_eq!(done(walk(shard_block(&full, 8), &[shard_block(&full, 5)], &prevs).await), (vec![8, 7], false));
    // the zerostate block is never walked
    assert_eq!(done(walk(shard_block(&full, 0), &[shard_block(&full, 0)], &prevs).await), (vec![], true));
}

#[tokio::test]
async fn test_shard_chain_after_a_split_and_after_a_merge() {
    let full = ShardIdent::with_tagged_prefix(0, 0x8000_0000_0000_0000).unwrap();
    let (left, right) = full.split().unwrap();
    let mut prevs = Prevs::new();
    // the parent went on to 11 after the previous masterchain block's top (10), then split
    prevs.insert(shard_block(&full, 11), (shard_block(&full, 10), None));
    prevs.insert(shard_block(&left, 12), (shard_block(&full, 11), None));
    prevs.insert(shard_block(&left, 13), (shard_block(&left, 12), None));
    prevs.insert(shard_block(&right, 12), (shard_block(&full, 11), None));
    let prev_tops = [shard_block(&full, 10)];
    // each child walks through the parent's new block (stored twice, harmlessly)
    assert_eq!(done(walk(shard_block(&left, 13), &prev_tops, &prevs).await), (vec![13, 12, 11], true));
    assert_eq!(done(walk(shard_block(&right, 12), &prev_tops, &prevs).await), (vec![12, 11], true));

    // a merge: both parents were the previous tops, the walk ends at the merge block
    let merged = shard_block(&full, 20);
    prevs.insert(merged.clone(), (shard_block(&left, 19), Some(shard_block(&right, 19))));
    prevs.insert(shard_block(&full, 21), (merged, None));
    let prev_tops = [shard_block(&left, 19), shard_block(&right, 19)];
    assert_eq!(done(walk(shard_block(&full, 21), &prev_tops, &prevs).await), (vec![21, 20], true));
    // whichever parent the walk would stop at, the merge block ends it: without that it
    // would go on into the other parent's already stored chain
    prevs.insert(shard_block(&left, 19), (shard_block(&left, 18), None));
    let prev_tops = [shard_block(&right, 19), shard_block(&left, 19)];
    assert_eq!(done(walk(shard_block(&full, 21), &prev_tops, &prevs).await), (vec![21, 20], true));
}

#[tokio::test]
async fn test_shard_chain_stops_with_the_node() {
    let full = ShardIdent::with_tagged_prefix(0, 0x8000_0000_0000_0000).unwrap();
    let walked = walk_shard_chain(shard_block(&full, 8), &[shard_block(&full, 5)], |_id: BlockIdExt| async {
        Ok::<_, ever_block::Error>(Loaded::<(BlockIdExt, (BlockIdExt, Option<BlockIdExt>))>::Stopping)
    }).await.unwrap();
    assert!(matches!(walked, Walk::Stopping));
}
