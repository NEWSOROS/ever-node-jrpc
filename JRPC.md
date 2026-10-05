# JRPC server inside the node

This fork is [everx-labs/ever-node](https://github.com/everx-labs/ever-node) (master,
version 0.60.11) plus one feature: the node itself answers the JSON-RPC API of
[broxus/everscale-jrpc](https://github.com/broxus/everscale-jrpc) - the API that nekoton
based wallets and tools speak. No other source of the node is changed.

It is an independent fork, not affiliated with EverX or Broxus.

## What it is for

A wallet or a script needs four things from the network: the state of an account, the
blockchain config, a way to send a message, and the transactions that followed. A node
has all of that in its state and its blocks. With this server it answers such requests
itself - no GraphQL stack, no separate indexer, no third-party endpoint.

* The state methods are served from what the node keeps anyway: nothing is indexed and
  nothing is copied. everscale-jrpc calls this set its "simple" API.
* The transaction history is opt-in: a RocksDB index of the node's own. The accounts you
  list in a file are kept for good; every other account is kept for a number of days (30
  by default) - what a wallet needs to work with any address, not an archive of the chain.
* It is off unless the node config has a `jrpc_server` section, and it cannot keep the
  node from starting: a section the node cannot use, a taken port or an unreadable
  accounts file is logged, the server (or the history) stays off until the node is
  started again, and the node runs as usual.

## Quick start

A node that is already set up: add the section to its `config.json` and start the node
again.

```json
"jrpc_server": {
    "listen_address": "127.0.0.1:8081"
}
```

A new node: the example config of this repository, `configs/default_config.json`, has
this section, so the `config.json` the node generates from it at its first start has it
too. Remove the section to keep the server off.

Then ask the node:

```
curl -s -H 'Content-Type: application/json' \
     -d '{"jsonrpc":"2.0","id":1,"method":"getStatus","params":{}}' http://127.0.0.1:8081/rpc
```

It answers `{"ready": false}` while it synchronizes and `{"ready": true}` afterwards. The
node's log says `JRPC server listening on 127.0.0.1:8081` - or `JRPC server is off: ...`
with the reason.

A wallet also asks for transactions. For that add a `history` subsection - empty, it keeps
every account's transactions of the last 30 days, from the moment the node is started with
it:

```json
"jrpc_server": {
    "listen_address": "127.0.0.1:8081",
    "history": {}
}
```

The accounts whose history must never be dropped go into a file: see
[Configuration](#configuration).

## Methods

HTTP `POST` on `/` or `/rpc`, JSON-RPC 2.0, HTTP/1.1 and HTTP/2 without TLS (h2c with
prior knowledge - what nekoton-transport's client speaks).

| method | params | result |
|---|---|---|
| `getCapabilities` | - | the everscale-jrpc methods this node serves |
| `getStatus` | - | `{"ready": bool}` - true once the node has finished its initial synchronization |
| `getTimings` | - | `last_mc_block_seqno`, `last_shard_client_mc_block_seqno`, `last_mc_utime`, `mc_time_diff`, `shard_client_time_diff`, `smallest_known_lt` |
| `getLatestKeyBlock` | - | `{"block": <BOC>}` |
| `getBlockchainConfig` | - | `{"globalId", "config": <BOC>, "seqno"}` - the config of the latest key block |
| `getContractState` | `address`, optional `lastTransactionLt` | `{"type": "notExists" \| "exists" \| "unchanged", "timings": {"genLt", "genUtime"}}`; for `exists` also `"account": <BOC>` and `"lastTransactionId": {"isExact", "lt", "hash"}` |
| `getLibraryCell` | `hash` | `{"cell": <BOC> \| null}` |
| `sendMessage` | `message`: BOC of an external inbound message | `null` |

With a `history` section also:

| method | params | result |
|---|---|---|
| `getTransactionsList` | `account`, `limit`, optional `lastTransactionLt` | transaction BOCs, newest first, with lt <= `lastTransactionLt`; at most 100 |
| `getTransaction` | `id`: transaction hash | the transaction BOC or `null` |
| `getDstTransaction` | `messageHash` | the BOC of the transaction that consumed this message, or `null` |
| `getHistoryStatus` | - | `accounts`, `startMcSeqno`, `lastMcSeqno`, `gaps`, `transactions`, `smallestKnownLt`, `otherAccountsDays`, `otherTransactions`, `otherBytes` |

BOCs are base64. Hashes are hex. An lt is accepted as a decimal string or as a number.
One request per HTTP body: JSON-RPC batches are not taken (-32600). A JSON-RPC error
comes with HTTP 200, like a result.

```
curl -s -H 'Content-Type: application/json' \
     -d '{"jsonrpc":"2.0","id":1,"method":"getTimings","params":{}}' http://127.0.0.1:8081/rpc
```

Notes:

* `getStatus.ready` does not go back to false when a synchronized node falls behind
  later: `getTimings` shows how far behind it is (`mc_time_diff`, in seconds).
* `getContractState` reads the latest applied state: the masterchain state for a
  masterchain account; for any other, the shard state of the last masterchain block
  whose shard blocks the node has applied too.
  * `account` is what nekoton reads as AccountStuff: the `Account` cell without its
    leading constructor bit - not a BOC of the `Account` itself.
  * `unchanged` is answered when the account's last transaction lt is not above
    `lastTransactionLt`.
  * `timings.genLt` is the lt of the account's last transaction, `"0"` for `notExists`.
* `sendMessage` hands the message to the node exactly like the console's `sendmessage`.
  `null` means "taken for broadcast", not "executed": follow the account state or
  `getDstTransaction` for the outcome.
* `getTransactionsList` for an account the index has nothing of answers `[]`, not an
  error.
* `smallest_known_lt` is `null` without history; with it, the smallest lt in the index
  (`18446744073709551615` while the index is empty).
* `getHistoryStatus` is not a method of everscale-jrpc, and `getCapabilities` does not
  list it. `accounts` is the number of listed accounts. `transactions` is RocksDB's
  estimate of the number of rows, not a count. `otherAccountsDays` is how long the accounts
  that are not listed are kept (`null`: they are not indexed), `otherTransactions` and
  `otherBytes` what the index has of them now - a count, and the bytes of their BOCs.

### Errors

| code | message | when |
|---|---|---|
| -32700 | Parse error | the body is not JSON |
| -32600 | Invalid request | no `method` |
| -32601 | Method not found | |
| -32602 | Invalid params | |
| -32001 | Not ready | the node cannot answer yet, or the request took more than 15 s |
| -32002 | Not supported | a history method on a node without history |
| -32003 | Connection error | `sendMessage`: the node did not take the message (e.g. it is not synchronized) |
| -32004 | Storage error | the history index failed |
| -32005 | Failed to serialize | |
| -32006 | Invalid account state | |
| -32007 | Invalid message | `sendMessage`: not a BOC of an external inbound message |
| -32009 | Too many requests | no free slot within 10 s; this answer has `"id": null` - the request was refused before its body was looked at |

HTTP itself: another path - 404, not `POST` - 405, a body above 1 MiB - 413, a body that
does not arrive within 15 s - 408.

### Differences from everscale-jrpc

* No `getAccountsByCodeHash`: that needs an index of every account.
* History is what the index holds: the listed accounts from the block where the index was
  started, every other account for the last `other_accounts_days` (see below).
* `getHistoryStatus` is an addition.
* Only JSON-RPC over `POST`: no protobuf endpoint, `GET` and `OPTIONS` are refused and no
  CORS headers are sent, so a web page cannot call the server from a browser.
* Where the parameters are checked, the server follows what the public endpoint
  jrpc.everwallet.net did: `limit` is required and must be a number, `0` gives `[]`, a
  value above 100 is cut to 100.

## Configuration

A `jrpc_server` section in the node's `config.json` (see also [config.md](config.md)).
The example config `configs/default_config.json` has the shortest one - only
`listen_address`; this is the full one:

```json
"jrpc_server": {
    "listen_address": "127.0.0.1:8081",
    "max_concurrent_requests": 32,
    "history": {
        "accounts_file": "/var/ever-node/jrpc-history-accounts.txt",
        "other_accounts_days": 30,
        "other_accounts_max_mb": 8192,
        "db_path": "/var/ever-node/jrpc_history"
    }
}
```

* `listen_address` - `IP:port`. Required.
* `max_concurrent_requests` - default 32. Further requests wait up to 10 s for a slot.
* `history` - optional; every key in it is optional too:
  * `accounts_file` - the accounts whose history is kept for good: one `wc:hex` address
    per line, `#` starts a comment. The file is re-read when it changes (checked every
    10 s); an added account is kept from then on, with what the index still has of it.
    Without a file no account is kept for good.
  * `other_accounts_days` - default 30: for how many days the transactions of every other
    account are kept. `0`: only the listed accounts are indexed, and what the index has
    of other accounts is swept away.
  * `other_accounts_max_mb` - default 8192: the most megabytes of transactions kept for
    the other accounts (their BOCs; the database adds its own overhead on disk). Beyond
    that the oldest go first, whatever their age: this is what bounds the index when the
    chain gets busy. `0`: no limit.
  * `db_path` - the index directory; default `<node db>/jrpc_history`. Keeping it outside
    the node's database lets the history survive a resync of the node (the indexer then
    continues after a recorded gap).
  * `start_from_mc_seqno` - while the index is still empty: index from this masterchain
    block. That needs the block before it as well; when the node no longer stores it,
    history starts right after the oldest block the node has. `0` or a block the node
    has not applied yet is ignored. Default: from the last applied block on.
  * `catch_up_mc_blocks_per_sec` - default 10: masterchain blocks per second at most
    while the indexer is behind (a backfill, or after the node was down), so that block
    application keeps its share of the machine. Above 1000: no limit.
  * `start_delay_sec` - default 600: the indexer does nothing for this long after every
    start of the node, so that the node's own startup goes first. Where history starts
    is fixed at boot, so nothing is skipped: the indexer catches up afterwards. Until it
    has, the history methods lag behind `getContractState`.

What cannot start stays off until the node is started again: the server when its address
is not on an interface yet or the port is taken, the history when the accounts file cannot
be read or when it is told to index nothing (no file and `other_accounts_days` 0). A
`history` subsection that does not parse turns the whole server off. When only the server
cannot start, the history is still indexed - it is there after the next start.

### History, precisely

The indexer follows applied masterchain blocks. For each one it takes the block itself and
the shard blocks between it and the previous masterchain block, and stores their
transactions, the id of the processed masterchain block and - when something was missing -
the gap, in one write: a restart resumes exactly there. It expects the node to apply the
shard blocks of every workchain, as a node of the Everscale mainnet does.

A transaction of a listed account is kept for good. A transaction of any other account is
kept until it is `other_accounts_days` old, and while the transactions of those accounts
together are within `other_accounts_max_mb`: after each masterchain block the old ones are
swept, with their lookups by hash and by message. The age is counted by the chain - from
the time of the masterchain block being indexed, not from the node's clock.

* History of the other accounts starts where the index, or this setting, was started and
  fills up as the node runs: nothing is taken from older blocks.
* A wallet whose address is not listed sees its recent transactions and the one it has
  just sent, which is what it needs to work. What is older than the window is not there:
  `getTransactionsList` ends earlier, `getTransaction` and `getDstTransaction` answer
  `null`.
* An account put on the list while the index still has transactions of it keeps them for
  good from then on. Taken off the list, it keeps what was kept for good; its new
  transactions are an other account's again.
* Size: in October 2026 the accounts of Everscale mainnet made about 230 000 transactions
  a day together, about 100 MB of BOCs - two thirds of them the system contracts' - so 30
  days are about 3 GB of BOCs, and the database's overhead on top.
* The index keeps its layout. One made when only the listed accounts were indexed opens as
  it is, and an index with other accounts in it still opens with that version of the node
  (which then leaves them where they are).

A node keeps blocks only from its cold boot on, and its archive GC drops older ones. What
the indexer can no longer read becomes a gap on record (`getHistoryStatus.gaps`, ranges of
masterchain seqnos whose transactions are missing or incomplete) instead of a stuck
indexer:

* a block whose data the node never stored or whose archive was collected;
* everything between the index and a node database that was built anew from a later block
  (a resync with the index kept outside the database). A node that is merely behind its
  index - it restarted after a crash - is waited for instead.

Transactions in the index do not depend on the node's blocks: they stay after the node
drops them. The listed accounts' part is never pruned - its size is what those accounts
transact.

## Security

* **There is no authorization**, and every request is served by the node process itself.
* The server therefore refuses to listen on anything but a loopback or private address:
  127.0.0.0/8, 10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16, 100.64.0.0/10 (the shared
  address space of RFC 6598, which overlay VPNs use), ::1, fc00::/7. `0.0.0.0` and `::`
  are refused too.
* For access from elsewhere put a VPN or an authenticating reverse proxy with TLS in front.
  Do not expose a validator's RPC to the internet.
* Requests are bounded: 1 MiB per body, 15 s for a body to arrive, 10 s of waiting for a
  slot, 15 s per request, `max_concurrent_requests` at once. Connections are not: there is
  no limit on their number and no idle timeout, so the port must be reachable only by
  clients you trust.

## Building

Prerequisites as upstream (README.md):

```
apt-get install pkg-config make clang libssl-dev libzstd-dev libgoogle-perftools-dev
```

```
git clone --recurse-submodules https://github.com/NEWSOROS/ever-node-jrpc.git
cd ever-node-jrpc
cargo build --release --locked
```

Two things differ from upstream here, to pin what the build is made of:

* `Cargo.lock` is committed (upstream ignores it) - the dependency versions this branch
  was built and tested with;
* `rust-toolchain.toml` and `recomended_rust` name Rust 1.97.1 (upstream: 1.81.0, whose
  cargo cannot build these dependency versions) with the `rustfmt` component, which the
  code generator of `ton_api` calls. rustup installs both on the first `cargo` run. Other
  toolchain versions were not tried with this lock file.

## Tests

```
cargo test --locked --lib jrpc
```

runs the tests of the server, of the history index and of the config section. Their
fixtures are real mainnet data; where they come from and how they are tied together is in
[src/tests/static/jrpc/README.md](src/tests/static/jrpc/README.md).

The indexer runs in the tests against a fake node - a table of applied masterchain and
shard blocks, some of them marked as collected by the archive GC - which covers following
the chain, the shard blocks a masterchain block adds, the start point, gaps, where history
continues when blocks are missing, keeping every account and sweeping the old rows by the
time of the chain. What the tests do not reach is the code that
reads a real node: which state an answer is taken from (`EngineBackend`), and the node's
own block storage behind the indexer.

## Status

* The server and the history index have been running on Everscale mainnet since
  2026-09-24.
* That running time is of the listed accounts' history. Added or reworked for this
  publication, and so not covered by it: keeping every other account for a number of days
  with the sweep of old rows, how the indexer continues when the node no longer stores
  blocks (after a resync of the node, or blocks collected by the archive GC), the pace of
  catching up, and a gap going into the same write as the marker. These parts are tested
  against a fake node; they have not run on a live one yet. Keeping every account was also
  run offline over real blocks: 400 consecutive masterchain blocks of mainnet with their
  shard blocks, read from a node's block archive on 2026-10-05, gave 3280 transactions of
  426 accounts - the same as counted by a separate program - stored, found and swept.
* EVER Wallet (a nekoton wallet), with such a node added as a custom network of type JRPC,
  sent a transfer on mainnet on 2026-10-02 from an account the node kept the history of,
  saw it confirmed and showed it in its history.
* This branch - upstream plus only this feature - builds and passes the tests. Its
  release binary was also started as a fresh node cut off from the network, from the
  example config (with `ip_address` set: a node told `0.0.0.0` asks the internet for its
  external address and does not start without an answer). The server came up with the
  node on 127.0.0.1:8081 and answered the way a node that is not synchronized yet should -
  `getStatus`, the `Not ready` errors, h2c, the history methods `Not supported` and, once
  a `history` subsection was added, served from an empty index - and the node stopped
  cleanly. It has not been run as a synchronized mainnet node itself.
* Answers were compared with broxus/everscale-jrpc's public endpoint (jrpc.everwallet.net)
  on 2026-09-24: account state, config, key block, transaction lists, lookups and the
  error behaviour. That endpoint was no longer reachable on 2026-10-02, so the test
  fixtures are recordings of this implementation.

## The patch

Everything is in a handful of files:

| file | |
|---|---|
| `src/network/jrpc.rs` | the server |
| `src/network/jrpc_history.rs` | the history index and its indexer |
| `src/config.rs` | the `jrpc_server` section, kept as written and parsed on use |
| `src/engine.rs` | starting the server and the indexer; the indexer's stop flag |
| `src/network/mod.rs`, `Cargo.toml` | the modules; `hyper` server features, `rocksdb` |
| `configs/default_config.json` | the example config: the server on, at `127.0.0.1:8081` |
| `src/tests/test_jrpc.rs`, `src/tests/test_jrpc_history.rs`, `src/tests/static/jrpc/` | tests and their data |
| `JRPC.md`, `config.md`, `README.md`, `CHANGELOG.md` | documentation |
| `Cargo.lock`, `.gitignore`, `rust-toolchain.toml`, `recomended_rust` | the pinned build |

## License

As the upstream repository: see [LICENSE](LICENSE).
