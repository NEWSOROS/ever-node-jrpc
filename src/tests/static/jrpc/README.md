# JRPC test fixtures

Public chain data of Everscale mainnet (global id 42), read from a mainnet node on
2026-10-02. Used by `src/tests/test_jrpc.rs` and `src/tests/test_jrpc_history.rs`.

## The accounts

| account | what it is |
|---|---|
| `-1:3333…3333` | the elector, a system contract of the masterchain. In every masterchain block it has a tick-tock transaction and a transaction that takes the block's fees (the block's `recover_create_msg`, an internal message from `-1:0000…0000`). |
| `-1:5555…5555` | the config contract: one tick-tock transaction per masterchain block. The tests leave it out of the index. |
| `0:a25af8eabe4bbe2bb0ef14eafad3393fa564a67e1222607777e6472c47de7677` | an ordinary wallet of workchain 0, picked from the blocks because its recent transactions were started both by external and by internal messages. |

## Recorded answers of the server

Answers of this JRPC server (`src/network/jrpc.rs`) running inside a mainnet node, saved
with their JSON-RPC envelope:

| file | request |
|---|---|
| `golden_state_wallet.json` | `getContractState` of the wallet |
| `golden_state_unchanged.json` | the same with `lastTransactionLt` = the lt of that answer |
| `golden_state_none.json` | `getContractState` of `0:2222…2222`, an address without an account |
| `golden_keyblock.json` | `getLatestKeyBlock` - masterchain key block 62542934 |
| `golden_config.json` | `getBlockchainConfig` - the config of that key block |

`wallet.account.boc` is the wallet's `Account` cell read the other way: the node console's
`getaccountstate`, called between two `getContractState` answers that named the same last
transaction (lt 76409127000006).

## Transactions cut from blocks

`history/*.json` are JSON arrays of transaction BOCs (base64), newest first - the shape of
a `getTransactionsList` answer. The transactions were cut from the blocks in the node's
block archive:

| file | transactions | blocks |
|---|---|---|
| `history/elector.json` | 100 consecutive transactions of the elector, lt 76411480000001..76411530000002 | masterchain 62558384..62558433 |
| `history/config.json` | the config contract's 3 newest in the same blocks | masterchain 62558431..62558433 |
| `history/wallet.json` | the wallet's 5 newest, lt 76409065000006..76409127000006 | shard `0:8000000000000000` 72491607..72491668 |

## What ties them together

The tests do not take the files on trust:

* every history file is one account's unbroken chain - each transaction names the next one
  in the file as its previous transaction, by lt and by hash;
* the wallet's newest transaction has the hash and the lt that the recorded
  `getContractState` answer gives as `lastTransactionId`, and its state update ends in the
  hash of `wallet.account.boc`: the block, the console and the server describe one state;
* the recorded config answer and the recorded key block are of the same key block;
* in the recorded key block, the message the elector consumed is found under its hash in
  the block's own list of imported messages (`InMsgDescr`) and is the block's
  `recover_create_msg`.

Checked with the blocks at hand when the fixtures were cut, and not repeated by the tests
(those blocks are not in the repository): the block numbers in the table above, and that
the inbound message hashes named in the history tests are keys of their blocks'
`InMsgDescr`.

## Compatibility with everscale-jrpc

The shapes of the answers and the error behaviour were compared with the answers of
broxus/everscale-jrpc's public endpoint (jrpc.everwallet.net) on 2026-09-24. That endpoint
was no longer reachable on 2026-10-02, which is why the fixtures are recordings of this
implementation and not of the reference one.
