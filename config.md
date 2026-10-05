Root section
------------

* `sync_by_archives`: possible values `true` and `false`. Default value `false`. If set `true` 
  allows to synchronize node by archives instead of single blocks. It may be useful in some 
  conditions, for example, long ping to other nodes.

`remp` section
------------

* `service_enabled`: possible values `true` and `false`. 
Enables participation in validator REMP protocols. Default value is `true`.

  The service allows the node to validate in REMP networks, but does not affect validation
  in non-REMP networks. So if the Network REMP capability is turned off now but may be activated 
  in the future, leave the default value.

  However, REMP protocols take some resources from the node even if the REMP capability is
  turned off. If the node is not expected to be a validator in REMP network,
  set this to `false`.

* `client_enabled`: possible values `true` and `false`. Default value `true`.

  Enables participation in client REMP protocols. With this option
  set to `false`, the node may not send external messages to 
  REMP network. As with `service_enabled` parameter, the client service is transparent
  for non-REMP networks, but may take extra hardware resources.

* `message_queue_max_len`: non-negative integer value.
  When specified, sets maximal number of external messages
  which can be handled by REMP simultaneously. Handling means all 
  message processing stages from its receiving by node till
  its expiration for replay protection purposes. The message count
  is performed for each shard separately.

  May be used to avoid node overloading by external messages. If the  
  queue becomes too long, all new messages are rejected, until some of the 
  messages from the queue become outdated (that is, their replay protection 
  period expires).

  If the value is not specified, no check of the message queue length is performed.
  
* `forcedly_disable_remp_cap`: possible values `true` and `false`. The parameter is
  available only in `remp_emergency` compilation configuration. Allows to locally 
  disable REMP capability even if the capability is enabled by the network. May be
  used for network recovery.

* `remp_client_pool`: integer value 0 to 255. Number of threads (as a percentage of CPU Cores number), 
  used for preliminary message processing in REMP client.
  Default value is 100% (the number of threads equals the number of CPU Cores). 
  At least one thread is started anyway.

  Before being sent to validators, any external REMP message is executed in test mode on a client
  (proper blockchain state is constructed, virtual machine is activated etc), and if the message
  processing results in error, it is rejected on the client and not sent to validators.

* `max_incoming_broadcast_delay_millis`: non-negative integer value. When external 
  messages are sent to validators via broadcast (legacy mechanism), they come to all nodes 
  in the network simultaneously, which may create a significant overload in 
  REMP Catchain. To overcome this, the messages coming to the validators may be 
  delayed for a random time, in a hope that only one copy of the message is
  processed and transferred to REMP Catchain. The random time distribution of the message
  copies gives enough time for the network to propagate message over it, so copies delayed
  for longer periods will be easily identified as duplicates (the validator will
  already have the same message received through Catchain from another validtor). 
  The parameter specifies maximal delay. 

* `smft_disabled`: manually disables participation of the node in SMFT protocol even if corresponding network config is set; false by default

`jrpc_server` section
------------

In-process JSON-RPC API (`src/network/jrpc.rs`): the "simple" API of broxus/everscale-jrpc
(getContractState, sendMessage, getLatestKeyBlock, getBlockchainConfig, getTimings,
getStatus, getCapabilities, getLibraryCell), served from the node's latest state over
HTTP POST on `/` or `/rpc`. Absent - off. The methods and the answers are in [JRPC.md](JRPC.md).
The example config `configs/default_config.json` has the section with `127.0.0.1:8081`.

* `listen_address`: `IP:port`, e.g. `"127.0.0.1:8081"`. Only a loopback or private address
  (127.0.0.0/8, 10/8, 172.16/12, 192.168/16, 100.64.0.0/10, ::1, fc00::/7) is accepted: the
  API has no authorization and runs in the node process.
* `max_concurrent_requests`: default `32`.
* `history`: transaction history (getTransactionsList, getTransaction, getDstTransaction,
  getHistoryStatus), indexed as blocks are applied (`src/network/jrpc_history.rs`): the
  listed accounts are kept for good, every other account for a number of days. Every key
  is optional - `"history": {}` keeps every account for 30 days:
  * `accounts_file`: the accounts kept for good, one `wc:hex` address per line, `#`
    comments; re-read when it changes. Without it no account is kept for good.
  * `other_accounts_days`: default `30` - days the transactions of every other account are
    kept. `0`: only the listed accounts are indexed.
  * `other_accounts_max_mb`: default `8192` - megabytes of transactions kept for the other
    accounts at most; beyond that the oldest go first. `0`: no limit.
  * `db_path`: the index directory; default `<internal db>/jrpc_history`. Better outside the
    node's database, so a resync of the node does not wipe the history: the indexer then
    continues after a recorded gap.
  * `start_from_mc_seqno`: while the index is empty, index from this masterchain block - a
    backfill of what the node still stores. It needs the block before that one too; when
    the node no longer has it, history starts right after the oldest block the node stores.
    `0` or a block the node has not applied yet: ignored. Default: from the last applied
    block on.
  * `catch_up_mc_blocks_per_sec`: default `10` - masterchain blocks per second at most
    while catching up (a backfill, or after the node was down). Above `1000`: no limit.
  * `start_delay_sec`: default `600` - the indexer does nothing for this long after every
    start of the node, then catches up. Where history starts is fixed at boot, so nothing
    is skipped.

  An unreadable accounts file or index, or nothing to index (no file and
  `other_accounts_days` 0), turns the history off with `JRPC history is off: ...` in the
  log; the node and the rest of the API run as usual.

  A section the node cannot use (a host name instead of an IP, a public address, a taken
  port, a `history` subsection that does not parse) turns the API off with the log line
  `JRPC server is off: ...`; the node starts and runs as usual, and the section is written
  back to the config unchanged. What is off stays off until the node is started again.
