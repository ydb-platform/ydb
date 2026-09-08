# NBS direct partition

This directory implements the NBS partition data path over DDisks and
PersistentBuffers (PBs). The neighboring
[partition tablet](../partition_direct_tablet/partition_direct_actor.h) owns
durable configuration and lifecycle; the fast path runs through NBS executors.

Start with the shared [DirectBlockGroup architecture](../../../../../../../docs/en/core/contributor/distributed-storage/direct-block-groups.md),
[DDisk contracts](../../../../../../../docs/en/core/contributor/distributed-storage/ddisk.md),
and [PersistentBuffer contracts](../../../../../../../docs/en/core/contributor/distributed-storage/persistent-buffer.md).
The pages here describe the policies that the NBS partition implements on top
of those contracts. The NBS-like test load actor has its own
[implementation notes](../../../../../../load_test/rfc/nbs_dbg_like/README.md);
it is a separate client and does not reproduce every partition policy.

## Read by task

- [Data path](docs/data-path.md): region/vChunk mapping, rotating host roles,
  write quorum and hedging, record identity, read routing and locks.
- [Lifecycle and recovery](docs/lifecycle-and-recovery.md): allocation and
  host growth, sessions, PB restoration, flush/erase gates, persisted dirty
  maps, repair, and deletion.

## Code map

| Component | Responsibility |
| --- | --- |
| [TFastPathService](fast_path_service.h) | `IStorage` entry point; regions, LSN generation, volume-wide cleanup and copy budget |
| [TRegion](region.cpp), [region geometry](model/region_geometry.cpp) | Stripe-to-vChunk address translation and vChunk-to-DBG mapping |
| [TVChunk](vchunk.cpp) | Per-vChunk requests, dirty map, flush/erase, host configuration and data copiers |
| [TVChunkConfig](model/vchunk_config.cpp), [THostRoles](model/host_roles.cpp) | Primary/handoff roles, availability, promotion, evacuation and watermarks |
| [TBlocksDirtyMap](dirty_map/dirty_map.cpp), [TInflightInfo](dirty_map/inflight_info.cpp) | Versioned PB records, read hints, locks and cleanup readiness |
| [TDDiskState](dirty_map/ddisk_state.cpp) | Readable watermark and ahead/behind ranges for recovering or lagging DDisks |
| [TWriteRequestExecutor](write_request.cpp) | Direct/indirect PB writes, quorum, timeout, hedging and late completions |
| [Read executors](read_request_executor.cpp) | One or multiple location hints, retry and read hedging |
| [Flush](flush_request.cpp), [erase](erase_request.cpp), [restore](restore_request.cpp) | Translate dirty-map work into DBG operations |
| [TDirectBlockGroup](direct_block_group_impl.cpp) | Connections, session locking, PB listing, protocol facade and operation statistics |
| [TOracle](model/oracle.h) | Host health, timing estimates, coordinator choice and request timing |
| [TDDiskDataCopier](ddisk_data_copier.cpp) | Copy missing/stale ranges using current read hints |
| [Storage transport](../storage_transport/) | Actor/interconnect transport and direct-session transport implementations |
| [Partition tablet](../partition_direct_tablet/) | BSC requests, local database transactions, startup and deletion |

## Tests

Choose the smallest relevant target. Build and test invocation rules are in
the repository instructions; these are target paths, not alternative build
commands.

- `ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/ut`: request
  executors, DBG connections, vChunks, fast path and copier tests. Start with
  [write_request_ut.cpp](write_request_ut.cpp),
  [read_request_ut.cpp](read_request_ut.cpp),
  [vchunk_ut.cpp](vchunk_ut.cpp), and
  [ddisk_data_copier_ut.cpp](ddisk_data_copier_ut.cpp).
- `ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/dirty_map/ut`:
  state transitions, overlapping ranges, persistence and lock invariants.
- `ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/ut`:
  host masks, role rotation, promotion, health and timing policy.
- `ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct_tablet/partition_ut`:
  [partition integration tests](../partition_direct_tablet/partition_direct_ut.cpp),
  including restart, barrier cleanup, add-host isolation and deletion errors.

When changing a shared wire contract, also examine the DDisk/PB implementation
and its tests. A mock DBG can validate partition policy without validating
the real wire or storage format.
