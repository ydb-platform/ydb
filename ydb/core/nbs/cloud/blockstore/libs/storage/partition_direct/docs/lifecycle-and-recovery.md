# NBS direct-partition lifecycle and recovery

This page describes partition ownership and recovery policy. The shared
[DirectBlockGroup](../../../../../../../../docs/en/core/contributor/distributed-storage/direct-block-groups.md),
[DDisk](../../../../../../../../docs/en/core/contributor/distributed-storage/ddisk.md),
and [PersistentBuffer](../../../../../../../../docs/en/core/contributor/distributed-storage/persistent-buffer.md)
pages own the allocation and wire contracts. [Data path](data-path.md)
describes normal writes, reads and record identity.

## Allocation and startup

The [partition tablet](../../partition_direct_tablet/partition_direct_actor.cpp)
persists volume configuration, DBG connections, vChunk configuration and
dirty-map state in its local database. Initial allocation sends
`TEvControllerAllocateDDiskBlockGroup` with the partition tablet ID, configured
DDisk/PB pool names and 32 legacy `Queries`. Query IDs are 0 through 31;
`TargetNumVChunks` is the volume's rounded-up 4 GiB region count. This is the
current NBS initial-allocation policy, not a universal DBG shape.

[part_storepartitionids.cpp](../../partition_direct_tablet/part_storepartitionids.cpp)
persists the returned connections before starting the fast path. Each DBG
gets an executor and a `TDirectBlockGroup`; regions create vChunks from
persisted configs, or use the initial rotating roles when none exist.
`TVChunk` loads persisted DDisk state before restoring PB records.

[TDirectBlockGroup](../direct_block_group_impl.cpp) establishes separate PB
and DDisk connections. DDisk connections also have a session-lock phase and
an incrementing session sequence number for reconnects. Initial readiness
requires the configured minimum locked DDisk sessions and a PB connection
quorum. The transport retains connect credentials and returned session
information; subsequent wire operations use the shared DDisk token contract.
Blocked-generation errors cause the partition to stop rather than serving
with stale ownership.

## Restoring PB records

`DoListPBuffers` runs [TRestoreRequestExecutor](../restore_request.cpp),
aggregates PB metadata by vChunk and makes it available through
`RestoreDBGPBuffers`. `TVChunk::UpdateDirtyMap` inserts each recovered record's
original `(Generation, Lsn)`, range and host into the dirty map. Reads and
writes wait for the vChunk's dirty-map readiness future.

The dirty map merges replicas of the same key. A recovered record without a
PB quorum starts in `PBufferIncompleteWrite`; it becomes written after a
quorum is known. Restored data resumes the normal flush and erase machinery.
PB's own restart replay is a separate layer described in the shared PB page.

Two implementation boundaries need care when extending recovery:

- `TRestoreRequestExecutor::Run` currently enumerates the five initial host
  positions. Do not assume that this scans all positions appended later.
- `TInflightInfo` registers incomplete records in `ReadyToClone`, but the
  current vChunk maintenance path does not consume that queue. The presence
  of the state is not evidence of an implemented automatic PB re-replication
  worker. Reads overlapping such a record wait on its quorum-ready future.

Tests of complete-quorum restart do not establish behavior for either case.

## Flush and erase gates

The per-record progression is:

```text
PendingWrite -- write quorum --> Written --> Flushing --> Flushed
IncompleteWrite -- restored quorum --^                     |
                                                          v
                                                       Erasing --> Erased
```

[TVChunk::DoFlush](../vchunk.cpp) obtains dirty-map hints and creates one
[TFlushRequestExecutor](../flush_request.cpp) per PB-source/DDisk-destination
batch. `SyncWithPBuffer` is the NBS facade method; its current wire event is
unified `TEvSync`. The destination DDisk pulls the identified PB records.

The gates in [TBlocksDirtyMap](../dirty_map/dirty_map.cpp) and
[TInflightInfo](../dirty_map/inflight_info.cpp) are:

1. A PB record must have write quorum before it is flushable.
2. Normal maintenance waits for `SyncRequestsBatchSize` ready records. Forced
   maintenance lowers the batch threshold to one; it does not bypass the
   correctness gates.
3. At least three desired, enabled DDisks must be available. Flush targets
   **all** desired enabled DDisks, which may be more than the initial three.
4. Overlapping DDisk reads or range synchronization postpone the flush.
5. The source is preferably the confirmed PB at the destination's host
   position; otherwise another confirmed PB is used. The same position is
   a logical pairing, not a physical locality guarantee.
6. A record becomes flushed only after every desired enabled destination
   confirms and at least three confirmations exist. A failure clears that
   destination's requested bit and requeues the record.
7. PB locks postpone erase. Erase also waits for durable ahead/behind state
   when the record overlaps a tracked outdated range.

Write completion starts flush work. Flush completion starts erase and state
persistence. The cleanup timer retries small tails when no writes or flushes
are in flight. Its forced pass still honors read locks, host availability and
the persist-before-erase condition.

Explicit erase batches target the PB positions where writes were requested,
including handoffs. They are separate requests to each PB. Failed erases are
eligible for retry. Disabled hosts can be accounted as erased locally, with
barrier cleanup responsible for their residual records. Belated successful
writes have a separate erase queue. For the disk-level meaning of exact
erases, compact erase records and barriers, use the shared PB page.

## Persisted DDisk state and repair

[TDDiskState](../dirty_map/ddisk_state.cpp) combines an operational watermark
with two range sets:

- Ahead ranges have newer data beyond the normal copied prefix.
- Behind ranges missed a flush and contain outdated data.

The configuration watermark initializes a fresh DDisk. Flush results update
the range sets, incrementing the dirty-map state generation. `DoPersistDirtyMap`
sends that state to
[part_updatedirtymapstate.cpp](../../partition_direct_tablet/part_updatedirtymapstate.cpp).
Only transaction completion advances the dirty map's persisted generation.
`CheckEraseAbility` records which generation must be durable before an
overlapping PB record can be erased. This preserves the information needed
to repair a lagging DDisk after restart.

[TDDiskDataCopier](../ddisk_data_copier.cpp) repairs missing or stale ranges
selected by `GetFreshRange`. It takes the volume copy budget, establishes a
range-sync barrier, and waits for conflicting flushes. It then obtains normal
dirty-map read hints and reads the current data, potentially combining PB and
DDisk sources, before `WriteBlocksToDDisk` writes the destination.

Copies are currently capped at 1 MiB; progress notifications occur every
8 MiB. Retryable errors use backoff; non-retryable errors complete the copier
with an error. Successful `EndRangeSync` updates readable/repair state.
`TVChunk` persists progress and configuration changes. This algorithm uses
partition memory and ordinary read/write operations; do not describe it as
one peer-to-peer DDisk wire sync.

## Host growth and recovery of allocation intent

Host health and vChunk configuration are separate: temporary unavailability
does not itself remove a host's DDisk role. Promotion, demotion and evacuation
update masks and watermarks; new/fresh destinations need repair before they
can serve their full range.

[part_add_host_to_dbg.cpp](../../partition_direct_tablet/part_add_host_to_dbg.cpp)
serializes add-host work with one tablet-wide in-flight slot and checks the
persisted connection count against `MaxHostCount`. It persists an add-host
intent before contacting BSC. The request uses `DefineDirectBlockGroup` with
the desired final DDisk/PB counts and chunks per DDisk. Replaying that target
state is idempotent.

The response is checked for the expected DBG ID, exactly one additional
DDisk/PB position and no duplicate returned endpoint. The updated connection
list and removal of the intent are persisted together before notifying the
fast path. `TDirectBlockGroup::OnAddHostResult` appends connections and starts
sessions for the new position. Other DBGs keep their existing connections.

[part_loadstate.cpp](../../partition_direct_tablet/part_loadstate.cpp) reloads
an outstanding intent after restart; the tablet replays it after the fast
path becomes ready. Keep initial allocation and growth separate when
updating this code: the former still uses legacy allocation queries and the
latter uses the desired-state operation.

## Barrier cleanup and deletion

`TFastPathService` periodically gathers the smallest live PB key across all
vChunks/DBGs. Pending writes participate so their records cannot be erased
before write completion. A vChunk still restoring contributes an LSN-zero
blocking bound. With a nonzero minimum, cleanup sends the LSN immediately
below it; repeated bounds are deduplicated per PB endpoint. The current code
skips a cleanup pass if it finds no minimum or a zero bound.

For partition deletion,
[delete_partition.cpp](../../partition_direct_tablet/delete_partition.cpp)
stops the fast path and starts
[TPartitionCleanupActor](../../partition_direct_tablet/partition_cleanup_actor.cpp).
Cleanup wipes PB records, deletes DDisk tablet chunks and then requests BSC
deallocation. This is an explicit resource lifecycle; stopping an ordinary
worker or completing a user write does not imply deletion of its DBG.

Useful integration cases include `ShouldRestorePartitionAfterRestart`,
`ShouldNotBarrierEraseUnerasedRecords`, `ShouldNotResendSameBarrierToPBuffer`,
`ShouldKeepOtherDBGConnectionsWhenAddingHosts`, and the `ShouldDeletePartition`
family in [partition_direct_ut.cpp](../../partition_direct_tablet/partition_direct_ut.cpp).
