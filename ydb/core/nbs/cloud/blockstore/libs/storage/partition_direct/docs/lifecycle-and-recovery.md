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

For a PB connection, `TICStorageTransportActor` retains the successful
`TEvConnect` response while it obtains a single-use token through
`TEvGetPersistentBufferRegistrationToken` and sends `TEvRegisterPersistentBuffer`
with that token and the connection's tablet/generation/DBG identity. Registration
`BUSY` or `OVERLOADED` responses retry after 100 ms within the same connection
attempt, reusing the token without extending its lifetime. Token acquisition
errors complete the connection attempt with an error. An expired or consumed
token produces `OUTDATED`; the caller must start a new connection attempt to
obtain a fresh token. After registration succeeds or is rejected as a duplicate,
the transport probes with `TEvListPersistentBuffer`. Only a successful probe
completes the connection promise successfully. This prevents an existing
but retiring registration from being published as a usable PB connection.
The later recovery listing still supplies the records to the dirty map.

## Restoring PB records

`DoListPBuffers` runs [TRestoreRequestExecutor](../restore_request.cpp),
aggregates PB metadata by vChunk and makes it available through
`RestoreDBGPBuffers`. `TVChunk::UpdateDirtyMap` inserts each recovered record's
original `(Generation, Lsn)`, range and host into the dirty map. Reads and
writes wait for the vChunk's dirty-map readiness future.

The dirty map merges replicas of the same key. `RestorePBuffer` applies one
recovered copy and marks that host confirmed. While fewer than three hosts
are confirmed the record is `PBufferIncompleteWrite`. The call that confirms
the third host moves it to `PBufferWritten`. When every listed host has
answered, `FinishPBufferRestore` discards a record that is still below three
copies: the copies found are erased by address, and the restore barrier
finishes the record.

A host whose list returns an error makes the aggregated response partial.
`TRestoreRequestExecutor` returns the copies already collected and sets
`response.Error`. `TVChunk::UpdateDirtyMap` applies those copies and does
not call `FinishPBufferRestore`. A host that did not answer may still hold
a copy, so a write that reached quorum must not be discarded. Any record
still below three copies stays in `PBufferIncompleteWrite`.

The PB recovers its own on-disk records before it answers the list. That
recovery is described on the shared
[PersistentBuffer](../../../../../../../../docs/en/core/contributor/distributed-storage/persistent-buffer.md)
page. The partition consumes the list that recovery publishes.

Two implementation boundaries need care when extending recovery:

- `TRestoreRequestExecutor::Run` currently enumerates the five initial host
  positions. Do not assume that this scans all positions appended later.
- `TInflightInfo` registers an incomplete record in `ReadyToClone`, but the
  vChunk maintenance path does not consume that queue. The state is not an
  automatic re-replication worker. A read overlapping a record that is still
  `PBufferIncompleteWrite` waits on its quorum-ready future.

Tests of complete-quorum restart do not establish behavior for either case.

## Flush and erase gates

The per-record progression is:

```text
PBufferPendingWrite -- OnWriteWithoutQuorum --> PBufferDiscarded -----+
                                                     erase or barrier |
PBufferPendingWrite -- RestorePBuffer --> PBufferIncompleteWrite      |
                       below quorum         |                         |
                                            | RestorePBuffer          |
                                            | confirms the third copy |
                                            v                         |
PBufferPendingWrite -- OnWritten --> PBufferWritten                   |
                                            |                         |
                                            | RequestFlush            |
                                            v                         |
                                     PBufferFlushing                  |
                                            |                         |
                                            | MaybeAdvanceToFlushed   |
                                            v                         |
                                     PBufferFlushed                   |
                                            |                         |
                                            | erase or barrier        |
                                            v                         |
                                      PBufferErased <-----------------+
```

The vertical `RestorePBuffer` arrow is one call per recovered host. The
call that confirms the third copy enters `PBufferWritten`. After a complete
list, `FinishPBufferRestore` moves a record still in
`PBufferIncompleteWrite` to `PBufferDiscarded`.

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
7. PB locks postpone erase. Erase also waits for durable behind state
   when the record overlaps a tracked outdated range.

Write completion starts flush work. Flush completion starts erase and state
persistence. The cleanup timer retries small tails when no writes or flushes
are in flight. Its forced pass still honors read locks, host availability and
the persist-before-erase condition.

Explicit erase batches go to PBs that confirmed the write, including
handoffs. Each PB is a separate request, and a failed erase is retried.
The partition sends the erase only after that host's write reply.

One interconnect session and one channel deliver one sender's events into
the recipient mailbox in send order. A replacement session is a new
boundary: an event on the new session and an event still in flight on the
old one can be handled in either order. An indirect write is forwarded by
a coordinator on another node, so it shares no session with this erase.

On the PBuffer, batch erase looks only at `Records`. A missing key is
answered `OK` with an empty trace. That status does not mean the copy is
gone; another status would mean the same. The key enters `Records` in
`FinishPersistentBufferWrite`, after disk IO. The write handler has
already returned by then, so an erase processed in between does not see
the key. An erase `OK` before the write reply only means the key was not
in `Records` yet. A write `OK` means the key was in `Records` when the
reply was sent.

A host that never confirms is not erased by address. Its unanswered write
does not hold the restore barrier, so the record can be covered while that
write is still in flight. A belated success while the record is still in
the dirty map confirms the host, and the erase is sent then. A success
after the barrier is committed finds the record already gone and is
ignored.

Two rules decide whether a copy is still valid:

- Address erase deletes a confirmed copy on that PB.
- The restore barrier forgets a dirty-map record whose remaining copies
  are on disabled hosts or on hosts that never confirmed the write. On
  restart `TBlocksDirtyMap::RestorePBuffer` skips every key at or below
  the persisted barrier, so those copies are not restored. A key above
  the barrier with fewer than three copies is discarded: the copies
  found are erased by address, and this barrier finishes the record.

Bytes left on the PBuffer after that are garbage. The PB cleanup barrier
removes every key below the oldest record still in the dirty map,
including a copy written after the restore barrier was committed. This
pass only frees space. See
[Barrier cleanup and deletion](#barrier-cleanup-and-deletion).

The restore barrier moves as follows:

1. The dirty map takes the largest key among such records that no read holds
   as the barrier target. The target stays below every record that is not
   flushed yet, because recovery drops everything up to the barrier.
2. The target is saved in the local database together with the dirty map
   state.
3. Once that state is committed, the records at or below the barrier count as
   erased and leave the dirty map.

For the disk-level meaning of exact erases, compact erase records and
barriers, use the shared PB page.

## Persisted DDisk state and repair

[TDDiskState](../dirty_map/ddisk_state.cpp) keeps the ranges that do not have
up-to-date data in its Behind field. Only the continuous prefix before the
first Behind range can be read. Successful flush and copy operations remove
their ranges from Behind, while a flush missed by a lagging DDisk adds its
range.

For a touched vChunk, adding a DDisk initializes its Behind field to the full
range; for an untouched vChunk, it starts empty. A configuration change and
the corresponding Behind state are committed atomically. Flush results
update the Behind field, incrementing the dirty-map state generation.
`DoPersistDirtyMap` sends standalone state updates through the same ordered
transaction queue in
[part_updatevchunkstate.cpp](../../partition_direct_tablet/part_updatevchunkstate.cpp).
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
update host roles; new/fresh destinations need repair before they
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
Cleanup sends `TEvUnregisterPersistentBuffer` for every tablet/DBG registration
in the persisted connections. Endpoints are deduplicated within each DBG,
so two DBGs sharing a PB still produce separate unregister requests. Each
successful response follows the PB's maximum-barrier write, twice the
registration timeout, and durable removal of the barrier. Cleanup treats
an absent registration (`INCORRECT_REQUEST`) as already removed and retries
`BUSY` after 100 ms within its existing 60-second timeout. It waits for all
PB registrations before deleting DDisk tablet chunks and requesting BSC
deallocation. This is an explicit resource lifecycle; stopping an ordinary
worker or completing a user write does not imply deletion of its DBG.

Useful integration cases include `ShouldRestorePartitionAfterRestart`,
`ShouldNotBarrierEraseUnerasedRecords`, `ShouldNotResendSameBarrierToPBuffer`,
`ShouldKeepOtherDBGConnectionsWhenAddingHosts`, and the `ShouldDeletePartition`
family in [partition_direct_ut.cpp](../../partition_direct_tablet/partition_direct_ut.cpp).
