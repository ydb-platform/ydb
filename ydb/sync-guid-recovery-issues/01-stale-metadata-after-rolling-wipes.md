# VDisk returns Good after rolling peer wipes but misses committed Block metadata

**Status:** reproduced on `mr006` in an eight-VDisk `Erasure4Plus2Block` group. This is a metadata correctness issue. **Umbrella:** [[EPIC] Make VDisk recovery safe across rolling wipes](%5BEPIC%5D.md).

## Reproduction provenance

- **Reproduced:** 2026-11-12, with captured test events around 00:29:21 UTC+3.
- **Tested Git SHA:** `c28196d089ffdd61c365f15abc76cee36af8cbde`; commit date: 2026-11-12T00:27:29+03:00.
- **Production baseline:** [`6e3629b18daf4a2441d2b129fa3b1ae84564610f`](https://github.com/ydb-platform/ydb/commit/6e3629b18daf4a2441d2b129fa3b1ae84564610f); commit date: 2026-11-11T17:52:44+03:00. The test revision adds regressions without production changes.
- **Evidence:** [captured final console log](evidence/mr006-c28196d089f.log) and [shared test details](%5BEPIC%5D.md#reproduced-revision-and-test-evidence). UTC log timestamps `2026-11-11T21:29:21–22Z` correspond to the reproduction date above.

## Description

A VDisk with preserved but outdated local state can return from an outage, receive `EDecision::Good`, report `Replicated`, and still lack a `Block` committed while it was unavailable. Sequentially wiping and recovering the other VDisks removes the original incremental synchronization history. The replacement peers retain the metadata in Hull, but `TEvVSyncResult(RESTART)` lets the returning VDisk resume after that history without reading it.

`Good` is consistent with the documented identity-integrity contract of `SyncGuidRecovery`: the local database was not erased. The defect is the missing freshness guarantee in the subsequent metadata synchronization protocol.

## Reproduction

The existing `TestRollingWipe(false)` helper implements this scenario:

1. Initialize eight VDisks using `Erasure4Plus2Block` and stop VDisk `A`, preserving its PDisk state.
2. Commit `TEvBlock(TabletId=1, Generation=42)` through the group proxy; require `TEvBlockResult(OK)`.
3. Wipe each of the other seven VDisks sequentially. For each replacement, wait for its new `TEvSyncGuidRecoveryDone(OK)` and `Replicated` status before starting the next wipe.
4. Directly query a recovered peer with `TEvVGetBlock`; require `OK` and generation 42. This confirms the block survived the rolling wipes.
5. Start `A`. Delay two peer `TEvVDiskGuidObtained` replies so its own persisted `Final` reply participates in the first six-response quorum. Release the delayed events once six replies arrive; do not modify reply contents.
6. Require `EDecision::Good`, allow another simulated minute, and wait for `A` to report `Replicated`.
7. Query `A` directly using `TEvVGetBlock`.

## Expected and actual behavior

Expected: after `A` returns to operation and reports `Replicated`, it returns `TEvVGetBlockResult(OK, Generation=42)`.

Actual: `A` returns `NODATA`. The recovered-peer control passed before `A` restarted, and the `Good` and replication assertions passed before the direct query failed.

Regression: `RecoverLostDataRestart::RollingWipeWithPreservedLocalState` in [recover_lost_data_restart.cpp](../core/blobstorage/ut_blobstorage/recover_lost_data_restart.cpp).

Final-run failure:

```text
recover_lost_data_restart.cpp:283
(result->Record.GetStatus() == NKikimrProto::OK) failed: (NODATA != OK)
```

## Source-derived causal path

1. [`TSyncerRecoverLostDataActor::CallOsiris`](https://github.com/ydb-platform/ydb/blob/6e3629b18daf4a2441d2b129fa3b1ae84564610f/ydb/core/blobstorage/vdisk/syncer/blobstorage_syncer_recoverlostdata.cpp#L209) establishes `DbBirthLsn` after importing recovery metadata. The boundary is intended to keep imported history from synchronizing back as new local changes.
2. Incoming [`TLoggedRecLocalSyncData::Replay`](https://github.com/ydb-platform/ydb/blob/6e3629b18daf4a2441d2b129fa3b1ae84564610f/ydb/core/blobstorage/vdisk/skeleton/skeleton_loggedrec.cpp#L258) updates Hull and forwards `TEvLocalSyncData` to SyncLog. That forwarding does not append the imported records to the ordinary outgoing log: [`TSyncLogKeeperState::ProcessLocalSyncData`](https://github.com/ydb-platform/ydb/blob/6e3629b18daf4a2441d2b129fa3b1ae84564610f/ydb/core/blobstorage/vdisk/synclog/blobstorage_synclogkeeper_state.cpp#L550) updates `PhantomFlagStorageState`.
3. Each wiped peer has a new `VDiskIncarnationGuid`. [`TSyncLogActor::Handle(TEvVSync)`](https://github.com/ydb-platform/ydb/blob/6e3629b18daf4a2441d2b129fa3b1ae84564610f/ydb/core/blobstorage/vdisk/synclog/blobstorage_synclog.cpp#L164) answers the old-incarnation request with `RESTART(newGuid, DbBirthLsn)`.
4. [`TSyncerJobTask::HandleRestart`](https://github.com/ydb-platform/ydb/blob/6e3629b18daf4a2441d2b129fa3b1ae84564610f/ydb/core/blobstorage/vdisk/syncer/syncer_job_task.cpp#L249) adopts the new cursor and continues `EJustSync`. Unlike `NODATA`, `RESTART` does not enter full recovery.
5. [`WhatsNext`](https://github.com/ydb-platform/ydb/blob/6e3629b18daf4a2441d2b129fa3b1ae84564610f/ydb/core/blobstorage/vdisk/synclog/blobstorage_synclogreader.cpp#L39) accepts `SyncedLsn=DbBirthLsn` at the new log base. An empty post-recovery log can therefore produce `ALREADY` without transferring the missing historical metadata.

[`TSyncer::StandardMode`](https://github.com/ydb-platform/ydb/blob/6e3629b18daf4a2441d2b129fa3b1ae84564610f/ydb/core/blobstorage/vdisk/syncer/blobstorage_syncer.cpp#L446) reports successful GUID recovery before starting ordinary synchronization. Readiness and `Replicated` do not establish that the Blocks database contains the missing history.

The regression observes `Good`, the additional simulated minute, `Replicated`, and the direct `NODATA` result. It does not explicitly capture per-peer `SyncDone`/`DisksSynced` or every `RESTART`/`ALREADY` response; the protocol chain above is established by reading the code rather than by a complete event trace in the captured console log.

## Impact and limits

The reproduced consequence is a missing committed block on a VDisk that is operational and reports replication complete. A group-proxy read can hide this because other VDisks retain generation 42.

The same history-coverage gap can affect `LogoBlob` and `Barrier` metadata. Blob balancing can incidentally repair a missing part, which is why this regression uses `TEvVGetBlock`. No separate `LogoBlob` or `Barrier` regression was executed, and this test does not establish successful stale writes at group level or irreversible acknowledged payload loss. A live VDisk returning from a network partition can encounter the same `RESTART` path without rerunning `SyncGuidRecovery`; that extension was not independently tested.

## Fix considerations

- A peer incarnation change must not advance a durable synchronization cursor past metadata the receiver never obtained. Full metadata synchronization on incarnation mismatch is a candidate; it adds scan and network cost for changed incarnations.
- A startup-only fix is insufficient for live actors returning from partitions.
- Preserving peer eternal GUID witnesses addresses [issue 02](02-guid-witness-loss-after-rolling-wipes.md), but does not fill this Hull metadata gap.
- VDisks whose cursors already advanced may need explicit full synchronization; changing future `RESTART` handling alone does not repair existing holes.

Related merged work includes [PR #57091](https://github.com/ydb-platform/ydb/pull/57091), which advances and persists the recovery base, and [PR #56555](https://github.com/ydb-platform/ydb/pull/56555), which protects `LostData` during GUID publication. Both are included in the failing baseline. Their precise relationship and other search results are recorded in [related GitHub work](04-related-github-work.md).

Open [issue #43217](https://github.com/ydb-platform/ydb/issues/43217) concerns sync-protocol failure with `SyncLogDataCutting`; its linked merged PR #43159 addresses premature `SyncState` persistence. The available issue content does not establish it as a duplicate of this rolling-wipe case. Open draft [PR #57055](https://github.com/ydb-platform/ydb/pull/57055) addresses GUID history and does not change `HandleRestart`'s cursor adoption.

Open [issue #58640](https://github.com/ydb-platform/ydb/issues/58640) is a closer symptom match: it describes committing an advanced cursor after interrupted full recovery, leaving part of a peer's index absent. The reproduction here exercises a different trigger, `RESTART` cursor adoption without that interrupted transfer. Open [PR #57945](https://github.com/ydb-platform/ydb/pull/57945) adds metadata synchronization before repeated mirror-3-dc Osiris, but retains cursor adoption in `HandleRestart`; it was not applied or tested against this case.
