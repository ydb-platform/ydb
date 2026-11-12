# SyncGuidRecovery rejects intact local state after rolling wipes erase peer GUID witnesses

**Status:** reproduced on `mr006` in an eight-VDisk `Erasure4Plus2Block` group. This is an availability failure whose outcome depends on GUID reply order. **Umbrella:** [[EPIC] Make VDisk recovery safe across rolling wipes](%5BEPIC%5D.md).

## Reproduction provenance

- **Reproduced:** 2026-11-12, with captured test events around 00:29:21 UTC+3.
- **Tested Git SHA:** `c28196d089ffdd61c365f15abc76cee36af8cbde`; commit date: 2026-11-12T00:27:29+03:00.
- **Production baseline:** [`6e3629b18daf4a2441d2b129fa3b1ae84564610f`](https://github.com/ydb-platform/ydb/commit/6e3629b18daf4a2441d2b129fa3b1ae84564610f); commit date: 2026-11-11T17:52:44+03:00. The test revision adds regressions without production changes.
- **Evidence:** [captured final console log](evidence/mr006-c28196d089f.log) and [shared test details](%5BEPIC%5D.md#reproduced-revision-and-test-evidence). UTC log timestamps `2026-11-11T21:29:21–22Z` correspond to the reproduction date above.

## Description

While VDisk `A` is unavailable, sequentially wiping its peers removes their stored `TSyncGuidInfo` records for `A`. Recovery does not reconstruct these witnesses, and only `A` can republish its own GUID through its normal propagators.

When `A` returns with a preserved `TLocalSyncerState` whose `State` is `TLocalGuidInfo::Final`, a first quorum consisting of wiped peers can report `Empty` for `A`. `TDecisionMaker` then returns `Inconsistency`, even though `A` retains both its local database and its eternal GUID. If its own persisted GUID reply arrives earlier, the same disk can instead receive `Good` and republish the GUID.

## Reproduction

The existing `TestRollingWipe(true)` helper implements this scenario:

1. Initialize eight VDisks using `Erasure4Plus2Block` and stop VDisk `A`, preserving its PDisk state.
2. Wipe the other seven VDisks one at a time, waiting for each replacement's new `TEvSyncGuidRecoveryDone(OK)` and replication completion.
3. Start `A`, temporarily holding its own `TEvVDiskGuidObtained` event.
4. Let six peer replies arrive. Those peers have recovered from wipes but retain no GUID witness for `A`, so all six replies contain `Empty` for the queried source.
5. Release the actual held event, without changing its contents.
6. Assert that `A` can recover its preserved local identity as `Good`.

The test also commits and verifies one `Block` on a recovered peer, using the scenario shared with [issue 01](01-stale-metadata-after-rolling-wipes.md). That data operation is not necessary for the GUID-witness failure.

## Expected and actual behavior

Expected: finite response delays and peer recovery should not cause a VDisk with intact local `Final` state to be classified as inconsistent merely because its peers lost their witness records.

Actual: the first all-`Empty` quorum produces `Inconsistency`. Releasing the preserved self-witness does not repair the completed decision.

Regression: `RecoverLostDataRestart::RollingWipeWithDelayedSelfGuid` in [recover_lost_data_restart.cpp](../core/blobstorage/ut_blobstorage/recover_lost_data_restart.cpp).

Final-run evidence, with the generated GUID value omitted:

```text
Decision# Inconsistency
VDisk Quorum Decision is Empty, but local state is [State# Final ... DbBirthLsn# 0]
```

The failing assertion is at `recover_lost_data_restart.cpp:275`: expected `EDecision::Good`, observed `Inconsistency`.

## Why the state is reachable

[`TSyncer::RunPropagators`](https://github.com/ydb-platform/ydb/blob/6e3629b18daf4a2441d2b129fa3b1ae84564610f/ydb/core/blobstorage/vdisk/syncer/blobstorage_syncer.cpp#L282) publishes the running VDisk's own `TVDiskEternalGuid` to all peers. It does not relay the GUIDs of other unavailable VDisks. A wiped peer can complete Hull recovery while retaining `Empty` peer-GUID information for `A`.

[`TVDiskGuidRecoveryActor::Handle(TEvVDiskGuidObtained)`](https://github.com/ydb-platform/ydb/blob/6e3629b18daf4a2441d2b129fa3b1ae84564610f/ydb/core/blobstorage/vdisk/syncer/guid_recovery.cpp#L696) stops at the first response quorum and cancels outstanding requests. [`VDiskQuorumDecision`](https://github.com/ydb-platform/ydb/blob/6e3629b18daf4a2441d2b129fa3b1ae84564610f/ydb/core/blobstorage/vdisk/syncer/guid_recovery.cpp#L398) aggregates only received states; [`HandleQuorumEmpty`](https://github.com/ydb-platform/ydb/blob/6e3629b18daf4a2441d2b129fa3b1ae84564610f/ydb/core/blobstorage/vdisk/syncer/guid_recovery.cpp#L467) maps local `Final` plus quorum `Empty` to `Inconsistency`.

For this eight-VDisk topology the response quorum is six. The executed regression wipes all seven peers. Six peer wipes are already enough for this failure if both `A`'s reply and the remaining unwiped peer's reply arrive after the first quorum; that smaller threshold is derived from the quorum geometry rather than separately tested.

## Consequences

[`TSyncer::InconsistentState`](https://github.com/ydb-platform/ydb/blob/6e3629b18daf4a2441d2b129fa3b1ae84564610f/ydb/core/blobstorage/vdisk/syncer/blobstorage_syncer.cpp#L354) sends `TEvSyncGuidRecoveryDone(ERROR, 0)`. [`TSkeleton::Handle(TEvSyncGuidRecoveryDone)`](https://github.com/ydb-platform/ydb/blob/6e3629b18daf4a2441d2b129fa3b1ae84564610f/ydb/core/blobstorage/vdisk/skeleton/blobstorage_skeleton.cpp#L2844) transitions to `SyncGuidRecoveryError`.

The completed recovery actor no longer consumes late witnesses. `InconsistencyModeHandle(TEvVGenerationChange)` only updates group information; it does not restart GUID recovery. This is therefore a persistent failure of the current actor lifetime, not just a delayed startup.

## Fix considerations

- Preserve or reconstruct immutable peer GUID witnesses during wipe recovery, durably before the replacement is treated as recovered.
- Retain detection of genuinely conflicting `Final` GUIDs.
- Waiting for an intact self-witness could address this response-order case, but does not resolve the ambiguity when the returning VDisk is also locally empty; see [issue 03](03-false-first-run-after-witness-loss.md).
- Simply mapping local `Final` plus remote `Empty` to `Good` would not establish metadata freshness. [Issue 01](01-stale-metadata-after-rolling-wipes.md) reproduces the independent metadata gap after `Good`.

An existing **open draft [PR #57055 — Preserve VDisk GUID history across realm recovery](https://github.com/ydb-platform/ydb/pull/57055)** directly overlaps the witness-preservation problem. Its inspected history-collection path is gated to mirror-3-dc VDisks starting in fail-realm state `R`; the reproduced regression here is 4+2, and the draft was not applied or tested. This report can supply additional failure coverage to that proposal. See [related GitHub work](04-related-github-work.md) for the scope comparison, prior fixes, and duplicate-search findings.
