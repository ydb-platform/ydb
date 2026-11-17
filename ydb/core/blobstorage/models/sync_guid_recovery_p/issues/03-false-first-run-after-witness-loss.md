# Existing VDisk can be classified as FirstRun after local loss and erosion of peer GUID witnesses

**Status:** reproduced in the reduced P model for `Erasure4Plus2Block` and normal `ErasureMirror3dc`; no dedicated end-to-end C++ regression has been run. It requires local `Empty` state in addition to temporary unavailability.

**Umbrella:** [[EPIC] Make VDisk recovery safe across rolling wipes](%5BEPIC%5D.md).

## Validation provenance

- **P reproduction:** 2026-11-12, `tcFalseFirstRun` and `tcMirror3dcFalseFirstRun`. Tested working-tree base Git SHA: `a68bd12f36af20329023baf319990155fa1ebf10`; commit date: 2026-11-12T20:12:20+03:00. The model changes were uncommitted; [results.json](../sync_guid_recovery_p/Evidence/results.json) records exact source hashes. Both saved schedules were replayed successfully.
- **C++ reproduction:** not executed; there is no dedicated C++ reproduction SHA or date for this scenario.
- **Code inspection date:** 2026-11-12.
- **Inspected production Git SHA:** [`6e3629b18daf4a2441d2b129fa3b1ae84564610f`](https://github.com/ydb-platform/ydb/commit/6e3629b18daf4a2441d2b129fa3b1ae84564610f); commit date: 2026-11-11T17:52:44+03:00.
- **Inspected checkout Git SHA:** `c28196d089ffdd61c365f15abc76cee36af8cbde`; commit date: 2026-11-12T00:27:29+03:00. The conditional path is present in this checkout, but its two executed regressions cover issues 01 and 02, not this scenario.

This scenario shares the witness-erasure root cause with [issue 02](02-guid-witness-loss-after-rolling-wipes.md). It can be tracked with that issue unless a separate safety-focused report is useful.

The P scenarios wipe VDisk 0, leave it offline, then wipe and fully recover every
other VDisk sequentially. On return, the real empty first quorum triggers
`FirstRun`, new-GUID publication, and local `Final`; the monitor rejects the
reinitialization of an existing slot. Mirror-3-dc uses nine VDisks, three physical
realms, `NORMAL`, all realms `INCLUDED`, and first quorum `{0,1,2,3,4}`.
See the [mirror-3-dc trace](../sync_guid_recovery_p/Evidence/tcMirror3dcFalseFirstRun.excerpt.txt)
and [checker commands](../sync_guid_recovery_p/README.md).

An existing **open draft [PR #57055 — Preserve VDisk GUID history across realm recovery](https://github.com/ydb-platform/ydb/pull/57055)** explicitly describes peer-witness loss causing a later wiped realm to appear `FirstRun`. This is direct conceptual overlap, rather than a previously unknown proposal. The PR's inspected history collection targets mirror-3-dc realm state `R`; the candidate sequence below uses 4+2 and has not been run with that PR applied.

## Description and preconditions

`SyncGuidRecovery` distinguishes a genuinely new VDisk from a VDisk that lost data using peer copies of its `TVDiskEternalGuid`. If the unavailable VDisk has already been wiped and enough peer witnesses are subsequently erased by rolling wipes, an all-`Empty` first survey quorum no longer identifies that it is an existing slot.

Unlike the two executed regressions, this scenario requires the unavailable VDisk `A` to have local `TLocalGuidInfo::Empty` state. A stopped VDisk whose original local `Final` state survives does not enter this branch.

## Minimal C++ reproduction candidate

For an initialized eight-VDisk `Erasure4Plus2Block` group:

1. Wipe `A` and keep it unavailable before it can complete GUID recovery or republish its identity.
2. Sequentially wipe and recover five other VDisks while `A` remains unavailable.
3. Start `A` with local `Empty` state and an empty self-entry.
4. Let the five wiped peers and `A`'s own empty entry form the first six-response quorum. Delay the two unwiped peers, which can still retain the old `Final` witness.
5. Observe `TEvVDiskGuidRecovered::Outcome`, the selected eternal GUID, and whether `TSyncerRecoverLostDataActor` and Osiris run.

The five-wipe threshold is derived from quorum geometry and the decision code. This minimal sequence has not been executed; the P reproduction above wipes all seven peers (eight for mirror-3-dc).

## Expected and code-derived actual behavior

Expected: recognize an existing VDisk that lost its data, retain its original `TVDiskEternalGuid`, and complete the required lost-data recovery. If initialization history cannot be recovered, do not silently treat the slot as a fresh empty database.

Code-derived actual behavior:

1. [`HandleQuorumEmpty`](https://github.com/ydb-platform/ydb/blob/6e3629b18daf4a2441d2b129fa3b1ae84564610f/ydb/core/blobstorage/vdisk/syncer/guid_recovery.cpp#L467) maps local `Empty` plus quorum `Empty` to `TDecision::FirstRun(ACTION_GenerateGuid, 0)`.
2. [`TVDiskGuidFirstRunState::GenerateGuid`](https://github.com/ydb-platform/ydb/blob/6e3629b18daf4a2441d2b129fa3b1ae84564610f/ydb/core/blobstorage/vdisk/syncer/guid_firstrun.cpp#L126) chooses a new GUID. [`TVDiskGuidFirstRunActor::WriteFinalLocally`](https://github.com/ydb-platform/ydb/blob/6e3629b18daf4a2441d2b129fa3b1ae84564610f/ydb/core/blobstorage/vdisk/syncer/guid_firstrun.cpp#L467) commits local `Final` with `DbBirthLsn=0`.
3. [`TSyncer::Handle(TEvVDiskGuidRecovered)`](https://github.com/ydb-platform/ydb/blob/6e3629b18daf4a2441d2b129fa3b1ae84564610f/ydb/core/blobstorage/vdisk/syncer/blobstorage_syncer.cpp#L313) handles `FirstRun` like `Good`, calling `StandardMode` directly. Only `LostData` invokes `StartRecoverLostData`.
4. [`StandardMode`](https://github.com/ydb-platform/ydb/blob/6e3629b18daf4a2441d2b129fa3b1ae84564610f/ydb/core/blobstorage/vdisk/syncer/blobstorage_syncer.cpp#L446) publishes the GUID and sends `TEvSyncGuidRecoveryDone(OK, ...)` before starting ordinary synchronization.

The wrong classification bypasses the full-sync quorum and `TEvCallOsiris` in [`TSyncerRecoverLostDataActor`](https://github.com/ydb-platform/ydb/blob/6e3629b18daf4a2441d2b129fa3b1ae84564610f/ydb/core/blobstorage/vdisk/syncer/blobstorage_syncer_recoverlostdata.cpp#L156). It also replaces an eternal GUID whose intended lifetime is the BlobStorage group's lifetime.

The surviving witnesses do not provide an independent veto during publication: for a writable peer, [`TSyncer::Handle(TEvVSyncGuid)`](https://github.com/ydb-platform/ydb/blob/6e3629b18daf4a2441d2b129fa3b1ae84564610f/ydb/core/blobstorage/vdisk/syncer/blobstorage_syncer.cpp#L573) assigns the incoming `Info` and commits the requested state/GUID without rejecting a different previously stored `Final` GUID. Delaying the old witnesses during the survey therefore does not require making them unavailable for the later publication writes. This is also a code-derived observation.

## Consequences and limits

The code permits normal operation without the prerequisites of the `LostData` branch. Ordinary synchronization is not an equivalent replacement for the lost-data full-sync quorum and Osiris's restoration of ingress responsibility. Once the original source histories have been wiped, it can also encounter the history-coverage gap in [issue 01](01-stale-metadata-after-rolling-wipes.md).

No end-to-end C++ log or payload-loss assertion is claimed for this scenario. A focused C++ regression should assert `LostData`, preservation of the original eternal GUID, and an Osiris invocation before adding a larger data-loss simulation.

## Fix considerations

- Initialization evidence must survive rolling replacements independently of the unavailable VDisk. Candidate approaches include durable reconstruction of peer GUID witnesses or an authoritative per-slot identity/initialization record.
- Waiting longer for replies cannot solve the case where every copy of the historical witness has actually been erased.
- Fixing `RESTART` metadata synchronization alone does not prevent the wrong eternal GUID selection or restore the missing `LostData` branch semantics.
- The `EFirstRunMode::PublishOnly` protection from [PR #56555](https://github.com/ydb-platform/ydb/pull/56555) applies when an existing GUID has been recovered and needs republication. This scenario selects `FirstRun` and `Initialize` instead, so it is a different branch. See [related GitHub work](04-related-github-work.md).
