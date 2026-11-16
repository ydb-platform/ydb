# Protocol fix and C++ validation

## Revision

- Implementation Git SHA: `7605b01dcffeb16e985971863aadf26e5ad2f792`, committed 2026-11-13T20:06:04+03:00.
- Remote test snapshot: `b22755429b087d71f05b31260ee4e6c418a4299a`, committed 2026-11-13T20:06:04+03:00. Its Git tree is identical to the implementation revision.
- The snapshot is rooted at the existing remote baseline because the `mr006` bare repository rejected the implementation branch's ancestry due to missing objects.

## Changes

- `TSyncerJobTask::HandleRestart` requests full metadata synchronization. An unsuccessful full snapshot restores `OldSyncState` before any scheduler commit; successful completion follows the local import acknowledgement.
- `TSyncerData::GuidHistory` persists immutable GUID observations through the syncer entry point. Recovery reads and commits history from a surviving group quorum excluding self, for redundant erasures, before deciding how to recover its own identity. Relayed history preserves the original witness and contributes no current `Final` votes.
- `TDecisionMaker::ReachAVerdict` uses historical or intact local identity to republish the original GUID when necessary. Historical identity with local `Empty` selects `LostData`. Incompatible `Final` evidence remains an error; tentative `InProgress` does not replace an immutable identity.
- Incoming GUID publication cannot overwrite a different committed identity. Read-only publication does not alter the in-memory observation cache.
- Startup `Replicated` and `DurabilityReplicated` remain false until a quorum successfully completes metadata catch-up. Failed attempts do not count. `TEvSyncGuidRecoveryDone` remains the identity-phase notification, allowing internal synchronization to run during catch-up.

## Remote validation

Validated on **2026-11-13**, with test events around **20:14:45–46 UTC+3**.
Build succeeded; **exactly two tests ran and both passed**; `ya make` exited **0**:

- `RecoverLostDataRestart::RollingWipeWithPreservedLocalState`
- `RecoverLostDataRestart::RollingWipeWithDelayedSelfGuid`

Remote checkout: `/tmp/sync-guid-protocol-fix-82910db`.

[Captured final console log](evidence/mr006-b22755429b0.log).
The tests and their assertions were unchanged. The recovered-peer generation-42
control, returning-VDisk `Good`, replication checkpoint and final Block query
all passed, including the case with delayed self-GUID replies.

From the remote repository root:

```sh
./ya make --build relwithdebinfo -j64 --link-threads=2 -R -tA \
  ydb/core/blobstorage/ut_blobstorage/ut_replication \
  -F 'RecoverLostDataRestart::RollingWipe*'
```

These parallelism settings are for `mr006`.

## Limits

Mixed-version responders retain an explicitly incomplete-history fallback; full witness-preservation guarantees require supporting peers. Already-skipped cursors whose peer incarnation has not changed are not automatically repaired by the new `RESTART` handling. No additional C++ regression for false `FirstRun`, interrupted snapshots, or mirror-3-dc realm transitions is claimed by this two-test run.
