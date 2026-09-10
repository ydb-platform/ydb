# CutHistory boot proof — E2E scheme (arm C)

Branch `dev/KIKIMR-26208-bsrange-pr`. Written for review: this describes what the E2E covers,
what it deliberately does not, and where the coverage is still thin.

## What the feature does

CutHistory removes a drained entry from a ColumnShard channel's history so the group behind it can
be released. Removing an entry is **irreversible**: that generation range stops resolving to a
group, so any blob still there becomes unreachable forever and any `DoNotKeep` still owed to it can
never be delivered.

Arm C replaces *candidate discovery*. Instead of a 60 s cadence that scans every portion, the tablet
proves ranges once at boot by asking BlobStorage directly:

```
tablet boot
  └─ LoadLists()                     BlobsToDelete / BlobsToKeep restored from local DB
  └─ SetupCutHistory()
       └─ TryNominateAtBoot()
            for each channel >= FirstDataChannel:
              for each history entry that HAS a successor:
                 skip if CutState != None                 (round already running for it)
                 skip if !SeenGroupsCheckPasses           (an earlier uncuttable entry reuses the group)
                 defer if HasPendingDeletesInRange        (a DoNotKeep is still owed -> would be stranded)
                 else -> candidate
            if candidates: SweepInFlight = true, send TEvStartCutHistorySweep
  └─ TCutHistoryRangeProbeActor
       TEvBlobStorage::TEvRange per candidate, <= MaxRangeProbesInFlight at once
       any non-OK status / timeout / missing successor  -> disprove (fail closed)
       any returned blob of ours in [from, next) that is not (DoNotKeep && !Keep) -> disprove
  └─ OnRangeProbeComplete -> OnBatchComplete
       survivors re-checked: Counters == 0, IsDrained (BlobsToKeep, BlobsToDelete, delayed,
       GCTaskInFlight, shared blobs), SeenGroupsCheckPasses again, group still resolvable
  └─ TCutHistoryBarrierActor
       hard TEvCollectGarbage(collect=true, hard=true, collectGen=next-1)
       only on OK / ALREADY -> TEvCutTabletHistory to Hive
```

Two independent gates stand before the barrier: the **range probe** (BlobStorage ground truth) and
the **IsDrained re-check** (this tablet's own queues). Both must pass.

## Config surface

| knob | where | meaning |
|---|---|---|
| `EnableColumnshardGroupDecommission` | feature flag | master switch; with it off every cutter entry point returns early and the wrapper does nothing |
| `enable_cut_history` | feature flag | the platform-level cutter for channels 0/1; separate from the above |
| `CutHistoryProofSource` | `TColumnShardConfig` 80 | `PORTIONS` (cadence, default) / `BS_RANGE` (boot proof) / `COMPARE` |
| `CutHistoryMeasureOnly` | `TColumnShardConfig` 79, default **true** | runs the whole proof, stops before the barrier |
| `cut_history_deny_list` | `THiveConfig` | Hive's own gate; ColumnShard is denied by default |

## E2E coverage — `test_cut_history_boot.py`

Fixture `RestartToAnotherVersionFixture`, `CUT_HISTORY_PROOF_BS_RANGE`, `cut_history_measure_only: False`
so the proof really runs to the barrier.

### `test_boot_probe_never_cuts_a_live_range`

Creates a 4-partition column table, writes 200 rows, then three rounds of
`tablet_kill` on every ColumnShard followed by another 200 rows. Each restart is a boot, so each
restart runs the boot proof over ranges that still hold this table's data.

Asserted every round and again after a 90 s settle:

- `Entries/Cut/Count == 0` — the safety invariant. A range holding live data must never be cut.
- `Channels/Poisoned == 0` — a poisoned channel means a refcount underflow, a real defect.
- `Barriers/Failed/Count == 0`
- row count matches after every round — data survives the churn

Sensors are summed over **all** nodes (`_node_http_endpoints`), not just node 1, so a wrong cut on
any node is visible.

### `test_boot_probe_survives_restart_to_another_version`

Write, restart the shards, then `change_cluster_version()`. A version change is just another boot,
so the proof runs again on the new binary. Asserts readability, no poisoning, nothing cut.

## What this does NOT cover — read this part

1. **A successful cut is never exercised.** Every candidate in these tests is disproved, because the
   table's data is still live in the old ranges. The tests prove the feature is *safe*, not that it
   *works*. Proving a cut needs a drained range: write, delete, let cleanup and GC run to completion,
   and only then restart. That test does not exist yet and is the most valuable one missing.
2. **The deferral path is only unit-tested.** `BootProbeDefersEntryWithPendingDeletes` covers it in
   `ut_cut_history`, but no E2E drives a real pending `DoNotKeep` into a candidate range and checks
   `BootProbe/Deferred` rises while nothing is cut.
3. **Probe failure paths are unit-only.** Non-OK statuses and the 1 min timeout are covered by
   `RangeProbeErrorFailsClosed` / `RangeProbeTimeoutFailsClosed`; no E2E injects a BS failure.
4. **Group reuse across a boot round.** `SeenGroupsCheckPasses` is unit-tested, but a boot round
   nominates many entries at once and no E2E builds a history where an earlier uncuttable entry
   shares a `GroupID` with a later candidate.
5. **Concurrency with MoveData.** These tests never run MoveData, so the interaction between a move
   in flight and a boot proof is untested end to end.

## Known issues found by audit, and their state

- **GC-blocked boot round** — `SetupGC()` runs in the same turn as the boot nomination and sets
  `GCTaskInFlight`, so `IsDrained` refuses every candidate and, with no cadence in BsRange mode,
  nothing re-nominated until the next restart. **Fixed**: `SetupCutHistory` now retries
  `TryNominateAtBoot` on later passes, and the nominator skips entries whose round is still running.
  Covered by `BootProbeRetriesAfterGcBlockedRound` and `BootProbeSkipsEntryAlreadyInFlight`.
- **Probe actor killed before its timeout** leaves `SweepInFlight` latched for that tablet
  incarnation, so nothing is nominated until restart. No data loss. **Open** — the 1 min
  `ProbeTimeout` covers every case except an actor kill between registration and the deadline.
- **`HasPendingDeletesInRange` checks neither `BlobsToKeep` nor shared blobs.** Safe today only
  because the range probe and the later `IsDrained` both catch those; the nomination-level check
  reads as if it handled them and does not.

## Unit coverage backing this

`ydb/core/tx/columnshard/ut_cut_history` — 31 tests. Boot-specific:
`BootProbeNominatesCleanEntry`, `BootProbeDefersEntryWithPendingDeletes`,
`BootProbeRetriesAfterGcBlockedRound`, `BootProbeSkipsEntryAlreadyInFlight`,
`ProofSourceLatchedForTheRound`, plus the range-probe set
(`RangeProbeOurBlobDisproves`, `RangeProbeWindowEdges`, `RangeProbeIgnoresForeignAndOtherChannel`,
`RangeProbeIgnoresCollectedGarbage`, `RangeProbeErrorFailsClosed`, `RangeProbeTimeoutFailsClosed`,
`RangeProbeVerdictLeavesEntryUncut`).
