# Checked model results

Validated on **2026-11-12**, using installed P **3.1.0**,
.NET SDK **8.0.427**, and runtime **8.0.34**. The existing host/runtime on this
machine were reached through a temporary launcher under `/tmp`; no system
installation is required by the model.

The P files were uncommitted when checked. [results.json](results.json) records
the SHA-256 of every checked P source file, the seed, bounds, checker exit codes,
and actual checker statistics. Do not interpret the C++ baseline commit as a
commit containing these new P files.

All seven checks matched their expected outcomes.

| Case | Actual schedules | Bugs | Maximum scheduling depth | Result |
| --- | ---: | ---: | ---: | --- |
| `tcStaleMetadata` | 1 | 1 | 629 | Intended `MetadataIsCompleteWhenSynced` assertion. |
| `tcDelayedSelfGuid` | 1 | 1 | 534 | Intended `PreservedIdentityRemainsRecoverable` assertion. |
| `tcFalseFirstRun` | 1 | 1 | 563 | Intended `NeverReinitializeExistingVDisk` assertion. |
| `tcNoWipes` | 50 | 0 | 166 | Control completed. |
| `tcRetainedSource` | 50 | 0 | 572 | Control completed. |
| `tcWipedWithWitnesses` | 50 | 0 | 150 | Control completed. |
| `tcFreshGroup` | 50 | 0 | 86 | Control completed. |

Seed was 1, the per-schedule step bound was 10,000, and step-limit failures were
enabled. Negative cases stop at the first discovered bug. These are **203
randomized bounded schedules**, not an exhaustive proof.

## Counterexamples

- [Stale metadata excerpt](tcStaleMetadata.excerpt.txt): ordinary synchronization
  receives successful results from all seven peers, then fails with
  `All 7 peers synced, but Hull Block generation 0 misses committed 42`.
  The source reply sequence includes `RESTART` at the rebuilt birth LSN followed
  by `ALREADY`, without a Block import.
- [Delayed self-witness excerpt](tcDelayedSelfGuid.excerpt.txt): the first six
  accepted replies have erased witnesses, producing
  `Intact local Final rejected after an Empty first reply quorum`.
- [False FirstRun excerpt](tcFalseFirstRun.excerpt.txt): the existing locally
  erased slot completes new-GUID publication and triggers
  `Existing VDisk was classified FirstRun after its GUID witnesses were erased`.

The excerpts contain selected actual trace lines, not a complete trace. Each
`p check --outdir` directory contains its full traces. In reply traces, `EReplyStatus` values
are `OK=0`, `RESTART=1`, `ALREADY=2`, `NODATA=3`; verdict values are
`FirstRun=0`, `Good=1`, `LostData=2`, `Inconsistency=3`.

## Replay

Saved schedules:

- [tcStaleMetadata.schedule](tcStaleMetadata.schedule)
- [tcDelayedSelfGuid.schedule](tcDelayedSelfGuid.schedule)
- [tcFalseFirstRun.schedule](tcFalseFirstRun.schedule)

With the same P version and unchanged source, replay a saved schedule against the
compiled assembly, for example:

```sh
p check PGenerated/PChecker/net8.0/SyncGuidRecovery.dll \
  --replay Evidence/tcStaleMetadata.schedule
```

The stale-metadata saved schedule was replayed successfully and reported
`Reproduced 1 bug.` P 3.1.0 returns zero for that successful replay. The other two
schedules were captured by their failing checks but were not separately replayed.

## Relationship to the C++ evidence

The original C++ regressions ran at
`c28196d089ffdd61c365f15abc76cee36af8cbde` on 2026-11-12, Europe/Moscow,
using production code from
`6e3629b18daf4a2441d2b129fa3b1ae84564610f`. Their [captured log](../../sync-guid-recovery-issues/evidence/mr006-c28196d089f.log)
confirms the missing Block and false `Inconsistency` cases.

The false-`FirstRun` result above is additional evidence from this reduced P
model. It does not change the original report's statement that no dedicated
end-to-end C++ reproduction of that third case has been run.
