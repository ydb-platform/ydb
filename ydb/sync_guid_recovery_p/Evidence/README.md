# Checked model results

Validated on **2026-11-12**, using installed P **3.1.0**,
.NET SDK **8.0.428**, and runtime **8.0.34**, using `p` directly.

The checked working tree is based on Git SHA
`a68bd12f36af20329023baf319990155fa1ebf10`, committed
**2026-11-12T20:12:20+03:00**, with uncommitted model changes.
[results.json](results.json) records
the SHA-256 of every checked P source file, the seed, bounds, checker exit codes,
and actual checker statistics. Do not interpret the C++ baseline commit as a
commit containing these new P files.

All fourteen checks matched their expected outcomes. The `tcMirror3dc…` cases
use nine VDisks, three physical realms, `NORMAL`, all realms `INCLUDED`, and
the actual group/domain quorum predicate. `(3,2,0)` responses form a quorum;
`(2,2,2)` do not.

| Case | Actual schedules | Bugs | Maximum scheduling depth | Result |
| --- | ---: | ---: | ---: | --- |
| `tcStaleMetadata` | 1 | 1 | 629 | Intended `MetadataIsCompleteWhenSynced` assertion. |
| `tcDelayedSelfGuid` | 1 | 1 | 534 | Intended `PreservedIdentityRemainsRecoverable` assertion. |
| `tcFalseFirstRun` | 1 | 1 | 563 | Intended `NeverReinitializeExistingVDisk` assertion. |
| `tcNoWipes` | 50 | 0 | 166 | Control completed. |
| `tcRetainedSource` | 50 | 0 | 572 | Control completed. |
| `tcWipedWithWitnesses` | 50 | 0 | 150 | Control completed. |
| `tcFreshGroup` | 50 | 0 | 86 | Control completed. |
| `tcMirror3dcStaleMetadata` | 1 | 1 | 743 | Intended `MetadataIsCompleteWhenSynced` assertion. |
| `tcMirror3dcDelayedSelfGuid` | 1 | 1 | 633 | Intended `PreservedIdentityRemainsRecoverable` assertion. |
| `tcMirror3dcFalseFirstRun` | 1 | 1 | 659 | Intended `NeverReinitializeExistingVDisk` assertion. |
| `tcMirror3dcNoWipes` | 50 | 0 | 182 | Control completed. |
| `tcMirror3dcRetainedSource` | 50 | 0 | 686 | Control completed. |
| `tcMirror3dcWipedWithWitnesses` | 50 | 0 | 162 | Control completed. |
| `tcMirror3dcFreshGroup` | 50 | 0 | 90 | Control completed. |

Seed was 1, the per-schedule step bound was 10,000, and step-limit failures were
enabled. Negative cases stop at the first discovered bug. These are **406
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
- [Mirror-3-dc stale metadata](tcMirror3dcStaleMetadata.excerpt.txt): `Good`, then
  `RESTART`/`ALREADY` and `DisksSynced` from all eight peers, while local Hull
  still lacks committed generation 42.
- [Mirror-3-dc delayed self-witness](tcMirror3dcDelayedSelfGuid.excerpt.txt): five
  erased witnesses from `{3,4,5,6,7}` form the first quorum and reject local `Final`.
- [Mirror-3-dc false FirstRun](tcMirror3dcFalseFirstRun.excerpt.txt): the locally
  erased existing VDisk generates and commits a new eternal GUID.

The excerpts contain selected actual trace lines, not a complete trace. Each
`p check --outdir` directory contains its full traces. In reply traces, `EReplyStatus` values
are `OK=0`, `RESTART=1`, `ALREADY=2`, `NODATA=3`; verdict values are
`FirstRun=0`, `Good=1`, `LostData=2`, `Inconsistency=3`.

## Replay

Saved schedules:

- [tcStaleMetadata.schedule](tcStaleMetadata.schedule)
- [tcDelayedSelfGuid.schedule](tcDelayedSelfGuid.schedule)
- [tcFalseFirstRun.schedule](tcFalseFirstRun.schedule)
- [tcMirror3dcStaleMetadata.schedule](tcMirror3dcStaleMetadata.schedule)
- [tcMirror3dcDelayedSelfGuid.schedule](tcMirror3dcDelayedSelfGuid.schedule)
- [tcMirror3dcFalseFirstRun.schedule](tcMirror3dcFalseFirstRun.schedule)

With the same P version and unchanged source, replay a saved schedule against the
compiled assembly, for example:

```sh
p check PGenerated/PChecker/net8.0/SyncGuidRecovery.dll \
  --testcase tcMirror3dcStaleMetadata --replay Evidence/tcMirror3dcStaleMetadata.schedule
```

All six saved schedules were replayed successfully and reported
`Reproduced 1 bug.` P 3.1.0 returns zero for that successful replay.

## Relationship to the C++ evidence

The original C++ regressions ran at
`c28196d089ffdd61c365f15abc76cee36af8cbde` on 2026-11-12, Europe/Moscow,
using production code from
`6e3629b18daf4a2441d2b129fa3b1ae84564610f`. Their [captured log](../../sync-guid-recovery-issues/evidence/mr006-c28196d089f.log)
confirms the missing Block and false `Inconsistency` cases.

The false-`FirstRun` and mirror-3-dc results are evidence from this reduced P
model. No dedicated end-to-end C++ reproduction of those cases has been run.
