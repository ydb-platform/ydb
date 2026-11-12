# SyncGuidRecovery in P

Reduced C++ recovery model: `Erasure4Plus2Block` (8 VDisks) and
`ErasureMirror3dc` (3 realms × 3 VDisks, `NORMAL`, all realms `INCLUDED`).
Requires P 3.1.0 and .NET 8.

## Run

From `sync_guid_recovery_p`:

```sh
p compile --pproj Recovery.pproj --mode pchecker --outdir PGenerated
assembly=PGenerated/PChecker/net8.0/SyncGuidRecovery.dll

for testcase in tcMirror3dcStaleMetadata tcMirror3dcDelayedSelfGuid tcMirror3dcFalseFirstRun; do
  p check "$assembly" --testcase "$testcase" --seed 1 --outdir "PCheckerOutput/$testcase"
done
```

Each check should fail its monitor and write a counterexample trace.

| Test | Expected failure |
| --- | --- |
| `tcMirror3dcStaleMetadata` | Eight peers report synchronized; local Block generation is 0 instead of 42. |
| `tcMirror3dcDelayedSelfGuid` | Intact local `Final` is rejected after an all-`Empty` first quorum. |
| `tcMirror3dcFalseFirstRun` | An existing wiped VDisk is classified as `FirstRun`. |

For 4+2, remove `Mirror3dc` from the testcase names. Its GUID quorum is six;
mirror-3-dc uses realm placement: `(3,2,0)` replies form a quorum, `(2,2,2)` do not.

Replay a saved counterexample:

```sh
p check "$assembly" --testcase tcMirror3dcStaleMetadata --replay Evidence/tcMirror3dcStaleMetadata.schedule
```

Controls should find no bugs:

```sh
for testcase in tcMirror3dcNoWipes tcMirror3dcRetainedSource tcMirror3dcWipedWithWitnesses tcMirror3dcFreshGroup; do
  p check "$assembly" --testcase "$testcase" --schedules 50 --seed 1 \
    --max-steps 10000 --fail-on-maxsteps --outdir "PCheckerOutput/$testcase"
done
```

These are bounded randomized checks, not exhaustive verification.

[Model and C++ correspondence](MODEL.md) · [Recorded results](Evidence/README.md)
