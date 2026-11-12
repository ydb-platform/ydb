# SyncGuidRecovery in P

Reduced C++ recovery model: eight VDisks, `Erasure4Plus2Block`, quorum six.
Requires P 3.1.0 and .NET 8.

## Run

From `sync_guid_recovery_p`:

```sh
p compile --pproj Recovery.pproj --mode pchecker --outdir PGenerated
assembly=PGenerated/PChecker/net8.0/SyncGuidRecovery.dll

p check "$assembly" --testcase tcStaleMetadata --seed 1 --outdir PCheckerOutput/tcStaleMetadata
p check "$assembly" --testcase tcDelayedSelfGuid --seed 1 --outdir PCheckerOutput/tcDelayedSelfGuid
p check "$assembly" --testcase tcFalseFirstRun --seed 1 --outdir PCheckerOutput/tcFalseFirstRun
```

Each check should fail its monitor and write a counterexample trace.

| Test | Expected failure |
| --- | --- |
| `tcStaleMetadata` | Seven peers report synchronized; local Block generation is 0 instead of 42. |
| `tcDelayedSelfGuid` | Intact local `Final` is rejected after an all-`Empty` first quorum. |
| `tcFalseFirstRun` | An existing wiped VDisk is classified as `FirstRun`. |

Replay a saved counterexample:

```sh
p check "$assembly" --replay Evidence/tcStaleMetadata.schedule
```

Controls should find no bugs:

```sh
for testcase in tcNoWipes tcRetainedSource tcWipedWithWitnesses tcFreshGroup; do
  p check "$assembly" --testcase "$testcase" --schedules 50 --seed 1 \
    --max-steps 10000 --fail-on-maxsteps --outdir "PCheckerOutput/$testcase"
done
```

These are bounded randomized checks, not exhaustive verification.

[Model and C++ correspondence](MODEL.md) · [Recorded results](Evidence/README.md)
