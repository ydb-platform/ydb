# SyncGuidRecovery in P

Reduced C++ recovery model: `Erasure4Plus2Block` (8 VDisks) and
`ErasureMirror3dc` (3 realms × 3 VDisks, `NORMAL`, all realms `INCLUDED`).
Requires P 3.1.0 and .NET 8.

The model now includes the proposed repairs: full metadata sync on `RESTART`,
durable GUID records restored during lost-data recovery, and catch-up before `Ready`.

## Run

From `sync_guid_recovery_p`:

```sh
p compile --pproj Recovery.pproj --mode pchecker --outdir PGenerated
assembly=PGenerated/PChecker/net8.0/SyncGuidRecovery.dll

for topology in '' Mirror3dc; do
  for scenario in StaleMetadata DelayedSelfGuid FalseFirstRun NoWipes RetainedSource WipedWithWitnesses FreshGroup; do
    testcase="tc${topology}${scenario}"
    p check "$assembly" --testcase "$testcase" --schedules 50 --seed 1 \
      --max-steps 10000 --fail-on-maxsteps --outdir "PCheckerOutput/$testcase"
  done
done
```

All 14 existing cases should find no bugs with unchanged monitors.
These are bounded randomized checks, not exhaustive verification.
Saved counterexamples in `Evidence/` belong to the original model, before these repairs.

## PEx

```sh
p compile --pproj Recovery.pproj --mode pex --outdir PGenerated
jar=PGenerated/PEx/target/SyncGuidRecovery-jar-with-dependencies.jar
p check "$jar" --mode pex --testcase tcFreshGroup \
  --sch-pex dfs --schedules 0 --seed 1 --memout 1 \
  --max-steps 10000 --fail-on-maxsteps \
  --checker-args :--state-caching:exact --outdir PCheckerOutput/tcFreshGroup
```

Change `--testcase` to select another existing case. Require exit code 0,
`correct for any depth`, and zero unexplored choices. A memory/time limit
means incomplete exploration. Both `FreshGroup` cases complete after state
reduction; the other cases remain incomplete in the recorded capped samples.

[Model and C++ correspondence](MODEL.md) · [Recorded results](Evidence/README.md)
