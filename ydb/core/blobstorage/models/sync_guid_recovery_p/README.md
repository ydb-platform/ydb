# SyncGuidRecovery model

P 3.1.0, .NET 8; PEx also needs Java/Maven. Run commands from this directory.
The baseline-model commit intentionally exposes six failures; the corrected
model checks the same properties plus degraded startup and partial publication.
See [audit and correspondence](AUDIT.md) for scope, abstractions and results.

## PChecker

```sh
p compile --pproj Recovery.pproj --mode pchecker --outdir PGenerated
for topology in '' Mirror3dc; do
  for scenario in StaleMetadata DelayedSelfGuid FalseFirstRun NoWipes RetainedSource WipedWithWitnesses FreshGroup DegradedRestart DegradedFreshGroup; do
    p check PGenerated/PChecker/net8.0/SyncGuidRecovery.dll \
      --testcase "tc${topology}${scenario}" --schedules 50 --seed 1 \
      --max-steps 10000 --fail-on-maxsteps \
      --outdir "PCheckerOutput/tc${topology}${scenario}" || break
  done
done
```

Repeat new scenarios with seeds 2 and 3. Successful randomized checks are bounded
evidence, not exhaustive verification.

## PEx

```sh
p compile --pproj Recovery.pproj --mode pex --outdir PGenerated
p check PGenerated/PEx/target/SyncGuidRecovery-jar-with-dependencies.jar \
  --mode pex --testcase tcFreshGroup --sch-pex dfs --schedules 0 --seed 1 \
  --memout 4 --max-steps 10000 --fail-on-maxsteps \
  --checker-args :--state-caching:exact --outdir PCheckerOutput/PExFreshGroup
```

Also run `tcMirror3dcFreshGroup`. Require exit 0, `correct for any depth` and
zero unexplored choices. Resource exhaustion is an incomplete search.
[Compact component checks](Proofs/README.md) cover the closed survey, conflicting
GUID evidence and interrupted full-sync retry. These execute P code, not C++.
