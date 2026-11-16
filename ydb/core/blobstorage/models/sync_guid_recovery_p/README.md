# SyncGuidRecovery in P

Reduced C++ recovery model: `Erasure4Plus2Block` (8 VDisks) and
`ErasureMirror3dc` (3 realms × 3 VDisks, `NORMAL`, all realms `INCLUDED`).
Requires P 3.1.0 and .NET 8.

Full metadata sync on `RESTART`, durable GUID history, and catch-up before `Ready`.
History sources must intersect every possible `Final` publication quorum; self
counts only with retained `LocalFinal`. History is committed before the verdict.
Catch-up counts retained/recovered local metadata; `LostData` still requires a
full-sync quorum excluding self.

## Run

From `sync_guid_recovery_p`:

```sh
p compile --pproj Recovery.pproj --mode pchecker --outdir PGenerated
assembly=PGenerated/PChecker/net8.0/SyncGuidRecovery.dll

for topology in '' Mirror3dc; do
  for scenario in StaleMetadata DelayedSelfGuid FalseFirstRun NoWipes RetainedSource WipedWithWitnesses FreshGroup DegradedRestart DegradedFreshGroup; do
    testcase="tc${topology}${scenario}"
    p check "$assembly" --testcase "$testcase" --schedules 50 --seed 1 \
      --max-steps 10000 --fail-on-maxsteps --outdir "PCheckerOutput/$testcase"
  done
done
```

18 cases: the original 14 plus restart and fresh initialization with two peers
unavailable, for both erasures. `DegradedFreshGroup` checks GUID initialization;
`DegradedRestart` also checks metadata catch-up. The metadata completeness
assertion is preserved; its quorum check now includes the local VDisk.

2026-11-16: all 18 passed 50 schedules each (seed 1, bounds above).
Restoring either old quorum gate makes the corresponding two degraded cases fail.
Tested working-tree changes on `e2d9710abc7dc917f5122d67fd693d5cff054fa8`
(commit date 2026-11-16). These are bounded randomized checks.

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
means incomplete exploration.

[Component proof](Proofs/README.md) · [Issue drafts](issues/EPIC.md)
