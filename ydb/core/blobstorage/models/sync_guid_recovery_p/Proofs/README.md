# Component obligations

```sh
p compile --pproj Proofs.pproj --mode pex --outdir PGeneratedProofs
jar=PGeneratedProofs/PEx/target/SyncGuidRecoveryProofs-jar-with-dependencies.jar
p check "$jar" --mode pex --testcase tcProofClosedSurvey --sch-pex dfs \
  --schedules 0 --seed 1 --memout 1 --max-steps 10000 --fail-on-maxsteps \
  --checker-args :--state-caching:exact --outdir PCheckerOutput/ProofClosedSurvey
```

`tcProofClosedSurvey` exercises the actual `TVDiskGuidRecoveryActor` through
initialization, intact-Final and lost-data paths. Conflicting late replies are
injected before each publication/commit acknowledgement and after the result.
`ClosedSurveyIsSilent` forbids additional votes or verdicts after the first
verdict. Expected publication/commit GUIDs and single completion are checked.
Inspection of the corresponding `ignore TEvVDiskGuidObtained` handlers extends
this argument to every payload: consuming a late reply changes only its queue.
This does not forbid the legitimate publications/commits after a verdict.

2026-11-13: PEx 0.4.1, exact DFS, seed 1: 48 states, 3 schedules,
`correct for any depth`, exit 0.
