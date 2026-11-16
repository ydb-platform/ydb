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
injected before each history/publication/local-commit acknowledgement and after
the result. `ClosedSurveyIsSilent` closes voting at `eCommitGuidHistory` and
requires its acknowledgement before the verdict. Expected history and
publication/commit GUIDs and single completion are checked.
Inspection of the corresponding `ignore TEvVDiskGuidObtained` handlers extends
this argument to every payload: consuming a late reply changes only its queue.
This does not forbid the legitimate publications/commits after a verdict.

2026-11-16: exact DFS, seed 1: 57 states, 3 schedules,
`correct for any depth`, exit 0. PChecker also passed 50 schedules.
Removing the history-commit wait fails this proof's monitor on the first schedule.
Revision: working-tree changes on `e2d9710abc7dc917f5122d67fd693d5cff054fa8`
(commit date 2026-11-16).
