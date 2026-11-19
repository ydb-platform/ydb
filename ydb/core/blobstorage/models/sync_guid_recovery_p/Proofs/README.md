# Compact component checks

```sh
p compile --pproj Proofs.pproj --mode pex --outdir PGeneratedProofs
for testcase in tcProofClosedSurvey tcProofFullSyncRetry tcProofGuidConflict tcProofGuidPublication; do
  p check PGeneratedProofs/PEx/target/SyncGuidRecoveryProofs-jar-with-dependencies.jar \
    --mode pex --testcase "$testcase" --sch-pex dfs --schedules 0 --seed 1 \
    --memout 1 --max-steps 10000 --fail-on-maxsteps \
    --checker-args :--state-caching:exact --outdir "PCheckerOutput/$testcase" || break
done
```

- `tcProofClosedSurvey`: the P `TVDiskGuidRecoveryActor` ignores late conflicting
  replies after starting history commit, waits for its acknowledgement, then
  publishes/commits the expected GUID and completes once.
- `tcProofFullSyncRetry`: `RESTART` or `NODATA`, a partial snapshot followed by
  interruption, unchanged `OldSyncState`, another full-sync request, then
  `SyncDone` only after local import acknowledgement. Scheduler retry is a
  small harness contract, not an execution of the C++ scheduler.
- `tcProofGuidConflict`: a minority `RemoteFinal` conflicts with retained identity;
  tentative `InProgress` cannot replace that identity.

- `tcProofGuidPublication`: successful durable writes in arbitrary order, first quorum
  notification, and optional post-quorum cancellation in both topologies. Failed
  requests are exercised separately by TRecoveryStorage, outside this harness.

These are checks of P components and their explicit harness assumptions.
They do not establish general C++ refinement. See [model scope](../README.md#scope-and-assumptions).
