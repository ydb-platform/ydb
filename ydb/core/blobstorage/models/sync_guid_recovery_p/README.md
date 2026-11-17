# SyncGuidRecovery baseline model

This commit models the existing protocol before the repair.
`tcStaleMetadata`, `tcDelayedSelfGuid`, and `tcFalseFirstRun`, and their
`tcMirror3dc` counterparts, are expected to fail their monitors.
The NoWipes, RetainedSource, WipedWithWitnesses, and FreshGroup controls
are expected to pass. PChecker results are bounded checks, not C++ proofs.

Requires P 3.1.0 and .NET 8.

```sh
p compile --pproj Recovery.pproj --mode pchecker --outdir PGenerated
p check PGenerated/PChecker/net8.0/SyncGuidRecovery.dll --testcase tcStaleMetadata --schedules 50 --seed 1 --max-steps 10000 --fail-on-maxsteps
```
