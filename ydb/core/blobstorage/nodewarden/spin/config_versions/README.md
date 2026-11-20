# Configuration versions

[distconf.pml](distconf.pml) checks recovery, FIFO metadata writes, proposal- and
committed-quorum acknowledgment, refill and replacement of an old same-generation conflict.
[check.py](check.py) has candidate, compatibility, legacy and recovery suites; compatibility
compares participant rules with the same history under `ALL_OLD=1`, not binaries.

Run from the repository root with Python 3, SPIN and a C compiler:

```bash
python3 ydb/core/blobstorage/nodewarden/spin/config_versions/check.py --suite all --decompose --timeout 300
python3 ydb/core/blobstorage/nodewarden/spin/config_versions/check.py --suite legacy --scenario 30 31 32 --check liveness --legacy-repair 0
python3 ydb/core/blobstorage/nodewarden/spin/config_versions/check.py --suite recovery --output /tmp/distconf-recovery
python3 ydb/core/blobstorage/nodewarden/spin/config_versions/check_admission.py --timeout 90
python3 ydb/core/blobstorage/nodewarden/spin/config_versions/check_tree.py --decompose --timeout 90
```

The second command is a negative control: disabling legacy-conflict repair must
prevent convergence. Admission controls must expose unsafe bypasses or blocked
expansion. Only explicitly declared counterexamples are accepted. Timeout,
resource exhaustion, incomplete search or a missing witness never count as PASS.
Runners print the directory containing model snapshots, commands and traces.

Every newly chosen configuration must extend earlier acknowledged configurations.
Recovery variants cover quorum turnover and interrupted persistence.

The primary model is nonbridge: `N=3/Q=2`, one disk per node, fixed membership,
bounded fault prefixes and a stable suffix under weak process fairness. It checks
same-generation safety and convergence in these histories. It does not prove
bootstrap, membership changes, erasure geometry or the full C++ tree/protocol.
Interrupted expansion may require an external Replace retry in the implementation.

[targeted_admission.pml](targeted_admission.pml) separately checks challenge fencing
and expansion `1→2`, `3→6`, `3→7` in one DC, without full recovery or bridge.
The combined [tree model](../three_node_convergence.pml) uses `CONFIG_VERSIONS=1`.
Its default bounded cases cover stable bindings, committed propagation and an
empty intermediary; full race/disconnect/MAIN Replace searches remain incomplete.
`--decompose` requires both goal closure and fair reachability to pass.
