# Distconf SPIN checks

Requires Python 3, SPIN and a C compiler. Run from the repository root:

```bash
python3 ydb/core/blobstorage/nodewarden/spin/config_versions/check.py --suite all --decompose --timeout 300
python3 ydb/core/blobstorage/nodewarden/spin/config_versions/check_tree.py --decompose --timeout 90
python3 ydb/core/blobstorage/nodewarden/spin/silent_parent/check.py --timeout 90
python3 ydb/core/blobstorage/nodewarden/spin/run_three_node.py --suite baseline
python3 ydb/core/blobstorage/nodewarden/spin/test_spin_runner.py
```

| Model | Scope |
|---|---|
| [Configuration versions](config_versions/README.md) | Recovery, persistence, Replace, refill, old/new rules; separate admission checks. |
| [Tree](three_node_convergence.pml) | Three-node bindings, quorum, timeout and disconnect fencing; `CONFIG_VERSIONS=1` adds configuration operations. |
| [Silent parent](silent_parent/README.md) | Heartbeat, rebinding, late callbacks and one queued bridge-proposal history. |

These are bounded protocol models, not a proof of the complete C++ implementation
or general bridge consensus. The primary configuration model has fixed membership
and `N=3/Q=2`. Broader tree/race searches remain incomplete; the tree runner's
`bootstrap` suite has known binding cycles. A reachability witness is not a proof
that every execution succeeds.

Runners print the artifact directory with snapshots, commands, results and traces.
Declared broken variants must produce their expected counterexamples; timeout,
resource exhaustion and incomplete searches never count as PASS. Use `--help` for
case selection and limits; `config_versions/check.py` has no `--memory-limit`.
Shared runner helpers can be checked with Python alone using the last command.
