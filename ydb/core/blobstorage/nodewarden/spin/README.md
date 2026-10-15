The primary compatibility-candidate checks use [config_versions/distconf.pml](config_versions/distconf.pml)
and [config_versions/check.py](config_versions/check.py). The preserved compact model passed 22 bounded scenarios
and all 44 safety/liveness checks; its scope and compatibility checks are described in
[config_versions/README.md](config_versions/README.md).

`three_node_convergence.pml` models tree convergence, quorum, role exclusion and operation fencing under timeouts and disconnects.
The default mode retains fixed configs, finite faults, eventual timely replies, weak process fairness and an online working-config host when present.
It uses cyclic peer selection and abstract timers; subscription handshakes and bridge semantics are omitted.
Bootstrap checks only first-tree assembly and still has failing cases with repeating binding cycles; C++ convergence is not proven.

`CONFIG_VERSIONS=1` adds configuration recovery, disk persistence, metadata references, routed scatter/gather,
publication and causal refill of blank nodes to the same tree model. This mode uses a three-node, two-vote quorum
and allows the root to change. Its bounded checks and limitations are described in [config_versions/README.md](config_versions/README.md).

Requires Python 3, SPIN and GCC. Run from the repository root:

```bash
python3 ydb/core/blobstorage/nodewarden/spin/config_versions/check.py
python3 ydb/core/blobstorage/nodewarden/spin/run_three_node.py --suite baseline
python3 ydb/core/blobstorage/nodewarden/spin/run_three_node.py --suite quorum
python3 ydb/core/blobstorage/nodewarden/spin/run_three_node.py --suite bootstrap
python3 ydb/core/blobstorage/nodewarden/spin/config_versions/check_tree.py
```

Results and traces go to the printed temporary directory. Expected counterexamples validate broken variants and reachability.
Per-process limits: `--timeout` (seconds), `--memory-limit` (MB); exhausted limits report `INCOMPLETE`.
