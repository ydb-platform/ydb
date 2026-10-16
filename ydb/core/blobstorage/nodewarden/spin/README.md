Three-node SPIN model: tree convergence, quorum, role exclusion and operation fencing under timeouts and disconnects.
Assumes fixed configs, finite faults, eventual timely replies, weak process fairness and an online working-config host when present.
Uses cyclic peer selection and abstract timers; omits subscription handshakes, config creation/persistence and bridge semantics.
Bootstrap checks only first-tree assembly and still has failing cases with repeating binding cycles; C++ convergence is not proven.

Requires Python 3, SPIN and GCC. Run from the repository root:

```bash
python3 ydb/core/blobstorage/nodewarden/spin/run_three_node.py
python3 ydb/core/blobstorage/nodewarden/spin/run_three_node.py --suite bootstrap
```

Results and traces go to the printed temporary directory. Expected counterexamples validate broken variants and reachability.
Per-process limits: `--timeout` (seconds), `--memory-limit` (MB); exhausted limits report `INCOMPLETE`.
