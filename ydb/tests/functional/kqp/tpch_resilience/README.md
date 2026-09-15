# TPC-H query recovery under compute node restarts

This test starts eight storage processes with block-4-2 erasure and eight compute
processes serving a separate database. It imports the generated TPC-H SF1 dataset
into column tables and verifies all eight table row counts. Four workers repeatedly
execute the repository's Q15 query through Query Service without query retries.

The two isolated scenarios use SIGTERM (controlled stop) and SIGKILL respectively.
Each restarts compute slots 1, 4, and 8, one at a time, keeping each slot down for
15 seconds while load continues. Storage processes stay running.

The healthy window requires 100% success for at least 30 seconds and 20 attempts.
During faults, failures are allowed, but at least one successful query must start
and finish entirely within a confirmed node-down interval. After all nodes return,
the original driver and session pool have up to 180 seconds to recover. The final
window requires 100% success for at least 60 seconds and 40 attempts, including
progress from every worker. Outstanding attempts are drained at phase boundaries;
a late failure still counts against its original measurement window.

Run both scenarios (Linux; large test, 32 GiB RAM and 8 CPUs):

```bash
./ya make --build relwithdebinfo -tA ydb/tests/functional/kqp/tpch_resilience 2>&1 | tail -100
```

Select one scenario with `-F '*test_kill'` or `-F '*test_controlled_stop'`.
Use `--test-retries 3` to repeat the tests for stability checks.

Each test's output directory contains `attempts.jsonl`, `summary.json`, and CLI
import/initialization stdout and stderr. Attempts include worker and session node
IDs, submission/execution/completion times from the monotonic clock, phase, failure
stage, and status. The summary records outage intervals and per-phase counts,
status histograms, and completion rates. Throughput is diagnostic only. Cluster
logs are retained by the standard harness.
