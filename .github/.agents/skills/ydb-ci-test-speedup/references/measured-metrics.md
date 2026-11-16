# Measured metrics (try_1)

Replace `<N>` with the PR-check Actions run id.

```text
https://storage.yandexcloud.net/ydb-gh-logs/ydb-platform/ydb/PR-check/<N>/ya-x86-64/try_1/tests_metrics/dashboard.html
https://storage.yandexcloud.net/ydb-gh-logs/ydb-platform/ydb/PR-check/<N>/ya-x86-64/try_1/tests_metrics/trace_chunks.csv
https://storage.yandexcloud.net/ydb-gh-logs/ydb-platform/ydb/PR-check/<N>/ya-x86-64/try_1/tests_metrics/dashboard_suggestions.json
https://storage.yandexcloud.net/ydb-gh-logs/ydb-platform/ydb/PR-check/<N>/ya-x86-64/try_1/tests_metrics/resources_monitor.jsonl
```

| Metric | How |
|--------|-----|
| makespan | `max(run_end_sec) − min(run_start_sec)` in `trace_chunks.csv` |
| sum chunk wall | sum `duration_report_sec` |
| per-suite wall / cores | `duration_report_sec`, `cores`, `cpu_sec_report` |
| peak / p95 | `peak_self_cpu_cores_during_suite`, `p95_cores` in `dashboard_suggestions.json` |
| cgroup timeline | `resources_monitor.jsonl` (`kind=test` when custom ya is on) |

## PSI and Delayed by

- High **PSI some** on a chunk = that cgroup **waited** (victim), not “delayed others”.
- On the measured dashboard, open Top by PSI I/O / CPU / mem, select a chunk:
  **Delayed by** lists neighbors ranked by disk/CPU/RAM use at the victim's
  peak-PSI sample (precomputed in `measured_tests[].contention`).
- A neighbor with high throughput and low PSI is usually the hog; the selected
  chunk with high PSI and low own throughput is the victim.
- Host `disk_busy` near 100% with many waiting victims means oversubscription,
  not healthy utilization.

## Lab branch

`dev/ci-measured-lab` carries custom ya-bin (`custom_ya_bin_s3_uri`),
`--account-test-cgroup`, nested cgroup setup
(`.github/scripts/custom_ya_launch.sh`, `.github/scripts/ya_cgroup_exec.sh`),
and the sharded measured dashboard (including Delayed by). Those paths are on
the lab tip, not on `main`. For research A/B, apply the same lab tip to
baseline and treatment (skill step 2b), then keep cold harness knobs.
