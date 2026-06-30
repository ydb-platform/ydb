# compare_index_performance

A stress test that **compares vector / fulltext index workload performance**
between two `ydbd` binaries — a *baseline* and a *current* one — and reports the
throughput difference, statistical significance, and (optionally) CPU
flamegraphs.

It replaces the former `ydb/tests/stress/compare_index_performance.sh` script:
all orchestration now lives inside a single `py3test`, and every knob is passed
through `ya make --test-param`.

## What it does

For each enabled workload (`vector`, `fulltext`) and each iteration:

1. Starts an in-process `KiKiMR` cluster on the **baseline** `ydbd`, runs the
   workload binary against it, and parses the final `Total ... Txs/Sec` line.
2. Does the same on the **current** `ydbd`.
3. (Baseline and current runs are interleaved per iteration to cancel out
   machine drift.)

After all iterations it computes, per workload:

- **median**, **mean**, and **sample standard deviation** of Txs/Sec for each side,
- the relative **diff** (`current` vs `baseline`),
- a **3σ significance** verdict (pooled standard error; needs ≥ 2 iterations),

and writes a markdown report.

The two workloads are exposed as separate test methods — `test_vector` and
`test_fulltext` — so they can be selected with `--workload` or filtered with
`-F '*test_vector*'`.

## Binaries being compared

| side | resolved from (first match wins) |
|---|---|
| **current** | `compare_current_ydbd` (path) → `compare_current_ref` (S3) → locally built `ydbd` (build harness) |
| **baseline** | `compare_baseline_ydbd` (path) → S3 download by `compare_ref` |

Both S3 downloads use `compare_build_preset` and the bucket
`https://storage.yandexcloud.net/ydb-builds/<ref>/<preset>/ydbd`.

> **Note:** the test's own build provides a single preset for the *locally built*
> `ydbd`. `compare_build_preset` only selects which **prebuilt** binary is
> fetched from S3 (and labels the report). For a fair comparison both sides
> should use the same preset.

Supplying both `compare_ref` and `compare_current_ref` (or explicit paths) lets
you compare two arbitrary prebuilt binaries — e.g. `main` vs `main` (a noise
sanity check) or two different refs — **without a local build**.

## Parameters (`--test-param compare_*=...`)

| param | default | meaning |
|---|---|---|
| `compare_workload` | `all` | `all` / `vector` / `fulltext` |
| `compare_iterations` | `3` | iterations per workload (median reported; ≥ 2 for significance) |
| `compare_duration` | `60` | measured seconds per workload run |
| `compare_warmup` | `30` | vector warmup seconds before the measured select |
| `compare_rows` | `10000` | rows in the generated database |
| `compare_threads` | `10` | client threads |
| `compare_targets` | `1000` | number of query targets |
| `compare_ref` | `main` | baseline S3 ref (download URL + report label) |
| `compare_current_ref` | `` | current S3 ref; empty → use the locally built `ydbd` |
| `compare_build_preset` | `relwithdebinfo` | S3 preset for downloads + report label |
| `compare_baseline_ydbd` | `` | explicit baseline `ydbd` path (skips S3) |
| `compare_current_ydbd` | `` | explicit current `ydbd` path (skips build/S3) |
| `compare_baseline_feature_flags` | `` | comma-separated feature flags for the baseline cluster |
| `compare_current_feature_flags` | `` | comma-separated feature flags for the current cluster |
| `compare_baseline_table_service_config` | `` | comma-separated `key=value` for the baseline `table_service_config` |
| `compare_current_table_service_config` | `` | comma-separated `key=value` for the current `table_service_config` |
| `compare_flamegraph` | `` | `1`/`true` → collect CPU flamegraphs (see below) |
| `compare_perf_sudo` | `` | `1`/`true` → run `perf` under `sudo` |
| `compare_perf_freq` | `50` | `perf record -F` sampling frequency |

`table_service_config` values like `enable_vector_index_read=true` are parsed
into booleans; everything else is kept as a string. Feature flags are appended
to the cluster config; the fulltext test additionally enables
`enable_fulltext_index` on both sides automatically.

## Flamegraphs (optional)

With `compare_flamegraph=1`, every workload run is profiled with Linux `perf`
(`perf record --call-graph dwarf --pid <ydbd>`), and the profile is rendered with
the toolkit in `contrib/tools/flame-graph` (shipped to the sandbox via `DATA`):

- per-side, per-iteration CPU flamegraph: `<workload>_<side>_<i>.svg`
- a **differential** flamegraph between the baseline and current runs of each
  iteration: `<workload>_diff_<i>.svg` — frame widths = current profile, colored
  **red = hotter / blue = colder** vs baseline; plus a
  `<workload>_diff_before_<i>.svg` variant with baseline frame widths.

> `perf record --pid` of another process needs `kernel.perf_event_paranoid <= 1`
> or root. On a host with `perf_event_paranoid = 2`, pass `compare_perf_sudo=1`
> (passwordless sudo) or relax the sysctl. **If perf cannot profile, the test
> fails loudly** rather than silently skipping the flamegraph.
>
> Profiling adds overhead, so measured Txs/Sec are perturbed on every iteration
> when flamegraphs are on. Compare with-vs-without intentionally.

## Running

```bash
# Default quick run: locally built ydbd vs S3 `main` baseline, all workloads.
./ya make --build relwithdebinfo -tA \
  ydb/tests/stress/compare_index_performance/tests

# Compare two S3 refs (no local build, no network for the current side
# beyond the two downloads): main vs main, more iterations for a stable signal.
./ya make --build relwithdebinfo -tA \
  ydb/tests/stress/compare_index_performance/tests \
  --test-param compare_ref=main \
  --test-param compare_current_ref=main \
  --test-param compare_iterations=5

# Offline / fast smoke: same local binary on both sides, fulltext only.
./ya make --build relwithdebinfo -tA \
  ydb/tests/stress/compare_index_performance/tests \
  -F '*test_fulltext*' \
  --test-param compare_workload=fulltext \
  --test-param compare_iterations=1 \
  --test-param compare_duration=10 \
  --test-param compare_baseline_ydbd=$(readlink -f ydb/apps/ydbd/ydbd)

# A/B a feature flag on the current side, with flamegraphs.
./ya make --build relwithdebinfo -tA \
  ydb/tests/stress/compare_index_performance/tests \
  --test-param compare_workload=vector \
  --test-param compare_current_feature_flags=enable_vector_index \
  --test-param compare_current_table_service_config=enable_vector_index_read=true \
  --test-param compare_flamegraph=1 \
  --test-param compare_perf_sudo=1
```

## Results

Everything is written to the test's output directory:

```
ydb/tests/stress/compare_index_performance/tests/test-results/py3test/testing_out_stuff/
```

| file | content |
|---|---|
| `report_vector.md`, `report_fulltext.md` | markdown comparison tables (median, mean, σ, diff, 3σ verdict) |
| `<workload>_main_<i>.log`, `<workload>_current_<i>.log` | workload stdout (the `Total ... Txs/Sec` line is parsed from here); `.err` = stderr |
| `<workload>_<side>_<i>.svg` | per-side CPU flamegraph (when enabled) |
| `<workload>_diff_<i>.svg`, `<workload>_diff_before_<i>.svg` | differential flamegraphs (when enabled) |
| `<workload>_<side>_<i>.svg.perf.{data,log}` | raw `perf record` capture + perf tooling log |
| `ydbd-<ref>-<preset>` | cached S3-downloaded baseline/current binary |

Example report:

```
## Performance Comparison: `main` vs `current(main)` (vector select)

**Build preset:** `relwithdebinfo` | **Duration:** 60s per workload | **Iterations:** 1 (median reported)

| Workload | main (Txs/Sec) | current(main) (Txs/Sec) | Diff | 3σ significance |
|---|---|---|---|---|
| vector select | 50.167 | 49.983 | -0.4% | N/A (need >= 2 iterations) |
```

## CI

The `.github/workflows/compare_index_performance.yml` workflow runs this test
nightly and on `workflow_dispatch`, mapping its inputs to the `compare_*`
params. It posts the report files to the job summary and uploads the whole
`testing_out_stuff` tree (reports, logs, flamegraphs) as the `benchmark-results`
artifact.

## Layout

- `test_compare.py` — the test (orchestration, stats, report, flamegraphs).
- `ya.make` — `PY3TEST` that `DEPENDS` on the two workload programs
  (`ydb/tests/stress/vector_workload`, `ydb/tests/stress/fulltext_workload`) and
  ships `contrib/tools/flame-graph` via `DATA`.

The workload binaries themselves live in
[`vector_workload`](../../vector_workload) and
[`fulltext_workload`](../../fulltext_workload) and are reused unchanged.
