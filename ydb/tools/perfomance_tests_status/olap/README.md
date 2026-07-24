# OLAP Now report

Now-first status for OLAP suites: **missing / failing / slower** on the last
few runs, with deep dive + history on demand.

**Path:** `ydb/tools/perfomance_tests_status/olap`  
Sibling of [`../tpcc`](../tpcc).

## Quick start

1. Suite dump with `Report` + `CiVersion`: [`queries/fetch_olap_suites.sql`](queries/fetch_olap_suites.sql) → `out/raw.json`
2. Daily per-query (scan via `ydb_wrapper`, no CLI 1000-row cap):

```bash
cd ydb/tools/perfomance_tests_status/olap
python3.12 -m venv .venv && .venv/bin/pip install -r requirements.txt
export CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS=/path/to/sa-key.json
./.venv/bin/python fetch_daily.py --since 2026-06-08 -o out/raw_test_daily.json
```

3. Optional query names: [`queries/fetch_olap_test_issues.sql`](queries/fetch_olap_test_issues.sql) → `out/raw_tests.json`
4. Generate:

```bash
python3 generate.py \
  --input out/raw.json \
  --tests-input out/raw_tests.json \
  --tests-daily-input out/raw_test_daily.json \
  --since 2026-06-08 \
  --output out/olap-report.html --open
```

## Now rules

| Rule | Value |
|------|--------|
| Now | last **3** runs |
| Baseline | previous **7** runs |
| Slower | YdbSumMeans **≥ +10%**; **>3×** → broken |
| Failing | last ≥50%, or ≥2/3 runs ≥10% elevated |
| Wave / missing | `CiVersion × DbAlias`; expected = ≥50% of waves / 14d |
| Scope | `main`/`trunk` + focus DbAliases; inbox = hot only |
| Suites | Clickbench*, Tpch*, Tpcds*, UploadTpch*, WorkloadManager* |

UI: counters → heatmap → problem inbox → deep dive → Show history.

See [`AGENTS.md`](AGENTS.md).
