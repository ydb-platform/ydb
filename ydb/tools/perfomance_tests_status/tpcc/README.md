# TPC-C performance report

Pull rows from `perfomance/tpcc`, build an interactive HTML report
(regressions + bar charts + branch compare).

**Path:** `ydb/tools/perfomance_tests_status/tpcc`

## Quick start

1. Fetch data with [`queries/fetch_tpcc.sql`](queries/fetch_tpcc.sql)
   (MCP `user-ydb-qa` / `ydb_query`, or any YDB client).
2. Save JSON as `out/raw.json`.
3. Generate:

```bash
cd ydb/tools/perfomance_tests_status/tpcc
python3 generate.py --input out/raw.json --since 2026-07-13 --output out/tpcc-report.html --open
```

## Rules

| Rule | Value |
|------|--------|
| Latency regression | NewOrder p90 **> +10%** vs early baseline |
| Latency watch | **+7…10%** |
| tpmC regression | drop beyond noise-based tol (usually ±3%) |
| Cap | `lat90 >= 32768` → broken |
| Baseline window | first **2 days** after `--since` |
| Recent window | from day 8 after `--since` (or `--recent-from`) |
| Branch compare | recency-weighted mean, weight `0.5^(age_days/2)` |

## Ask an LLM

> Сгенерируй TPC-C report с 13.07

Follow [`AGENTS.md`](AGENTS.md).
