# TPC-C Now report

Now-first status for TPC-C: **broken / lat↑ / tpmC↓ / missing** on the last
few runs, with deep dive + history on demand.

**Path:** `ydb/tools/perfomance_tests_status/tpcc`  
Sibling of [`../olap`](../olap).

## Quick start

1. Dump: [`queries/fetch_tpcc.sql`](queries/fetch_tpcc.sql) → `out/raw.json`
2. Generate:

```bash
cd ydb/tools/perfomance_tests_status/tpcc
python3 generate.py --input out/raw.json --since 2026-07-01 --output out/tpcc-report.html --open
```

## Now rules

| Rule | Value |
|------|--------|
| Now | last **3** runs |
| Baseline | previous **7** runs |
| Lat↑ | NewOrder p90 **≥ +10%**; **>3×** → broken |
| Broken | lat **≥ 30000** (cap) |
| tpmC↓ | tpmC **≤ −10%** |
| Wave / missing | day × Branch × Cluster; expected = ≥50% of day-waves / 14d |
| Scope | `main` + stables with enough points; inbox = hot only |

UI: counters → heatmap → problem inbox → deep dive → Show history.

See [`AGENTS.md`](AGENTS.md).
