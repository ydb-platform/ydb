# Agent instructions — TPC-C report

Toolkit: `ydb/tools/perfomance_tests_status/tpcc`

When the user asks for a TPC-C / tpcc / perf latency report:

## 1. Fetch data

Use MCP `user-ydb-qa` → `ydb_query` with SQL from `queries/fetch_tpcc.sql`.
Replace `{{SINCE}}` with `YYYY-MM-DDT00:00:00Z`.

Save the full tool JSON to `out/raw.json` under this directory.

## 2. Generate HTML

```bash
cd ydb/tools/perfomance_tests_status/tpcc
python3 generate.py --input out/raw.json --since YYYY-MM-DD --output out/tpcc-report.html --open
```

## 3. Deliver

- Path to `out/tpcc-report.html`
- Short summary: regressions / broken; top latency Δ

Do not invent dashboard baselines; do not change the +10% lat rule unless asked.
