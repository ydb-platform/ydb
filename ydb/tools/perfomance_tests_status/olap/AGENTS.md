# Agent instructions — OLAP Now report

Toolkit: `ydb/tools/perfomance_tests_status/olap`

When the user asks for an OLAP / Clickbench / Tpch / Tpcds / suites performance report:

## Goal (Now-first)

Answer **what is red right now**, not historical dips:

1. **Failing / slower** — **last completed** suite run vs previous **7**
2. **Missing** — expected suite absent from latest **CiVersion × DbAlias** wave
3. **Stale** — no fresh wave on a focus cluster

History charts are deep-dive only (do not drive alerts).

## 1. Fetch suite-level data (required)

SQL: `queries/fetch_olap_suites.sql` (must include `Report`, `CiVersion`, `FailTests`).

Prefer **ydb CLI scan** (`table query execute -t scan`) or chunked `yql` —
plain `yql`/`sql` often caps ~1000 rows. MCP truncates large results.

```bash
# merge chunks into MCP-shaped JSON:
# {"result_sets":[{"columns":[...],"rows":[...]}]}
# → out/raw.json
```

Endpoint/DB: see `.github/config/ydb_qa_config.json`.  
Auth: `CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS` / `--sa-key-file`.

Default lookback: **last 30 days** (`--since` / `fetch_daily.py --days 30`).

## 2. Per-query run series (required for slow-query drill-down)

SQL: `queries/fetch_olap_test_runs.sql` — **one point per launch** (datetime), not day AVG.
Legacy day buckets: `fetch_olap_test_daily.sql` via `fetch_daily.py --mode daily`.

**Do not use plain ydb CLI `yql`** — it truncates. Use scan via `ydb_wrapper`:

```bash
cd ydb/tools/perfomance_tests_status/olap
python3.12 -m venv .venv && .venv/bin/pip install -r requirements.txt
export CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS=/path/to/sa-key.json
./.venv/bin/python fetch_daily.py -o out/raw_test_runs.json   # default: last 30d
```

Also ok: `out/raw_tests.json` — fallback names without history.

Runs dump is loaded only for hot/ok suites (not embedded wholesale into HTML).
Query Now alert = **last completed run** vs previous **7 runs** (same as suite).

## 3. Generate HTML

```bash
cd ydb/tools/perfomance_tests_status/olap
python3 generate.py \
  --input out/raw.json \
  --tests-input out/raw_tests.json \
  --tests-daily-input out/raw_test_runs.json \
  --output out/olap-report.html --open
# default --since = today-30d; override with --since YYYY-MM-DD
```

## 4. Deliver

- Path to `out/olap-report.html`
- Summary counts: **missing / failing / slower / stale**
- Note window + that Now uses last-3 vs prev-7 runs

## Rules (v1)

| Signal | Rule |
|--------|------|
| Now | **last completed run** / slice (dive still shows last 3 for context) |
| Baseline | previous 7 runs |
| Slower (hard) | thr=`max(+10%, 2×noise%)`; last run > base; hard if pct ≥ `max(+25%, thr)` → slow; soft (thr ≤ pct < hard) → **watch**; **>3×** → broken |
| Noise | `noise% = pstdev(prev7) / median(prev7) · 100` |
| Failing | last run fail_rate ≥50% → broken; ≥10% (elevated vs baseline) → fail/regression |
| No data | per-query mart null-template (`Success=0` + `Color` NULL) → kind `nodata`, not fail |
| Wave | `CiVersion × DbAlias` |
| Expected suite | present in ≥50% of waves for that DbAlias over ~14d |
| Missing | expected suite absent from last wave (if wave age ≥6h; else only dropouts vs previous wave) |
| Stale | no wave ≥36h on focus DbAlias |
| Scope | `main`/`trunk`/`stable-*`/`prestable-*` + focus DbAliases; inbox = hot only |
| Branch dimension | UI filter; heatmap/counters/waves = `Branch × DbAlias`; wave = `CiVersion × Branch × DbAlias` |
| Date interval | From/To (started day); filters inbox, heatmap cells, last wave, history charts; Reset → full `--since..until` |
| Wave view | UI toggle **finished** (default) = last completed run in heatmap/inbox; **all** = latest wave state (prefer in_progress when suite not in current wave yet) |

Focus DbAliases: `sas_big/small`, `cloud_slonnn_64/128`, `vla_big/small`, `vla_3_node`.

Branch for cloud often from `CiBranch` (`trunk`) when `Branch`/`Version` empty.

## UI layers

1. **Now** — counters + Db×family heatmap + problem inbox  
2. **Deep dive** — click inbox row → last runs, bad queries, report links  
3. **History** — “Show history” charts (suite-level only)
