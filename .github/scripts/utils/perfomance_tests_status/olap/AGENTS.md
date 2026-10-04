# Agent instructions — OLAP Now report

Toolkit: `.github/scripts/utils/perfomance_tests_status/olap`

When the user asks for an OLAP / Clickbench / Tpch / Tpcds / suites performance report:

## Goal (Now-first)

Answer **what is red right now**, not historical dips:

1. **Failing / slower** — **last completed** suite run vs previous **7**
2. **Missing** — expected suite absent from latest **CiVersion × DbAlias** wave
3. **Stale** — no fresh wave on a focus cluster

History charts are deep-dive only (do not drive alerts).

## 1. Fetch suite-level data (required)

SQL: `queries/fetch_olap_suites.sql` (must include `Report`, `CiVersion`, `FailTests`).

Use [`fetch_daily.py`](fetch_daily.py) → [`../common/ydb_client.py`](../common/ydb_client.py) (YDBWrapper scan). **Do not use MCP / plain ydb CLI `yql`** (truncates).

```bash
cd .github/scripts/utils/perfomance_tests_status/olap
# SA: env, --sa-key-file, or:
# eval "$(python3 ../duty_agent/dutyctl.py init-token --shell)"
python3 fetch_daily.py --mode suites -o out/raw.json          # last 30d
```

Endpoint/DB: see `.github/config/ydb_qa_config.json`.  
Auth: `CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS` / `--sa-key-file` / duty `init-token`.

Defaults: [`report_config.json`](report_config.json) — lookback **`window_days` = 30**.

## 2. Per-query run series (required for slow-query drill-down)

SQL: `queries/fetch_olap_test_runs.sql` — **one point per launch** (datetime), not day AVG.
Legacy day buckets: `fetch_olap_test_daily.sql` via `fetch_daily.py --mode daily`.

```bash
cd .github/scripts/utils/perfomance_tests_status/olap
python3 fetch_daily.py -o out/raw_test_runs.json   # default mode=runs, last 30d
```

Also ok: `out/raw_tests.json` — fallback names without history.

Runs dump is loaded only for hot/ok suites (not embedded wholesale into HTML).
Query Now alert = **last completed run** vs previous **7 runs** (same as suite).

## 3. Generate HTML

```bash
cd .github/scripts/utils/perfomance_tests_status/olap
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
- Note window + that Now uses last completed vs prev-7; stale is wall-clock; fail pills show timeout/diff/other

## Rules (v1)

| Signal | Rule |
|--------|------|
| Now | **last completed run** / slice (dive shows last 7 for context) |
| Baseline | previous 7 runs |
| Slower (hard) | thr=`max(+10%, 2×noise%)`; last run > base; hard if pct ≥ `max(+25%, thr)` → slow; soft (thr ≤ pct < hard) → **watch**; **>3×** → broken |
| Noise | `noise% = pstdev(prev7) / median(prev7) · 100` |
| Failing | last run fail_rate **≥10% always fail** (suite / query / JS); **≥50% → broken**, 10–50% → failing (not broken badge) |
| No data | per-query mart null-template (`Success=0` + `Color` NULL) → kind `nodata`, not fail |
| Wave | `CiVersion × DbAlias` |
| Expected suite | present in ≥50% of waves for that DbAlias over ~14d |
| Missing | expected suite absent from last wave (if wave age ≥6h; else only dropouts vs previous wave) |
| Stale | no wave ≥36h on focus DbAlias |
| Scope | `main`/`trunk`/`stable-*`/`prestable-*` + focus DbAliases; inbox = hot only |
| Branch dimension | UI filter; heatmap/counters/waves = `Branch × DbAlias`; wave = `CiVersion × Branch × DbAlias` |
| Date interval | From/To (started day); filters inbox, heatmap cells, last wave, history charts; Reset → full `--since..until` |
| Wave view | UI toggle **finished** (default) = last completed run in heatmap/inbox; **all** = latest wave state (prefer in_progress when suite not in current wave yet) |
| Compare wave | Per-cluster select from `wave_list`; heatmap `was → now` both reclassified vs **same prev7 before compare day** (like TPC-C). **Paint** only on significant hard changes; fail↑+slow↓ → **mixed**; watch/soft-only → solid now. Heatmap click sets Issue from now-side status (compare-aware). Dive: Last-runs click = **focus (now)**; compare is separate. Charts mark **now** (blue) and **cmp** (amber). |
| History window | `history_max_points` in `report_config.json` (default 100). Pure OLAP rules: `classify_rules.py` + `tests/test_classify_rules.py` (TPC-C compare → `../tpcc/`). |

Focus DbAliases: `sas_big/small`, `cloud_slonnn_64/128`, `vla_big/small`, `vla_3_node`.

Branch for cloud often from `CiBranch` (`trunk`) when `Branch`/`Version` empty.

## UI layers

1. **Now** — counters + Db×family heatmap + problem inbox  
2. **Deep dive** — click inbox row → last runs (clickable focus), query catalog, report links  
3. **History** — “Show history” charts (suite-level only); query charts highlight now/compare runs  
4. **Duty context** — dive **Save context** / **Copy context** → `perf-duty-context/v1` for [`../duty_agent/`](../duty_agent/) (shared with TPC-C)
5. **Known tickets** — `generate.py` searches **open + closed** issues for `perf-duty-match` in body (no label); joins to inbox by `affected.suite`/`db`; open pills blue `#N · title`, closed grey `#N · closed · title`. Agent expands `affected` via `dutyctl annotate-issue` when the same fingerprint hits another suite/query.
6. **wait_next_wave reports** — `generate.py` also fetches public `duty_decisions/index.json` (from `dutyctl upload-report --no-issue`); matching `now_runs` show a **wait next** pill → analysis.md instead of red `no ticket`.
