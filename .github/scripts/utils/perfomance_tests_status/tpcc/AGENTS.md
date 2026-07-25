# Agent instructions — TPC-C Now report

Toolkit: `.github/scripts/utils/perfomance_tests_status/tpcc`

When the user asks for a TPC-C / tpcc / lat90 / tpmC performance report:

## Goal (Now-first)

Answer **what is red right now**, not historical dips:

1. **Broken / Lat↑ / tpmC↓** — **last completed** run vs previous **7** (median)
2. **Missing** — expected slice absent from latest **day × Branch × Cluster** wave
3. **Stale** — no fresh day-wave on a focus cluster

History charts are deep-dive only (do not drive alerts).

## 1. Fetch data

SQL: `queries/fetch_tpcc.sql` (replace `{{SINCE}}`).

Prefer MCP `user-ydb-qa` → `ydb_query`, or ydb CLI. Save MCP-shaped JSON:

```json
{"result_sets":[{"columns":[...],"rows":[...]}]}
```

→ `out/raw.json`

Endpoint/DB: see `.github/config/ydb_qa_config.json`.

Default lookback: **~1 month** (`DEFAULT_WINDOW_DAYS = 30`).  
Fetch with `{{SINCE}}` = today − 30d (`YYYY-MM-DDT00:00:00Z`).

## 2. Generate HTML

```bash
cd .github/scripts/utils/perfomance_tests_status/tpcc
python3 generate.py \
  --input out/raw.json \
  --output out/tpcc-report.html --open
```

`--since` optional (default: today − 30 days). Override only if the user asks.

## 3. Deliver

- Path to `out/tpcc-report.html`
- Summary counts: **missing / broken / lat↑ / tpmC↓ / stale**
- Note window + that Now uses last completed vs prev-7 (median)

## Rules (v1)

| Signal | Rule |
|--------|------|
| Now | **last completed run** / slice (dive shows last `DISPLAY_RUNS`=3 for context) |
| Baseline | previous 7 runs (median) |
| History window | full `--since` window (day-grain; `HISTORY_MAX_POINTS=0`). OLAP caps run-history at 100. |
| Lat↑ | NewOrder p90 ≥ **+10%** vs baseline; **>3×** → broken |
| Broken | `lat ≥ 30000` (cap) on last run |
| tpmC↓ | tpmC ≤ **−10%** vs baseline |
| Wave | calendar day × Branch × Cluster |
| Expected slice | present in ≥50% of day-waves for that Branch×Cluster over ~14d |
| Missing | expected slice absent from last completed day-wave |
| Stale | no day-wave ≥36h on focus cluster |
| Scope | `main` + `stable-*` / `prestable-*` / `26*` with enough points; inbox = hot only |
| Slice | `Branch × Cluster × run_type@warehouses` |
| Date interval | From/To; filters inbox, heatmap, history; Reset → full `--since..until` |
| Wave view | UI toggle **finished** (default) = last completed run in heatmap/inbox; **all** = latest day-wave state (prefer in_progress when slice not in today yet) |
| Compare wave | Per-cluster select = past day-waves (≥1 suite). Dive cards = **this slice’s** history days (incomplete cluster days OK — other families may show no data). Selecting cmp recalculates baseline / Δ% / heatmap. Alert prev7 kept as secondary note. |
| Heatmap compare | Both sides = `%` vs the **same** prev7 baseline (before compare day). Paint only on **significant** hard changes: cross lat↑(+10%) / tpmC↓(−10%) / broken, or already-hard and moved another full ≥10pp. Opposing lat↔tpmC → **mixed**. Watch/noise → solid now color. Dive Δ still pairwise vs compare run. |

## UI layers

1. **Now** — compact top-filters (Wave + Branch/dates) · counters · heatmap · inbox
2. **Deep dive** — compare-run cards + charts on row expand
3. **History** — tpmC + lat by run/commit date
