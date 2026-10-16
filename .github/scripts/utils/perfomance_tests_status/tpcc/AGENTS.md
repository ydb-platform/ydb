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

SQL:
- `queries/fetch_tpcc.sql` — metrics from `perfomance/tpcc`
- `queries/fetch_tpcc_reports.sql` — Allure URLs from `perfomance/olap/tests_results` (`TpccW*`)

Use [`fetch_daily.py`](fetch_daily.py) → [`../common/ydb_client.py`](../common/ydb_client.py) (YDBWrapper scan). **Do not use MCP.**

```bash
cd .github/scripts/utils/perfomance_tests_status/tpcc
# SA: env, --sa-key-file, or:
# eval "$(python3 ../duty_agent/dutyctl.py init-token --shell)"
python3 fetch_daily.py -o out/raw.json   # → out/raw.json + out/reports.json
```

Endpoint/DB: see `.github/config/ydb_qa_config.json`.

Defaults: [`report_config.json`](report_config.json) — lookback **`window_days` = 60** (~2 months).

`generate.py` joins reports onto points: `perfN` ↔ `oltp-perf-N`, `ydb_cli_{snapshot|serializable}_*`@WH ↔ `TpccW{WH}T0{Snapshot|Serializable}`, nearest timestamp ≤6h. Dive / chart click / Save context → `focus_run.report` for [`../duty_agent/`](../duty_agent/).

## 2. Generate HTML

```bash
cd .github/scripts/utils/perfomance_tests_status/tpcc
python3 generate.py \
  --input out/raw.json \
  --output out/tpcc-report.html --open
```

Auto-loads `out/reports.json` when present (`--reports-input` to override).  
`--since` optional (default: `window_days` from `report_config.json`). Override only if the user asks.

## 3. Deliver

- Path to `out/tpcc-report.html`
- Summary counts: **missing / broken / lat↑ / tpmC↓ / stale**
- Note window + that Now uses last completed vs prev-7 (median)

## Local unit tests

```bash
cd .github/scripts/utils/perfomance_tests_status/tpcc
python3 tests/test_classify_rules.py
```

Compare-delta paint rules (Python mirror of `template.html` `compareDeltaTpcc`): `classify_rules.py`.  
Now classification stays in `generate.py` (`classify_slice`).

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
4. **Duty context** — dive **Save context** / **Copy context** → `perf-duty-context/v1` for [`../duty_agent/`](../duty_agent/) (shared with OLAP)
5. **Known tickets** — open + closed issues with `<!-- perf-duty-match -->` in body (no label) joined to inbox by `affected`; open pills blue `#N · title`, closed grey `#N · closed · title`. Expand via `dutyctl annotate-issue`.
6. **wait_next_wave reports** — `generate.py` fetches public `duty_decisions/index.json` (from `dutyctl upload-report --no-issue`); matching suites show a **wait next** pill → analysis.md.
