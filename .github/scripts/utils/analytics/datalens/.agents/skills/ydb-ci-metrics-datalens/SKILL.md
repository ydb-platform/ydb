---
name: ydb-ci-metrics-datalens
description: "Pull, edit, or publish the live CI metrics DataLens dashboard. Chart JS and SQL are not in git: pull first. Adding rows to analytics/ci_metrics or analytics/ci_capacity belongs in github_actions."
---

# CI metrics DataLens

Live dashboard: https://datalens.ru/135ob2ntmr0ok-ci-metrics
CLI: `.github/scripts/utils/analytics/datalens/dl.py` from the repo root.
Do not invent RPC payloads; use `dl.py`. Do not commit `objects/`.

## Source map

| Responsibility | Source |
|---|---|
| Object ids | `.github/scripts/utils/analytics/datalens/manifest.json` |
| Tabs, charts, tables | [dashboard.md](references/dashboard.md) |
| Must-not-break rules | [requirements.md](references/requirements.md) |
| Pull, diff, publish, YDB | `.github/scripts/utils/analytics/datalens/dl.py` |
| Job and step timings | `.github/scripts/utils/analytics/github_actions/` → `analytics/ci_metrics` |
| Runner quota snapshots | `.github/scripts/utils/analytics/github_actions/export_runner_capacity.py` → `analytics/ci_capacity` |

## Trace the changed contract

`PY=.github/scripts/utils/analytics/datalens/dl.py`

1. `python3 "$PY" pull` (or `pull <name>`) so `objects/` matches live.
2. Edit the pulled SQL or JS under `.github/scripts/utils/analytics/datalens/objects/`. Each `(run_id, run_attempt)` is its own run; do not merge attempts.
3. `python3 "$PY" check` then `python3 "$PY" diff`.
4. `python3 "$PY" publish --apply <name>`. `dl.py` saves, then publishes with the draft `revId`.
5. After publish, `python3 "$PY" pull <name>` and confirm the edit is in the pulled file.
6. Leave `objects/` untracked.

A name that is not in `manifest.json`: `python3 "$PY" create dataset <name> --title "..." --apply` needs `objects/datasets/<name>/query.sql` first, then `create chart <name> --title "..." --dataset <name> --apply`. A new tab: `create tab --tab-id <id> --title "..." --widget widgetId|title|chartName|height --apply` after `pull dashboard`. Do not create a name that already has an id.

To read `analytics/ci_metrics`: `python3 "$PY" table-info`, then `query` / `describe`, or MCP `user-ydb-qa` `ydb_query`. `describe analytics/ci_capacity` for the runner table. Do not use `test_results/test_runs_column` as the only source for a fresh PR-check job.

Auth: `DATALENS_TOKEN` or `yc iam create-token`. YDB: `YDB_TOKEN`, `YDB_SA_KEY_FILE`, or `yc`. Never commit a token.

## Validation

```bash
PY=.github/scripts/utils/analytics/datalens/dl.py
python3 "$PY" pull
python3 "$PY" check
python3 "$PY" diff
```
