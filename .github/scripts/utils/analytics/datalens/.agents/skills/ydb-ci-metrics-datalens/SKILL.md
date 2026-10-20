---
name: ydb-ci-metrics-datalens
description: "Pull, edit, or publish the live CI metrics DataLens dashboard. Chart JS and SQL are not in git: pull first. Adding rows to analytics/ci_metrics or analytics/ci_capacity belongs in github_actions."
---

# CI metrics DataLens

Live: https://datalens.ru/135ob2ntmr0ok-ci-metrics
`PY=.github/scripts/utils/analytics/datalens/dl.py` from the repo root.
Do not invent RPC payloads. Do not commit `objects/`.

## Source map

| Responsibility | Source |
|---|---|
| Object ids | `.github/scripts/utils/analytics/datalens/manifest.json` |
| Tabs, charts, tables | [dashboard.md](references/dashboard.md) |
| Pull / diff / publish | `.github/scripts/utils/analytics/datalens/dl.py` |
| Job timings | `.github/scripts/utils/analytics/github_actions/` → `analytics/ci_metrics` |
| Runner snapshots | `.github/scripts/utils/analytics/github_actions/export_runner_capacity.py` → `analytics/ci_capacity` |

## Trace the changed contract

1. `python3 "$PY" pull` (or `pull <name>`).
2. Edit `.github/scripts/utils/analytics/datalens/objects/`. Each `(run_id, run_attempt)` is its own run. Duration column is `ci_run_id`, not `run_id`. Do not `COALESCE` a missing conclusion to `in_progress`. Keep tab `captab01` and zone default `ru-central1-d`. Dates `interval` default last 4 days.
3. `python3 "$PY" diff` then `python3 "$PY" publish --apply <name>`.
4. `python3 "$PY" pull <name>` and confirm the edit. Leave `objects/` untracked.

Auth: `DATALENS_TOKEN` or `yc iam create-token`. Never commit a token.
Read tables with MCP `user-ydb-qa` `ydb_query`.

## Validation

```bash
PY=.github/scripts/utils/analytics/datalens/dl.py
python3 "$PY" pull
python3 "$PY" diff
```
