---
name: ydb-ci-metrics-datalens
description: "Edit, pull, or publish the CI metrics DataLens dashboard, charts, and datasets in .github/scripts/utils/analytics/datalens. Adding measurements to analytics/ci_metrics belongs in github_actions."
---

# CI metrics DataLens

Live dashboard: https://datalens.ru/135ob2ntmr0ok-ci-metrics
Workbook `ev4an7f2ncmk2`, org `bpfssp32nspchhs4df3c`.
CLI: `.github/scripts/utils/analytics/datalens/dl.py`.
Do not invent RPC payloads; the CLI already has the working shapes.

## Source map

Paths are relative to `.github/scripts/utils/analytics/datalens/`.

| Responsibility | Source |
|---|---|
| Object ids | `manifest.json` |
| Duration / Gantt / p-diff JS | `objects/charts/<name>/{sources,prepare}.js` |
| Dataset YQL | `objects/datasets/<name>/query.sql` |
| Selectors and layout | `objects/dashboard/entry.json` |
| Pull, diff, publish, YDB | `dl.py` |
| Must-not-break rules | [requirements.md](references/requirements.md) |
| Table that feeds the dash | `../github_actions/ci_metrics.py`, `../github_actions/README.md` |

## Trace the changed contract

1. `python3 dl.py pull` (or `pull <name>`) so `objects/` matches live.
2. Edit SQL or JS in `objects/`. Keep `(run_id, run_attempt)` as one run.
3. `python3 dl.py check` then `python3 dl.py diff`.
4. `python3 dl.py publish --apply <name>`. Charts: save, then publish with the draft `revId`. Datasets: `data.dataset`, not a root `dataset`.
5. After publish, `python3 dl.py pull <name>` and confirm your marker is in the published object (`getEditorChart` without `revId` is published).

To read `analytics/ci_metrics`: `python3 dl.py table-info`, then `query` / `describe`, or MCP `user-ydb-qa` `ydb_query`. Do not use `test_results/test_runs_column` as the only source for a fresh PR-check job.

Auth: `DATALENS_TOKEN` or `yc iam create-token`. YDB: `YDB_TOKEN`, `YDB_SA_KEY_FILE`, or `yc`. Never commit a token.

## Validation

```bash
python3 -m unittest discover -s .github/scripts/utils/tests/analytics/datalens -p 'test_*.py'
python3 .github/scripts/utils/analytics/datalens/dl.py check
```
