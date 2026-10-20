---
name: ydb-ci-metrics-datalens
description: "Edit or publish the live CI metrics DataLens dashboard via the DataLens MCP; do not use this skill to add rows to analytics/ci_metrics or analytics/ci_capacity."
---

# CI metrics DataLens

Do not add a Python DataLens client. Use only the input schemas from `describe_commands`.

## Source map

| Responsibility | Source |
|---|---|
| Tabs, ids, tables, defaults | [dashboard.md](references/dashboard.md) |
| DataLens API | MCP `@datalens-tech/mcp` ([README](https://github.com/datalens-tech/datalens-mcp)) |
| Job timings | `.github/scripts/utils/analytics/github_actions/` → `analytics/ci_metrics` |
| Runner snapshots | `.github/scripts/utils/analytics/github_actions/export_runner_capacity.py` → `analytics/ci_capacity` |

## Trace the changed contract

1. If DataLens MCP tools are missing, stop. Tell the human to add `@datalens-tech/mcp` with `DATALENS_ORG_ID` from [dashboard.md](references/dashboard.md). Default auth is `yc iam create-token`. A static token needs `DATALENS_YC_STATIC_AUTH=1` and `DATALENS_API_AUTH_HEADER`. Never commit a token.
2. `list_commands`, then `describe_commands` for the get/update of chart, dataset, or dashboard (`getEditorChart` / `updateEditorChart`, `getDataset` / `updateDataset`, `getDashboard` / `updateDashboard`). Call the `invoke_tool` named there (`invoke_read_command` or `invoke_write_command`).
3. Get the live object by the id in [dashboard.md](references/dashboard.md). Edit only the intended field: chart `data.sources` or `data.prepare`; dataset `dataset.sources[].parameters.subsql`; dashboard `data.tabs`. Keep the defaults listed in [dashboard.md](references/dashboard.md).
4. Each `(run_id, run_attempt)` is its own run. In Duration and p-diff dataset SQL the run id field is `ci_run_id` (DataLens alias), not `run_id`. Do not `COALESCE` a missing conclusion to `in_progress`.
5. Chart or dashboard: write with `mode` `save`, take `revId`, then `mode` `publish` with that `revId`. If `updateDashboard` returns HTTP 400 mentioning `workbookId` or `dashboardId`, retry without those keys. Dataset: one `updateDataset` using the revision field from `describe_commands` (`revision_id` or `rev_id`, not `revId`).

If MCP `user-ydb-qa` is present, read tables with `ydb_query`. Otherwise ask the human.

## Validation

Get the published object and confirm the edited field. If SQL changed and `ydb_query` is available, run the same select.
