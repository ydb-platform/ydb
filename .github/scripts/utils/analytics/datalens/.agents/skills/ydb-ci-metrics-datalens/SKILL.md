---
name: ydb-ci-metrics-datalens
description: "Edit or publish the live CI metrics DataLens dashboard via the DataLens MCP; do not use this skill to add rows to analytics/ci_metrics or analytics/ci_capacity."
---

# CI metrics DataLens

[CI metrics](https://datalens.ru/135ob2ntmr0ok-ci-metrics), id `135ob2ntmr0ok`.
Org `bpfssp32nspchhs4df3c`. Workbook `ev4an7f2ncmk2`.
Do not add a Python DataLens client. Use only the input schemas from `describe_commands`.

## Source map

| Responsibility | Source |
|---|---|
| DataLens API | MCP `@datalens-tech/mcp` ([README](https://github.com/datalens-tech/datalens-mcp)) |
| Job timings | `.github/scripts/utils/analytics/github_actions/` → `analytics/ci_metrics` |
| Runner snapshots | `.github/scripts/utils/analytics/github_actions/export_runner_capacity.py` → `analytics/ci_capacity` |
| Cron | `.github/workflows/collect_analytics_fast.yml` |

## Objects

| Table | Writer | Who reads it |
|---|---|---|
| `analytics/ci_metrics` | `.github/scripts/utils/analytics/github_actions/ci_metrics.py` in the job, `.github/scripts/utils/analytics/github_actions/export_github_job_metrics.py` after the run | Tab **CI** |
| `analytics/ci_metrics_state` | `.github/scripts/utils/analytics/github_actions/export_github_job_metrics.py` | Nobody on the dash |
| `analytics/ci_capacity` | `.github/scripts/utils/analytics/github_actions/export_runner_capacity.py` | Tab **Runners capacity** |

Schema: `.github/scripts/utils/analytics/github_actions/README.md`.

Tab **CI** `citab01`. Dates `interval` default last 4 days. Job status default `success` and `failure`.

| Name | Title | Id | Dataset | Meaning |
|---|---|---|---|---|
| `duration` | Duration | `egg8s3g477lmx` | `duration-ds` `z11tcvkyw21gi` | Selected job or step over Dates. Click a point to open Gantt. |
| `gantt` | Gantt | `466sto4vl396n` | `gantt-ds` `tvvegjgj046oc` | One `(run_id, run_attempt)` as a timeline. |
| `pdiff` | p diff | `svm2b1l1s15yb` | `duration-ds` | Two windows of the same step. |
| `pickers-ds` | PR / Run pickers | `nr9x5jtcy8xc6` | itself | PR and Run selectors. |

Tab **Runners capacity** `captab01`. Self-hosted ghrun and Compute quota, not `ubuntu-latest`. Dates same as CI. Zone `cap_zone` default `ru-central1-d`. `cap_pc` default hide. `cap_by` workflow or branch.

| Name | Title | Id | Dataset | Meaning |
|---|---|---|---|---|
| `capacity-queue` | Queue p90 | `8d0ow1ovos8ar` | `duration-ds` | p90 wait for a runner (`github_job` / `queue`). |
| `capacity-tight` | Free quota | `di5qp2d1kb64w` | `capacity-ds` `di5qgcbc4td0w` | Free instances / vCPU / RAM / SSD and how many preset VMs still fit. |
| `capacity-use` | Who holds the RAM | `gl8tsc0e1nnez` | `capacity-ds` | RAM by workflow or branch on a UTC timeline. |
| `capacity-pool` | Runners | `bg3ondk342pku` | `capacity-ds` | Latest snapshot: busy / idle / waiting, percents of the zone quota. |

`capacity-ds` reads `analytics/ci_capacity`. Kinds `quota` / `fit` / `summary` / `runner` / `queue`. PK `(sampled_at, date, kind, name)`, TTL 1 year.

## Trace the changed contract

1. If DataLens MCP tools are missing, stop. Tell the human to add `@datalens-tech/mcp` with `DATALENS_ORG_ID=bpfssp32nspchhs4df3c`. Default auth is `yc iam create-token`. A static token needs `DATALENS_YC_STATIC_AUTH=1` and `DATALENS_API_AUTH_HEADER`. Never commit a token.
2. `list_commands`, then `describe_commands` for the get/update of chart, dataset, or dashboard (`getEditorChart` / `updateEditorChart`, `getDataset` / `updateDataset`, `getDashboard` / `updateDashboard`). Call the `invoke_tool` named there (`invoke_read_command` or `invoke_write_command`).
3. Get the live object by id above. Edit only the intended field: chart `data.sources` or `data.prepare`; dataset `dataset.sources[].parameters.subsql`; dashboard `data.tabs`. Keep the defaults in Objects.
4. Each `(run_id, run_attempt)` is its own run. In Duration and p-diff dataset SQL the run id field is `ci_run_id` (DataLens alias), not `run_id`. Do not `COALESCE` a missing conclusion to `in_progress`.
5. Chart or dashboard: write with `mode` `save`, take `revId`, then `mode` `publish` with that `revId`. If `updateDashboard` returns HTTP 400 mentioning `workbookId` or `dashboardId`, retry without those keys. Dataset: one `updateDataset` using the revision field from `describe_commands` (`revision_id` or `rev_id`, not `revId`).

If MCP `user-ydb-qa` is present, read tables with `ydb_query`. Otherwise ask the human.

## Validation

Get the published object and confirm the edited field. If SQL changed and `ydb_query` is available, run the same select.
