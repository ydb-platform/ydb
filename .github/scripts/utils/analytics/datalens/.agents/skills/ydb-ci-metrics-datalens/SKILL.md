---
name: ydb-ci-metrics-datalens
description: "Edit or publish the live CI metrics DataLens dashboard via the DataLens MCP; do not use this skill to add rows to analytics/ci_metrics or analytics/ci_capacity."
---

# CI metrics DataLens

[CI metrics](https://datalens.ru/135ob2ntmr0ok-ci-metrics), id `135ob2ntmr0ok`.
Org `bpfssp32nspchhs4df3c`. Workbook `ev4an7f2ncmk2`.
This file: https://github.com/ydb-platform/ydb/blob/main/.github/scripts/utils/analytics/datalens/.agents/skills/ydb-ci-metrics-datalens/SKILL.md
Do not add a Python DataLens client. Use only the input schemas from `describe_commands`.
Keep `ciagent01` on tab CI and `capagent01` on tab Runners capacity as the top full-width text row (`w` 36, `x` 0, `y` 0).
Their `data.text` is `To change this dashboard, send an agent the [instructions](<this file URL>).`
The same URL is `annotation.description`.

## Source map

| Responsibility | Source |
|---|---|
| DataLens API | MCP `@datalens-tech/mcp` ([README](https://github.com/datalens-tech/datalens-mcp)) |
| Job timings | `.github/scripts/utils/analytics/github_actions/` → `analytics/ci_metrics` |
| Runner snapshots | `.github/scripts/utils/analytics/github_actions/export_runner_capacity.py` → `analytics/ci_capacity` |
| Cron | `.github/workflows/collect_analytics_fast.yml` (hourly) |

## Objects

| Table | Writer | Who reads it |
|---|---|---|
| `analytics/ci_metrics` | `.github/scripts/utils/analytics/github_actions/ci_metrics.py` in the job, `.github/scripts/utils/analytics/github_actions/export_github_job_metrics.py` after the run | Tab **CI** |
| `analytics/ci_metrics_state` | `.github/scripts/utils/analytics/github_actions/export_github_job_metrics.py` | Nobody on the dash |
| `analytics/ci_capacity` | `.github/scripts/utils/analytics/github_actions/export_runner_capacity.py` | Tab **Runners capacity** |

Columns: `.github/scripts/utils/analytics/github_actions/README.md`.

Tab **CI** `citab01`. Dates `interval` default last 4 days. Job status default `success` and `failure`.

| Widget | Chart | Dataset | Meaning |
|---|---|---|---|
| `ciagent01` | — | — | Top row. Link to this skill. |
| `pdiffw01` | `svm2b1l1s15yb` | `z11tcvkyw21gi` | Two windows of the same step. |
| `ciselect01` | — | `z11tcvkyw21gi` | Step / Dates / branch / workflow / job filters. |
| `ckoutw01` | `egg8s3g477lmx` | `z11tcvkyw21gi` | Selected job or step over Dates. Click a point to open Gantt. |
| `cigantt01` | — | `nr9x5jtcy8xc6` | PR / Run selectors. |
| `ganttw01` | `466sto4vl396n` | `tvvegjgj046oc` | One `(run_id, run_attempt)` as a timeline. |

Tab **Runners capacity** `captab01`. Self-hosted ghrun and Compute quota, not `ubuntu-latest`. Dates same as CI. Zone `cap_zone` default `ru-central1-d`. Postcommit `cap_pc` default hide. Split RAM `cap_by` default workflow.

| Widget | Chart | Dataset | Meaning |
|---|---|---|---|
| `capagent01` | — | — | Top row. Same link as `ciagent01`. |
| `capby01` | — | — | Dates / Zone / Postcommit / Split RAM filters. |
| `capqueue01` | `8d0ow1ovos8ar` | `z11tcvkyw21gi` | p90 wait for a runner (`github_job` / `queue`). |
| `captight01` | `di5qp2d1kb64w` | `di5qgcbc4td0w` | Free instances / vCPU / RAM / SSD and how many preset VMs still fit. |
| `capuse01` | `gl8tsc0e1nnez` | `di5qgcbc4td0w` | RAM by workflow or branch on a UTC timeline. |
| `cappool01` | `bg3ondk342pku` | `di5qgcbc4td0w` | Latest snapshot: busy / idle / waiting, percents of the zone quota. |

`di5qgcbc4td0w` reads `analytics/ci_capacity`. Kinds `quota` / `fit` / `summary` / `runner` / `queue`. PK `(sampled_at, date, kind, name)`, TTL 1 year.

## Setup

Do this before any get/update. The human adds the MCP in the client; you cannot.

1. If DataLens tools (`list_commands`) are missing, tell the human to put this server in Cursor MCP config and stop until the tools appear. File: `~/.cursor/mcp.json` (all projects) or `<repo>/.cursor/mcp.json` (this repo only). Windows: `%USERPROFILE%\.cursor\mcp.json`. If the file already has `mcpServers`, add only the `datalens` key; do not replace the file. [Cursor MCP](https://cursor.com/docs/mcp) [README](https://github.com/datalens-tech/datalens-mcp)

```json
"datalens": {
  "command": "npx",
  "args": ["-y", "--prefer-online", "@datalens-tech/mcp@latest"],
  "env": { "DATALENS_ORG_ID": "bpfssp32nspchhs4df3c" }
}
```

2. If `yc` is not on `PATH`, propose `curl -sSL https://storage.yandexcloud.net/yandexcloud-yc/install.sh | bash` ([quickstart](https://yandex.cloud/docs/cli/quickstart)). After the human approves, run it. Then tell them to run `yc init` themselves (browser login). Do not run `yc init`.
3. After `yc` exists, run `yc iam create-token >/dev/null` and check only the exit code. Do not print the token. Never commit a token. If create-token fails, tell the human to run `yc init`. Static token: `DATALENS_YC_STATIC_AUTH=1` and `DATALENS_API_AUTH_HEADER="Bearer <iam-token>"`.
4. `getDashboard` `dashboardId` `135ob2ntmr0ok` `workbookId` `ev4an7f2ncmk2` `branch` `saved`. 401/403 means the human needs access to that workbook in org `bpfssp32nspchhs4df3c`. Stop.

## Trace the changed contract

1. `list_commands`, then `describe_commands` for the get/update (`getEditorChart` / `updateEditorChart`, `getDataset` / `updateDataset`, `getDashboard` / `updateDashboard`). Invoke with `invoke_read_command` or `invoke_write_command`: `command_name` plus `parameters` for the command inputs.
2. `getDashboard` with `dashboardId`, `workbookId`, `branch` `saved`. The MCP envelope is `{trust, data}` where `data` is a JSON string of `{entry:{data,revId,savedId,publishedId,annotation}}`. `updateDashboard` replaces the whole dash: send that `entry.data` (`counter`, `salt`, `tabs`, `settings`, and `schemeVersion` when get or a 400 names it; current value is 8), plus `entryId` and `meta`. Do not send a tab diff. Do not put `workbookId` or `dashboardId` on the update. If HTTP 400 names those keys, retry without them.
3. Change only the intended object. Chart: keep `entry.type` from get (`advanced-chart_node` here) and edit `data.sources` or `data.prepare`. Dataset: `dataset.sources[].parameters.subsql`. Dashboard: the widget row in Objects (`tabs[].items` and `tabs[].layout`). A new item needs `id`, `namespace` `default`, a layout cell `{i,h,w,x,y}`, and a higher `counter`. A text item is `type` `text`, `data.text` markdown, `data.autoHeight` true.
4. Each `(run_id, run_attempt)` is its own run. In Duration and p-diff dataset SQL the run id field is `ci_run_id` (DataLens alias), not `run_id`. Do not `COALESCE` a missing conclusion to `in_progress`.
5. Chart or dashboard: `update*` with `mode` `save`, take `entry.revId` or `savedId` from the parsed response, then the same body with `mode` `publish` and that id on `entry.revId`. Dataset: one `updateDataset`. Put the revision on the field `describe_commands` lists for that command.

If MCP `user-ydb-qa` is present, read tables with `ydb_query`. Otherwise ask the human.

## Validation

`getDashboard` with `branch` `published`. Confirm the edited field and `publishedId` equals `savedId`. If SQL changed and `ydb_query` is available, run the same select.
