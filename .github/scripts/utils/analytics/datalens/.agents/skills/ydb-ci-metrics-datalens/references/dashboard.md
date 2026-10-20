# Live dashboard catalog

[CI metrics](https://datalens.ru/135ob2ntmr0ok-ci-metrics), id `135ob2ntmr0ok`.
Ids also live in `.github/scripts/utils/analytics/datalens/manifest.json`.
Cron: `.github/workflows/collect_analytics_fast.yml`.

## Tables

| Table | Writer | Who reads it |
|---|---|---|
| `analytics/ci_metrics` | `.github/scripts/utils/analytics/github_actions/ci_metrics.py` in the job, `export_github_job_metrics.py` after the run | Tab **CI**: Duration, Gantt, p-diff, Queue p90, PR/Run pickers |
| `analytics/ci_metrics_state` | `export_github_job_metrics.py` | Nobody on the dash. Export watermark only. |
| `analytics/ci_capacity` | `export_runner_capacity.py` | Tab **Runners capacity**: Free quota, Who holds the RAM, Runners |

Schema: `.github/scripts/utils/analytics/github_actions/README.md`. Read rows with MCP `user-ydb-qa` `ydb_query`.

## Tab CI (`citab01`)

Job and step time. Dataset SQL reads `analytics/ci_metrics`.

| Manifest | Widget title | Id | Dataset | Meaning |
|---|---|---|---|---|
| `duration` | Duration | `egg8s3g477lmx` | `duration-ds` `z11tcvkyw21gi` | Duration of the selected job or step over Dates. Click a point to open Gantt. |
| `gantt` | Gantt | `466sto4vl396n` | `gantt-ds` `tvvegjgj046oc` | One `(run_id, run_attempt)` as a timeline. |
| `pdiff` | p diff | `svm2b1l1s15yb` | `duration-ds` | Compare two windows of the same step. |
| `pickers-ds` | PR / Run pickers | `nr9x5jtcy8xc6` | itself | Values for the PR and Run selectors. |

Shared Dates `interval` default last 4 days. Job status default `success` and `failure`.

## Tab Runners capacity (`captab01`)

Self-hosted ghrun pool and Yandex Cloud Compute quota. Not GitHub-hosted `ubuntu-latest`.

| Manifest | Widget title | Id | Dataset | Meaning |
|---|---|---|---|---|
| `capacity-queue` | Queue p90 | `8d0ow1ovos8ar` | `duration-ds` | p90 of how long a job waited for a runner (`github_job` / `queue` in `ci_metrics`). Follows Dates. |
| `capacity-tight` | Free quota | `di5qp2d1kb64w` | `capacity-ds` `di5qgcbc4td0w` | Free instances / vCPU / RAM / SSD and how many preset VMs still fit. |
| `capacity-use` | Who holds the RAM | `gl8tsc0e1nnez` | `capacity-ds` | RAM in use by workflow or target branch on a UTC timeline. |
| `capacity-pool` | Runners | `bg3ondk342pku` | `capacity-ds` | Latest snapshot: busy / idle / waiting, percents of the zone quota. |

Selectors: Dates (`interval`, same as CI, default last 4 days), Zone (`cap_zone`, default `ru-central1-d`), Postcommit jobs (`cap_pc`, default hide), Split RAM by (`cap_by`, workflow or branch).

`capacity-ds` reads `analytics/ci_capacity`. Snapshot kinds: `quota`, `fit`, `summary`, `runner`, `queue`. PK `(sampled_at, date, kind, name)`, TTL 1 year.
