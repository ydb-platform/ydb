# GitHub data in internal YDB

`sync_github_data_to_inner.py` runs in the `github_data_inner_sync` job of
`collect_analytics_fast.yml`, after the regular cloud exports (every 30 minutes).
It copies the dashboard projection of both configured source tables (`issues`
and `pull_requests`) into `github_data/issues` and `github_data/pull_requests` in
the internal database. Cloud exports and their credentials remain unchanged.

## Setup

Before enabling the job, configure the repository secret `YDB_INNER_TOKEN` with
an access token allowed to create the `github_data` directory, create row
tables and bulk-upsert in the target database. Ensure that the analytics runner
can reach the internal cluster. Missing credentials or failed writes fail
the separate sync job without stopping the other analytics jobs.

Defaults match the existing CI duty DataLens connection:

- `YDB_INNER_ENDPOINT`: `grpc://vault-klg.ydb.yandex.net:2135`
- `YDB_INNER_DATABASE`: `/vault-klg/kikimr/prod/ydb-qa-db-inner`

These two defaults can be overridden through repository variables with the same
names. Source configuration uses the existing `YDB_QA_CONFIG` and
`CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS`.

## Data semantics

The internal row tables contain the object number, URL, title, state, repository,
labels and creation/update/closure/export timestamps. They intentionally omit
large bodies and other fields not used by the dashboard. URL is the primary key;
multiple source rows for project memberships collapse to the latest `exported_at`.

Every run reads all source rows, including old open objects, closed/merged objects
and rows without labels. This bootstraps old reviews and propagates label removal
and closure. Writes are idempotent batches and a failed run can be retried in full.
The two tables/batches are not published atomically. Source deletions are not
propagated; the existing source exporters retain objects and update their state.

`exported_at` remains the source GitHub export timestamp. `synced_at` records the
copy time, not a new GitHub refresh. Copying does not fix stale source records or
provide the time an AI review label was applied.

Filter dashboard counts with `state = "OPEN"`; inspect label names for
`ai_review`, `ai_review_in_process`, and `ai_review_failed`. Compute totals from
all matching rows, not from a `LIMIT 200` detail list.

## Validation

```sh
python3 -m unittest discover -s .github/scripts/analytics/tests -p 'test_sync_github_data_to_inner.py'
```
