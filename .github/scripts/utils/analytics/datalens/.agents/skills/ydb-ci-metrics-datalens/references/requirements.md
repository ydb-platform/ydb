# Requirements (do not revert)

A GitHub re-run keeps `run_id` and increments `run_attempt`. Duration, Gantt,
p90, and the Run selector treat each `(run_id, run_attempt)` as its own run.
Do not merge attempts.

## Duration dataset (`duration-ds`)

- Run column name is `ci_run_id` (Utf8). Not `run_id`. URL `?run_id=` binds only Gantt.
- Select `run_attempt`.
- `job_conclusion` comes from `github_job` + `name='job'`. Prefer `failure` then `cancelled` over lexical `MAX(conclusion)`. No `COALESCE(..., 'in_progress')`.
- Keep `github_step` rows except `Post %`. Do not limit the dataset to `Checkout` / `Build and test`.

## Gantt and picker datasets

`run_label` format (date first so the selector sorts lexicographically):

```
YYYY-MM-DD HH:MM 🟡 · <commit7> · <run_id> · #<attempt>
```

Icons: 🔴 failure, ⚫ cancelled, 🟢 success, 🟡 no job conclusion.
Do not put the icon first. Join `started_at` and `run_icon` on `(run_id, run_attempt)`.

## Charts

- Gantt: `parseRunRef`, `keepOneAttempt`. Choose run as `run_label` (selector), then `gantt_run`, then URL `run_id`.
- Hide GitHub wrapper step `Build and test` only when that job has `ya_phase` rows.
- Duration and p-diff points key: `ci_run_id|run_attempt|github_job_id`. p-diff sources must select `run_attempt`.
- Duration click action params: `gantt_run`, `gantt_attempt`, `pr_number`/`pr` from the point, empty `run_label`, `selected_id`. A short `{runId} · #{attempt}` is not a picker `run_label` and empties the PR selector. Do not dump every row field into dashboard params.
- Percentile buckets and interval bounds use UTC. Split `__interval___relative_-4d___relative_+0d` on the second `__relative_`.
- Apply Dates in `sources.js` before the row limit. Do not `console.log` Gantt params.

## Dashboard

- Job status selector is dataset-backed. Default `success` and `failure`. Do not preselect `in_progress`.
- Run selector (`selrun`) reads `run_label` from `pickers-ds`. Width 520px.
- Keep the Runners capacity tab (`captab01`). Zone selector default is `ru-central1-d`.
- Dates uses the same `interval` param on both tabs. Default last 4 days. Capacity `sources.js` apply it before the row limit.

## Publish

- `getEditorChart` without `revId` is published.
- Chart and dashboard publish: `mode=save`, then `mode=publish` with the draft `revId` on the entry. Do not use `entry.id` as a revision.
- Dataset update: `{datasetId, workbookId, data: {dataset}}`. A root `dataset` field is 400.
- Do not commit `objects/`.
