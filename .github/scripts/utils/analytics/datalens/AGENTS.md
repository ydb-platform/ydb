# CI metrics DataLens

These instructions apply to `.github/scripts/utils/analytics/datalens/`.

Do not rebuild the dashboard. Edit files under `objects/` and use `dl.py`.
Treat `(run_id, run_attempt)` as one run. Do not rename Duration `ci_run_id`
back to `run_id`. Do not `COALESCE` a missing job conclusion to `in_progress`.
Publish charts only as save, then publish with the draft `revId`.
Steps: [`.agents/skills/ydb-ci-metrics-datalens/SKILL.md`](.agents/skills/ydb-ci-metrics-datalens/SKILL.md).
