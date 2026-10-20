# CI metrics DataLens

These instructions apply to `.github/scripts/utils/analytics/datalens/`.

Live DataLens is the source. Do not commit `objects/`.
From the repo root: `python3 .github/scripts/utils/analytics/datalens/dl.py pull`,
then edit `objects/`, then `publish --apply`.
New datasets and charts: `dl.py create` with a name that is not already in
`manifest.json`. Do not post create RPC bodies by hand.
Each `(run_id, run_attempt)` is its own run. Do not merge attempts. Do not
rename Duration `ci_run_id` back to `run_id`. Do not `COALESCE` a missing job
conclusion to `in_progress`.
Steps: [`.agents/skills/ydb-ci-metrics-datalens/SKILL.md`](.agents/skills/ydb-ci-metrics-datalens/SKILL.md).
