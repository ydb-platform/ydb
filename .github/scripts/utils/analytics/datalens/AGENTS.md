# CI metrics DataLens

These instructions apply to `.github/scripts/utils/analytics/datalens/`.

Live DataLens is the source. Do not commit `objects/`.
From the repo root: `python3 .github/scripts/utils/analytics/datalens/dl.py pull`,
then edit `objects/`, then `publish --apply`.
Each `(run_id, run_attempt)` is its own run. Do not merge attempts.
Steps: [`.agents/skills/ydb-ci-metrics-datalens/SKILL.md`](.agents/skills/ydb-ci-metrics-datalens/SKILL.md).
