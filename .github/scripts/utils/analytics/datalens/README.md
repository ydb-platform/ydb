# CI metrics DataLens

Live [CI metrics](https://datalens.ru/135ob2ntmr0ok-ci-metrics). Chart JS and SQL
are not stored here. Catalog: [dashboard.md](.agents/skills/ydb-ci-metrics-datalens/references/dashboard.md).
Agent steps: [SKILL.md](.agents/skills/ydb-ci-metrics-datalens/SKILL.md).

```bash
PY=.github/scripts/utils/analytics/datalens/dl.py
python3 "$PY" list
python3 "$PY" pull
python3 "$PY" diff
python3 "$PY" publish --apply gantt
```
