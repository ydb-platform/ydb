# CI metrics DataLens

Live dashboard [CI metrics](https://datalens.ru/135ob2ntmr0ok-ci-metrics).
Chart JS, dataset SQL, and dashboard JSON are not stored here. Pull them,
edit, publish. Measurements stay in [`../github_actions/`](../github_actions/README.md).

What each tab and chart is: [dashboard.md](.agents/skills/ydb-ci-metrics-datalens/references/dashboard.md).
How an agent must work: [`AGENTS.md`](AGENTS.md) and
[the skill](.agents/skills/ydb-ci-metrics-datalens/SKILL.md).

## CLI

From the repo root:

```bash
PY=.github/scripts/utils/analytics/datalens/dl.py
python3 "$PY" list
python3 "$PY" pull                 # live → objects/ (do not commit)
python3 "$PY" check
python3 "$PY" diff
python3 "$PY" publish --apply gantt duration-ds
python3 "$PY" table-info
python3 "$PY" query --preset run
python3 "$PY" describe analytics/ci_metrics
```

`create` is only for a name that is not already in `manifest.json`.
Auth for DataLens: `DATALENS_TOKEN` or `yc iam create-token`.
Auth for YDB: `YDB_TOKEN`, `YDB_SA_KEY_FILE`, or `yc`.
