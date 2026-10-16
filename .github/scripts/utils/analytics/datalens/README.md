# CI metrics DataLens

Source of the live dashboard [CI metrics](https://datalens.ru/135ob2ntmr0ok-ci-metrics).
Charts, datasets, and dashboard JSON live here. Measurements into
`analytics/ci_metrics` stay in [`../github_actions/`](../github_actions/README.md).

## Layout

| Path | What |
| --- | --- |
| `manifest.json` | org, workbook, object ids |
| `objects/charts/*/sources.js` + `prepare.js` | editor-chart JS |
| `objects/datasets/*/query.sql` | dataset YQL |
| `objects/dashboard/entry.json` | tabs, selectors, params |
| `dl.py` | pull / diff / publish / YDB |

Auth for DataLens: `DATALENS_TOKEN` or `yc iam create-token`.
Auth for YDB: `YDB_TOKEN`, `YDB_SA_KEY_FILE`, or `yc`. Endpoint comes from
`.github/config/ydb_qa_config.json`.

## CLI

```bash
PY=.github/scripts/utils/analytics/datalens/dl.py
python3 "$PY" list
python3 "$PY" pull                 # live → objects/
python3 "$PY" check                # local invariants, no network
python3 "$PY" diff                 # local vs live
python3 "$PY" publish              # dry-run
python3 "$PY" publish --apply gantt duration-ds
python3 "$PY" sql gantt-ds
python3 "$PY" table-info
python3 "$PY" query --preset run
python3 "$PY" describe analytics/ci_metrics
```

`getEditorChart` without `revId` is the **published** revision. Publish is
save, then publish with the draft `revId`. `updateDataset` wraps the body as
`{datasetId, workbookId, data: {dataset}}`.

## Agent instructions

Read [`AGENTS.md`](AGENTS.md) and
[`.agents/skills/ydb-ci-metrics-datalens/SKILL.md`](.agents/skills/ydb-ci-metrics-datalens/SKILL.md).
Invariants: [`references/requirements.md`](.agents/skills/ydb-ci-metrics-datalens/references/requirements.md).
