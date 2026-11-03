# CI analytics

Тайминги CI в YDB: фазы сборки, job и step — таблица `analytics/ci_metrics`.
Снимок квоты Compute и пула ghrun — `analytics/ci_capacity`. Дашборд —
[DataLens](https://datalens.ru/135ob2ntmr0ok-ci-metrics).

Профили компиляции сюда не входят.

| Задача | Куда |
| --- | --- |
| Скопировать буфер в другой проект | [`collector/`](collector/README.md) |
| Добавить измерение / фазу / колонку в YDB CI | [`github_actions/`](github_actions/README.md#добавить-измерение) |
| Что уже лежит в таблице | [`github_actions/`](github_actions/README.md#что-уже-пишется) |
| Править живой дашборд | [`datalens/` skill](datalens/.agents/skills/ydb-ci-metrics-datalens/SKILL.md) |

## Как это работает

Два независимых пути в одну таблицу.

```mermaid
flowchart LR
  subgraph inJob [Внутри job]
    bash["build_and_test_ya: analytics start / analytics end"] --> cli["ci_metrics.py"]
    evlog["ya_evlog_phases.py"] --> cli
    cli --> jsonl[("JSONL на диске раннера")]
    jsonl -->|"flush / send"| ydb
  end
  subgraph after [После того как run закончился]
    cron["collect_analytics_fast.yml, раз в час"] --> exp["export_github_job_metrics.py"]
    gh["GitHub REST API"] --> exp
    exp --> ydb
    exp <--> st[("analytics/ci_metrics_state")]
  end
  ydb[("analytics/ci_metrics")] --> dl["DataLens"]
```

1. **Внутри job.** Шаги пишут JSONL и заливают закрытые строки. Так измеряются
   фазы `ya make` — того, чего нет в GitHub API.
2. **После run.** Тот же hourly job спрашивает у GitHub, сколько шли job и
   step, и пишет те же колонки. Чтобы не выгружать одно и то же каждый час,
   крон помнит «до какого момента уже залил» в `analytics/ci_metrics_state`.
   Это не измерения — дашборд туда не смотрит. В том же job
   `export_runner_capacity.py` пишет снимок квоты и пула в
   `analytics/ci_capacity`.

Чтобы на дашборде связать фазу с job: у фазы в `labels` лежит
`parent_span_id = job-456`, у строки этого job в колонке `span_id` то же
`job-456`.

`collector/` про GitHub не знает. `github_actions/` добавляет колонки Actions и
выгрузку.

## Первый запуск таблиц

```bash
python3 .github/scripts/utils/analytics/github_actions/provision_tables.py
```

Повторный запуск ничего не ломает: `CREATE TABLE IF NOT EXISTS`. В
`collect_analytics_fast.yml` вызывается перед выгрузкой.

## Тесты

```bash
python3 -m unittest discover -s .github/scripts/utils/tests/analytics/collector -p 'test_*.py'
python3 -m unittest discover -s .github/scripts/utils/tests/analytics/ci -p 'test_*.py'
```
