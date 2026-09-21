# CI analytics client

Клиент отправки CI-метрик в ydb-qa. Модель как в продуктовых SDK: `start` / `end` / `track` / `send`.

Живёт здесь, рядом с `utils/metrics` и `utils/dashboard`, а не в общей `.github/scripts/analytics/` (там витрины тестов, mute, issues). `YDBWrapper` по-прежнему берётся из `.github/scripts/analytics/ydb_wrapper.py`.

Базовый контекст подставляется сам: колонки workflow/job/commit/PR плюс `github.*` / `github.event.*` из env и `$GITHUB_EVENT_PATH`. Из любого workflow достаточно one-liner — отдельный сборщик не нужен:

```bash
# Длительность: start до работы, send после (duration считается сам)
python3 .github/scripts/utils/analytics/ci_metrics.py start my_step \
  --source my_wf --attr cache_mode=dist_cache --runner
# ... work ...
python3 .github/scripts/utils/analytics/ci_metrics.py send --conclusion success --usage

# Уже готовое измерение / gauge / событие
python3 .github/scripts/utils/analytics/ci_metrics.py track wait \
  --source my_wf --duration-sec 12 --json '{"lock":"schema"}'
python3 .github/scripts/utils/analytics/ci_metrics.py track ydbd_size \
  --kind gauge --value 123456 --unit bytes --source my_wf

# Большой снимок (не duration): kind=info, имя build_info, тело в labels.payload
python3 .github/scripts/utils/analytics/ci_metrics.py track build_info \
  --kind info --source my_wf --json-file modules.json
python3 .github/scripts/utils/analytics/ci_metrics.py send
```

`--json` / `--json-file` с плоским объектом обогащают атрибуты текущей записи. Список компонентов / `nodes` / `modules` / `cpp_compilation_times` уходит в `labels.payload` отдельной строкой `build_info` (`kind=info`). `send --json-file modules.json` после `start` закроет span и допишет этот снимок в тот же пакет.

`--runner` один раз снимает инвентарь хоста (`labels["runner.inventory"]`: `boot_time`, `cpu_count`, `cpu_model`, `mem_total_bytes`, `disk_total_bytes`, `disks`) и кладёт его в кэш (`$CI_RUNNER_INFO_FILE` или `$RUNNER_TEMP/ci_runner_info.json`). Повторные вызовы с `--runner` переиспользуют кэш. `--usage` каждый раз заново снимает загрузку (`labels["runner.usage"]`: `cpu_pct`, `loadavg_*`, `mem_*`, `disk_*`) только для этого события. Без флагов `/proc` не читается. Nightly `ydbd_cached_build` и clean `ydbd_clean_build` передают `--runner` на `start` и `--usage` на `end`.

Или composite action:

```yaml
- uses: ./.github/actions/analytics_track
  with:
    command: start
    name: my_step
    source: my_wf
    labels: cache_mode=dist_cache
    runner: true
# ... work ...
- uses: ./.github/actions/analytics_track
  with:
    command: send
    conclusion: success
    json-file: modules.json
    usage: true
```

`track` / `start` только пишут в JSONL. `send` закрывает открытые span'ы и отправляет пакет в ydb-qa (`analytics/ci_metrics`) через YDBWrapper.

| Файл | Роль |
| --- | --- |
| `ci_metrics.py` | клиент `start` / `end` / `track` / `send` |
| `runner_info.py` | инвентарь раннера (кэш) и usage-снимок |
| `export_ya_nodes.py` | сырые узлы evlog / clang time-trace + `build_info` |
| `export_github_job_metrics.py` | queue / job / GHA-step после факта (отдельный collector) |

Что пишется:

- PR-check: GitHub job/step (`export_github_job_metrics.py`) + in-job ya phases (`graph_compare`, `ya_make_try_*`, dashboard, s3)
- Nightly-Build (remote cache): `ydbd_cached_build` вокруг `ya make`, `ydbd_size`, evlog-узлы + `build_info` (`source=nightly_build`, `cache_mode=dist_cache`)
- ydbd-clean-build (без кеша): те же метрики — `ydbd_clean_build`, `ydbd_size`, evlog-узлы + `build_info` (`source=clean_build`, `cache_mode=none`)
- Build-analytics-run: сырые `time_s` / `mean_compilation_time_s` + `inclusion_count` из `html_cpp_impact/output.json` и `html_headers_impact/output.json` (`source=build_bloat`), плюс evlog (`source=build_analytics`)

`export_ya_nodes.py` пишет одну строку на узел (`labels.node_kind` = `Compile` / `Link` / `Header`). Для заголовков `value` — mean time, `labels.inclusion_count` — как в output.json. Агрегаты — SQL; сравнение PR-check с target — следующий этап.
