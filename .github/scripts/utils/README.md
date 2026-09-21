# CI scripts utils

Скрипты для сбора метрик и построения дашбордов.

## Структура

- **metrics/** — сбор метрик во время ya make
  - `monitor_resources.py` — CPU, RAM, disk I/O (JSONL; optionally appends `ram_usage.txt` with `--ram-usage-file`)

- **dashboard/** — построение дашбордов
  - `runner_footprint.py` — загрузка provisioned лимитов из `.github/config/runners_footprints.yml`
  - `analyze_resources.py` — resources_report.html (CPU/RAM/disk по данным monitor)
  - **test_metrics/** — дашборд тестов (evlog + report + overlay метрик)
    - `tests_resource_dashboard.py` — основной скрипт
    - `dashboard_*.py` — рендеринг HTML
    - `ya_make_requirements.py` — чтение REQUIREMENTS из ya.make

Конфиг раннеров: `.github/config/runners_footprints.yml` — provisioned maximum (vcpu/ram) по build preset. Фактическое потребление — из `resources_monitor.jsonl`; на дашборде красная линия = monitor, фиолетовая пунктирная = лимит из конфига.

Общие CI-метрики пишет `.github/scripts/analytics/ci_metrics.py` (модель как в продуктовых SDK: `start` / `end` / `track` / `send`). Базовый контекст (workflow, run/job id, commit, preset) подставляется сам. Из любого workflow достаточно one-liner — скрипт-сборщик не нужен:

```bash
# Длительность: start до работы, send после (duration считается сам)
python3 .github/scripts/analytics/ci_metrics.py start my_step \
  --source my_wf --attr cache_mode=dist_cache
# ... work ...
python3 .github/scripts/analytics/ci_metrics.py send --conclusion success

# Уже готовое измерение / gauge / событие
python3 .github/scripts/analytics/ci_metrics.py track wait \
  --source my_wf --duration-sec 12 --json '{"lock":"schema"}'
python3 .github/scripts/analytics/ci_metrics.py track ydbd_size \
  --kind gauge --value 123456 --unit bytes --source my_wf

# Большой снимок (не duration): kind=info, имя build_info, тело в labels.payload
python3 .github/scripts/analytics/ci_metrics.py track build_info \
  --kind info --source my_wf --json-file modules.json
python3 .github/scripts/analytics/ci_metrics.py send
```

`--json` / `--json-file` с плоским объектом обогащают атрибуты текущей записи. Список компонентов / `nodes` / `modules` / `cpp_compilation_times` и т.п. уходит в `labels.payload` отдельной строкой `build_info` (`kind=info`), а не в duration. `send --json-file modules.json` после `start` закроет span и допишет этот снимок в тот же пакет.

Или composite action:

```yaml
- uses: ./.github/actions/analytics_track
  with:
    command: start
    name: my_step
    source: my_wf
    labels: cache_mode=dist_cache
# ... work ...
- uses: ./.github/actions/analytics_track
  with:
    command: send
    conclusion: success
    json-file: modules.json
```

`track` / `start` только пишут в JSONL. `send` закрывает открытые span'ы и отправляет пакет в ydb-qa (`analytics/ci_metrics`) через YDBWrapper. Пачку узлов собирает `export_ya_nodes.py` (строка на узел + один `build_info` со всеми компонентами). Агрегаты — SQL; сравнение PR-check с target — следующий этап.

Что пишется:

- PR-check: GitHub job/step (`export_github_job_metrics.py`) + in-job ya phases (`graph_compare`, `ya_make_try_*`, dashboard, s3)
- Nightly-Build (remote cache): `ydbd_cached_build` вокруг `ya make`, `ydbd_size`, evlog-узлы + `build_info` (`source=nightly_build`, `cache_mode=dist_cache`)
- ydbd-clean-build (без кеша): те же метрики — `ydbd_clean_build`, `ydbd_size`, evlog-узлы + `build_info` (`source=clean_build`, `cache_mode=none`)
- Build-analytics-run: сырые `time_s` / `mean_compilation_time_s` + `inclusion_count` из `html_cpp_impact/output.json` и `html_headers_impact/output.json` (`source=build_bloat`), плюс evlog (`source=build_analytics`). HTML treemap по-прежнему в S3 и в `code-agility/*` через `ydb_upload.py`

Модули: `.github/scripts/analytics/export_ya_nodes.py` пишет одну строку на узел (`labels.node_kind` = `Compile` / `Link` / `Header`). Для заголовков `value` — mean time, `labels.inclusion_count` — как в output.json, без `mean * count`.
