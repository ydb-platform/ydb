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

Общие CI-метрики пишутся в ydb-qa таблицу `analytics/ci_metrics` клиентом `.github/scripts/analytics/ci_metrics.py` (`emit` / `flush`). Скрипты только копируют сырые значения; агрегаты, топы и регрессии — SQL по таблице. Сравнение PR-check с target branch — отдельный следующий этап.

Что пишется:

- PR-check: GitHub job/step (`export_github_job_metrics.py`) + in-job ya phases (`graph_compare`, `ya_make_try_*`, dashboard, s3)
- Nightly-Build (remote cache): `ydbd_cached_build`, `ydbd_size`, все Compile/Link узлы из `ya_evlog.jsonl` (`source=nightly_build`, `cache_mode=dist_cache`)
- ydbd-clean-build (без кеша): `ydbd_clean_build`, `ydbd_size`, те же узлы evlog (`source=clean_build`, `cache_mode=none`)
- Build-analytics-run: сырые `time_s` / `mean_compilation_time_s` + `inclusion_count` из `html_cpp_impact/output.json` и `html_headers_impact/output.json` (`source=build_bloat`), плюс evlog (`source=build_analytics`). HTML treemap по-прежнему в S3 и в `code-agility/*` через `ydb_upload.py`

Модули: `.github/scripts/analytics/export_ya_nodes.py` пишет одну строку на узел (`labels.node_kind` = `Compile` / `Link` / `Header`). Для заголовков `value` — mean time, `labels.inclusion_count` — как в output.json, без `mean * count`.
