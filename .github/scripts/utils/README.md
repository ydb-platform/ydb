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

Общие CI-метрики пишутся в ydb-qa таблицу `analytics/ci_metrics` клиентом `.github/scripts/analytics/ci_metrics.py` (`emit` / `flush`). Сейчас так снимаются длительности PR-check (GitHub job/step + in-job ya phases) и cache-free ydbd build (duration + binary size).
