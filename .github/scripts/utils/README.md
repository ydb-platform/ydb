# CI scripts utils

Скрипты для сбора метрик и построения дашбордов.

## Структура

- **metrics/** — сбор метрик во время ya make
  - `monitor_resources.py` — CPU, RAM, disk I/O (JSONL + ram_usage.txt)

- **dashboard/** — построение дашбордов
  - `analyze_resources.py` — resources_report.html (CPU/RAM/disk по данным monitor)
  - **tests/** — дашборд тестов (evlog + report + overlay метрик)
    - `tests_resource_dashboard.py` — основной скрипт
    - `dashboard_*.py` — рендеринг HTML
    - `ya_make_requirements.py` — чтение REQUIREMENTS из ya.make
