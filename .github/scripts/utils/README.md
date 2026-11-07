# CI scripts utils

Скрипты для сбора метрик раннера и построения дашборда тестов.

## Структура

- **metrics/** — сбор метрик во время `ya make`
  - `monitor_resources.py` — CPU, RAM, disk I/O в JSONL

- **tests/** — unit-тесты (`tests/dashboard`, `tests/metrics`), не в продуктовых пакетах

- **dashboard/** — построение дашбордов
  - `runner_footprint.py` — capacity дашборда из measured `/proc` (`resources_monitor.jsonl`)
  - `analyze_resources.py` — HTML по данным monitor
  - **test_metrics/** — дашборд тестов (evlog + report + overlay CPU/RAM/disk)
    - `tests_resource_dashboard.py` — основной скрипт
    - `dashboard_*.py` — рендеринг HTML
    - `ya_make_requirements.py` — чтение REQUIREMENTS из ya.make

`.github/config/runners_footprints.yml` — inventory флота + tuning-доли (`dashboard:`). Лимиты/рекомендации на дашборде берутся только с реального раннера; без monitor — без фиктивных потолков.

Unit-тесты лежат отдельно в `tests/` (не рядом с продуктовым кодом):

```bash
python3 -m unittest discover -s .github/scripts/utils/tests -p 'test_*.py'
```
