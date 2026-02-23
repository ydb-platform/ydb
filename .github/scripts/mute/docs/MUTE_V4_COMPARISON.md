# Сравнение систем mute: Legacy vs Current vs v4_direct

## Режимы сравнения

| Режим | Скрипт | Источник данных | flaky_tests_window / tests_monitor |
|-------|--------|-----------------|------------------------------------|
| **Legacy** | create_new_muted_ya --legacy | tests_monitor | **Требуются** |
| **Current** | create_new_muted_ya | tests_monitor | **Требуются** |
| **v4_direct** | create_new_muted_ya_v4 | test_results | **Не используются** |

**v4_direct** — новая версия, читает напрямую из `test_results` (test_runs_column). Отказ от flaky_tests_window и tests_monitor.

## Как сравнивать

### 1. Через workflow (рекомендуется)

**Ручной запуск:**
- Actions → Compare Mute Systems (Legacy vs v4) → Run workflow
- Опционально: указать branch (по умолчанию main)

**Расписание:** воскресенье 06:00 UTC

**Результат:**
- Артефакт `mute-comparison-{branch}` (retention 14 дней)
- Содержит: `comparison_report.md`, папки `legacy/`, `current/`, `v4_direct/`
- Step Summary в run содержит отчёт

### 2. Локально (все три режима)

```bash
# 1. Checkout main
git checkout main

# 2. Получить base файлы из целевой ветки
BRANCH=main
git fetch origin $BRANCH
git show origin/$BRANCH:.github/config/muted_ya.txt > base_muted_ya.txt
git show origin/$BRANCH:.github/config/quarantine.txt > quarantine.txt 2>/dev/null || touch quarantine.txt

# 3. Для legacy и current: заполнить flaky_tests_window и tests_monitor
python3 .github/scripts/analytics/flaky_tests_history.py --branch=$BRANCH --build_type=relwithdebinfo
python3 .github/scripts/analytics/tests_monitor.py --branch=$BRANCH --build_type=relwithdebinfo

# 4. Запустить сравнение (legacy, current, v4_direct)
python3 .github/scripts/tests/compare_mute_systems.py \
  --branch=$BRANCH \
  --build_type=relwithdebinfo \
  --muted_ya_file=base_muted_ya.txt \
  --quarantine_file=quarantine.txt
```

### 3. Только v4_direct (без flaky/monitor)

```bash
# v4_direct не требует flaky_tests_history и tests_monitor
python3 .github/scripts/tests/compare_mute_systems.py \
  --branch=main --build_type=relwithdebinfo \
  --muted_ya_file=base_muted_ya.txt --quarantine_file=quarantine.txt \
  --skip_legacy --skip_current
```

### 4. Отдельный запуск каждого режима

```bash
# Legacy (tests_monitor, без quarantine)
python3 .github/scripts/tests/create_new_muted_ya.py update_muted_ya \
  --branch=main --build_type=relwithdebinfo \
  --muted_ya_file=base_muted_ya.txt --quarantine_file=quarantine.txt \
  --output_folder=comparison/legacy --legacy

# Current (tests_monitor, с quarantine)
python3 .github/scripts/tests/create_new_muted_ya.py update_muted_ya \
  --branch=main --build_type=relwithdebinfo \
  --muted_ya_file=base_muted_ya.txt --quarantine_file=quarantine.txt \
  --output_folder=comparison/current

# v4_direct (test_results только, без flaky/monitor)
python3 .github/scripts/tests/create_new_muted_ya_v4.py \
  --branch=main --build_type=relwithdebinfo \
  --muted_ya_file=base_muted_ya.txt --quarantine_file=quarantine.txt \
  --output_folder=comparison/v4_direct
```

## Что сравнивается

| Файл | Описание |
|------|----------|
| to_mute.txt | Кандидаты на mute |
| to_unmute.txt | Кандидаты на unmute |
| to_delete.txt | Кандидаты на удаление (нет запусков) |
| new_muted_ya.txt | Итоговый список замьюченных тестов |

## Различия режимов

| Аспект | Legacy | Current | v4_direct |
|--------|--------|---------|-----------|
| Quarantine | Нет | Да | Да |
| Graduation | Нет | Да | Да |
| mute_decisions | Нет | Да | Да |
| Источник данных | tests_monitor | tests_monitor | **test_results** |
| flaky_tests_window | Да | Да | **Нет** |
| tests_monitor | Да | Да | **Нет** |

## Потоки данных

**Legacy / Current:**
```
test_results (test_runs_column)
       ↓ flaky_tests_history.py
flaky_tests_window
       ↓ tests_monitor.py (+ all_tests_with_owner_and_mute)
tests_monitor
       ↓ create_new_muted_ya.py (execute_query)
mute/unmute/delete
```

**v4_direct (новая версия):**
```
test_results (test_runs_column)
       ↓ create_new_muted_ya_v4.py (mute_data_from_test_results.fetch_from_test_results)
агрегация в памяти
       ↓ mute_logic.aggregate_test_data
mute/unmute/delete
```

v4_direct не использует flaky_tests_window и tests_monitor.
