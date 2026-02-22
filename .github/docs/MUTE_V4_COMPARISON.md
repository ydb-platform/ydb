# Сравнение старой и новой системы mute (Legacy vs v4)

## Как сравнивать

### 1. Через workflow (рекомендуется)

**Ручной запуск:**
- Actions → Compare Mute Systems (Legacy vs v4) → Run workflow
- Опционально: указать branch (по умолчанию main)

**Расписание:** воскресенье 06:00 UTC

**Результат:**
- Артефакт `mute-comparison-{branch}` (retention 14 дней)
- Содержит: `comparison_report.md`, папки `legacy/`, `current/`
- Step Summary в run содержит отчёт

### 2. Локально

Требуется доступ к YDB и предварительный запуск пайплайна данных:

```bash
# 1. Checkout main
git checkout main

# 2. Получить base файлы из целевой ветки
BRANCH=main  # или stable-25-3 и т.д.
git fetch origin $BRANCH
git show origin/$BRANCH:.github/config/muted_ya.txt > base_muted_ya.txt
git show origin/$BRANCH:.github/config/quarantine.txt > quarantine.txt 2>/dev/null || touch quarantine.txt

# 3. Заполнить данные (flaky_tests_history → flaky_tests_window, tests_monitor)
python3 .github/scripts/analytics/flaky_tests_history.py --branch=$BRANCH --build_type=relwithdebinfo
python3 .github/scripts/analytics/tests_monitor.py --branch=$BRANCH --build_type=relwithdebinfo

# 4. Запустить сравнение
python3 .github/scripts/tests/compare_mute_systems.py \
  --branch=$BRANCH \
  --build_type=relwithdebinfo \
  --muted_ya_file=base_muted_ya.txt \
  --quarantine_file=quarantine.txt

# 5. Отчёт в comparison/comparison_report.md
cat comparison/comparison_report.md
```

### 3. Отдельный запуск legacy и current

```bash
# Legacy (без quarantine, без graduation)
python3 .github/scripts/tests/create_new_muted_ya.py update_muted_ya \
  --branch=main --build_type=relwithdebinfo \
  --muted_ya_file=base_muted_ya.txt --quarantine_file=quarantine.txt \
  --output_folder=comparison/legacy \
  --legacy

# Current (v4 с quarantine, graduation)
python3 .github/scripts/tests/create_new_muted_ya.py update_muted_ya \
  --branch=main --build_type=relwithdebinfo \
  --muted_ya_file=base_muted_ya.txt --quarantine_file=quarantine.txt \
  --output_folder=comparison/current

# Сравнить вручную
diff comparison/legacy/mute_update/new_muted_ya.txt comparison/current/mute_update/new_muted_ya.txt
```

## Что сравнивается

| Файл | Описание |
|------|----------|
| to_mute.txt | Кандидаты на mute |
| to_unmute.txt | Кандидаты на unmute |
| to_delete.txt | Кандидаты на удаление (нет запусков) |
| new_muted_ya.txt | Итоговый список замьюченных тестов |

## Различия Legacy vs v4

| Аспект | Legacy | v4 |
|--------|--------|-----|
| Quarantine | Нет | Есть — защита от re-mute после ручного unmute |
| Graduation | Нет | Есть — 4+ runs, 1+ pass в 1 день → выход из quarantine |
| Исключение из to_mute | — | Тесты в quarantine не попадают в to_mute |
| mute_decisions | Не пишется | Пишется в YDB |

## Использование таблиц

Обе системы (legacy и v4) используют **одни и те же таблицы**:

```
test_results (test_runs_column)
       ↓ flaky_tests_history.py
flaky_tests_window
       ↓ tests_monitor.py (+ all_tests_with_owner_and_mute)
tests_monitor
       ↓ create_new_muted_ya.py (execute_query)
mute/unmute/delete решения
```

- **flaky_tests_window** — агрегат по дням (pass/fail/mute/skip), заполняется flaky_tests_history из test_results
- **tests_monitor** — статусы тестов по дням (state, is_muted, pass_count, fail_count…), заполняется tests_monitor из flaky_tests_window

create_new_muted_ya читает **только tests_monitor**. evaluate_pr_check_rules и pattern-алерты читают **test_results** напрямую (для PR-check и regression runs с duration).
