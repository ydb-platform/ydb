# 📊 Граф зависимостей таблиц в системе аналитики YDB

## 📋 Уровни таблиц и их зависимости

### Уровень 0: Источники данных (внешние)

```
┌─────────────────────────────────────┐
│     test_results/test_runs_column   │
│     (основная таблица результатов)  │
│     🌿 Ветки: ВСЕ (все тесты)       │
└─────────────────┬───────────────────┘
                  │
                  ▼
┌─────────────────────────────────────┐
│        .github/config/muted_ya.txt  │
│        (файл с правилами mute)      │
│        🌿 Ветки: main, stable-25-1, │
│                  stable-25-1-analytics│
└─────────────────┬───────────────────┘
```

### Уровень 1: Базовые аналитические таблицы

```
┌─────────────────────────────────────┐
│ test_results/analytics/testowners   │
│ (владельцы тестов)                  │
│ 🌿 Ветки: ВСЕ (плоский список)      │
│ 🔄 Обновление: каждые 2 часа        │
│ 📝 Скрипт: upload_testowners.py     │
│ 📊 Источник: test_runs_column        │
└─────────────────┬───────────────────┘
                  │
                  ▼
┌─────────────────────────────────────┐
│ test_results/all_tests_with_owner_  │
│ and_mute                           │
│ (все тесты с владельцами и mute)   │
│ 🌿 Ветки: main (collect_analytics) + │
│          main, stable-25-2,        │
│          stable-25-2-1, stable-25-3,│
│          stable-25-3-1,            │
│          stream-nb-25-1            │
│          (update_muted_ya.yml)     │
│ 🔄 Обновление: каждые 2 часа        │
│ 📝 Скрипт: get_muted_tests.py       │
│ 📊 Источники: testowners +          │
│              muted_ya.txt +         │
│              test_runs_column       │
│              (run_timestamp_last)   │
└─────────────────┬───────────────────┘
```

### Уровень 2: Промежуточные агрегированные таблицы

```
┌─────────────────────────────────────┐
│ test_results/analytics/             │
│ flaky_tests_window_{N}_days         │
│ (история тестов за N дней)          │
│ 🌿 Ветки: main (collect_analytics) + │
│          main, stable-25-2,         │
│          stable-25-2-1, stable-25-3,│
│          stable-25-3-1,             │
│          stream-nb-25-1             │
│          (update_muted_ya.yml)      │
│ 🔄 Обновление: каждые 2 часа        │
│ 📝 Скрипт: flaky_tests_history.py   │
│ 📊 Источники: test_runs_column +    │
│              testowners             │
│ 🏗️ Build types: main - release-asan,│
│    release-msan, release-tsan;      │
│    update_muted_ya - relwithdebinfo │
└─────────────────┬───────────────────┘
```

### Уровень 3: Финальная аналитическая таблица

```
┌─────────────────────────────────────┐
│ test_results/analytics/tests_monitor│
│ (полная аналитика тестов)           │
│ 🌿 Ветки: main (collect_analytics) + │
│          main, stable-25-2,         │
│          stable-25-2-1, stable-25-3,│
│          stable-25-3-1,             │
│          stream-nb-25-1             │
│          (update_muted_ya.yml)      │
│ 🔄 Обновление: каждые 2 часа        │
│ 📝 Скрипт: tests_monitor.py         │
│ 📊 Источники: INNER JOIN между      │
│              flaky_tests_window +   │
│              all_tests_with_owner_  │
│              and_mute               │
│ 🏗️ Build types: main - release-asan,│
│    release-msan, release-tsan;      │
│    update_muted_ya - relwithdebinfo │
└─────────────────┬───────────────────┘
```

### Уровень 4: Data Marts (агрегированные представления)

```
┌─────────────────────────────────────┐
│ test_results/analytics/             │
│ test_monitor_mart                   │
│ (агрегированная аналитика)          │
│ 🌿 Ветки: main + все stable-*       │
│ 🔄 Обновление: каждые 30 минут      │
│ 📝 Скрипт: data_mart_executor.py    │
│ 📊 Источник: tests_monitor          │
└─────────────────┬───────────────────┘
                  │
                  ▼
┌─────────────────────────────────────┐
│ test_results/analytics/             │
│ test_muted_monitor_mart             │
│ (только muted тесты)                │
│ 🌿 Ветки: main + все stable-*       │
│ 🔄 Обновление: каждые 30 минут      │
│ 📝 Скрипт: data_mart_executor.py    │
│ 📊 Источник: tests_monitor          │
└─────────────────┬───────────────────┘
                  │
                  ▼
┌─────────────────────────────────────┐
│ perfomance/olap/fast_results        │
│ (производительность OLAP)           │
│ 🌿 Ветки: main + все stable-*       │
│ 🔄 Обновление: каждые 30 минут      │
│ 📝 Скрипт: data_mart_executor.py    │
│ 📊 Источник: test_runs_column       │
└─────────────────┬───────────────────┘
                  │
                  ▼
┌─────────────────────────────────────┐
│ nemesis/aggregated_mart             │
│ (агрегированная стабильность)       │
│ 🌿 Ветки: main + все stable-*       │
│ 🔄 Обновление: каждые 30 минут      │
│ 📝 Скрипт: data_mart_executor.py    │
│ 📊 Источник: test_runs_column       │
└─────────────────────────────────────┘
```


## 📊 Сравнение всех workflow'ов

| Функция | [collect_analytics.yml](.github/workflows/collect_analytics.yml) | [collect_analytics_fast.yml](.github/workflows/collect_analytics_fast.yml) | [update_muted_ya.yml](.github/workflows/update_muted_ya.yml) |
|---------|----------------------|---------------------------|-------------------|
| **Частота** | каждые 2 часа (1:00-23:00) | каждые 30 минут | каждые 2 часа (6:00-20:00) |
| **Ветки** | только main | main + все stable-* (фильтр в SQL) | main, stable-25-2, stable-25-2-1, stable-25-3, stable-25-3-1, stream-nb-25-1 |
| **Build Types** | release-asan, release-msan, release-tsan | все (фильтр в SQL) | только relwithdebinfo |
| **Checkout** | main | main + все stable-* | BASE_BRANCH (целевая ветка) |
| **testowners** | ✅ [upload_testowners.py](.github/scripts/analytics/upload_testowners.py) (плоский список) | ❌ Не обновляет | ✅ [upload_testowners.py](.github/scripts/analytics/upload_testowners.py) (плоский список, до matrix) |
| **all_tests_with_owner_and_mute** | ✅ [get_muted_tests.py](.github/scripts/tests/get_muted_tests.py) | ❌ Не обновляет | ✅ [get_muted_tests.py](.github/scripts/tests/get_muted_tests.py) |
| **flaky_tests_window** | ✅ [flaky_tests_history.py](.github/scripts/analytics/flaky_tests_history.py) | ❌ Не обновляет | ✅ [flaky_tests_history.py](.github/scripts/analytics/flaky_tests_history.py) |
| **tests_monitor** | ✅ [tests_monitor.py](.github/scripts/analytics/tests_monitor.py) | ❌ Не обновляет | ✅ [tests_monitor.py](.github/scripts/analytics/tests_monitor.py) |
| **GitHub issues** | ❌ Не экспортирует | ✅ [export_issues_to_ydb.py](.github/scripts/analytics/export_issues_to_ydb.py) | ❌ Не экспортирует |
| **GitHub issue mapping** | ❌ Не обновляет | ✅ [github_issue_mapping.py](.github/scripts/analytics/github_issue_mapping.py) | ❌ Не обновляет |
| **Data Marts** | ❌ Не обновляет | ✅ [data_mart_executor.py](.github/scripts/analytics/data_mart_executor.py) | ❌ Не обновляет |
| **Performance OLAP** | ❌ Не обновляет | ✅ [data_mart_executor.py](.github/scripts/analytics/data_mart_executor.py) | ❌ Не обновляет |
| **Stability data** | ❌ Не обновляет | ✅ [data_mart_executor.py](.github/scripts/analytics/data_mart_executor.py) | ❌ Не обновляет |
| **muted_ya.txt PR** | ❌ Не создает | ❌ Не создает | ✅ [create_new_muted_ya.py](.github/scripts/tests/create_new_muted_ya.py) |
| **Особенности** | Выполняется из main, использует muted_ya.txt из main | Выполняется из main + stable-*, обновляет data marts | Выполняется из BASE_BRANCH, использует muted_ya.txt из BASE_BRANCH |

