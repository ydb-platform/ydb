# 📊 Граф зависимостей таблиц в системе аналитики YDB

## 📊 Сравнение всех workflow'ов

| Функция | .github/workflows/collect_analytics.yml | .github/workflows/collect_analytics_fast.yml | .github/workflows/update_muted_ya.yml) |
|---------|----------------------|---------------------------|-------------------|
| **Частота** | ❌ **manualy**| ✅ каждые 30 минут | ✅ каждые 2 часа (6:00-20:00) |
| **Ветки** | только main | main | main, stable-25-2, stable-25-2-1, stable-25-3, stable-25-3-1, stream-nb-25-1 |
| **Build Types** | release-asan, release-msan, release-tsan | не применимо | только relwithdebinfo |
| **testowners** | ✅ [upload_testowners.py](.github/scripts/analytics/upload_testowners.py) (плоский список) | ❌ Не обновляет | ✅ [upload_testowners.py](.github/scripts/analytics/upload_testowners.py) |
| **all_tests_with_owner_and_mute** | ✅ [get_muted_tests.py](.github/scripts/tests/get_muted_tests.py) | ❌ Не обновляет | ✅ [get_muted_tests.py](.github/scripts/tests/get_muted_tests.py) |
| **flaky_tests_in_window** | ✅ [flaky_tests_history.py](.github/scripts/analytics/flaky_tests_history.py) | ❌ Не обновляет | ✅ [flaky_tests_history.py](.github/scripts/analytics/flaky_tests_history.py) |
| **tests_monitor** | ✅ [tests_monitor.py](.github/scripts/analytics/tests_monitor.py) | ❌ Не обновляет | ✅ [tests_monitor.py](.github/scripts/analytics/tests_monitor.py) |
| **GitHub issues** | ❌ Не обновляет | ✅ [export_issues_to_ydb.py](.github/scripts/analytics/export_issues_to_ydb.py) | ❌ Не обновляет |
| **GitHub issue mapping with muted tests** | ❌ Не обновляет | ✅ [github_issue_mapping.py](.github/scripts/analytics/github_issue_mapping.py) | ❌ Не обновляет |
| **Tests history and mute marts** | ❌ Не обновляет | ✅ [data_mart_executor.py](.github/scripts/analytics/data_mart_executor.py) | ❌ Не обновляет |
| **Perf OLAP marts** | ❌ Не обновляет | ✅ [data_mart_executor.py](.github/scripts/analytics/data_mart_executor.py) | ❌ Не обновляет |
| **Nemesis mart** | ❌ Не обновляет | ✅ [data_mart_executor.py](.github/scripts/analytics/data_mart_executor.py) | ❌ Не обновляет |
| **muted_ya.txt PR** | ❌ Не обновляет | ❌ Не обновляет | ✅ [create_new_muted_ya.py](.github/scripts/tests/create_new_muted_ya.py) |
| **Особенности** | Выполняется из main, использует muted_ya.txt из main | -  | Выполняется из BASE_BRANCH, использует muted_ya.txt из BASE_BRANCH |

