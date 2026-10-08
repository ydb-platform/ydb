# Validation guide

Запускать из корня рабочей копии. Требуется доступ к зависимостям сборки ya.
Тесты включают сборку; не добавлять -j и не форсировать пересборку.

```bash
set -o pipefail
./ya make --build relwithdebinfo -tA ydb/core/tablet_flat/ut -F 'Backup::*' 2>&1 | tail -80
```

Ожидания: допускаются все системные типы при Normal независимо от TenantPathId;
Recovery и пользовательские типы отвергаются; tenant snapshot/changelog читаются и дают
ожидаемое состояние; исключённый ID и ненастроенный backend не создают файлов.
Существующие тесты suite проверяют ротацию, ошибки, исключения колонок/таблиц и reader.

Результат выполнения и ограничения фиксируются отдельно после фактического запуска.

Проверка штатного восстановления:

```bash
set -o pipefail
./ya make --build relwithdebinfo -tA ydb/core/mind/hive/ut -F 'THiveTest::TestTenantSystemTabletRecovery*' 2>&1 | tail -80
```

Ожидания: настоящая системная таблетка, прежний ID и tenant metadata, восстановленное
состояние после явного normal; до команды normal запрещён, restart Hive сохраняет режим.
2026-10-06 обе новые проверки прошли. В объединённом прогоне с Backup и пятью
существующими тестами Hive — 81 OK, exit 0; подробности в [validation.md](validation.md).
Опрос длительной сборки — раз в 30 минут.
