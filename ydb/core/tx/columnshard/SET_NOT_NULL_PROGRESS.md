# ColumnShard SET NOT NULL: состояние на 2026-09-19

Работа остановлена по просьбе пользователя. Изменения находятся в рабочем дереве,
коммитов не создавал. **Полная операция ALTER пока не реализована**: включение
нового флага не делает `SET NOT NULL` работоспособным. Существующие запреты
остались, поэтому обойти проверку данных этим изменением нельзя.

Подробный проект решения: [SET_NOT_NULL_DESIGN.md](SET_NOT_NULL_DESIGN.md).

## Что сделано

1. Добавлен `EnableColumnStoreSetNotNull = 336`, по умолчанию `false`, в
   `ydb/core/protos/feature_flags.proto`.
2. Добавлены проверки флага в SQL
   (`ydb/core/kqp/provider/yql_kikimr_exec.cpp`) и прямом запросе SchemeShard
   (`ydb/core/tx/schemeshard/schemeshard_set_column_constraint__create.cpp`).
   Общий флаг `EnableSetColumnConstraint` по-прежнему необходим.
3. Реализован самостоятельный потоковый `TNotNullValidator`:
   [validator.h](validation/not_null/validator.h),
   [validator.cpp](validation/not_null/validator.cpp).
   Проверяет выбранные колонки во всех Arrow chunks, сохраняет первую ошибку,
   выдаёт успех только после `Finish()` на успешном EOF. Отсутствующая/неоднозначная
   колонка, NULL, ошибка чтения или dictionary без декодирования не дают успех.
   Сам валидатор не запускает сканирование, не блокирует записи и не меняет схему.
4. Добавлены 14 unit-тестов валидатора:
   [validator_ut.cpp](validation/not_null/ut/validator_ut.cpp), отдельный `ya.make`
   и регистрация в дереве ColumnShard. Покрыты late NULL, EOF, ошибки, пустые
   данные, проекции, старые nullable-схемы, неверная nonnullable-метка, slices,
   вычисление неизвестного null count, dictionary.
5. Добавлены regression-тесты допуска:
   - `KqpOlap::AlterTableSetNotNullOnColumnTableFeatureDisabled` в
     `ydb/core/kqp/ut/olap/kqp_olap_ut.cpp`: чистые данные не обходят выключенный
     флаг, после отказа NULL по-прежнему записывается.
   - `SetNotNullTest::ColumnTableFeatureDisabled` в
     `ydb/core/tx/schemeshard/ut_set_column_constraint/ut_set_column_constraint.cpp`:
     прямой запрос отвергается, схема неизменна, долгой операции не создаётся.
   - Проверка выключенного default в
     `ydb/core/base/generated/runtime_feature_flags_ut.cpp`.
6. Добавлен `ydb/tests/compatibility/olap/test_set_not_null.py`, зарегистрирован
   в `ya.make`. Активный сценарий проверяет nullable-схему/данные и отказ при
   выключенном флаге через переходы old/current. Два будущих сценария полностью
   написаны, но явно помечены `skip`: успешный SET → downgrade → upgrade и отказ
   при NULL → downgrade. Снимать skip можно только после реализации протокола.

## Проверки и результаты

Успешно выполнено, exit code 0:

```bash
./ya make --build relwithdebinfo -tA \
  ydb/core/tx/columnshard/validation/not_null/ut \
  ydb/core/base/generated/ut 2>&1 | tail -80
```

Итог `ya`: `OK: 19`, `SKIPPED: 1` (общая статистика тестов и style-проверок).
Валидатор содержит 14 тестов, runtime flags — 4. Первая попытка обнаружила
игнорирование `arrow::Status` и форматирование: исправлено, повторный запуск
успешен. Лог: `/tmp/columnshard_not_null_unit_tests.log`.

Сборка следующих тестов **остановлена по просьбе пользователя**, exit code 130;
результат этих тестов не подтверждён:

```bash
./ya make --build relwithdebinfo -tA \
  ydb/core/kqp/ut/olap \
  ydb/core/tx/schemeshard/ut_set_column_constraint \
  -F 'KqpOlap::AlterTable*NotNullOnColumnTable*' \
  -F 'SetNotNullTest::ColumnTableFeatureDisabled' \
  -F 'SetNotNullTest::BasicRequest' 2>&1 | tail -80
```

Лог незавершённой сборки: `/tmp/columnshard_not_null_integration_tests.log`.
Фоновых сборок этой задачи после остановки не оставлено.

`git diff --check` и синтаксическая проверка Python прошли. Compatibility-тесты
на кластере **не запускались**. В песочнице `~/.ya` доступен только для чтения,
поэтому сборки запускались с разрешением на запись в стандартный кэш.

## Решение, которое предстоит подключить

Первый вариант — standalone-таблицы и временная блокировка всех записей в
конкретную таблицу; чтение продолжает работать. Для таблиц в общем store нужна
отдельная поддержка, поскольку schema preset затрагивает несколько таблиц.

Последовательность:

1. SchemeShard фиксирует операцию и блокирует изменения схемы/набора шардов.
2. Каждый ColumnShard сохраняет блокировку записей с владельцем/epoch и завершает
   ранее принятые записи. Подготовленные распределённые транзакции нельзя
   односторонне отменять; их решения должны завершиться.
3. На защищённом snapshot сканируются видимые строки с учётом dedup/deletes,
   старых схем и отсутствовавших колонок. Результаты передаются валидатору.
4. Только после успеха всех шардов публикуется существующий `NotNull=true`.
   При NULL/ошибке/cancel nullable-схема сохраняется. Блокировки снимаются по
   устойчивому к рестартам протоколу.

Критические детали из исследования:

- Row-table flow в SchemeShard пригоден как основа, но `CreateLock`, выбор
  partitions, сообщения валидации и recovery сейчас ориентированы на DataShard.
- Не все OLAP ALTER/DROP/MOVE/COPY/reshard проверяют долгую блокировку пути:
  `NotUnderOperation()` недостаточно; нужны проверки `CheckLocks`.
- ColumnShard принимает историческую schema version из запроса. Проверка только
  новой Arrow-схемы не закрывает stale-schema writes. Нужна проверка актуальных
  ограничений, включая финальный результат partial update/defaults.
- Учитывать SQL, BulkUpsert, buffered blob writes, long transactions и commit
  paths; проверки только входящего `TEvWrite` недостаточно.
- `TEvInternalScan` можно переиспользовать, но сейчас его `ItemsLimit` и
  `SchemaVersion` не проходят в read description. Проверить snapshot readiness,
  pinning и успешный EOF. Статистики NULL в старых portions нет.
- Включение флага в смешанном кластере требует поддержки протокола всеми
  участниками и возможными узлами размещения tablets.
- Downgrade небезопасен не только во время операции: старый бинарник после
  завершения может принять запись со старой nullable-схемой. Нужны прямые
  stale-schema compatibility-тесты и backport enforcement либо запрет downgrade
  ниже поддерживаемой версии. SQL/BulkUpsert тестов для доказательства мало.

## С чего продолжить

1. Прочитать этот файл и полный design; проверить `git diff`.
2. Дождаться успешной сборки/запуска SQL и SchemeShard regression-тестов выше.
3. Реализовать durable write fence и завершение всех старых writes в ColumnShard,
   включая восстановление до приёма новых записей.
4. Подключить internal scan к валидатору и сохранять proof с operation/schema/
   snapshot/attempt identity; ошибки и старые ответы не должны давать успех.
5. Расширить SchemeShard operation/locks и coordinated finish/rollback.
6. Добавить actor-тесты конкуренции, рестартов и всех write paths по матрице из
   design. Только затем открывать успешный ALTER и снимать compatibility skip.

Не относящиеся к задаче файлы уже были в рабочем дереве и не менялись:
`kv_volume_ddl.md`, `kv_volume_ddl_ru.md`, `ydb/apps/ydb/old_config.yaml`,
`ydb/apps/ydb/solomon_new.yaml`, `ydb/apps/ydb/solomon_orig.yaml`.
