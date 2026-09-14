# Функциональный аудит CDC для строковых таблиц

Дата проверки: 12 сентября 2026 года.

Версия: `main`, commit `35fe6b99b6b321ebc724e0c644432b2301f49da6`.

## Итог

CDC для строковых таблиц в основной части работает в соответствии с документацией: все пять JSON-режимов создаются, операции изменения данных сериализуются ожидаемо, порядок для одного ключа и видимость после commit соблюдаются, initial scan продолжает обычный поток изменений, Debezium формирует `c/u/d/r`, настройки обычного топика меняются через `ALTER TOPIC`, а changefeed удаляется явно или каскадно вместе с таблицей.

Найдено **3 продуктовых дефекта**, для каждого добавлен отдельный regression-тест. Все три теста воспроизводимо падают на текущем `main`.

Также найдено **7 проблем и пробелов документации**. По предварительной договорённости отдельные тесты для чисто документационных проблем не добавлялись.

Короткий ответ на вопрос «можно ли пользоваться CDC по документации»: **да, но с перечисленными ниже ограничениями**. Особую осторожность требуют интервалы барьеров меньше секунды, большие значения partition count и восстановление конфигурации через `SHOW CREATE TABLE`.

## Методика и покрытие

Проверялись только строковые таблицы. Основная матрица исследовалась на одном локальном кластере командами собранного `ydb` CLI. Для чтения payload использовались Topic API/CLI и точечные функциональные проверки. Полный декартов перебор не выполнялся: использована pairwise-матрица, в которой каждый режим, формат, основной вид DML и существенная настройка встречаются хотя бы в одном сценарии.

Изученные нормативные документы:

- [основная документация CDC](ydb/docs/ru/core/concepts/cdc.md);
- [ADD/DROP CHANGEFEED и параметры](ydb/docs/ru/core/yql/reference/syntax/alter_table/changefeed.md);
- [ALTER TOPIC](ydb/docs/ru/core/yql/reference/syntax/alter-topic.md);
- [SHOW CREATE](ydb/docs/ru/core/yql/reference/syntax/show_create.md);
- [рекомендации по CDC](ydb/docs/ru/core/dev/cdc.md);
- документация по async replication, transfer, SCD1/SCD2, dump/restore и backup collections.

### Матрица результатов

| Область | Проверенные сценарии | Результат |
|---|---|---|
| Режимы JSON | `KEYS_ONLY`, `UPDATES`, `NEW_IMAGE`, `OLD_IMAGE`, `NEW_AND_OLD_IMAGES` | Соответствует |
| DML | `INSERT`, `UPSERT`, частичный `UPDATE`, `REPLACE`, `DELETE` | Соответствует |
| JSON-структура | `key`, `update/reset/erase`, `newImage/oldImage`, отсутствие PK в images | Соответствует |
| Debezium | `c/u/d/r`, before/after, source metadata, initial scan | В основном соответствует; есть документационная неточность об отсутствующих полях |
| Транзакции | нет события до commit; rollback не создаёт нового события | Соответствует |
| Initial scan | существующие строки и последующее изменение; Debezium `op=r`, `snapshot=true` | Соответствует |
| Virtual timestamps | opt-in поле `ts`, совместная работа с секундными barriers | Соответствует |
| Barriers | интервал 1 секунда; интервал 500 мс | 1 секунда работает; 500 мс сломан |
| Retention | default 24 часа, 12 часов, изменение через `ALTER TOPIC`, граница 31 день | Работает; документация ошибочно говорит о максимуме 30 дней |
| Partitions | fixed/autopartitioned topic, min/max, изменение auto topic, запрет изменения fixed topic | Соответствует, кроме переполнения `uint32` |
| Topic lifecycle | describe, consumer add, read, alter, drop, table cascade | Соответствует |
| DDL | ADD/DROP, `SHOW CREATE`, запреты `TRUNCATE` и `RENAME` | Два запрета работают; `SHOW CREATE` теряет настройки |
| Типы | составной PK и репрезентативные scalar-типы; существующий exhaustive `SupportedTypes` | Соответствует |
| Async replication / transfer | существующий `Replication.Types`, UUID и составной ключ | Соответствует smoke-сценарию |
| Export/import | `ChangefeedsExportRestore`, сохранение changefeed при восстановлении | Соответствует, 4 варианта прошли |

Репрезентативная CLI-проверка сериализации включала `Bool`, `Int64`, максимальный `Uint64`, `Double`, `Decimal(22,9)`, `DyNumber`, `Date`, `Datetime`, `Timestamp`, `Utf8`, `String`, `JsonDocument`, `Uuid` и составной ключ `Uint64 + Utf8`. Дополнительно существующий exhaustive-тест `SupportedTypes` прошёл.

## Продуктовые дефекты

### P1. Положительный subsecond `BARRIERS_INTERVAL` молча отключает барьеры

Запрос с `BARRIERS_INTERVAL = Interval('PT0.5S')` успешно создаёт changefeed, но барьеры не появляются. Положительное и синтаксически допустимое значение превращается в нулевой интервал без ошибки.

Причина видна в [yql_kikimr_exec.cpp](ydb/core/kqp/provider/yql_kikimr_exec.cpp#L3113): значение сначала проверяется как положительное, затем `TDuration` записывается в protobuf только через `interval.Seconds()`. Для 500 мс это `0`.

Ожидаемое поведение: либо поддерживать точность, допускаемую типом `Interval`, либо отклонять значения меньше минимально поддерживаемой гранулярности. Молчаливое отключение недопустимо.

Тест: `TestCdcDocumentationRegressions::test_subsecond_barrier_interval_emits_barriers`.

Фактический результат: `positive 500 ms barrier interval was silently disabled`.

### P2. `TOPIC_MAX_ACTIVE_PARTITIONS` переполняется и тихо усекается до `uint32`

Значение `4294967297` принимается при создании changefeed, но в topic description становится равным `1`.

Парсер читает число как `i64`, проверяя только `> 0`, после чего передаёт его в protobuf-поле меньшей разрядности: [yql_kikimr_exec.cpp](ydb/core/kqp/provider/yql_kikimr_exec.cpp#L3167). Аналогичный код используется для `TOPIC_MIN_ACTIVE_PARTITIONS`.

Ожидаемое поведение: отклонять значения вне диапазона целевого protobuf-поля до преобразования. В тесте используется только безопасный случай с огромным `MAX` и `MIN = 1`, чтобы не инициировать создание огромного количества партиций.

Тест: `TestCdcDocumentationRegressions::test_max_partition_count_rejects_uint32_overflow`.

Фактический результат: ожидалась ошибка, но DDL завершился успешно.

### P3. `SHOW CREATE TABLE` не сохраняет существенные настройки CDC-топика

Для changefeed, созданного с:

```yql
BARRIERS_INTERVAL = Interval('PT1S'),
TOPIC_AUTO_PARTITIONING = 'ENABLED',
TOPIC_MIN_ACTIVE_PARTITIONS = 2,
TOPIC_MAX_ACTIVE_PARTITIONS = 4
```

`SHOW CREATE TABLE` возвращает только `TOPIC_MIN_ACTIVE_PARTITIONS = 2`. В результате исполнение возвращённого DDL создаст fixed topic вместо auto-partitioned topic, потеряет верхнюю границу и отключит barriers.

Это противоречит контракту [SHOW CREATE](ydb/docs/ru/core/yql/reference/syntax/show_create.md#show-create): команда должна вернуть DDL, необходимый для воссоздания структуры объекта.

В formatter действительно выводятся virtual timestamps, retention и min partitions, но отсутствуют barriers, auto-partitioning strategy и max partitions: [create_table_formatter.cpp](ydb/core/sys_view/show_create/formatters/create_table_formatter.cpp#L1281).

Тест: `TestCdcDocumentationRegressions::test_show_create_preserves_changefeed_settings`.

Фактический результат: отсутствуют все три ожидаемых фрагмента.

## Проблемы документации

### D1. Не описана матрица совместимости режима, формата и настроек

[Список параметров changefeed](ydb/docs/ru/core/yql/reference/syntax/alter_table/changefeed.md#changefeed-options) выглядит как свободно комбинируемый. На практике сервер отклоняет:

- `MODE = 'UPDATES'` с `FORMAT = 'DEBEZIUM_JSON'`;
- `VIRTUAL_TIMESTAMPS = TRUE` с `DEBEZIUM_JSON`;
- `BARRIERS_INTERVAL` с `DEBEZIUM_JSON`.

Ограничения явно реализованы в [schemeshard__operation_create_cdc_stream.cpp](ydb/core/tx/schemeshard/schemeshard__operation_create_cdc_stream.cpp#L183), но в пользовательской документации отсутствуют. Нужна небольшая таблица совместимости.

### D2. Для Debezium не определено отсутствие неприменимых `before` и `after`

Основная страница говорит, что `before` и `after` присутствуют в соответствующих image-режимах. Реализация полностью опускает `before` для insert/read и `after` для delete, вместо JSON `null` или пустого объекта.

Особенно заметно расхождение в SCD1: документация утверждает, что при удалении отсутствующей строки формируется `op=d` с пустыми `before` и `after`; CLI действительно получил `op=d`, но оба поля были **отсутствующими**:

```json
{"payload":{"op":"d","source":{"connector":"ydb","version":"1.0.0","snapshot":false,...}}}
```

Следует явно зафиксировать omission semantics, так как downstream JSON-выражения различают отсутствующее поле, `null` и `{}`.

### D3. В SCD1 указан неверный путь к timestamp

В [SCD1 transfer](ydb/docs/ru/core/analyst/practical-guides/scd/scd1-transfer.md#L62) указан `payload.ts_ms`. Реальное поле и корректный пример ниже на той же странице используют `payload.source.ts_ms`.

### D4. Документированный максимум retention равен 30 дням, реализация допускает 31 день

[Документация CDC](ydb/docs/ru/core/concepts/cdc.md#retention-period) говорит, что время хранения можно увеличить до 30 дней. CLI успешно создал и описал CDC topic с `RETENTION_PERIOD = Interval('P31D')` и показал `RetentionPeriod: 31d`.

Это соответствует константе реализации `MaxPQLifetimeSeconds = 31 * 86400` в [schemeshard_impl.h](ydb/core/tx/schemeshard/schemeshard_impl.h#L223). Нужно либо исправить документацию на 31 день, либо изменить продуктовую границу на заявленные 30 дней.

### D5. Документация требует уже не действующий feature flag

[Справочник feature flags](ydb/docs/ru/core/reference/configuration/feature_flags.md#L20) продолжает описывать `enable_topic_autopartitioning_for_cdc` как управляющий флаг. В protobuf он помечен `deprecated` и `always true`: [feature_flags.proto](ydb/core/protos/feature_flags.proto#L175).

Упоминание следует удалить из текущей конфигурационной документации; оно уместно только в историческом changelog.

### D6. Не задокументирован запрет `RENAME`/`MOVE` таблицы с changefeed

CLI получает `PRECONDITION_FAILED: Cannot move table with cdc streams`. Ограничение жёстко задано в [schemeshard__operation_move_tables.cpp](ydb/core/tx/schemeshard/schemeshard__operation_move_tables.cpp#L76), но отсутствует в основных ограничениях CDC и в документации `RENAME`.

### D7. Публичный список параметров расходится с принимаемым синтаксисом

Парсер принимает `SCHEMA_CHANGES` и alias `RESOLVED_TIMESTAMPS`: [yql_kikimr_exec.cpp](ydb/core/kqp/provider/yql_kikimr_exec.cpp#L3113). В публичном списке есть только `BARRIERS_INTERVAL`, а `SCHEMA_CHANGES` отсутствует полностью.

Нужно решить, являются ли эти параметры публичными. Если да — документировать семантику и совместимость. Если нет — не принимать их из обычного пользовательского YQL либо явно пометить experimental/internal.

## Подтверждённые ограничения, не являющиеся дефектами

- Явное количество partitions CDC topic поддерживается только при первом компоненте PK типа `Uint32` или `Uint64`. Для `Utf8` CLI вернул ожидаемый `BAD_REQUEST`.
- Fixed CDC topic нельзя перевести на другое количество partitions через `ALTER TOPIC`; сервер отклоняет операцию, как указано в русской документации.
- `TRUNCATE TABLE` при наличии CDC отклоняется с `Cannot truncate table with CDC streams`.
- `DEBEZIUM_JSON` не поддерживает `UPDATES`, virtual timestamps и barriers.
- При `DELETE` отсутствующей строки Debezium формирует событие `d`, но без `before` и `after`.
- `INITIAL_SCAN` временно несовместим с некоторыми эксплуатационными эффектами, указанными в документации: во время сканирования нет barriers и приостанавливается table auto-partitioning.

## Что не доказывалось этим аудитом

- Систематические reboot, network partition, tablet failure и recovery-сценарии — они были явно исключены из области проверки.
- Exhaustive-доказательство exactly-once при всех сбоях и повторных подключениях.
- Истечение retention в реальном времени на интервалах 24–31 день.
- Стабильность autosplit под длительной высокой нагрузкой.
- Kafka API key для Debezium; payload проверялся через Topic API/CLI.
- Полная матрица `USER_SIDS`, TTL identity и `TRACE_IDS`; код и существующие unit-тесты просмотрены, явных расхождений не найдено.
- Все комбинации async replication, transfer и backup collection. Выполнены representative smoke-тесты.

## Открытые вопросы

1. Для `BARRIERS_INTERVAL` меньше секунды нужно сохранить subsecond precision или явно установить минимальное значение в одну секунду?
2. Должен ли диапазон `TOPIC_MIN/MAX_ACTIVE_PARTITIONS` проверяться в YQL parser, SchemeShard или в обоих слоях?
3. Должен ли `SHOW CREATE` сохранять topic settings, изменённые после создания через `ALTER TOPIC`, или только исходные параметры changefeed? Для воссоздания структуры нужен первый вариант.
4. Следует ли Debezium совместимости использовать стандартные `null` для неприменимых images или официально закрепить отсутствие полей?
5. Правильный предел retention — 30 или 31 день?
6. `SCHEMA_CHANGES` и `RESOLVED_TIMESTAMPS` являются публичными возможностями или внутренними параметрами?
7. Планируется ли поддержать `RENAME`/`MOVE` таблицы с CDC, или запрет нужно закрепить как постоянное ограничение?
8. Где проходит точная граница заявленной exactly-once гарантии: запись в topic, чтение consumer после commit offset или end-to-end обработка?

## Добавленные тесты

Файл: [test_cdc_doc_audit.py](ydb/tests/functional/cdc_doc_audit/test_cdc_doc_audit.py).

Target подключён в [ydb/tests/functional/ya.make](ydb/tests/functional/ya.make), конфигурация находится в [cdc_doc_audit/ya.make](ydb/tests/functional/cdc_doc_audit/ya.make).

Команда:

```bash
./ya make --build relwithdebinfo -tA ydb/tests/functional/cdc_doc_audit
```

Результат на проверенном commit:

```text
3 tests: 3 - FAIL
```

Это ожидаемый результат до исправления трёх продуктовых дефектов. Сборка и style-check target прошли.

Дополнительные прогоны:

```text
datashard ChangeExchange.SupportedTypes       OK: 1
functional Replication.Types                 OK: 1
SchemeShard ChangefeedsExportRestore         OK: 4
JSON mode/operation pairwise probe           OK: 5
```

## Рекомендованный порядок исправления

1. Исправить тихое переполнение partition count и округление barriers: оба случая принимают корректно распарсенный ввод, но сохраняют другое значение без предупреждения.
2. Дополнить `SHOW CREATE` всеми настройками, влияющими на структуру и поведение CDC topic.
3. Синхронизировать документацию с фактической матрицей Debezium и omission semantics.
4. Исправить retention boundary, SCD1 timestamp, feature flag и ограничение `RENAME`.
5. Принять продуктовое решение по скрытым параметрам и точной формулировке exactly-once.
