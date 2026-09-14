# Сводный отчёт функционального аудита YDB

Дата отчёта: 2026-09-14.

Проверенная версия: `main`, commit
`35fe6b99b6b321ebc724e0c644432b2301f49da6`.

Основная проверка выполнена на локальном кластере через YDB CLI, Query API и
Table API. Использовалась обычная сборка `relwithdebinfo`, без ASAN. Там, где
область применима к разным типам таблиц, аудит ограничен строковыми таблицами.
Задачи в трекере этим отчётом не создаются.

## Проверенные области

### CDC строковых таблиц

Проверены пять JSON-режимов, Debezium, основные виды DML, commit/rollback,
initial scan, virtual timestamps, barriers, retention, fixed и auto-partitioned
topics, lifecycle changefeed/topic, `SHOW CREATE`, составной PK, набор scalar
types, async replication, transfer и export/import.

Результат: 3 дефекта реализации и 7 проблем документации. Подробности:
`cdc_documentation_audit.md`.

### Автоматическое партиционирование строковых таблиц

Проверены defaults, CREATE/ALTER всех параметров, uniform partitions, явные
границы простых и составных ключей, отрицательные значения и типы, сочетания
initial/min/max, `SHOW CREATE`, маршрутизация, size-based split и поведение после
массового удаления.

Результат: 2 дефекта реализации и 2 проблемы документации. Подробности:
`automatic_partitioning_documentation_audit.md`.

### Backup collections и восстановление

Проверены full и incremental backup двух таблиц, secondary index, восстановление
цепочки, конфликт с существующим объектом, повторный full, быстрый full →
incremental, продолжение цепочки после restore, статусы операций и команды
мониторинга из документации. Использовалось хранилище `cluster`.

Результат: 2 дефекта реализации/контракта и 2 проблемы документации. Подробности:
`backup_collection_documentation_audit.md`.

### JSON-индексы

Проверены `Json` и `JsonDocument`, explicit `VIEW` и auto-select,
`JSON_EXISTS`/`JSON_VALUE`, JSON `null` и SQL `NULL`, DML и rollback, параметры,
граница `2^53`, строковый и составной PK, prefixed index, `SHOW CREATE` и
read-your-writes.

Результат: 1 дефект реализации с шестью тестовыми вариантами и 3 проблемы
документации. Подробности: `json_index_documentation_audit.md`.

### Полнотекстовый и гибридный поиск

Проверены plain/relevance/N-граммные/filtered indexes, `String` и `Utf8`, разные
PK, DML, `Keywords`, `Query`, `Wildcard`, `LIKE`/`ILIKE`, minimum-should-match,
hybrid RRF/linear, веса и lambda, а также Query и Scripting API.

Результат: 4 дефекта реализации и 2 проблемы документации. Подробности:
`fulltext_hybrid_documentation_audit.md`.

### Векторные индексы

Проверены все документированные метрики, `float`/`uint8`/`int8`, global,
filtered и covering indexes, `NULL` и malformed vectors, граничные размерности,
кластерные параметры, DML/read-your-writes, drop/recreate, Query и Scripting API.

Результат: дефектов реализации не найдено, найдены 2 проблемы документации.
Подробности: `vector_index_documentation_audit.md`.

### Транзакции и MVCC

Проверены Serializable RW, Snapshot RW/RO, Stale RO, Online RO, конфликты,
phantoms, write skew, распределённые транзакции, rollback, read-your-writes через
secondary index и ограничения DDL/read-only modes.

Результат: дефектов реализации не найдено, найдены 2 проблемы документации.
Подробности: `transactions_mvcc_documentation_audit.md`.

### TTL строковых таблиц

Проверены `Date`, `Datetime`, `Timestamp`, `Uint32`, `Uint64`, `DyNumber`, все
единицы времени, границы и `NULL`, включение/отключение TTL, изменение до
удаления, TTL по PK, secondary index, DDL и `SHOW CREATE`.

Результат: проблем реализации и документации не найдено. Подробности:
`ttl_row_tables_documentation_audit.md`.

### Глобальные secondary indexes

Проверены sync/async/unique/covering indexes, составные ключи, `NULL`, DML,
unique rollback, online build с конкурентными изменениями, lifecycle операции,
rename, optimizer и schema evolution.

Результат: 1 дефект реализации и 1 проблема документации. Подробности:
`global_secondary_indexes_documentation_audit.md`.

### Локальные индексы строковых таблиц

Проверен `LOCAL USING bloom_filter`: левые префиксы PK, point/range/IN,
5000 строк, DML, drop/recreate, параметры и `SHOW CREATE`. Column-only
`bloom_ngram_filter` и `min_max` проверены на корректное отклонение.

Результат: 1 дефект реализации и 1 проблема документации. Подробности:
`local_indexes_row_tables_documentation_audit.md`.

### VIEW и SHOW CREATE

Проверены `TablePathPrefix`, вложенные views, циклы, `security_invoker`, schema
evolution, drop/recreate underlying table, `SELECT *`, typed parameters,
read-only ограничения и round-trip `SHOW CREATE VIEW`.

Результат: дефектов реализации не найдено, найдена 1 проблема документации.
Подробности: `views_show_create_documentation_audit.md`.

### BATCH DML, Bulk Upsert и пагинация

Проверены BATCH UPDATE/DELETE на 30 000 строках и 12 партициях, sync index,
отрицательные сценарии, Bulk Upsert для обычной таблицы и async/sync indexes,
пустой batch, `NOT NULL`, keyset pagination по составному и nullable PK, а также
изменения данных между страницами.

Результат: проблем реализации и документации не найдено. Подробности:
`batch_dml_bulk_upsert_paging_documentation_audit.md`.

## Баги, которые я бы завёл

Всего описано **37 кандидатов**: 14 на реализацию/продуктовый контракт и 23 на
документацию. Сейчас предлагаются к заведению **14 задач**, ещё **23 задачи** с
пометкой `[IGNORE]` сохранены в документе, но пока не должны заводиться. Каждый
заголовок соответствует одной самостоятельной задаче.

### Реализация (14, из них 2 `[IGNORE]`)

#### CDC: subsecond BARRIERS_INTERVAL молча отключает барьеры

**Задача:** [YDBBUGS-847](https://st.yandex-team.ru/YDBBUGS-847).

**Предлагаемый Area:** `area/core`.

**Предлагаемый приоритет:** `Средний`.

**Обоснование:** валидная настройка принимается без предупреждения, но
запрошенные события вообще не создаются; это тихая потеря части CDC-протокола.

**Тип:** реализация. Положительный `Interval('PT0.5S')` принимается, но при
записи в protobuf усекается до нуля секунд, и barriers не появляются. Следует
сохранять subsecond precision либо отклонять интервал меньше поддерживаемого
минимума. Тест: `test_subsecond_barrier_interval_emits_barriers` — падает.

**Шаги воспроизведения:**

1. Создать строковую таблицу и changefeed `KEYS_ONLY/JSON` с
   `BARRIERS_INTERVAL = Interval('PT0.5S')`.
2. Добавить consumer и начать чтение topic.
3. Читать сообщения не менее трёх секунд, отбирая записи с полем `resolved`.

**Ожидаемое поведение:** сервер либо создаёт barrier примерно каждые 500 мс,
либо отклоняет неподдерживаемый интервал при создании changefeed.

**Фактическое поведение:** DDL завершается успешно, но за время ожидания не
приходит ни одного barrier; интервал молча превращается в отключённый.

**Полезные детали:** значение сначала проверяется как положительное, затем в
`ydb/core/kqp/provider/yql_kikimr_exec.cpp` записывается через
`interval.Seconds()`, поэтому 500 мс превращаются в `0`. Контрольный интервал
`PT1S` создаёт barriers. Regression target:
`ydb/tests/functional/cdc_doc_audit`.

**Ссылки:** [regression-тест в PR #53018](https://github.com/ydb-platform/ydb/pull/53018/files#diff-fd6836cfc41dbff4f8be5767e68ba91951527547b3ef0a548bb5e34490129445R55-R88),
[усечение интервала до секунд](https://github.com/ydb-platform/ydb/blob/35fe6b99b6b321ebc724e0c644432b2301f49da6/ydb/core/kqp/provider/yql_kikimr_exec.cpp#L3113-L3128).

#### CDC: TOPIC_MAX_ACTIVE_PARTITIONS переполняется до uint32

**Задача:** [YDBBUGS-848](https://st.yandex-team.ru/YDBBUGS-848).

**Предлагаемый Area:** `area/core`.

**Предлагаемый приоритет:** `Средний`.

**Обоснование:** сервер принимает значение, а затем молча сохраняет совершенно
другую конфигурацию партиционирования, что может привести к перегрузке topic.

**Тип:** реализация. Значение `4294967297` проходит валидацию, затем тихо
превращается в `1`; аналогичный риск есть для min partitions. Значения вне
диапазона protobuf-поля должны отклоняться до преобразования. Тест:
`test_max_partition_count_rejects_uint32_overflow` — падает.

**Шаги воспроизведения:**

1. Создать строковую таблицу.
2. Добавить auto-partitioned changefeed с `TOPIC_MIN_ACTIVE_PARTITIONS = 1` и
   `TOPIC_MAX_ACTIVE_PARTITIONS = 4294967297`.
3. Описать созданный topic через CLI.

**Ожидаемое поведение:** DDL отклоняется, потому что значение не помещается в
целевое `uint32`-поле.

**Фактическое поведение:** DDL принимается, а в topic description максимальное
число партиций равно `1`.

**Полезные детали:** парсер читает число как `i64` и проверяет только `> 0`, а
целевое protobuf-поле имеет разрядность `uint32`. В тесте используется безопасная
пара `MIN=1`, `MAX=4294967297`, чтобы не инициировать создание огромного числа
партиций. После DDL topic description показывает `MAX=1`.

**Ссылки:** [regression-тест в PR #53018](https://github.com/ydb-platform/ydb/pull/53018/files#diff-fd6836cfc41dbff4f8be5767e68ba91951527547b3ef0a548bb5e34490129445R89-R106),
[парсинг и преобразование partition limits](https://github.com/ydb-platform/ydb/blob/35fe6b99b6b321ebc724e0c644432b2301f49da6/ydb/core/kqp/provider/yql_kikimr_exec.cpp#L3167-L3193).

#### CDC: SHOW CREATE TABLE теряет настройки changefeed topic

**Задача:** [YDBBUGS-849](https://st.yandex-team.ru/YDBBUGS-849).

**Предлагаемый Area:** `area/core`.

**Предлагаемый приоритет:** `Средний`.

**Обоснование:** DDL, заявленный как достаточный для восстановления структуры,
создаёт changefeed с другой семантикой и может нарушить процедуру recovery.

**Тип:** реализация. Для changefeed с barriers, auto partitioning и max
partitions вывод сохраняет только min partitions. Повторное исполнение DDL
создаёт семантически другой topic. Нужно выводить все существенные настройки.
Тест: `test_show_create_preserves_changefeed_settings` — падает.

**Шаги воспроизведения:**

1. Создать таблицу и changefeed с barriers, auto partitioning, min=2 и max=4.
2. Выполнить `SHOW CREATE TABLE`.
3. Сравнить настройки changefeed в полученном DDL с исходными либо создать по
   нему вторую таблицу и описать её topic.

**Ожидаемое поведение:** возвращённый DDL сохраняет все настройки, необходимые
для семантически эквивалентного changefeed.

**Фактическое поведение:** вывод содержит только min=2; interval barriers,
стратегия auto partitioning и max=4 потеряны.

**Полезные детали:** исходный changefeed создаётся с
`BARRIERS_INTERVAL=PT1S`, `TOPIC_AUTO_PARTITIONING=ENABLED`, `MIN=2`, `MAX=4`.
В выводе остаётся только `TOPIC_MIN_ACTIVE_PARTITIONS=2`. Пропуски находятся в
`ydb/core/sys_view/show_create/formatters/create_table_formatter.cpp`; formatter
уже выводит retention и virtual timestamps, но не эти три настройки.

**Ссылки:** [regression-тест в PR #53018](https://github.com/ydb-platform/ydb/pull/53018/files#diff-fd6836cfc41dbff4f8be5767e68ba91951527547b3ef0a548bb5e34490129445R107-R134),
[formatter changefeed в SHOW CREATE](https://github.com/ydb-platform/ydb/blob/35fe6b99b6b321ebc724e0c644432b2301f49da6/ydb/core/sys_view/show_create/formatters/create_table_formatter.cpp#L1226-L1331),
[контракт SHOW CREATE](https://github.com/ydb-platform/ydb/blob/35fe6b99b6b321ebc724e0c644432b2301f49da6/ydb/docs/ru/core/yql/reference/syntax/show_create.md#L19-L38).

#### Partitioning: NULL в PARTITION_AT_KEYS приводит к INTERNAL_ERROR

**Задача:** [YDBBUGS-850](https://st.yandex-team.ru/YDBBUGS-850).

**Предлагаемый Area:** `area/datashard`.

**Предлагаемый приоритет:** `Средний`.

**Обоснование:** пользовательский ввод попадает во внутреннюю ошибку и ухудшает
диагностику, однако воспроизведение требует невалидного DDL и не повреждает БД.

**Тип:** реализация. Невалидная граница `(NULL)` для `Uint64 NOT NULL` выдаёт
`INTERNAL_ERROR` с assertion-подобным `index out of range` вместо пользовательской
ошибки схемы/типа. Тест:
`test_null_partition_boundary_is_rejected_without_internal_error` — падает.

**Шаги воспроизведения:**

1. Выполнить `CREATE TABLE` с PK `id Uint64 NOT NULL`.
2. В `WITH` указать `PARTITION_AT_KEYS = (NULL)`.
3. Повторить запрос через Query API и Scripting API.

**Ожидаемое поведение:** невалидная граница отклоняется как пользовательская
ошибка типа или схемы.

**Фактическое поведение:** оба API возвращают `INTERNAL_ERROR` и внутреннюю
диагностику `index out of range`.

**Полезные детали:** одинаково воспроизводится через Query API (`ydb sql`) и
Scripting API (`ydb yql`); диагностика содержит
`yql/essentials/ast/yql_expr.h:2074: index out of range`. Соседние ошибки —
String вместо Uint64 и лишняя компонента составного ключа — корректно
возвращаются как type/scheme errors.

**Ссылки:** [regression-тест в PR #53018](https://github.com/ydb-platform/ydb/pull/53018/files#diff-bdbe358ab060c6227eea2599f3aac7f818bb4fa4d2cc26d3ff2b495eddb28a48R33-R47),
[формирование границ в KQP gateway](https://github.com/ydb-platform/ydb/blob/35fe6b99b6b321ebc724e0c644432b2301f49da6/ydb/core/kqp/host/kqp_gateway_proxy.cpp#L242-L268).

#### Partitioning: CREATE и ALTER принимают MIN_PARTITIONS_COUNT больше MAX

**Задача:** [YDBBUGS-851](https://st.yandex-team.ru/YDBBUGS-851).

**Предлагаемый Area:** `area/datashard`.

**Предлагаемый приоритет:** `Средний`.

**Обоснование:** сервер сохраняет внутренне противоречивую политику, последствия
которой для автоматических split/merge не определены и проявятся асинхронно.

**Тип:** реализация. Сервер сохраняет одновременно min=4 и max=3, создавая
противоречивую политику. Пара должна валидироваться как `min <= max`. Тест:
`test_min_partitions_count_cannot_exceed_max_partitions_count`, параметры
`create` и `alter` — оба падают.

**Шаги воспроизведения:**

1. Создать таблицу с `AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 4` и
   `AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = 3`.
2. Для второй обычной таблицы установить ту же пару через `ALTER TABLE ... SET`.
3. Выполнить `scheme describe` для обеих таблиц.

**Ожидаемое поведение:** CREATE и ALTER отклоняются с сообщением, что min не
может превышать max.

**Фактическое поведение:** обе операции успешны; схема сохраняет min=4 и max=3.

**Полезные детали:** `scheme describe` после успешного DDL одновременно
показывает `Min partitions count: 4` и `Max partitions count: 3`. UI и topic DDL
уже проверяют `minimum <= maximum`; table DDL — нет. Начальное число партиций
выше max сюда не относится: initial layout и предел последующих auto-split имеют
разную семантику.

**Ссылки:** [параметризованный regression-тест в PR #53018](https://github.com/ydb-platform/ydb/pull/53018/files#diff-bdbe358ab060c6227eea2599f3aac7f818bb4fa4d2cc26d3ff2b495eddb28a48R48-R77),
[существующая проверка min/max в соседнем KQP-пути](https://github.com/ydb-platform/ydb/blob/35fe6b99b6b321ebc724e0c644432b2301f49da6/ydb/core/kqp/provider/yql_kikimr_type_ann.cpp#L2685-L2696).

#### [IGNORE] Backup: full и incremental backup конфликтуют в пределах одной секунды

**Предлагаемый Area:** `area/core`.

**Предлагаемый приоритет:** `Средний`.

**Обоснование:** корректная последовательность backup-операций детерминированно
падает из-за внутреннего имени и делает надёжную автоматизацию резервирования
невозможной без недокументированной задержки.

**Тип:** реализация. После успешно завершившегося full немедленный incremental
может получить то же timestamp-имя внутреннего CDC stream и упасть с `path
exist`. Пользователь не должен добавлять искусственную секундную задержку.
Тест: `test_rapid_consecutive_backups_do_not_collide` — стабильно падает.

**Шаги воспроизведения:**

1. Создать backup collection с включёнными incremental backups.
2. В начале секунды выполнить full backup и дождаться появления snapshot.
3. Изменить строку и в ту же секунду запустить incremental backup.

**Ожидаемое поведение:** после завершившегося full следующий incremental
успешно создаёт новый snapshot независимо от временного интервала между вызовами.

**Фактическое поведение:** incremental падает с `path exist` для внутреннего
`*_continuousBackupImpl`, получившего то же секундное имя.

**Полезные детали:** внутренний путь имеет вид
`<table>/<YYYYMMDDhhmmss>Z_continuousBackupImpl`; timestamp содержит только
секунды. Full уже завершил YQL-операцию, но incremental получает то же имя и
ошибку `type: EPathTypeCdcStream`. Прогон с `--test-retries 2` воспроизвёл
коллизию оба раза. Target: `ydb/tests/functional/backup_collection_doc_audit`.

**Ссылки:** [regression-тест в PR #53018](https://github.com/ydb-platform/ydb/pull/53018/files#diff-108b3d01601bd1c907aea31880f6be0a3f90c94d4ec05ce95e1be31e8264ad5eR69-R104),
[формирование секундного имени continuous stream](https://github.com/ydb-platform/ydb/blob/35fe6b99b6b321ebc724e0c644432b2301f49da6/ydb/core/tx/schemeshard/schemeshard__operation_alter_continuous_backup.cpp#L146-L166).

#### [IGNORE] Backup: после restore нельзя продолжить incremental-цепочку

**Предлагаемый Area:** `area/core`.

**Предлагаемый приоритет:** `Средний`.

**Обоснование:** после disaster-recovery теряется возможность продолжать
incremental backup без нового полного снимка, что увеличивает recovery window и
стоимость хранения.

**Тип:** реализация/контракт. После успешного restore full+incremental следующий
incremental падает с `Last continuous backup stream is not found`; помогает
только новый full. Нужно восстановить продолжение цепочки либо явно объявить
restore-цепочку read-only и дать понятную диагностику. Тест:
`test_incremental_backup_continues_after_restore` — падает.

**Шаги воспроизведения:**

1. Создать full backup, изменить данные и создать incremental backup.
2. Удалить исходную таблицу и выполнить restore коллекции.
3. Проверить восстановление значения из incremental snapshot.
4. Ещё раз изменить строку и запустить следующий incremental backup.

**Ожидаемое поведение:** восстановленная коллекция продолжает incremental-цепь
либо операция заранее документированно требует начать новую full-цепочку.

**Фактическое поведение:** данные восстановлены, но следующий incremental падает
с `Last continuous backup stream is not found`; новый full устраняет ошибку.

**Полезные детали:** до последнего шага тест проверяет, что restore действительно
вернул значение из первого incremental backup. Новый full после restore снова
разрешает incremental, но начинает новую цепочку. То есть проблема не в
незавершённом restore, а в отсутствии восстановленного continuous stream state.

**Ссылки:** [regression-тест в PR #53018](https://github.com/ydb-platform/ydb/pull/53018/files#diff-108b3d01601bd1c907aea31880f6be0a3f90c94d4ec05ce95e1be31e8264ad5eR105-R157),
[поиск последнего continuous stream и ошибка](https://github.com/ydb-platform/ydb/blob/35fe6b99b6b321ebc724e0c644432b2301f49da6/ydb/core/tx/schemeshard/schemeshard__operation_alter_continuous_backup.cpp#L90-L115).

#### JSON index: нарушен read-your-writes внутри транзакции

**Задача:** [YDBBUGS-852](https://st.yandex-team.ru/YDBBUGS-852).

**Предлагаемый Area:** `area/datashard`.

**Предлагаемый приоритет:** `Средний`.

**Обоснование:** синхронный индекс возвращает устаревший или отсутствующий
результат внутри транзакции, напрямую нарушая корректность прикладной логики.

**Тип:** реализация. После INSERT/UPSERT индекс не видит новую строку, после
UPDATE видит старое значение, после DELETE продолжает видеть удалённую строку.
После commit состояние корректно. Параметризованные тесты для `Json` и
`JsonDocument`, операций insert/update/delete — 6 вариантов падают.

**Шаги воспроизведения:**

1. Создать таблицу с колонкой `Json` или `JsonDocument` и синхронным JSON index.
2. В одном serializable multi-statement запросе выполнить INSERT/UPSERT, UPDATE
   индексируемого пути либо DELETE.
3. Следующим statement прочитать строку через `VIEW json_idx`.
4. После commit повторить SELECT отдельным запросом.

**Ожидаемое поведение:** индексное чтение в транзакции видит собственную вставку,
новое значение после UPDATE и не видит строку после DELETE.

**Фактическое поведение:** до commit индекс видит старое состояние; после commit
отдельный запрос возвращает корректный результат.

**Полезные детали:** после insert индекс возвращает `[]`; после update не находит
новое значение и сохраняет старое; после delete возвращает удалённый `id=1`.
Эквивалентный обычный `GLOBAL SYNC` index проходит. Существующие контрольные
гарантии находятся в `ydb/core/kqp/ut/tx/kqp_read_your_writes_ut.cpp`. Target:
`ydb/tests/functional/json_index_doc_audit`.

**Ссылки:** [три параметризованных regression-теста в PR #53018](https://github.com/ydb-platform/ydb/pull/53018/files#diff-2bc290cb64fb8f02a94f8a457b6ed44e299cca2b173c21d60e8803893e47c2bfR55-R116),
[контрольные RYW-тесты обычного secondary index](https://github.com/ydb-platform/ydb/blob/35fe6b99b6b321ebc724e0c644432b2301f49da6/ydb/core/kqp/ut/tx/kqp_read_your_writes_ut.cpp#L526-L578).

#### Fulltext: индекс не соблюдает read-your-writes

**Задача:** [YDBBUGS-853](https://st.yandex-team.ru/YDBBUGS-853).

**Предлагаемый Area:** `area/datashard`.

**Предлагаемый приоритет:** `Средний`.

**Обоснование:** поиск возвращает состояние до собственных INSERT/UPDATE/DELETE
в той же транзакции, что является ошибкой корректности синхронного индекса.

**Тип:** реализация. В одной serializable multi-statement транзакции fulltext
VIEW не видит INSERT/UPSERT, после UPDATE сохраняет старый терм, после DELETE
видит удалённую строку. После commit индекс корректен. Тест параметризован по
insert/update/delete и `String`/`Utf8`: 6 вариантов падают.

**Шаги воспроизведения:**

1. Создать fulltext index по колонке `String` или `Utf8`.
2. В одном serializable запросе выполнить INSERT/UPSERT, UPDATE текста либо
   DELETE и затем `SELECT ... VIEW ft_idx` по затронутому терму.
3. Повторить поиск отдельным запросом после commit.

**Ожидаемое поведение:** fulltext VIEW отражает собственные изменения текущей
транзакции так же, как обычный синхронный secondary index.

**Фактическое поведение:** до commit виден старый индекс; после commit поиск
становится корректным.

**Полезные детали:** расхождение ограничено чтением до commit; отдельный запрос
после commit видит правильный индекс. Тест
`test_fulltext_index_observes_writes_in_same_transaction` параметризован по
операции и типу текста. Обычный синхронный secondary index использовался как
контроль.

**Ссылки:** [параметризованный regression-тест в PR #53018](https://github.com/ydb-platform/ydb/pull/53018/files#diff-73602f2cb1f311d5fa866ccbfd5821911da9cae99f4f33d20c3ec96e33c0aaf6R69-R113),
[документированный контракт обновления fulltext index](https://github.com/ydb-platform/ydb/blob/35fe6b99b6b321ebc724e0c644432b2301f49da6/ydb/docs/ru/core/dev/fulltext-indexes.md#L202-L221).

#### Fulltext: N-граммный LIKE отклоняет многосегментный Utf8-шаблон

**Задача:** [YDBBUGS-854](https://st.yandex-team.ru/YDBBUGS-854).

**Предлагаемый Area:** `area/datashard`.

**Предлагаемый приоритет:** `Средний`.

**Обоснование:** документированный Unicode-сценарий не работает, но область
ошибки ограничена типом Utf8 и конкретной формой многосегментного шаблона.

**Тип:** реализация. Документированный шаблон вида `%обуч%ние%` с Utf8-литералом
даёт `Unsupported index access`; соседние варианты и явный Wildcard работают.
Тест `test_multisegment_like_accepts_literal_of_column_type`: String проходит,
Utf8 падает.

**Шаги воспроизведения:**

1. Создать N-граммный fulltext index по `Utf8`-колонке.
2. Записать строки `обучение`, `переобучение` и контрольную строку.
3. Выполнить через index view `WHERE body LIKE "%обуч%ние%"u`.

**Ожидаемое поведение:** запрос возвращает первые две строки, как обещает пример
LIKE/ILIKE для N-граммного индекса.

**Фактическое поведение:** запрос отклоняется с `Unsupported index access`.

**Полезные детали:** проходят String-колонка со String-шаблоном, Utf8-колонка со
String-шаблоном, простой Utf8-шаблон `%обучение%` и явный
`FulltextMatch(..., "Wildcard" AS Mode)`. Падает только согласованный по типу
многосегментный Utf8-шаблон из документации.

**Ссылки:** [regression-тест в PR #53018](https://github.com/ydb-platform/ydb/pull/53018/files#diff-73602f2cb1f311d5fa866ccbfd5821911da9cae99f4f33d20c3ec96e33c0aaf6R114-R137),
[документированный LIKE/ILIKE и шаблон](https://github.com/ydb-platform/ydb/blob/35fe6b99b6b321ebc724e0c644432b2301f49da6/ydb/docs/ru/core/yql/reference/syntax/select/fulltext_index.md#L61-L71).

#### Fulltext: Query mode не применяет отрицательные термы

**Задача:** [YDBBUGS-855](https://st.yandex-team.ru/YDBBUGS-855).

**Предлагаемый Area:** `area/datashard`.

**Предлагаемый приоритет:** `Средний`.

**Обоснование:** запрос успешно выполняется, но возвращает семантически неверные
документы; тихий неправильный результат опаснее явного отказа.

**Тип:** реализация. `+machine -databases` возвращает документ с `databases`;
минус разбирается, но отрицание не применяется. Тест для `String` и `Utf8`:
`test_query_mode_excludes_terms_prefixed_with_minus` — оба варианта падают.

**Шаги воспроизведения:**

1. Создать fulltext index и записать документы `machine learning`,
   `machine databases`, `databases only`.
2. Выполнить `FulltextMatch(body, "+machine -databases", "Query" AS Mode)`.
3. Повторить для `String` и `Utf8`.

**Ожидаемое поведение:** возвращается только `machine learning`, поскольку
`machine` обязателен, а `databases` запрещён.

**Фактическое поведение:** возвращается `machine databases`; отрицательный терм
не исключает документ.

**Полезные детали:** на документах `machine learning`, `machine databases` и
`databases only` запрос `+machine -databases` возвращает второй документ вместо
первого. Минимальный запрос `-databases` также не исключает документы с этим
термом; quoted phrases при этом работают.

**Ссылки:** [regression-тест в PR #53018](https://github.com/ydb-platform/ydb/pull/53018/files#diff-73602f2cb1f311d5fa866ccbfd5821911da9cae99f4f33d20c3ec96e33c0aaf6R138-R160),
[описание отрицательных термов Query mode](https://github.com/ydb-platform/ydb/blob/35fe6b99b6b321ebc724e0c644432b2301f49da6/ydb/docs/ru/core/yql/reference/builtins/fulltext.md#L20-L33).

#### Hybrid search: linear mode падает через Scripting API

**Задача:** [YDBBUGS-856](https://st.yandex-team.ru/YDBBUGS-856).

**Предлагаемый Area:** `area/datashard`.

**Предлагаемый приоритет:** `Средний`.

**Обоснование:** документированный режим полностью недоступен через публичный
Scripting API и выдаёт внутренний invariant вместо пользовательской ошибки.

**Тип:** реализация. Корректный HybridRank linear работает через Query API, но
через Scripting API падает на внутреннем invariant
`requirement resultsSize == 1 failed`; RRF через оба API работает. Тест:
`test_scripting_api_hybrid_linear_mode_returns_ranked_rows` — падает.

**Шаги воспроизведения:**

1. Создать таблицу с `fulltext_relevance` и `vector_kmeans_tree` indexes и
   загрузить четыре документа.
2. Выполнить `ORDER BY HybridRank(..., "linear" AS Mode)` через Query API.
3. Выполнить тот же запрос через Scripting API; повторить с `Normalize=false`.

**Ожидаемое поведение:** оба публичных API возвращают один и тот же ранжированный
набор.

**Фактическое поведение:** Query API проходит, Scripting API падает на внутреннем
`requirement resultsSize == 1 failed`.

**Полезные детали:** ошибка воспроизводится для default normalization и
`Normalize=false`. Тот же запрос с теми же fulltext/vector indexes проходит
через Query API (`ydb sql`); RRF проходит через оба API. Внутренняя проверка
срабатывает в `ydb/core/kqp/executer_actor/kqp_tasks_graph.cpp:3745`.

**Ссылки:** [regression-тест в PR #53018](https://github.com/ydb-platform/ydb/pull/53018/files#diff-73602f2cb1f311d5fa866ccbfd5821911da9cae99f4f33d20c3ec96e33c0aaf6R161-R225),
[падающий invariant в tasks graph](https://github.com/ydb-platform/ydb/blob/35fe6b99b6b321ebc724e0c644432b2301f49da6/ydb/core/kqp/executer_actor/kqp_tasks_graph.cpp#L3738-L3749),
[контракт linear mode](https://github.com/ydb-platform/ydb/blob/35fe6b99b6b321ebc724e0c644432b2301f49da6/ydb/docs/ru/core/yql/reference/syntax/select/hybrid_search.md#L54-L61).

#### Global index: успешный build публикует error issues для пустых партиций

**Задача:** [YDBBUGS-857](https://st.yandex-team.ru/YDBBUGS-857).

**Предлагаемый Area:** `area/datashard`.

**Предлагаемый приоритет:** `Средний`.

**Обоснование:** ложная ошибка ломает мониторинг и автоматическую оценку
операции, но сам индекс строится успешно и данные остаются корректными.

**Тип:** реализация. Операция завершается `SUCCESS`, но содержит `Error: Shard
or requested range is empty` для каждого пустого key range. Пустой scan —
штатный результат и не должен выглядеть как ошибка успешной операции. Тест:
`test_successful_build_on_empty_partitions_has_no_error_issues` — падает.

**Шаги воспроизведения:**

1. Создать пустую строковую таблицу с `UNIFORM_PARTITIONS = 4`.
2. Запустить online build global secondary index.
3. Дождаться завершения и выполнить `operation get`.
4. Повторить на таблице, где заполнена только часть key ranges.

**Ожидаемое поведение:** успешная операция не содержит error-level issues для
штатных пустых ranges.

**Фактическое поведение:** статус равен `SUCCESS`, но для каждой пустой партиции
опубликован `Error: Shard or requested range is empty`.

**Полезные детали:** на пустой таблице с четырьмя партициями операция имеет
`ready=true`, `status=SUCCESS`, `state=Done`, `progress=100%`, но выдаёт issue на
каждый shard; на заполненной таблице — на три пустых ranges. В shard details
`Status=DONE`, но `UploadStatus=STATUS_CODE_UNSPECIFIED`. Вероятные места:
`ydb/core/tx/datashard/build_index/secondary_index.cpp` и `common_helper.h`.

**Ссылки:** [regression-тест в PR #53018](https://github.com/ydb-platform/ydb/pull/53018/files#diff-b41d2ee4e3fb9689c6b4d4bb629093871c1709c40c1118cf746a205785e39453R49-R77),
[issue в secondary index scan](https://github.com/ydb-platform/ydb/blob/35fe6b99b6b321ebc724e0c644432b2301f49da6/ydb/core/tx/datashard/build_index/secondary_index.cpp#L218-L232),
[тот же issue в общем helper](https://github.com/ydb-platform/ydb/blob/35fe6b99b6b321ebc724e0c644432b2301f49da6/ydb/core/tx/datashard/build_index/common_helper.h#L181-L195).

#### Local bloom index: ALTER INDEX не меняет параметры row index

**Задача:** [YDBBUGS-858](https://st.yandex-team.ru/YDBBUGS-858).

**Предлагаемый Area:** `area/datashard`.

**Предлагаемый приоритет:** `Средний`.

**Обоснование:** документированная операция полностью не работает для row bloom
index, однако существующий индекс и запросы остаются корректными.

**Тип:** реализация. Документированный ALTER
`false_positive_probability` отклоняется с `Only index with one impl table is
supported`, значение не меняется. Нужно реализовать row-local путь либо сузить
публичный контракт. Тест:
`test_alter_false_positive_probability_on_row_bloom_index` — падает.

**Шаги воспроизведения:**

1. Создать row table с PK `(tenant, id)` и local bloom index по `(tenant)` с
   `false_positive_probability = 0.01`.
2. Выполнить `ALTER TABLE ... ALTER INDEX ... SET` со значением `0.5`.
3. Проверить настройку через `SHOW CREATE TABLE`.

**Ожидаемое поведение:** ALTER успешен, а SHOW CREATE показывает `0.5`.

**Фактическое поведение:** ALTER отклоняется с
`Only index with one impl table is supported`; остаётся `0.01`.

**Полезные детали:** индекс корректно создан по левому префиксу составного PK с
FPP `0.01`; `ALTER ... SET (false_positive_probability=0.5)` отклоняется, а
`SHOW CREATE TABLE` сохраняет `0.01`. Сообщение указывает, что общий ALTER-путь
ожидает отдельную implementation table, которой у row-local bloom index нет.

**Ссылки:** [regression-тест в PR #53018](https://github.com/ydb-platform/ydb/pull/53018/files#diff-7ec4e857f553171236c8c10b6b93181420eb882292c3a037ce8b08b3195b8f17R33-R57),
[место формирования ошибки](https://github.com/ydb-platform/ydb/blob/35fe6b99b6b321ebc724e0c644432b2301f49da6/ydb/core/kqp/provider/yql_kikimr_exec.cpp#L2947-L2960),
[документированный ALTER INDEX](https://github.com/ydb-platform/ydb/blob/35fe6b99b6b321ebc724e0c644432b2301f49da6/ydb/docs/ru/core/yql/reference/syntax/alter_table/indexes.md#L157-L193).

### Документация (23, из них 21 `[IGNORE]`)

#### [IGNORE] CDC docs: отсутствует матрица совместимости mode, format и options

**Предлагаемый Area:** `area/core`.

**Предлагаемый приоритет:** `Средний`.

**Обоснование:** пользователь обнаруживает несовместимые сочетания только при
выполнении DDL, но данные не повреждаются и сервер возвращает явную ошибку.

**Тип:** документация. Параметры выглядят свободно комбинируемыми, хотя
Debezium несовместим с `UPDATES`, virtual timestamps и barriers. Нужна явная
таблица допустимых сочетаний.

**Шаги воспроизведения:** открыть список changefeed options и попытаться создать
Debezium changefeed последовательно с `MODE=UPDATES`, `VIRTUAL_TIMESTAMPS=TRUE`
и `BARRIERS_INTERVAL`.

**Ожидаемое поведение:** документация заранее перечисляет допустимые сочетания и
объясняет каждое ограничение.

**Фактическое поведение:** список выглядит свободно комбинируемым; все три
варианта сервер отклоняет только при выполнении DDL.

**Полезные детали:** сервер явно отклоняет `UPDATES + DEBEZIUM_JSON`,
`VIRTUAL_TIMESTAMPS + DEBEZIUM_JSON` и `BARRIERS_INTERVAL + DEBEZIUM_JSON`.
Проверки уже находятся в
`ydb/core/tx/schemeshard/schemeshard__operation_create_cdc_stream.cpp`; их нужно
перенести в таблицу на странице `alter_table/changefeed.md`.

**Ссылки:** [публичный список changefeed options](https://github.com/ydb-platform/ydb/blob/35fe6b99b6b321ebc724e0c644432b2301f49da6/ydb/docs/ru/core/yql/reference/syntax/alter_table/changefeed.md#L20-L34),
[серверная проверка совместимости Debezium](https://github.com/ydb-platform/ydb/blob/35fe6b99b6b321ebc724e0c644432b2301f49da6/ydb/core/tx/schemeshard/schemeshard__operation_create_cdc_stream.cpp#L183-L265).

#### [IGNORE] CDC docs: не определена omission-семантика Debezium before и after

**Предлагаемый Area:** `area/core`.

**Предлагаемый приоритет:** `Средний`.

**Обоснование:** неоднозначность может сломать downstream JSON-обработчики, но
фактический payload стабилен и проблему можно обойти после изучения событий.

**Тип:** документация. Для insert/read отсутствует `before`, для delete —
`after`; при удалении отсутствующей строки оба поля отсутствуют, а не равны
`null` или `{}`. Это важно для downstream JSON-обработки и должно быть явно
закреплено.

**Шаги воспроизведения:** создать Debezium changefeed, выполнить insert/read из
initial scan и delete, затем сравнить JSON payload с описанием `before/after`.

**Ожидаемое поведение:** документация однозначно говорит, когда поле отсутствует,
когда равно JSON `null`, а когда содержит объект.

**Фактическое поведение:** insert/read не содержат `before`, delete не содержит
`after`, а delete отсутствующей строки не содержит ни одного image-поля; эта
семантика не описана.

**Полезные детали:** проверены Debezium-операции `c/u/d/r` и initial scan. При
delete отсутствующей строки приходит `op=d`, но оба image-поля полностью
отсутствуют. Документации следует различать absent, JSON `null` и `{}`.

**Ссылки:** [описание Debezium record structure](https://github.com/ydb-platform/ydb/blob/35fe6b99b6b321ebc724e0c644432b2301f49da6/ydb/docs/ru/core/concepts/cdc.md#L203-L234),
[SCD1-сценарий удаления](https://github.com/ydb-platform/ydb/blob/35fe6b99b6b321ebc724e0c644432b2301f49da6/ydb/docs/ru/core/analyst/practical-guides/scd/scd1-transfer.md#L50-L84).

#### [IGNORE] CDC docs: в SCD1 указан неверный путь к timestamp

**Предлагаемый Area:** `area/core`.

**Предлагаемый приоритет:** `Средний`.

**Обоснование:** ошибка локализована в одном выражении документации, рядом есть
корректный пример, а серверное поведение не затронуто.

**Тип:** документация. Указан `payload.ts_ms`, тогда как фактическое поле и
другой пример на той же странице используют `payload.source.ts_ms`.

**Шаги воспроизведения:** открыть SCD1 guide, взять путь `payload.ts_ms` из
описания и применить его к реальному Debezium CDC payload.

**Ожидаемое поведение:** путь извлекает timestamp события.

**Фактическое поведение:** поле находится в `payload.source.ts_ms`; указанный
путь не извлекает значение.

**Полезные детали:** затронут
`ydb/docs/ru/core/analyst/practical-guides/scd/scd1-transfer.md`; корректный путь
уже используется ниже на этой же странице, поэтому исправление локальное.

**Ссылки:** [неверный путь и корректное выражение на одной странице](https://github.com/ydb-platform/ydb/blob/35fe6b99b6b321ebc724e0c644432b2301f49da6/ydb/docs/ru/core/analyst/practical-guides/scd/scd1-transfer.md#L60-L84).

#### [IGNORE] CDC docs: максимум retention указан как 30 дней вместо 31

**Предлагаемый Area:** `area/core`.

**Предлагаемый приоритет:** `Низкий`.

**Обоснование:** расхождение консервативно ограничивает пользователя на один
день и не приводит к потере данных или неожиданному сокращению retention.

**Тип:** документация/контракт. Реализация принимает 31 день и содержит предел
`31 * 86400`, а документация обещает максимум 30. Нужно синхронизировать предел.

**Шаги воспроизведения:** прочитать заявленный максимум 30 дней, затем создать
CDC topic с `RETENTION_PERIOD=Interval('P31D')` и выполнить describe.

**Ожидаемое поведение:** документированный и серверный максимумы совпадают.

**Фактическое поведение:** 31 день принимается и отображается как `31d`, хотя
документация утверждает максимум 30 дней.

**Полезные детали:** CDC topic с `RETENTION_PERIOD=Interval('P31D')` успешно
создан и описан CLI как `RetentionPeriod: 31d`. Реализационный предел задан
константой `MaxPQLifetimeSeconds` в
`ydb/core/tx/schemeshard/schemeshard_impl.h`.

**Ссылки:** [документированный предел 30 дней](https://github.com/ydb-platform/ydb/blob/35fe6b99b6b321ebc724e0c644432b2301f49da6/ydb/docs/ru/core/concepts/cdc.md#L255-L267),
[реализационный предел 31 день](https://github.com/ydb-platform/ydb/blob/35fe6b99b6b321ebc724e0c644432b2301f49da6/ydb/core/tx/schemeshard/schemeshard_impl.h#L220-L229).

#### [IGNORE] CDC docs: требуется устаревший feature flag topic autopartitioning

**Предлагаемый Area:** `area/core`.

**Предлагаемый приоритет:** `Низкий`.

**Обоснование:** лишняя настройка вводит в заблуждение, но помечена реализацией
как always true и не меняет фактическую работу CDC.

**Тип:** документация. `enable_topic_autopartitioning_for_cdc` описан как
управляющий, хотя в protobuf он deprecated и always true. Упоминание следует
удалить из актуальной конфигурации.

**Шаги воспроизведения:** открыть таблицу feature flags, найти
`enable_topic_autopartitioning_for_cdc`, затем проверить определение поля в
protobuf и запустить CDC autopartitioning без явного флага.

**Ожидаемое поведение:** актуальная документация не требует настройки, которая
больше не управляет поведением.

**Фактическое поведение:** документация требует флаг, помеченный в коде как
deprecated и always true.

**Полезные детали:** расхождение находится в
`ydb/docs/ru/core/reference/configuration/feature_flags.md`; поле в
`ydb/core/protos/feature_flags.proto` помечено `deprecated` и `always true`.

**Ссылки:** [актуальная таблица feature flags](https://github.com/ydb-platform/ydb/blob/35fe6b99b6b321ebc724e0c644432b2301f49da6/ydb/docs/ru/core/reference/configuration/feature_flags.md#L1-L22),
[deprecated protobuf field](https://github.com/ydb-platform/ydb/blob/35fe6b99b6b321ebc724e0c644432b2301f49da6/ydb/core/protos/feature_flags.proto#L169-L177).

#### [IGNORE] CDC docs: не описан запрет RENAME и MOVE таблицы с changefeed

**Предлагаемый Area:** `area/core`.

**Предлагаемый приоритет:** `Средний`.

**Обоснование:** ограничение блокирует штатную схемную операцию и может сорвать
миграцию, хотя сервер отклоняет её безопасно и без изменения данных.

**Тип:** документация. Сервер возвращает `Cannot move table with cdc streams`,
но ограничение отсутствует в основных разделах CDC и RENAME.

**Шаги воспроизведения:** создать таблицу с changefeed и выполнить RENAME/MOVE
таблицы, предварительно проверив основные разделы ограничений CDC и RENAME.

**Ожидаемое поведение:** постоянный запрет явно указан до примеров миграции и
схемных операций.

**Фактическое поведение:** документация ограничение не содержит; сервер
возвращает `PRECONDITION_FAILED: Cannot move table with cdc streams`.

**Полезные детали:** запрет реализован в
`ydb/core/tx/schemeshard/schemeshard__operation_move_tables.cpp`. `TRUNCATE` при
наличии CDC тоже запрещён, но он уже описан; добавить нужно именно MOVE/RENAME.

**Ссылки:** [серверный запрет MOVE таблицы с CDC](https://github.com/ydb-platform/ydb/blob/35fe6b99b6b321ebc724e0c644432b2301f49da6/ydb/core/tx/schemeshard/schemeshard__operation_move_tables.cpp#L72-L89),
[основная документация CDC](https://github.com/ydb-platform/ydb/blob/35fe6b99b6b321ebc724e0c644432b2301f49da6/ydb/docs/ru/core/concepts/cdc.md#L1-L25).

#### [IGNORE] CDC docs: публичный список options расходится с принимаемым синтаксисом

**Предлагаемый Area:** `area/core`.

**Предлагаемый приоритет:** `Средний`.

**Обоснование:** непонятный статус принимаемых параметров создаёт риск опоры на
нестабильный контракт, но подтверждённого нарушения данных нет.

**Тип:** документация/контракт. Парсер принимает `SCHEMA_CHANGES` и alias
`RESOLVED_TIMESTAMPS`, но публичная справка их не описывает. Нужно либо
документировать семантику, либо не принимать internal/experimental параметры.

**Шаги воспроизведения:** сравнить публичный список changefeed options с ветками
парсера и выполнить DDL с `SCHEMA_CHANGES` и `RESOLVED_TIMESTAMPS`.

**Ожидаемое поведение:** каждый принимаемый публичным YQL параметр описан либо
явно помечен experimental/internal.

**Фактическое поведение:** parser принимает оба имени, но в справке они
отсутствуют или заменены другим названием без описанного alias.

**Полезные детали:** оба имени принимаются в
`ydb/core/kqp/provider/yql_kikimr_exec.cpp`; при этом публичный список содержит
`BARRIERS_INTERVAL`, но полностью пропускает `SCHEMA_CHANGES` и alias
`RESOLVED_TIMESTAMPS`.

**Ссылки:** [публичный список параметров](https://github.com/ydb-platform/ydb/blob/35fe6b99b6b321ebc724e0c644432b2301f49da6/ydb/docs/ru/core/yql/reference/syntax/alter_table/changefeed.md#L20-L34),
[принимаемые parser branches](https://github.com/ydb-platform/ydb/blob/35fe6b99b6b321ebc724e0c644432b2301f49da6/ydb/core/kqp/provider/yql_kikimr_exec.cpp#L3113-L3135).

#### [IGNORE] Partitioning docs: default partition size указан как 2000 MB вместо 2048

**Предлагаемый Area:** `area/datashard`.

**Предлагаемый приоритет:** `Низкий`.

**Обоснование:** отличие невелико и влияет преимущественно на расчёты ёмкости;
сервер применяет стабильное и наблюдаемое значение.

**Тип:** документация. Новая таблица показывает preferred size 2048 MB, тогда
как документация указывает 2000 MB и одновременно называет это 2 ГБ. Следует
зафиксировать одно значение и корректные единицы.

**Шаги воспроизведения:** создать row table без partitioning settings, выполнить
`scheme describe` и `SHOW CREATE`, затем сравнить preferred size с default в
документации.

**Ожидаемое поведение:** численное значение и единицы default совпадают.

**Фактическое поведение:** сервер показывает 2048 MB, документация — 2000 MB и
одновременно называет это 2 ГБ.

**Полезные детали:** `scheme describe` и `SHOW CREATE` новой таблицы стабильно
показывают `Preferred partition size (Mb): 2048`. Расхождение находится в
`ydb/docs/ru/core/concepts/datamodel/_includes/table.md`.

**Ссылки:** [описание 2000 MB и default](https://github.com/ydb-platform/ydb/blob/35fe6b99b6b321ebc724e0c644432b2301f49da6/ydb/docs/ru/core/concepts/datamodel/_includes/table.md#L48-L87).

#### Partitioning docs: default max partitions указан как 50 вместо 32768

**Задача:** [YDBBUGS-859](https://st.yandex-team.ru/YDBBUGS-859).

**Предлагаемый Area:** `area/datashard`.

**Предлагаемый приоритет:** `Средний`.

**Обоснование:** расхождение в сотни раз меняет ожидания capacity planning и
верхнюю границу auto-split, хотя само серверное поведение корректно.

**Тип:** документация. Поле новой таблицы не materialized, а effective default
равен 32768; документация указывает 50. Разница существенна для auto-split и
capacity planning.

**Шаги воспроизведения:** создать таблицу без max setting, проверить её describe
и effective default в `GetMaxPartitionsCount()`, затем сравнить с документацией.

**Ожидаемое поведение:** публичный default соответствует effective server
default либо сервер materializes заявленные 50.

**Фактическое поведение:** документация указывает 50; unset на сервере означает
32768.

**Полезные детали:** у новой таблицы поле max не установлено и CLI его не
показывает; effective default вычисляется `TTableInfo::GetMaxPartitionsCount()`
как `32 * 1024`. На той же странице документации указано 50.

**Ссылки:** [документированный max partitions](https://github.com/ydb-platform/ydb/blob/35fe6b99b6b321ebc724e0c644432b2301f49da6/ydb/docs/ru/core/concepts/datamodel/_includes/table.md#L98-L109),
[effective default 32768](https://github.com/ydb-platform/ydb/blob/35fe6b99b6b321ebc724e0c644432b2301f49da6/ydb/core/tx/schemeshard/schemeshard_info_types.h#L1315-L1323).

#### [IGNORE] Backup docs: для full backup и restore предлагается operation type incbackup

**Предлагаемый Area:** `area/core`.

**Предлагаемый приоритет:** `Средний`.

**Обоснование:** опубликованная команда не позволяет наблюдать критичные backup
и restore операции, но корректный тип можно подобрать через CLI help.

**Тип:** документация. Реальные типы различаются: `fullbackup`, `incbackup` и
`restore`. Команда `operation list incbackup` не наблюдает full и restore;
руководство должно выбирать правильный тип для каждой операции.

**Шаги воспроизведения:** выполнить full backup, incremental backup и restore;
после каждой операции запустить рекомендованный `ydb operation list incbackup`.

**Ожидаемое поведение:** документация использует тип операции, соответствующий
наблюдаемой команде.

**Фактическое поведение:** `incbackup` видит только incremental; full и restore
требуют соответственно `fullbackup` и `restore`.

**Полезные детали:** фактическое соответствие: `BACKUP collection` →
`fullbackup`, `BACKUP ... INCREMENTAL` → `incbackup`, `RESTORE collection` →
`restore`. Ошибка повторяется в getting-started, validation-and-testing и YQL
reference для backup/restore.

**Ссылки:** [getting started](https://github.com/ydb-platform/ydb/blob/35fe6b99b6b321ebc724e0c644432b2301f49da6/ydb/docs/ru/core/recipes/backup/backup-collections/getting-started.md#L34-L49),
[validation and testing](https://github.com/ydb-platform/ydb/blob/35fe6b99b6b321ebc724e0c644432b2301f49da6/ydb/docs/ru/core/recipes/backup/backup-collections/validation-and-testing.md#L7-L16),
[YQL BACKUP reference](https://github.com/ydb-platform/ydb/blob/35fe6b99b6b321ebc724e0c644432b2301f49da6/ydb/docs/ru/core/yql/reference/syntax/backup.md#L1-L35).

#### [IGNORE] Backup docs: monitoring script использует неподдерживаемый формат json

**Предлагаемый Area:** `area/core`.

**Предлагаемый приоритет:** `Средний`.

**Обоснование:** готовый monitoring script вообще не запускается, однако это
ошибка примера, а не механизма создания резервных копий.

**Тип:** документация. `ydb operation list incbackup --format json` текущим CLI
отклоняется. Нужно использовать поддерживаемый JSON-формат, например
`proto-json-base64`, и согласовать с ним jq-фильтры.

**Шаги воспроизведения:** скопировать monitoring script из
`validation-and-testing.md` и запустить его текущим YDB CLI.

**Ожидаемое поведение:** команда выдаёт JSON, после чего `jq` считает неуспешные
операции.

**Фактическое поведение:** CLI сразу отклоняет `--format json`; скрипт не доходит
до обработки `jq`.

**Полезные детали:** проблема находится в
`ydb/docs/ru/core/recipes/backup/backup-collections/validation-and-testing.md`.
CLI предлагает `pretty` и `proto-json-base64`; после замены формата нужно также
проверить имена полей, используемые опубликованным `jq`.

**Ссылки:** [неисполняемый monitoring script](https://github.com/ydb-platform/ydb/blob/35fe6b99b6b321ebc724e0c644432b2301f49da6/ydb/docs/ru/core/recipes/backup/backup-collections/validation-and-testing.md#L82-L94).

#### [IGNORE] JSON index docs: невалидный порядок RETURNING и PASSING

**Предлагаемый Area:** `area/datashard`.

**Предлагаемый приоритет:** `Средний`.

**Обоснование:** опубликованный запрос не компилируется, но парсер выдаёт явную
ошибку и рабочая форма показана в другом рецепте.

**Тип:** документация RU/EN. Показана форма `RETURNING ... PASSING ...`, которую
парсер отклоняет. Рабочий синтаксис: `PASSING ... RETURNING ...`.

**Шаги воспроизведения:** скопировать из RU или EN overview выражение
`JSON_VALUE(... RETURNING Int64 PASSING 42 AS v)` и выполнить его.

**Ожидаемое поведение:** опубликованный синтаксис парсится и использует
JsonPath-параметр.

**Фактическое поведение:** parser возвращает `mismatched input 'PASSING'`;
работает только порядок `PASSING ... RETURNING ...`.

**Полезные детали:** неверная форма приведена в RU и EN overview
`core/dev/json-indexes.md`; рецепт параметризованных запросов уже использует
правильный порядок. Ошибка парсера: `mismatched input 'PASSING'`.

**Ссылки:** [неверный RU-пример](https://github.com/ydb-platform/ydb/blob/35fe6b99b6b321ebc724e0c644432b2301f49da6/ydb/docs/ru/core/dev/json-indexes.md#L154-L166),
[неверный EN-пример](https://github.com/ydb-platform/ydb/blob/35fe6b99b6b321ebc724e0c644432b2301f49da6/ydb/docs/en/core/dev/json-indexes.md#L163-L175).

#### JSON index docs: ограничение primary key устарело

**Задача:** [YDBBUGS-860](https://st.yandex-team.ru/YDBBUGS-860).

**Предлагаемый Area:** `area/datashard`.

**Предлагаемый приоритет:** `Средний`.

**Обоснование:** документация ошибочно запрещает поддерживаемые схемы и может
вынудить пользователя усложнить модель данных, но рабочие сценарии не ломаются.

**Тип:** документация. Указан только одиночный integer PK, но реализация без
дополнительных flags поддерживает `Utf8` и составной PK через внутренний row-id
механизм. Следует описать актуальный контракт и version/flag boundaries.

**Шаги воспроизведения:** создать JSON index сначала на таблице с PK `Utf8`,
затем с составным PK `(Uint64, Utf8)`, загрузить данные и выполнить index lookup.

**Ожидаемое поведение:** документация перечисляет фактически поддерживаемые PK и
объясняет автоматически создаваемый row-id.

**Фактическое поведение:** обе схемы работают, хотя overview безусловно требует
одиночный integer PK.

**Полезные детали:** успешно проверены PK `Utf8` и составной `(Uint64, Utf8)`
без дополнительного row-id feature flag. Для строкового PK сервер создаёт
`__ydb_row_id` и `__ydb_unique_row_id`; запись без явного row id и поиск через
JSON index работают.

**Ссылки:** [устаревшее ограничение RU](https://github.com/ydb-platform/ydb/blob/35fe6b99b6b321ebc724e0c644432b2301f49da6/ydb/docs/ru/core/dev/json-indexes.md#L204-L214),
[то же ограничение EN](https://github.com/ydb-platform/ydb/blob/35fe6b99b6b321ebc724e0c644432b2301f49da6/ydb/docs/en/core/dev/json-indexes.md#L216-L226),
[актуальная row-id модель fulltext как аналог](https://github.com/ydb-platform/ydb/blob/35fe6b99b6b321ebc724e0c644432b2301f49da6/ydb/docs/ru/core/dev/fulltext-indexes.md#L154-L177).

#### [IGNORE] JSON index docs: не описан prefixed JSON index

**Предлагаемый Area:** `area/datashard`.

**Предлагаемый приоритет:** `Низкий`.

**Обоснование:** отсутствует описание дополнительной работающей возможности;
документированный базовый вариант остаётся корректным.

**Тип:** документация. Работает `ON (tenant, payload)`, но справка показывает
только одну JSON-колонку и не определяет типы prefix columns, допустимые
предикаты, обязательность prefix и взаимодействие с auto-select.

**Шаги воспроизведения:** создать JSON index `ON (tenant, payload)`, загрузить
строки разных tenants и выполнить запрос с равенством по tenant и JSON_VALUE.

**Ожидаемое поведение:** reference описывает эту поддерживаемую форму, правила
prefix columns и допустимые предикаты.

**Фактическое поведение:** prefixed index работает и возвращает правильную
строку, но справка утверждает, что индекс строится только по одной колонке.

**Полезные детали:** проверена форма `ON (tenant, payload)` и запрос с
`tenant=10` плюс `JSON_VALUE(...)`; он вернул ожидаемую строку. Дополнить нужно
`create_table/json_index.md`: допустимые типы/число prefix columns, условия
чтения и поддерживаемые сравнения.

**Ссылки:** [текущий одноколоночный JSON-index syntax](https://github.com/ydb-platform/ydb/blob/35fe6b99b6b321ebc724e0c644432b2301f49da6/ydb/docs/ru/core/yql/reference/syntax/create_table/json_index.md#L1-L38).

#### [IGNORE] Hybrid docs: quickstart строит vector index на пустой таблице без параметров

**Предлагаемый Area:** `area/datashard`.

**Предлагаемый приоритет:** `Средний`.

**Обоснование:** основной quickstart невозможно выполнить буквально, но ошибка
проявляется сразу и исправляется явным указанием двух параметров.

**Тип:** документация. Показанный DDL не исполняется: для пустой таблицы нельзя
вывести `vector_type` и `vector_dimension`. Нужно сначала загрузить данные либо
явно задать параметры индекса.

**Шаги воспроизведения:** выполнить quickstart буквально: создать показанную
пустую таблицу и сразу добавить vector index без `vector_type` и
`vector_dimension`.

**Ожидаемое поведение:** quickstart выполняется последовательно без скрытых
предусловий.

**Фактическое поведение:** ADD INDEX падает, потому что параметры нельзя вывести
из пустой таблицы.

**Полезные детали:** исходная ошибка:
`Cannot build vector index: table is empty and vector_type/vector_dimension were
not specified`. Для детерминированного quickstart лучше явно задать оба
параметра, а не полагаться на вывод из предварительно загруженных данных.

**Ссылки:** [неисполняемый DDL quickstart](https://github.com/ydb-platform/ydb/blob/35fe6b99b6b321ebc724e0c644432b2301f49da6/ydb/docs/ru/core/dev/hybrid-search.md#L27-L46),
[обязательные параметры vector index](https://github.com/ydb-platform/ydb/blob/35fe6b99b6b321ebc724e0c644432b2301f49da6/ydb/docs/ru/core/yql/reference/syntax/_includes/vector_index_parameters.md#L1-L12).

#### [IGNORE] Fulltext docs: quickstart содержит нестабильное/устаревшее значение BM25

**Предлагаемый Area:** `area/datashard`.

**Предлагаемый приоритет:** `Низкий`.

**Обоснование:** состав и порядок результата правильны; расходится только
пример численного score, который не должен использоваться как точный контракт.

**Тип:** документация. Для точных данных и DDL документация показывает score
`1.6215...`, реализация стабильно возвращает `0.9932...`. Если точный score не
является контрактом, его следует убрать или пометить version-dependent.

**Шаги воспроизведения:** выполнить точные DDL, INSERT и BM25-запрос из fulltext
quickstart и сравнить score первой строки с опубликованным выводом.

**Ожидаемое поведение:** пример либо показывает актуальное значение, либо не
фиксирует нестабильный score как точное число.

**Фактическое поведение:** набор и порядок совпадают, но значение стабильно
отличается от документации.

**Полезные детали:** два запуска точного quickstart дали тот же набор и порядок
строк, но score `0.9932448131764315` вместо `1.6215210957338408`. Следовательно,
ошибка ограничена опубликованным числом, а не поисковой семантикой.

**Ссылки:** [quickstart с точным BM25](https://github.com/ydb-platform/ydb/blob/35fe6b99b6b321ebc724e0c644432b2301f49da6/ydb/docs/ru/core/recipes/fulltext-search/fulltext-index-quickstart.md#L47-L62).

#### [IGNORE] Vector index docs: диапазон vector_dimension указан как 1..16384

**Предлагаемый Area:** `area/datashard`.

**Предлагаемый приоритет:** `Средний`.

**Обоснование:** неясная публичная граница влияет на совместимость схем и
планирование памяти, хотя документированный консервативный диапазон работает.

**Тип:** документация/валидация. Валидатор принимает 16385 и 65536 и отклоняет
65537 с явным диапазоном 1..65536. Нужно выбрать и синхронизировать публичный
предел.

**Шаги воспроизведения:** последовательно создать vector indexes с dimension 0,
1, 16385, 65536 и 65537 и сравнить ответы валидатора с диапазоном в reference.

**Ожидаемое поведение:** один и тот же публичный максимум используется в
документации и DDL validator.

**Фактическое поведение:** документация ограничивает dimension значением 16384,
но сервер принимает до 65536 включительно.

**Полезные детали:** DDL-матрица: 0 отклонён, 1 принят, 16385 принят, 65536
принят, 65537 отклонён с текстом `should be between 1 and 65536`. Затронут
`yql/reference/syntax/_includes/vector_index_parameters.md`.

**Ссылки:** [документированный диапазон 1..16384](https://github.com/ydb-platform/ydb/blob/35fe6b99b6b321ebc724e0c644432b2301f49da6/ydb/docs/ru/core/yql/reference/syntax/_includes/vector_index_parameters.md#L1-L12).

#### [IGNORE] Vector index docs: filter columns ошибочно названы «любыми»

**Предлагаемый Area:** `area/datashard`.

**Предлагаемый приоритет:** `Средний`.

**Обоснование:** пользователь может спроектировать неподдерживаемую схему, но
DDL безопасно отклоняется с указанием неверного key type.

**Тип:** документация. Например, `Json` в prefix filter отклоняется как
неподходящий key type. Следует сослаться на допустимые типы ключевых колонок и
описать ограничения prefix table.

**Шаги воспроизведения:** создать таблицу с `category Json` и `embedding String`,
затем добавить filtered vector index `ON (category, embedding)`.

**Ожидаемое поведение:** если filter columns названы «любыми», DDL принимается;
иначе документация заранее ограничивает их key-compatible типами.

**Фактическое поведение:** DDL отклоняется сообщением, что `Json` имеет неверный
key type.

**Полезные детали:** воспроизведение — `ON (category, embedding)` при
`category Json`; DDL возвращает `Column 'category' has wrong key type Json for
being key`. Нужна ссылка на точный список допустимых key types.

**Ссылки:** [утверждение про «любые» filter columns](https://github.com/ydb-platform/ydb/blob/35fe6b99b6b321ebc724e0c644432b2301f49da6/ydb/docs/ru/core/dev/vector-indexes.md#L49-L66),
[устройство prefix table](https://github.com/ydb-platform/ydb/blob/35fe6b99b6b321ebc724e0c644432b2301f49da6/ydb/docs/ru/core/dev/vector-indexes-kmeans-tree-type.md#L130-L146).

#### [IGNORE] Transactions docs: snapshot фиксируется не при явном BEGIN

**Предлагаемый Area:** `area/datashard`.

**Предлагаемый приоритет:** `Критический`.

**Обоснование:** неверное понимание границы snapshot может привести к принятию
бизнес-решения на данных, появившихся уже после формального начала транзакции.

**Тип:** документация. Изменение другой сессии между `BEGIN snapshot-ro/rw` и
первым data-запросом видно транзакции; snapshot фиксируется лениво первым
обращением к данным. Формулировка «на момент старта» описывает более сильную
гарантию.

**Шаги воспроизведения:** открыть Snapshot RO/RW транзакцию явным BEGIN; до её
первого SELECT закоммитить изменение из другой сессии; выполнить два чтения,
между ними сделав ещё один внешний commit.

**Ожидаемое поведение:** при формулировке «snapshot на момент старта» первый
SELECT видит состояние до внешнего commit после BEGIN.

**Фактическое поведение:** первый SELECT видит изменение после BEGIN; только с
первого data-запроса snapshot становится стабильным.

**Полезные детали:** сценарий одинаков для Snapshot RO и Snapshot RW: другая
сессия коммитит `405 → 505` после BEGIN, первый SELECT видит 505, последующие
конкурентные commits уже не видны. Затронут
`ydb/docs/ru/core/concepts/_includes/transactions.md`.

**Ссылки:** [контракт Snapshot RO](https://github.com/ydb-platform/ydb/blob/35fe6b99b6b321ebc724e0c644432b2301f49da6/ydb/docs/ru/core/concepts/_includes/transactions.md#L25-L33),
[описание атомарного применения транзакции](https://github.com/ydb-platform/ydb/blob/35fe6b99b6b321ebc724e0c644432b2301f49da6/ydb/docs/ru/core/concepts/_includes/transactions.md#L113-L124).

#### [IGNORE] Transactions docs: не объяснён one-shot характер Stale RO и Online RO

**Предлагаемый Area:** `area/datashard`.

**Предлагаемый приоритет:** `Средний`.

**Обоснование:** документация создаёт неверные архитектурные ожидания, но API
безопасно отклоняет неподдерживаемый BeginTransaction явной ошибкой.

**Тип:** документация. Query/Table API не позволяют открыть отдельную
интерактивную транзакцию в этих режимах, хотя обзор рассуждает о нескольких
SELECT одной транзакции. Нужно разделить BeginTransaction и one-shot
`BeginTx(...).CommitTx()`.

**Шаги воспроизведения:** попытаться выполнить отдельный BeginTransaction в
Stale RO и Online RO, затем выполнить допустимый one-shot SELECT с теми же
режимами и сопоставить это с обзором транзакций.

**Ожидаемое поведение:** документация явно разделяет unsupported interactive
transaction и поддерживаемый one-shot вызов.

**Фактическое поведение:** BeginTransaction отклоняется, one-shot проходит, но
обзор обсуждает несколько SELECT без ясного описания этой границы.

**Полезные детали:** experimental CLI прямо сообщает, что `stale-ro` нельзя
использовать с BEGIN; серверные unit tests ожидают `BAD_REQUEST` для отдельного
BeginTransaction в Stale RO и Online RO. One-shot SELECT в обоих режимах
проходит через Table CLI.

**Ссылки:** [Stale RO и Online RO в обзоре](https://github.com/ydb-platform/ydb/blob/35fe6b99b6b321ebc724e0c644432b2301f49da6/ydb/docs/ru/core/concepts/_includes/transactions.md#L35-L55).

#### [IGNORE] Global unique index docs: не определена семантика NULL

**Предлагаемый Area:** `area/datashard`.

**Предлагаемый приоритет:** `Средний`.

**Обоснование:** неоднозначность влияет на проектирование ограничений
уникальности, но фактическое поведение соответствует распространённой SQL-модели.

**Тип:** документация. Реализация допускает несколько строк с `NULL` и запрещает
повтор ненулевого значения. Нужно явно описать одно- и многоколоночный unique
key, включая частично `NULL` составной ключ.

**Шаги воспроизведения:** создать global unique index, вставить несколько строк
с `NULL` в indexed column, затем попытаться вставить две строки с одинаковым
ненулевым значением.

**Ожидаемое поведение:** документация заранее определяет, считается ли `NULL`
уникальным значением, в том числе для составного index key.

**Фактическое поведение:** несколько `NULL` разрешены, ненулевой дубликат
отклоняется; это правило в описании unique index отсутствует.

**Полезные детали:** несколько `NULL` entries доступны через index view;
повтор ненулевого значения отклоняется с `PRECONDITION_FAILED`. Отдельно остаётся
зафиксировать контракт составного ключа, где `NULL` имеет только часть колонок.

**Ссылки:** [текущий контракт unique secondary index](https://github.com/ydb-platform/ydb/blob/35fe6b99b6b321ebc724e0c644432b2301f49da6/ydb/docs/ru/core/concepts/_includes/secondary_indexes.md#L26-L31),
[DDL GLOBAL UNIQUE SYNC](https://github.com/ydb-platform/ydb/blob/35fe6b99b6b321ebc724e0c644432b2301f49da6/ydb/docs/ru/core/yql/reference/syntax/create_table/secondary_index.md#L27-L37).

#### [IGNORE] Local bloom docs: основной пример невалиден для созданной row table

**Предлагаемый Area:** `area/datashard`.

**Предлагаемый приоритет:** `Средний`.

**Обоснование:** основной пример невозможно выполнить буквально, но сервер
безопасно отклоняет DDL и ограничение корректно описано ниже на той же странице.

**Тип:** документация. Пример без `STORE=COLUMN` создаёт row table с PK `(id)`,
но индексирует `resource_id`, который не является левым префиксом PK. Нужно
сделать пример column-store, изменить PK или явно указать область применимости.

**Шаги воспроизведения:** скопировать основной CREATE TABLE из
`bloom-skip-indexes.md` и выполнить его как есть.

**Ожидаемое поведение:** опубликованный пример создаёт индекс на показанной
таблице.

**Фактическое поведение:** таблица является row-store с PK `(id)`, а индекс по
`resource_id` отклоняется как не являющийся левым префиксом PK.

**Полезные детали:** буквальная ошибка:
`Bloom filter column 'resource_id' does not match PK column 'id' at position 0`.
На той же странице ниже уже указано правило непрерывного левого префикса PK,
поэтому пример противоречит собственному объяснению.

**Ссылки:** [невалидный пример](https://github.com/ydb-platform/ydb/blob/35fe6b99b6b321ebc724e0c644432b2301f49da6/ydb/docs/ru/core/dev/bloom-skip-indexes.md#L31-L57),
[описание ограничений row bloom index](https://github.com/ydb-platform/ydb/blob/35fe6b99b6b321ebc724e0c644432b2301f49da6/ydb/docs/ru/core/dev/bloom-skip-indexes.md#L59-L75).

#### [IGNORE] CREATE VIEW docs: неверно описано сохранение TablePathPrefix

**Предлагаемый Area:** `area/core`.

**Предлагаемый приоритет:** `Средний`.

**Обоснование:** противоречие между двумя страницами заставляет использовать
ненужные абсолютные пути, но реализация и `SHOW CREATE` работают согласованно.

**Тип:** документация. Страница требует абсолютные underlying paths и говорит,
что prefix создания не виден view. Реализация сохраняет `TablePathPrefix`,
относительный view работает, а `SHOW CREATE VIEW` выводит сохранённый PRAGMA.
Нужно синхронизировать `create-view.md` с реализацией и `show_create.md`.

**Шаги воспроизведения:** задать `PRAGMA TablePathPrefix`, создать view с
относительной ссылкой на таблицу, прочитать view при другом внешнем prefix и
выполнить `SHOW CREATE VIEW`.

**Ожидаемое поведение:** `create-view.md` и `show_create.md` одинаково описывают,
сохраняется ли prefix и нужны ли абсолютные underlying paths.

**Фактическое поведение:** относительный view работает и SHOW CREATE сохраняет
PRAGMA, хотя `create-view.md` требует абсолютный путь и отрицает наследование.

**Полезные детали:** view создаётся с `PRAGMA TablePathPrefix="/local/vdir"` и
относительной ссылкой `t`, работает даже при другом внешнем prefix, а `SHOW
CREATE VIEW` возвращает сохранённый PRAGMA. Это подтверждает существующий unit
test `ydb/core/sys_view/ut_show_create.cpp::ViewWithTablePathPrefix`.

**Ссылки:** [противоречивое требование абсолютных путей](https://github.com/ydb-platform/ydb/blob/35fe6b99b6b321ebc724e0c644432b2301f49da6/ydb/docs/ru/core/yql/reference/syntax/create-view.md#L30-L36),
[контракт SHOW CREATE с сохранённым prefix](https://github.com/ydb-platform/ydb/blob/35fe6b99b6b321ebc724e0c644432b2301f49da6/ydb/docs/ru/core/yql/reference/syntax/show_create.md#L30-L38),
[unit test ViewWithTablePathPrefix](https://github.com/ydb-platform/ydb/blob/35fe6b99b6b321ebc724e0c644432b2301f49da6/ydb/core/sys_view/ut_show_create.cpp#L372-L410).

## Области без кандидатов в баги

В проверенной матрице TTL, BATCH UPDATE/DELETE, Bulk Upsert и keyset pagination
не дали подтверждённых дефектов реализации или документации. Их покрытие
сохранено выше и в отдельных подробных отчётах.

## Regression tests

Постоянные regression-targets добавлены только для реализационных проблем:

- `ydb/tests/functional/automatic_partitioning_doc_audit`;
- `ydb/tests/functional/backup_collection_doc_audit`;
- `ydb/tests/functional/cdc_doc_audit`;
- `ydb/tests/functional/fulltext_hybrid_doc_audit`;
- `ydb/tests/functional/global_index_doc_audit`;
- `ydb/tests/functional/json_index_doc_audit`;
- `ydb/tests/functional/local_index_doc_audit`.

Документационные проблемы отдельными тестами не покрывались. Временные проверки,
не подтвердившие дефект, в рабочем дереве не оставлены.
