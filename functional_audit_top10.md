# Топ-10 направлений для функционального аудита YDB

Список составлен по всей структуре документации YDB, а не только по разделам,
связанным с CDC. CDC, Debezium и специфичные для CDC сценарии намеренно
исключены: каждое направление ниже представляет отдельный новый аудит.

## 1. Потоковые запросы

Что проверить:

- watermarks, окна и опоздавшие события;
- checkpoint и восстановление после перезапуска;
- изменение запроса через `ALTER`, остановку и повторный запуск;
- запись результатов в таблицы;
- заявленную гарантию `at-least-once`;
- документированные аномалии control plane, включая первое неполное окно и
  пересоздание запроса.

Почему это перспективно: функциональность новая, асинхронная и хранит состояние.
Сочетание времени событий, checkpoint и перезапусков создаёт много граничных
состояний.

Документация:

- [Гарантии доставки данных](ydb/docs/ru/core/dev/streaming-query/guarantees.md)
- [Watermarks](ydb/docs/ru/core/dev/streaming-query/watermarks.md)
- [Checkpoint](ydb/docs/ru/core/dev/streaming-query/checkpoints.md)

## 2. JSON-индексы

Что проверить:

- `JSON_EXISTS` и `JSON_VALUE`;
- отсутствующее поле, JSON `null` и SQL `NULL`;
- несовпадающий тип значения;
- массивы, вложенные пути и переменные JsonPath;
- поддерживаемые и неподдерживаемые предикаты;
- актуализацию индекса после `INSERT`, `UPSERT`, `UPDATE` и `DELETE`;
- изменение типа значения в уже проиндексированном пути;
- соответствие результата запроса с индексом полному сканированию таблицы.

Почему это перспективно: здесь пересекаются семантика JSON, преобразование типов,
трёхзначная логика и асинхронное обслуживание индекса.

Документация:

- [JSON-индексы](ydb/docs/ru/core/dev/json-indexes.md)
- [Рецепты поиска по JSON](ydb/docs/ru/core/recipes/json-search/index.md)

## 3. Полнотекстовый и гибридный поиск

Что проверить:

- `fulltext_plain`, relevance, N-граммы и filtered index;
- Unicode, разные регистры, пунктуацию, пустые и очень длинные строки;
- разные поддерживаемые типы первичного ключа;
- обновление и удаление документов;
- удаление и повторное создание индекса;
- фильтрацию одновременно с ранжированием;
- объединение полнотекстового и векторного результатов в гибридном поиске;
- стабильность сортировки при одинаковом score.

Почему это перспективно: несколько видов индекса имеют разные правила
токенизации и построения, а гибридный поиск добавляет объединение результатов и
ранжирование.

Документация:

- [Полнотекстовые индексы](ydb/docs/ru/core/dev/fulltext-indexes.md)
- [Гибридный поиск](ydb/docs/ru/core/dev/hybrid-search.md)
- [Рецепты полнотекстового поиска](ydb/docs/ru/core/recipes/fulltext-search/index.md)

## 4. Векторные индексы

Что проверить:

- все документированные функции расстояния;
- глобальный, фильтрующий и покрывающий индексы;
- граничные размерности, `NULL`, пустые и одинаковые векторы;
- обновление данных во время построения индекса;
- удаление строк и повторную вставку с тем же ключом;
- партиционирование индексных таблиц и использование реплик;
- перестроение и удаление индекса;
- влияние `overlap` и других параметров поиска.

Результат приближённого поиска следует сравнивать с точным поиском, используя
явно заданный допустимый recall, а не требуя полного совпадения порядка.

Почему это перспективно: документация отдельно описывает неконсистентность при
обновлении во время построения и отсутствие пересчёта кластеров. Вокруг этих
ограничений особенно важны граничные и восстановительные сценарии.

Документация:

- [Векторные индексы](ydb/docs/ru/core/dev/vector-indexes.md)
- [vector_kmeans_tree](ydb/docs/ru/core/dev/vector-indexes-kmeans-tree-type.md)
- [Рецепты векторного поиска](ydb/docs/ru/core/recipes/vector-search/index.md)

## 5. Workload Manager

Что проверить:

- `CONCURRENT_QUERY_LIMIT`;
- `QUEUE_SIZE` и переполнение очереди;
- `DATABASE_LOAD_CPU_THRESHOLD`;
- распределение по `RESOURCE_WEIGHT`;
- пул по умолчанию и явный выбор пула;
- конфликтующие классификаторы и порядок их выбора;
- ACL пула и классификатора;
- отмену ожидающего и уже выполняющегося запроса;
- изменение или удаление пула, классификатора и пользователя при наличии
  очереди;
- соответствие plan, системных представлений и метрик фактическому состоянию.

Почему это перспективно: результат зависит сразу от конкурентности, очередей,
динамической конфигурации и авторизации. Особенно полезны гонки между отменой,
переклассификацией и выдачей ресурса.

Документация:

- [Workload Manager](ydb/docs/ru/core/dev/resource-consumption-management.md)

## 6. Backup collections и восстановление

Что проверить:

- резервную копию неоднородной схемы с таблицами, индексами, ACL и другими
  поддерживаемыми объектами;
- несколько последовательных резервных копий;
- изменение или удаление исходных объектов после создания копии;
- восстановление в пустой каталог;
- восстановление в каталог с конфликтующими объектами;
- повторный запуск операции после частичной ошибки;
- отмену и перезапуск операции;
- статусы, диагностические сообщения и очистку операций;
- идентичность данных, схемы и поддерживаемых метаданных после восстановления;
- экспорт во внешнее хранилище и импорт обратно.

Почему это перспективно: ошибки восстановления потенциально приводят к потере
данных, а сценарий проходит через большое число типов схемных объектов и
асинхронных операций.

Документация:

- [Проверка и тестирование резервных копий](ydb/docs/ru/core/recipes/backup/backup-collections/validation-and-testing.md)
- [Импорт и восстановление](ydb/docs/ru/core/recipes/backup/backup-collections/importing-and-restoring.md)
- [Резервное копирование и восстановление](ydb/docs/ru/core/devops/backup-and-recovery/index.md)

## 7. Федеративные запросы к S3-совместимому хранилищу

Что проверить:

- разумную pairwise-матрицу форматов и алгоритмов сжатия;
- соответствие типов YDB типам внешних форматов;
- `csv`, `tsv`, варианты JSON, Parquet и raw;
- отсутствующие, лишние и переставленные поля;
- повреждённые и пустые файлы;
- несколько файлов с различающимися схемами;
- partition projection и фильтрацию по виртуальным колонкам;
- predicate pushdown;
- импорт и экспорт данных;
- повтор операции после сетевой ошибки или частичной записи.

Для воспроизводимого локального аудита можно использовать MinIO вместо внешнего
облачного S3.

Почему это перспективно: число сочетаний форматов, сжатия, типов и правил
выведения схемы велико, но его можно разумно сократить pairwise-покрытием.

Документация:

- [Форматы и алгоритмы сжатия](ydb/docs/ru/core/concepts/query_execution/federated_query/s3/formats.md)
- [Проекция партиций](ydb/docs/ru/core/concepts/query_execution/federated_query/s3/partition_projection.md)
- [Импорт и экспорт](ydb/docs/ru/core/concepts/query_execution/federated_query/import_and_export.md)

## 8. Kafka API

Что проверить настоящими Kafka-клиентами:

- получение metadata;
- produce и fetch;
- consumer groups и несколько конкурирующих читателей;
- commit и восстановление offsets;
- ребалансировку при подключении и отключении клиента;
- reconnect после разрыва соединения;
- большие сообщения, пустые batch и граничные timeout;
- аутентификацию SASL;
- обработку неподдерживаемых запросов протокола;
- соответствие поведения опубликованному списку ограничений.

Почему это перспективно: black-box проверка совместимости обнаруживает
расхождения, которые не видны в тестах нативного Topic API. Важно запускать
разные версии хотя бы одного распространённого Kafka-клиента.

Документация:

- [Kafka API](ydb/docs/ru/core/reference/kafka-api/index.md)
- [Ограничения Kafka API](ydb/docs/ru/core/reference/kafka-api/constraints.md)
- [Примеры использования](ydb/docs/ru/core/reference/kafka-api/examples.md)

## 9. Автоматическое партицирование строковых таблиц

Что проверить:

- split и merge по размеру;
- split по нагрузке;
- минимальное и максимальное число партиций;
- явные границы партиций;
- простые и составные первичные ключи;
- последовательную смену настроек автошардирования;
- очередь операций split/merge;
- достижение лимитов уровня базы данных;
- согласованность данных и вторичных индексов после переразбиения;
- перезапуск узлов во время split или merge.

Почему это перспективно: операции асинхронны, зависят от нагрузки и затрагивают
маршрутизацию запросов. Проверка должна контролировать не только количество
партиций, но и непрерывную корректность чтения и записи во время изменения.

Документация:

- [Автоматическое партицирование](ydb/docs/ru/core/dev/tables/partitioning/auto/index.md)
- [Как работает партицирование](ydb/docs/ru/core/dev/tables/partitioning/index.md)

## 10. Авторизация, наследование ACL и владение объектами

Что проверить:

- пользователя, группу и вложенные группы;
- `GRANT` и `REVOKE` для каждого класса объектов;
- наследуемые права и переопределение на дочернем объекте;
- смену владельца;
- rename и move объекта с установленными ACL;
- создание дочернего объекта пользователем с ограниченными правами;
- обращение через уже открытую сессию после отзыва прав;
- удаление пользователя или группы, упомянутых в ACL;
- краткую и полную формы управления доступом;
- соответствие решений авторизации записям audit log.

Почему это перспективно: здесь важны не только отдельные разрешения, но и
наследование, кеширование результата аутентификации и изменение схемных объектов.
Ошибка может иметь последствия для безопасности.

Документация:

- [Авторизация](ydb/docs/ru/core/security/authorization.md)
- [Аудитный лог](ydb/docs/ru/core/security/audit-log.md)
- [Кеширование результатов аутентификации](ydb/docs/ru/core/security/caching-authentication-results.md)

## Рекомендуемый порядок

По ожидаемому соотношению вероятности найти функциональные дефекты и стоимости
локального воспроизведения:

1. Потоковые запросы.
2. JSON-индексы.
3. Полнотекстовый и гибридный поиск.
4. Workload Manager.
5. Backup collections и восстановление.
6. Векторные индексы.
7. Автоматическое партицирование.
8. Авторизация и ACL.
9. Kafka API.
10. Федеративные запросы к S3.

Первые три направления лучше всего подходят для следующего аудита: они содержат
много документированных сочетаний, сравнительно легко воспроизводятся локально и
имеют высокую вероятность функциональных расхождений.

## Требования к сборке и проверке

Последующие сборки и тестовые прогоны необходимо выполнять с AddressSanitizer:

```bash
./ya make --build relwithdebinfo --sanitize address -tA <folder> 2>&1 | tail
```

Это не заменяет функциональные проверки. Для каждого аудита нужны одновременно:

1. black-box проверка по документации через CLI, SDK или совместимый внешний
   клиент;
2. воспроизводящий автоматический тест;
3. запуск теста на ASAN-сборке для выявления ошибок работы с памятью;
4. итоговый Markdown-отчёт с отделением дефектов реализации от дефектов
   документации.

# Ещё 10 направлений для функционального аудита

Эта вторая десятка не повторяет предыдущий список и уже завершённые аудиты JSON,
fulltext/hybrid, vector indexes, backup collections и автоматического
партиционирования.

## 11. Транзакции, уровни изоляции и MVCC

Что проверить:

- read-only, online read-only, stale read-only и serializable read-write;
- конфликты write/write и read/write на одной и разных партициях;
- read-your-writes для INSERT, UPDATE, DELETE и secondary indexes;
- snapshot consistency при параллельном изменении нескольких таблиц;
- commit после timeout, cancel, disconnect и потери сессии;
- distributed transaction при split/merge или рестарте data shard;
- лимиты размера/числа затронутых шардов и качество диагностики.

Почему перспективно: комбинация нескольких таблиц, шардов и retry-семантики
часто обнаруживает расхождения, которых нет в однострочных happy-path тестах.

Документация:

- [Транзакции](ydb/docs/ru/core/concepts/transactions.md)
- [MVCC](ydb/docs/ru/core/concepts/query_execution/mvcc.md)
- [Управление транзакциями в SDK](ydb/docs/ru/core/recipes/ydb-sdk/tx-control.md)

## 12. TTL строковых таблиц

Что проверить:

- TTL по Date, Datetime, Timestamp и целочисленному Unix time;
- секунды, миллисекунды и микросекунды;
- `NULL`, даты в прошлом/будущем и граничные timestamp;
- включение, изменение и отключение TTL на заполненной таблице;
- удаление одновременно с UPDATE ключа или TTL-колонки;
- secondary indexes и changefeeds при TTL-удалении;
- рестарт узла во время очистки;
- соответствие фактической задержки и системных метрик документации.

Почему перспективно: TTL асинхронен и соединяет преобразование времени,
фоновое удаление, индексы и CDC.

Документация:

- [TTL](ydb/docs/ru/core/concepts/ttl.md)
- [YQL-рецепты TTL](ydb/docs/ru/core/yql/reference/recipes/ttl.md)
- [TTL через CLI](ydb/docs/ru/core/recipes/ydb-cli/ttl.md)

## 13. Глобальные secondary indexes

Что проверить:

- sync и async index, unique и covering-варианты;
- nullable и составные index keys;
- несколько строк с одинаковым ключом и переход к конфликтующему unique key;
- read-your-writes во всех DML-комбинациях;
- построение индекса при конкурентной записи;
- отмену, повтор, удаление и пересоздание build operation;
- изменение схемы основной таблицы при существующем индексе;
- принудительный `VIEW` против автоматического выбора оптимизатором.

Почему перспективно: индекс является отдельной таблицей, а build и обслуживание
данных проходят разными асинхронными путями.

Документация:

- [Вторичные индексы](ydb/docs/ru/core/dev/secondary-indexes.md)
- [CREATE TABLE: secondary index](ydb/docs/ru/core/yql/reference/syntax/create_table/secondary_index.md)
- [SELECT VIEW](ydb/docs/ru/core/yql/reference/syntax/select/secondary_index.md)

## 14. Локальные bloom и min/max skip indexes

Что проверить:

- допустимые типы и составные ключи;
- все параметры bloom/ngram и их границы;
- min/max для монотонных, случайных и `NULL`-значений;
- false-positive без false-negative на контрольном full scan;
- UPDATE/DELETE/UPSERT и compaction;
- добавление индекса на заполненную таблицу;
- ALTER параметров, DROP и повторное создание;
- корректность plan и результата при принудительном/автоматическом выборе.

Почему перспективно: skip indexes зависят от локальной организации данных и
compaction, поэтому особенно чувствительны к изменению уже записанных строк.

Документация:

- [Локальные индексы](ydb/docs/ru/core/dev/local-indexes/index.md)
- [Bloom skip index](ydb/docs/ru/core/dev/bloom-skip-indexes.md)
- [Min/max skip index quickstart](ydb/docs/ru/core/recipes/min_max-skip-index/min_max-skip-index-quickstart.md)

## 15. Представления и SHOW CREATE

Что проверить:

- `security_invoker` и права вызывающего пользователя;
- фиксацию `TablePathPrefix` и разрешение относительных путей;
- вложенные views и циклические зависимости;
- переименование/удаление исходных таблиц и колонок;
- изменение типов исходной схемы;
- параметры, типы и `NULL` в запросах к view;
- SHOW CREATE → DROP → выполнение восстановленного DDL;
- одинаковое поведение Query и Scripting API.

Почему перспективно: view хранит запрос и контекст его компиляции, поэтому
schema evolution и security context дают много граничных состояний.

Документация:

- [Представления](ydb/docs/ru/core/concepts/datamodel/view.md)
- [CREATE VIEW](ydb/docs/ru/core/yql/reference/syntax/create-view.md)
- [SHOW CREATE](ydb/docs/ru/core/yql/reference/syntax/show_create.md)

## 16. Native Topic API и consumer groups

Что проверить:

- запись с partition key, explicit partition и producer id;
- дедупликацию и продолжение sequence number после reconnect;
- commit offsets, повторную доставку и чтение с timestamp;
- ребалансировку между несколькими consumers;
- добавление/удаление consumer во время чтения;
- retention по времени и размеру;
- codecs, большие сообщения и transaction-bound writes;
- auto-partitioning topic и порядок сообщений после split.

Почему перспективно: это проверяет нативные гарантии topics независимо от слоя
совместимости Kafka, уже предложенного в первой десятке.

Документация:

- [Топики](ydb/docs/ru/core/concepts/datamodel/topic.md)
- [Выполнение запросов к топикам](ydb/docs/ru/core/concepts/query_execution/topics.md)
- [Topic API в SDK](ydb/docs/ru/core/reference/ydb-sdk/index.md)

## 17. Coordination service: lock, semaphore и leader election

Что проверить:

- exclusive/shared semaphore и ограничение count;
- session timeout, reconnect и автоматическое освобождение lock;
- одновременную конкуренцию нескольких клиентов;
- cancellation ожидающего acquire;
- leader handoff без двух одновременных лидеров;
- watch/revision при быстрых последовательных изменениях;
- изменение coordination-node settings во время активных сессий;
- ACL на node и операции semaphore.

Почему перспективно: корректность зависит от времени жизни сессии, сети и
порядка событий; ошибки проявляются как утечки lock или split brain.

Документация:

- [Coordination node](ydb/docs/ru/core/concepts/datamodel/coordination-node.md)
- [Coordination API](ydb/docs/ru/core/reference/ydb-sdk/coordination.md)
- [Distributed lock](ydb/docs/ru/core/recipes/ydb-sdk/distributed-lock.md)
- [Leader election](ydb/docs/ru/core/recipes/ydb-sdk/leader-election.md)

## 18. Асинхронная репликация

Что проверить:

- initial scan и переход к непрерывной репликации;
- INSERT/UPDATE/DELETE и schema mismatch;
- конфликт существующей destination table;
- остановку, возобновление и удаление replication object;
- кратковременную недоступность source/destination;
- повторное подключение без дублей и потерь;
- lag/status/system views и качество ошибок;
- восстановление после рестарта обеих сторон.

Почему перспективно: нужен стенд из двух локальных баз, зато проверяется
долгоживущий state machine с реальными отказами и повторной доставкой.

Документация:

- [Асинхронная репликация](ydb/docs/ru/core/concepts/async-replication.md)
- [CREATE ASYNC REPLICATION](ydb/docs/ru/core/yql/reference/syntax/create-async-replication.md)
- [ALTER ASYNC REPLICATION](ydb/docs/ru/core/yql/reference/syntax/alter-async-replication.md)

## 19. PostgreSQL compatibility layer

Что проверить:

- PostgreSQL wire protocol через `psql` и распространённый драйвер;
- prepared statements и bind-параметры разных типов;
- transaction begin/commit/rollback и autocommit;
- quoted identifiers, search path и системные metadata-запросы;
- date/time, numeric, bytea, arrays и `NULL`;
- pagination/cursors и большие result sets;
- SQLSTATE и mapping ошибок YDB;
- поведение ORM schema introspection.

Почему перспективно: black-box совместимость часто расходится не в результате
простого SELECT, а в протоколе, metadata и диагностике, от которых зависят
драйверы и ORM.

Документация:

- [Совместимость с PostgreSQL](ydb/docs/ru/core/postgresql/intro.md)
- [Интеграции с ORM](ydb/docs/ru/core/integrations/orm/index.md)

## 20. BATCH UPDATE/DELETE, Bulk Upsert и paging

Что проверить:

- операции на одной и нескольких партициях;
- точные границы batch size и лимита десяти одновременно обрабатываемых партиций;
- повтор после timeout/cancel и частично выполненной операции;
- конкурирующие изменения тех же строк;
- secondary indexes, TTL и `NOT NULL` constraints;
- malformed Bulk Upsert batch и смешение корректных/некорректных строк;
- paging token после изменения или удаления данных;
- отсутствие пропусков и дублей между страницами;
- соответствие счётчиков affected rows фактическому состоянию.

Почему перспективно: эти интерфейсы намеренно ослабляют атомарность или делят
операцию на части, поэтому особенно важны частичный результат и безопасный retry.

Документация:

- [BATCH UPDATE](ydb/docs/ru/core/yql/reference/syntax/batch-update.md)
- [BATCH DELETE](ydb/docs/ru/core/yql/reference/syntax/batch-delete.md)
- [Batch upload](ydb/docs/ru/core/dev/batch-upload.md)
- [Paging](ydb/docs/ru/core/dev/paging.md)

## Рекомендуемый порядок второй десятки

По ожидаемой отдаче и стоимости локального воспроизведения:

1. Транзакции и MVCC.
2. Глобальные secondary indexes.
3. TTL строковых таблиц.
4. BATCH UPDATE/DELETE, Bulk Upsert и paging.
5. Представления и SHOW CREATE.
6. Локальные skip indexes.
7. Native Topic API.
8. Coordination service.
9. PostgreSQL compatibility layer.
10. Асинхронная репликация.

Первые четыре можно проверять на одном локальном кластере через CLI/SDK и быстро
переносить найденные расхождения в functional tests. Coordination и async
replication требуют более сложного многоклиентского либо многокластерного стенда.
