# CREATE STREAMING QUERY

`CREATE STREAMING QUERY` создаёт [потоковый запрос](../../../concepts/streaming-query.md).

## Синтаксис

```sql
CREATE [OR REPLACE] STREAMING QUERY [IF NOT EXISTS] <query_name> [WITH (
    <key1> = <value1>,
    <key2> = <value2>,
    ...
)] AS
DO BEGIN
    <query_statement1>;
    <query_statement2>;
    ...
END DO
```

### Параметры

* `OR REPLACE` — если потоковый запрос с таким именем уже существует, то он будет заменён на новый запрос с сохранением смещений чтения из топика.
* `IF NOT EXISTS` — не выводить ошибку, если потоковый запрос с таким именем уже существует, в этом случае существующий запрос останется неизменённым.
* `query_name` — имя потокового запроса, который нужно создать.
* `WITH (<key> = <value>)` — список настроек нового потокового запроса, опционально.
* `AS DO BEGIN ... END DO` — текст нового потокового запроса, ограничения для текста запроса приведены в [{#T}](../../../concepts/streaming-query.md#limitations), примеры текста [см. ниже](#examples).

Настройки `OR REPLACE` и `IF NOT EXISTS` нельзя использовать одновременно.

Доступные параметры блока `WITH`:

* `RUN = (TRUE|FALSE)` — запустить запрос после создания, по умолчанию `TRUE`.
* `RESOURCE_POOL = <resource_pool_name>` — имя [пула ресурсов](../../../concepts/glossary.md#resource-pool), в котором будет выполняться запрос.

Примеры создания потокового запроса [см. ниже](#examples).

## Разрешения

Требуется [разрешение](./grant.md#permissions-list) `CREATE TABLE` на директорию, где будет создаваться потоковый запрос, пример выдачи такого разрешения для директории `my_queries/`:

```sql
GRANT CREATE TABLE ON my_queries TO `user@domain`
```

## Использование читателя {#consumer-usage}

Читатель создаётся через [CLI](../../../reference/ydb-cli/topic-consumer-add.md) или при создании топика с помощью [CREATE TOPIC](create-topic.md). Имя читателя указывается в тексте запроса: `PRAGMA pq.Consumer="my_consumer"`.

## Примеры {#examples}

### Запись в топик (JSON) {#example-topic-json}

Запрос читает события из входного топика, формирует JSON-объект из отдельных полей и записывает результат в выходной топик.

Функция `AsStruct` создаёт структуру из указанных полей, `Yson::From` преобразует её в Yson, `Yson::SerializeJson` сериализует в JSON-строку, а `ToBytes` конвертирует в тип `String`, который требуется для записи в топик.

{% note info %}

Запись в топики выполняется через [external data source](../../../concepts/datamodel/external_data_source.md). В примере `ydb_source` — это заранее созданный external data source, а `output_topic` и `input_topic` — топики, доступные через него.

{% endnote %}

```sql
CREATE STREAMING QUERY my_streaming_query AS
DO BEGIN

-- ydb_source — external data source для работы с топиками
INSERT INTO ydb_source.output_topic
SELECT
    -- Формирование JSON из отдельных полей
    ToBytes(Unwrap(Yson::SerializeJson(Yson::From(
        AsStruct(Id AS id, Name AS name)
    ))))
FROM
    -- Чтение из топика
    ydb_source.input_topic
WITH (
    FORMAT = json_each_row,  -- Формат входных данных
    SCHEMA = (               -- Схема входных данных
        Id Uint64 NOT NULL,
        Name Utf8 NOT NULL
    )
)

END DO
```

### Запись в таблицу {#example-table}

Запрос читает события из топика и записывает их в таблицу `output_table`. Таблица должна быть создана заранее со схемой, соответствующей выбираемым колонкам.

{% note warning %}

Запись в таблицы в потоковых запросах поддерживается **только в режиме UPSERT**. Операция `INSERT INTO` для таблиц не поддерживается. При UPSERT, если строка с таким первичным ключом уже существует, она будет обновлена, иначе будет вставлена новая строка.

{% endnote %}

```sql
CREATE STREAMING QUERY my_streaming_query AS
DO BEGIN

-- Запись в таблицу (только UPSERT, INSERT не поддерживается)
UPSERT INTO output_table
SELECT
    Id,
    Name
FROM
    -- ydb_source — external data source для работы с топиками
    ydb_source.input_topic
WITH (
    FORMAT = json_each_row,  -- Формат входных данных
    SCHEMA = (               -- Схема входных данных
        Id Uint64 NOT NULL,
        Name Utf8 NOT NULL
    )
)

END DO
```

### Запуск в пуле ресурсов {#example-resource-pool}

Запрос создаётся в указанном [пуле ресурсов](../../../concepts/glossary.md#resource-pool), но не запускается автоматически (`RUN = FALSE`). Это позволяет проверить конфигурацию перед запуском или запустить запрос позже через [ALTER STREAMING QUERY](alter-streaming-query.md).

```sql
CREATE STREAMING QUERY my_streaming_query WITH (
    RUN = FALSE,                      -- Не запускать автоматически
    RESOURCE_POOL = my_resource_pool  -- Пул ресурсов для выполнения
) AS
DO BEGIN

-- ydb_source — external data source для работы с топиками
INSERT INTO ydb_source.output_topic
SELECT
    ToBytes(Unwrap(Yson::SerializeJson(Yson::From(
        AsStruct(Id AS id, Name AS name)
    ))))
FROM
    ydb_source.input_topic
WITH (
    FORMAT = json_each_row,
    SCHEMA = (
        Id Uint64 NOT NULL,
        Name Utf8 NOT NULL
    )
)

END DO
```

Другие примеры: [{#T}](../../../dev/streaming-query/patterns.md).

## См. также

* [{#T}](../../../dev/streaming-query/patterns.md)
* [{#T}](../../../concepts/streaming-query.md)
* [{#T}](alter-streaming-query.md)
* [{#T}](drop-streaming-query.md)
