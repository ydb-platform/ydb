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

Создание и запуск запроса `my_streaming_query`:

```sql
CREATE STREAMING QUERY my_streaming_query AS
DO BEGIN

INSERT INTO
    ydb_source.output_topic
SELECT
    *
FROM
    ydb_source.input_topic

END DO
```

Создание запроса `my_streaming_query` в [пуле ресурсов](../../../concepts/glossary.md#resource-pool) `my_resource_pool` без запуска:

```sql
CREATE STREAMING QUERY my_streaming_query WITH (
    RUN = FALSE,
    RESOURCE_POOL = my_resource_pool
) AS
DO BEGIN

INSERT INTO
    ydb_source.output_topic
SELECT
    *
FROM
    ydb_source.input_topic

END DO
```

Примеры обработки данных в других форматах приведены в статье [{#T}](../../../dev/streaming-query/streaming-query-formats.md). Подробнее о возможностях и ограничениях потоковых запросов [см. в документации](../../../concepts/streaming-query.md).

## См. также

* [{#T}](../../../concepts/streaming-query.md)
* [{#T}](alter-streaming-query.md)
* [{#T}](drop-streaming-query.md)
