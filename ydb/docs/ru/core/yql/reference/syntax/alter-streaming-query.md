# ALTER STREAMING QUERY

<!-- markdownlint-disable proper-names -->

Вызов `ALTER STREAMING QUERY` изменяет настройки или текст [потоковых запросов](../../../concepts/streaming_query/index.md), а также управляет состоянием запроса (остановкой/запуском).

```sql
ALTER STREAMING QUERY [IF EXISTS] <query name> [SET (
    <key1> = <value1>,
    <key2> = <value2>,
    ...
)] [AS
DO BEGIN
    <query statement1>;
    <query statement2>;
    ...
END DO];
```

Настройки SET:

- `RUN = (TRUE|FALSE)` — запустить или остановить запрос.
Статус запроса (запущен или остановлен) не изменяется, если явно не указывать настройку `RUN`.
- `FORCE = (TRUE|FALSE)` — нужно ли разрешать изменение запроса, приводящее к невозможности его загрузки из чекпоинта, по умолчанию FALSE (`TRUE` в основном требуется указывать при изменении запроса, приводящем к изменению физического плана исполнения).
Примеры:

```sql
-- Stop query
ALTER STREAMING QUERY `my_queries/query_name` SET (
    RUN = FALSE
);

-- Start created query
ALTER STREAMING QUERY `my_queries/query_name` SET (
    RUN = TRUE
);

-- Change query text
ALTER STREAMING QUERY `my_queries/query_name` SET (
    FORCE = TRUE -- Allow to drop checkpoint in case of incompatible changes in query.
) AS
DO BEGIN
INSERT INTO ydb_source.output_topic_name
SELECT Data FROM ydb_source.input_topic_name;
END DO;

-- Change and start query
ALTER STREAMING QUERY `my_queries/query_name` SET (
    RUN = TRUE
) AS
DO BEGIN
    ...
END DO;
```
