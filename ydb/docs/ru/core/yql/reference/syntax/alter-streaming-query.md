# ALTER STREAMING QUERY

Вызов `ALTER STREAMING QUERY` изменяет настройки или текст [потоковых запросов](../../../concepts/streaming_query/index.md), а также управляет состоянием запроса (остановкой/запуском).

```yql
ALTER STREAMING QUERY [IF EXISTS] <query name> [SET (
    <key1> = <value1>,
    <key2> = <value2>,
    ...
)] [AS
DO BEGIN
    <query statement1>;
    <query statement2>;
    ...
END DO]
```

Настройки SET:

- `RUN = (TRUE|FALSE)` — запустить или остановить запрос.
Статус запроса (запущен или остановлен) не изменяется, если явно не указывать настройку `RUN`.
- `FORCE = (TRUE|FALSE)` — нужно ли разрешать изменение запроса, приводящее к невозможности его загрузки из чекпоинта, по умолчанию FALSE (`TRUE` требуется указывать при изменении текста запроса, приводящем к изменению физического плана исполнения).

Примеры:

- Остановка запроса:

   ```yql
   ALTER STREAMING QUERY streaming_query SET (
       RUN = FALSE
   )
   ```

- Запуск запроса:

   ```yql
   ALTER STREAMING QUERY streaming_query SET (
       RUN = TRUE
   )
   ```

- Изменение текста запроса со сбросом чекпоинта:

   ```yql
   ALTER STREAMING QUERY streaming_query SET (
       FORCE = TRUE
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
