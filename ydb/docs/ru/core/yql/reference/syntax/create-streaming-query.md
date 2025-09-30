# CREATE STREAMING QUERY

<!-- markdownlint-disable proper-names -->

Вызов `CREATE STREAMING QUERY` создает [стриминговый запрос](../../../concepts/streaming_query/index.md).

```sql
CREATE [OR REPLACE] STREAMING QUERY [IF NOT EXISTS] <query name> [WITH (
    <key1> = <value1>,
    <key2> = <value2>,
    ...
)] AS
BEGIN
    <query statement1>;
END;

Настройки WITH:

- `RUN = (TRUE|FALSE)` - запустить запрос после создания, по умолчанию TRUE
- `STREAMING_DISPOSITION` - настройка оффсетов при чтении из топика, разрешённые значения:
  Возможные значения:
  - `STREAMING_DISPOSITION = OLDEST` - запуск с самых старых оффсетов,
  - `STREAMING_DISPOSITION = FRESH` - запуск с самых новых оффсетов,
  - `STREAMING_DISPOSITION(FROM_TIME = "2025-05-04T11:30:34.336938Z")` - запуск с офсета соответствующего определённому UTC времени,
  - `STREAMING_DISPOSITION(TIME_AGO = "PT1H")` - запуск с офсета соответствующего некоторому времени до текущего момента в формате ISO 8601,
  - `STREAMING_DISPOSITION = FROM_CHECKPOINT` - запуск запроса с последнего чекпоинта, если чекпоинта нету, то будет возвращена ошибка,
  - `STREAMING_DISPOSITION = FROM_CHECKPOINT_FORCE` - запуск запроса с последнего чекпоинта, если чекпоинта нету, то будет использоваться OLDEST офсет.
  Значение по умолчанию: `FRESH`.

Пример:

```sql
CREATE STREAMING QUERY `my_queries/query_name` WITH (
    STREAMING_DISPOSITION (
        FROM_TIME = "2025-05-04T11:30:34.336938Z"
    ),
DO BEGIN
    $input = SELECT * FROM `source_name`.`input_topic_name`  WITH (
        FORMAT = "json_each_row",
        SCHEMA (
            time String NOT NULL,
            level String NOT NULL,
            host String NOT NULL));
    $filtered = SELECT * FROM $input WHERE level = "error";

    $number_errors =
        SELECT COUNT(*) AS error_count, CAST(HOP_START() as String) as ts FROM $filtered
        GROUP BY HOP(CAST(time AS Timestamp), 'PT600S', 'PT600S', 'PT0S'), host;
    
    $json = SELECT ToBytes(Unwrap(Json::SerializeJson(Yson::From(TableRow())))) FROM $number_errors;
    INSERT INTO `source_name`.`output_topic_name` SELECT * FROM $json;
END DO;
```
