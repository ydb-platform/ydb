# Синтаксис

Предварительно необходимо создать [внешний источник данных](../datamodel/external_data_source):

Пример:

```sql
CREATE EXTERNAL DATA SOURCE `source_name` WITH (
    SOURCE_TYPE = "YDB",
    LOCATION="localhost:2135",
    DATABASE_NAME="/Root"
);
```

Для управления стриминговыми запросами предназначена сущность STREAMING QUERY, которую можно создавать и изменять через DDL.
Запросы можно запускать и останавливать.

## CREATE STREAMING QUERY

Используется для создания стриминговых запросов.

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

- `RUN` = (TRUE|FALSE) - запустить запрос после создания, по умолчанию TRUE
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

### ALTER STREAMING QUERY

Используется для изменения настроек и текста запросов, а также управления запросами (остановкой/запуском).

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
- `RUN = (TRUE|FALSE)` - запустить или остановить запрос.
Если `RUN = TRUE`, то можно указать STREAMING_DISPOSITION для запуска запроса

Примеры:

```sql

-- Stop query
ALTER STREAMING QUERY `my_queries/query_name` SET (
    RUN = FALSE
);

-- Start created query
ALTER STREAMING QUERY `my_queries/query_name` SET (
    RUN = TRUE,
    STREAMING_DISPOSITION = OLDEST
);

-- Change query text
ALTER STREAMING QUERY `my_queries/query_name` SET (
    FORCE = TRUE   -- Allow to drop checkpoint in case of incompatible changes in query.
) AS
DO BEGIN
    PRAGMA FeatureR010="prototype";

    $input = SELECT * FROM `source_name`.`input_topic_name`  WITH (
        FORMAT = "json_each_row",
        SCHEMA (
            time String NOT NULL,
            event_class String NOT NULL,
            host String NOT NULL,
            message String NOT NULL));
    $filtered = SELECT * FROM $input WHERE event_class = "login";

    $matches = SELECT * FROM $filtered 
        MATCH_RECOGNIZE(
            MEASURES
                FIRST(F1.time) AS time,
                FIRST(F1.host) AS host
            ONE ROW PER MATCH
            PATTERN (F1 F2)
            DEFINE 
                F1 as F1.message = "login failed",
                F2 as F2.message = "login failed" AND F2.host = FIRST(F1.host)
        );

    $json = SELECT ToBytes(Unwrap(Json::SerializeJson(Yson::From(TableRow())))) FROM $matches;
    INSERT INTO `source_name`.`output_topic_name` SELECT * FROM $json;
END DO;

-- Change and start query
ALTER STREAMING QUERY `my_queries/query_name` SET (
    RUN = TRUE,
    STREAMING_DISPOSITION (
        TIME_AGO = "PT1H"
    )
) AS
DO BEGIN
    ...
END DO;
```

Пример:

### DROP STREAMING QUERY

Удаление стриминговых запросов.

```sql
DROP STREAMING QUERY [IF EXISTS] <query name>:
```

Пример:

```sql
DROP STREAMING QUERY `my_queries/query_name`;
```
