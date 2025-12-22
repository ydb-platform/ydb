# CREATE STREAMING QUERY

Вызов `CREATE STREAMING QUERY` создает [потоковый запрос](../../../concepts/streaming_query/index.md).

```yql
CREATE [OR REPLACE] STREAMING QUERY [IF NOT EXISTS] <query name> [WITH (
    <key1> = <value1>,
    <key2> = <value2>,
    ...
)] AS
DO BEGIN
    <query statement1>;
    <query statement2>;
    ...
END DO
```

Настройки WITH:

- `RUN = (TRUE|FALSE)` - запустить запрос после создания, по умолчанию TRUE

## Указание формата и схемы

Для указания формата и схемы данных используется секция [WITH](select/with.md):

- `FORMAT = "<format>",` - см. [форматы](../../../concepts/streaming_query/formats.md),
- `SCHEMA = (...)` —  описание схемы хранимых данных.

### Примеры

Запись одной строковой колонки:

```yql
CREATE STREAMING QUERY streaming_query AS
DO BEGIN

INSERT INTO
    ydb_source.output_topic
SELECT
    *
FROM
    ydb_source.input_topic

END DO
```

Парсинг данных в формате JSON:

```yql
CREATE STREAMING QUERY streaming_query AS
DO BEGIN

INSERT INTO
    ydb_source.output_topic
SELECT
    ToBytes(Unwrap(Yson::SerializeJson(Yson::From(TableRow()))))
FROM
    ydb_source.input_topic
WITH (
    FORMAT = json_each_row,
    SCHEMA = (
        Time String NOT NULL,
        ServiceId Uint32 NOT NULL,
        Message String NOT NULL
    )
)

END DO
```
