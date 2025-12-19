# CREATE STREAMING QUERY

<!-- markdownlint-disable proper-names -->

Вызов `CREATE STREAMING QUERY` создает [потоковый запрос](../../../concepts/streaming_query/index.md).

```sql
CREATE [OR REPLACE] STREAMING QUERY [IF NOT EXISTS] <query name> [WITH (
    <key1> = <value1>,
    <key2> = <value2>,
    ...
)] AS
DO BEGIN
    <query statement1>;
    <query statement2>;
    ...
END DO;
```

Настройки WITH:

- `RUN = (TRUE|FALSE)` - запустить запрос после создания, по умолчанию TRUE

## Указание формата и схемы

Для указания формата и схемы данных используется секция [WITH](select/with.md):

- `FORMAT = "<format>",` - см. [форматы](../../../concepts/streaming_query/formats.md),
- `SCHEMA = (...)` —  описание схемы хранимых данных.


### Примеры

```sql
CREATE STREAMING QUERY `streaming_test/query_name` AS
DO BEGIN
INSERT INTO ydb_source.output_topic_name
SELECT Data FROM ydb_source.input_topic_name;
END DO;
```

```sql
CREATE STREAMING QUERY `streaming_test/query_name` AS
DO BEGIN
INSERT INTO ydb_source.output_topic_name
SELECT
    ToBytes(Unwrap(Yson::SerializeJson(Yson::From(TableRow()))))
FROM ydb_source.input_topic_name;
WITH (
    FORMAT = 'json_each_row',
    SCHEMA = (time String NOT NULL, service_id UInt32 NOT NULL, message String NOT NULL)
);
END DO;
```