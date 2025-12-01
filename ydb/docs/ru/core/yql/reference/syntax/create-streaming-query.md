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
- `SCHEMA (...)` —  описание схемы хранимых данных.


### Пример


```sql
CREATE STREAMING QUERY `my_queries/query_name` AS
DO BEGIN
PRAGMA pq.Consumer = 'ConsumerName';

$input = (
    SELECT
        *
    FROM
        source_name.input_topic_name WITH (
            FORMAT = 'json_each_row',
            SCHEMA (Year Int32, Manufacturer Utf8, Model Utf8, Price Double)
        )
);

$filtered = (
    SELECT
        *
    FROM
        $input
    WHERE
        level == 'error'
);

$number_errors = (
    SELECT
        COUNT(*) AS error_count,
        CAST(HOP_START() AS String) AS ts
    FROM
        $filtered
    GROUP BY
        HOP (CAST(time AS Timestamp), 'PT600S', 'PT600S', 'PT0S'),
        host
);

$json = (
    SELECT
        ToBytes(Unwrap(Yson::SerializeJson(Yson::From(TableRow()))))
    FROM
        $number_errors
);

INSERT INTO source_name.output_topic_name
SELECT
    *
FROM
    $json
;
END DO;

```