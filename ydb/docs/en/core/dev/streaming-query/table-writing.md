# Writing to tables

Writing to tables lets you persist streaming query results for analysis with regular SQL. For example, you can aggregate events from a stream and store summaries in a table.

Writes use [UPSERT INTO](../../yql/reference/syntax/upsert_into.md) — insert a new row or update an existing row by primary key. UPSERT is idempotent by primary key: writing the same row again updates it rather than duplicating. That matters because streaming queries provide [at-least-once](../../concepts/streaming-query.md#guarantees) delivery — after recovery from a [checkpoint](checkpoints.md), some events may be processed more than once.

{% note alert %}

Not supported:

- [INSERT INTO](../../yql/reference/syntax/insert_into.md) — use UPSERT INTO instead. `INSERT INTO` would duplicate rows on retries under at-least-once delivery.
- Writing to {{ ydb-short-name }} tables in **external** databases. Currently only local tables can be written to.

{% endnote %}

## Example

The query reads events from a topic and writes them to `output_table`. `Ts` is cast from string to `Timestamp`, and [Unwrap](../../yql/reference/builtins/basic#unwrap) removes optionality.

```sql
CREATE STREAMING QUERY query_with_table_write AS
DO BEGIN

-- Read from topic and write to table
UPSERT INTO
    output_table
SELECT
    -- String to Timestamp
    Unwrap(CAST(Ts AS Timestamp)) AS Ts,
    Country,
    Count
FROM
    -- Read events from topic
    ydb_source.input_topic
WITH (
    -- Topic data format
    FORMAT = json_each_row,
    -- Data schema
    SCHEMA = (
        Ts String NOT NULL,
        Count Uint64 NOT NULL,
        Country Utf8 NOT NULL
    )
);

END DO
```
