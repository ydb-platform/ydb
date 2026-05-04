# Writing to tables

Writing to tables lets you persist streaming query results for analysis with regular SQL queries. For example, you can aggregate events from a stream and store the results in a table.

Writes use [UPSERT INTO](../../yql/reference/syntax/upsert_into.md) — insert a new row or update an existing row by primary key. UPSERT is idempotent by primary key: writing the same row again updates it rather than duplicating. This matters because streaming queries provide [at-least-once](../../concepts/streaming-query.md#guarantees) delivery — events may be processed again after recovery from a [checkpoint](checkpoints.md).

{% note alert %}

Not supported:

- [INSERT INTO](../../yql/reference/syntax/insert_into.md) — use `UPSERT INTO`. `INSERT INTO` is unsuitable because reprocessed events (at-least-once) would duplicate rows;
- Writes to {{ ydb-short-name }} tables in external databases. In the current release, writes are limited to local tables.

{% endnote %}

## Example

The query reads events from a topic and writes to `output_table`. Field `Ts` is cast from string to `Timestamp` with `CAST`, and [Unwrap](../../yql/reference/builtins/basic#unwrap) removes optionality.

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
