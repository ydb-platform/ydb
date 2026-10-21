# Writing to tables

Writing to tables lets you save the results of a streaming query for later analysis with regular SQL queries. For example, you can aggregate events from a stream and save the results to a table.

For writing, [UPSERT INTO](../../yql/reference/syntax/upsert_into) is used — inserting a new row or updating an existing one by primary key. The UPSERT operation is idempotent by primary key: rewriting the same row results in an update, not duplication. This is important because streaming queries provide the [at-least-once](../../concepts/streaming-query/streaming-query.md#guarantees) guarantee — when recovering from a [checkpoint](checkpoints.md), some events may be processed again.

{% note alert %}

Not supported:

- The [INSERT INTO](../../yql/reference/syntax/insert_into) command — use UPSERT INTO. INSERT INTO is not used because reprocessing events (at-least-once guarantee) would lead to duplicate rows.
- Writing to {{ ydb-short-name }} tables located in external databases. In the current version, writing is only possible to local tables.

{% endnote %}

## Example

The query reads events from a topic and writes them to the `output_table` table. The `Ts` field is converted from a string to the `Timestamp` type using `CAST`, and [Unwrap](../../yql/reference/builtins/basic#unwrap) removes the optionality of the result.


```sql
CREATE STREAMING QUERY query_with_table_write AS
DO BEGIN

-- Reading from a topic and writing to a table
UPSERT INTO
    output_table
SELECT
    -- Converting a string to Timestamp
    Unwrap(CAST(Ts AS Timestamp)) AS Ts,
    Country,
    Count
FROM
    -- Reading events from a topic
    input_topic
WITH (
    -- Data format in a topic
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


## Limitations

In a single streaming query:

- You cannot use the same table for [stream enrichment](./enrichment.md) using `JOIN` and for writing the query result.
- You cannot write to the same table multiple times.
