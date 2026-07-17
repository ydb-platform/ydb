# YQL queries to topics {#yql-syntax}

To read and write messages to [topics](../datamodel/topic.md), use familiar YQL constructs: [SELECT](../../yql/reference/syntax/select/index.md) for reading and [INSERT](../../yql/reference/syntax/insert_into.md) for writing.

## Local and external topics {#local-external-topics}

YQL queries to topics work the same regardless of whether the topic is in the current database or in another {{ ydb-short-name }} database. The source and receiver of messages can be either a topic **in the same database** where the query is executed or a topic **in another database**.

### Local topics {#local-topics}

**Local topics** are topics created in the **same {{ ydb-short-name }} database** as the query being executed.

In the query text, they are referred to **by a short name** — just like a table in the current database:


```yql
SELECT * FROM input_topic WITH (FORMAT = json_each_row, SCHEMA = (...));
```


```yql
INSERT INTO output_topic SELECT ...;
```


### External topics {#external-topics}

**External topics** are topics located **in another {{ ydb-short-name }} database**.

Access to them is only possible through a pre-created [external data source](../datamodel/external_data_source.md) with the YDB source type.

After creating a source, for example named `ext_source`, accessing the topic `input_topic` in an external database is written as follows:


```yql
SELECT * FROM ext_source.input_topic WITH (FORMAT = json_each_row, SCHEMA = (...));
```


The name `ext_source` in the documentation is **conditional** — in your database, the source may have a different name; it is important that it matches in `CREATE EXTERNAL DATA SOURCE` and in the prefix before the topic name.

## Reading from a topic {#topic-read}

Reading from a topic can be performed in [table](#table-read) and [streaming](#streaming-read) modes (not to be confused with streaming queries).

### Table reading {#table-read}

In table mode, reading is performed from the first to the last offset stored in the topic at the time the query is started. If data continues to be written to the topic, the query will stop after reaching the last offset known at startup. Specifying filters on [Service fields](#system-metadata) speeds up reading because reading occurs only over the specified ranges.


```yql
SELECT
    Data    -- message body
FROM
    input_topic  -- local topic; for external: ext_source.input_topic
LIMIT 10;
```


### Streaming reading {#streaming-read}

To read new messages, use the `WITH (STREAMING = "TRUE")` option — see the [Streaming reading of data from a topic](../../yql/reference/syntax/select/streaming.md) section for details. Reading starts from the current moment and continues until the number of messages specified in the `LIMIT` expression is read. The `LIMIT` parameter is required — without it, the query will not complete because it will wait for new messages indefinitely.


```yql
SELECT
    Data
FROM
    ext_source.input_topic  -- external topic; for local: input_topic
WITH (STREAMING = "TRUE")
LIMIT 10;
```


For continuous processing of incoming data, use [streaming queries](../glossary.md#streaming-query).

### Message format and schema {#format-schema}

When reading from a topic, the message body can be obtained in two ways: [raw data](#raw-read) and [formatted data](#formatted-read).

#### Raw data {#raw-read}

Use when the message content does not need to be parsed — it is enough to read the body as is.


```yql
SELECT
    Data
FROM
    input_topic  -- local topic; for external: ext_source.input_topic
WITH (
    FORMAT = raw,
    SCHEMA = (
        Data String
    )
)
LIMIT 10;
```


As a result, only the `Data` column is available — the message body in its original form.

The same result can be obtained without the `WITH` block — see [table reading](#table-read).

#### Formatted data {#formatted-read}

Use when messages are serialized in a known format (JSON, CSV, etc.). The `FORMAT` parameter specifies the parsing method, and `SCHEMA` specifies the names and types of fields that will appear in the `SELECT` result:


```yql
SELECT
    Id,
    Name
FROM
    input_topic  -- local topic; for external: ext_source.input_topic
WITH (
    FORMAT = json_each_row,
    SCHEMA = (
        Id Uint64 NOT NULL,
        Name Utf8 NOT NULL
    )
);
```


Fields from `SCHEMA` are available in `SELECT` by name — like table columns.

For more details on supported formats: [{#T}](../../dev/streaming-query/streaming-query-formats.md).

### Using a reader {#consumer-usage}

{% include [consumer-usage](../../_includes/consumer-usage.md) %}

### Transferring data from a topic to a table via UPSERT {#upsert-from-topic}

Data from a topic can be moved to a table using [UPSERT INTO](../../yql/reference/syntax/upsert_into.md):


```yql
UPSERT INTO
    table_name
SELECT
    Data            -- any transformations can be used
FROM
    ext_source.input_topic;  -- external topic; for local: input_topic
```


### Service fields {#system-metadata}

When reading, you can request service fields:


```yql
SELECT
    Data,                                                   -- message body
    __ydb_create_time AS CreateTime,                        -- message creation time
    __ydb_write_time AS WriteTime,                          -- message write time
    __ydb_offset AS Offset,                                 -- message offset in topic
    __ydb_partition_id AS Partition,                        -- partition number
    __ydb_message_group_id AS MessageGroupId,               -- message group ID
    __ydb_seq_no AS SeqNo                                   -- sequence number within partition
FROM
    input_topic  -- local topic; for external: ext_source.input_topic
LIMIT 10;
```


Filters on service fields are evaluated before reading data from the topic and significantly reduce the volume of messages read. Comparison operators (`=`, `<>`, `<`, `<=`, `>`, `>=`, `IN`), logical conditions (`AND`, `OR`), and fields `partition_id`, `write_time`, `offset` are supported. Predicates on other service fields do not limit the read volume.


```yql
SELECT
    Data
FROM
    ext_source.input_topic  -- external topic; for local: input_topic
WHERE
    __ydb_partition_id = 42
        AND __ydb_offset >= 1000
        AND __ydb_offset <= 1100
        AND __ydb_write_time > CurrentUtcTimestamp() - Interval("PT2H");
```


## Writing to a topic {#topic-write}

### Writing a single message {#simple-write}


```yql
INSERT INTO
    output_topic  -- local topic; for external: ext_source.output_topic
SELECT
    "my_message";       -- message body
```


### Writing data from a table {#write-from-table}

To write a table row with multiple columns to a topic, create a JSON object. The `TableRow` function creates a structure from all columns, `Yson::From` converts it to Yson, `Yson::SerializeJson` serializes it to a JSON string, and `ToBytes` converts the result to the `String` type required for writing to a topic:


```yql
INSERT INTO
    ext_source.output_topic  -- external topic; for local: output_topic
SELECT
    ToBytes(Unwrap(Yson::SerializeJson(Yson::From(TableRow()))))
FROM
    table_name;
```


## Limitations {#limitations}

{% note warning %}

Reading and writing [user attributes](../datamodel/topic.md#message) are not supported.

{% endnote %}

{% note warning %}

Transactional writes via YQL/`INSERT INTO` are not supported — partial query results may appear in the topic.

{% endnote %}

## See also

- [CREATE TOPIC](../../yql/reference/syntax/create-topic.md)
- [ALTER TOPIC](../../yql/reference/syntax/alter-topic.md)
- [DROP TOPIC](../../yql/reference/syntax/drop-topic.md)
- [Streaming queries](../../dev/streaming-query/index.md)
