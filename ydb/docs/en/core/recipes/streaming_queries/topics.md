# Quickstart: reading and writing topics

This tutorial walks you through your first [streaming query](../../concepts/streaming-query.md).

The query will:

- Read events from an input [topic](../../concepts/datamodel/topic.md);
- Keep only errors;
- Count errors per server over 10-minute windows;
- Write results to an output topic.

Events are JSON with timestamp, log level, and host name.

Steps:

* [Create topics](#step1);
* [Create an external data source](#step2);
* [Create the streaming query](#step3);
* [Check query state](#step4);
* [Produce sample input](#step5);
* [Read the output topic](#step6);
* [Delete the streaming query](#step7).

## Prerequisites {#requirements}

You need:

* A running {{ ydb-short-name }} database — see [quick start](../../quickstart.md);
* Feature flags `enable_external_data_sources` and `enable_streaming_queries` enabled.

{% list tabs %}

- Docker

  ```bash
  docker run -d --rm --name ydb-local -h localhost \
    --platform linux/amd64 \
    -p 2135:2135 -p 2136:2136 -p 8765:8765 -p 9092:9092 \
    -v $(pwd)/ydb_certs:/ydb_certs \
    -e GRPC_TLS_PORT=2135 -e GRPC_PORT=2136 -e MON_PORT=8765 \
    -e YDB_FEATURE_FLAGS=enable_external_data_sources,enable_streaming_queries \
    ydbplatform/local-ydb:25.4
  ```

- local_ydb

  ```bash
  ./local_ydb deploy \
    --ydb-working-dir=/absolute/path/to/working/directory \
    --ydb-binary-path=/path/to/kikimr/driver \
    --enable-feature-flag=enable_external_data_sources \
    --enable-feature-flag=enable_streaming_queries
  ```

{% endlist %}

{% include [ydb-cli-profile](../../_includes/ydb-cli-profile.md) %}

## Step 1. Create topics {#step1}

Create input and output [topics](../../concepts/datamodel/topic.md):

```sql
CREATE TOPIC input_topic;
CREATE TOPIC output_topic;
```

Verify:

```bash
./ydb --profile quickstart scheme ls
```

## Step 2. Create an external data source {#step2}

Create an [external data source](../../concepts/datamodel/external_data_source.md) with [CREATE EXTERNAL DATA SOURCE](../../yql/reference/syntax/create-external-data-source.md):

```sql
CREATE EXTERNAL DATA SOURCE ydb_source WITH (
    SOURCE_TYPE = "Ydb",
    LOCATION = "localhost:2136",
    DATABASE_NAME = "/local",
    AUTH_METHOD = "NONE"
);
```

{% note info %}

Set `LOCATION` and `DATABASE_NAME` to match your {{ ydb-short-name }} deployment.

{% endnote %}

## Step 3. Create the streaming query {#step3}

Create a [streaming query](../../concepts/streaming-query.md) with [CREATE STREAMING QUERY](../../yql/reference/syntax/create-streaming-query.md):

```sql
CREATE STREAMING QUERY query_example AS
DO BEGIN

$number_errors = SELECT
    Host,
    COUNT(*) AS ErrorCount,
    CAST(HOP_START() AS String) AS Ts  -- Window start time for the aggregate row
FROM
    ydb_source.input_topic
WITH (
    FORMAT = json_each_row,
    SCHEMA = (
        Time String NOT NULL,
        Level String NOT NULL,
        Host String NOT NULL
    )
)
WHERE
    Level = "error"
GROUP BY
    HOP(CAST(Time AS Timestamp), "PT600S", "PT600S", "PT0S"),  -- Non-overlapping 10-minute windows
    Host;

INSERT INTO
    ydb_source.output_topic
SELECT
    ToBytes(Unwrap(Yson::SerializeJson(Yson::From(TableRow()))))  -- Serialize columns to JSON
FROM
    $number_errors;

END DO
```

More detail:

- `GROUP BY HOP` and `HOP_START` — [{#T}](../../yql/reference/syntax/select/group-by.md#group-by-hop).
- Writing to topics — [{#T}](../../dev/streaming-query/streaming-query-formats.md#write_formats).
- JSON serialization: [TableRow](../../yql/reference/builtins/basic#tablerow), [Yson::From](../../yql/reference/udf/list/yson#ysonfrom), [Yson::SerializeJson](../../yql/reference/udf/list/yson#ysonserializejson), [Unwrap](../../yql/reference/builtins/basic#unwrap), [ToBytes](../../yql/reference/builtins/basic#to-from-bytes).

## Step 4. Check query state {#step4}

Inspect the `.sys/streaming_queries` system view [{#T}](../../dev/system-views.md#streaming_queries):

```sql
SELECT
    Path,
    Status,
    Issues,
    Run
FROM
    `.sys/streaming_queries`
```

`Status` should be `RUNNING`. Otherwise inspect `Issues`.

If the query is `SUSPENDED` or `Issues` contains errors, see troubleshooting documentation.

## Step 5. Produce sample input {#step5}

Write test messages with [{{ ydb-short-name }} CLI](../../reference/ydb-cli/index.md):

```bash
echo '{"Time": "2025-01-01T00:00:00.000000Z", "Level": "error", "Host": "host-1"}' | ./ydb --profile quickstart topic write input_topic
echo '{"Time": "2025-01-01T00:04:00.000000Z", "Level": "error", "Host": "host-2"}' | ./ydb --profile quickstart topic write input_topic
echo '{"Time": "2025-01-01T00:08:00.000000Z", "Level": "error", "Host": "host-1"}' | ./ydb --profile quickstart topic write input_topic
echo '{"Time": "2025-01-01T00:12:00.000000Z", "Level": "error", "Host": "host-2"}' | ./ydb --profile quickstart topic write input_topic
echo '{"Time": "2025-01-01T00:12:00.000000Z", "Level": "error", "Host": "host-1"}' | ./ydb --profile quickstart topic write input_topic
```

Results appear in the output topic after the 10-minute aggregation window closes.

## Step 6. Read the output topic {#step6}

```bash
./ydb --profile quickstart topic read output_topic --partition-ids 0 --start-offset 0 --limit 10 --format newline-delimited
```

Expected output:

```json
{"ErrorCount":1,"Host":"host-2","Ts":"2025-01-01T00:00:00Z"}
{"ErrorCount":2,"Host":"host-1","Ts":"2025-01-01T00:00:00Z"}
```

## Step 7. Delete the query {#step7}

```sql
DROP STREAMING QUERY query_example;
```

## Next steps {#next-steps}

- [Data formats](../../dev/streaming-query/streaming-query-formats.md) supported in streaming queries.
- [Enrich with a lookup](../../dev/streaming-query/enrichment.md) from a local table or S3.
- [Write results to tables](../../dev/streaming-query/table-writing.md).

## See also

* [{#T}](../../concepts/streaming-query.md);
* [{#T}](../../dev/streaming-query/streaming-query-formats.md).
