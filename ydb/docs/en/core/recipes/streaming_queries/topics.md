# Quick start: reading and writing to topics

In this guide, you will create your first [streaming query](../../concepts/streaming-query/streaming-query.md).

The query will:

- read events from an input [topic](../../concepts/datamodel/topic.md);
- filter only errors;
- count the number of errors per server over 10 minutes;
- write the result to an output topic.

Events arrive in JSON format with fields: time, log level, and server name.

You will perform the following steps:

* [creating topics](#step1);
* [creating a streaming query](#step2);
* [viewing the query status](#step3);
* [populating the input topic with data](#step4);
* [checking the output topic contents](#step5);
* [deleting the streaming query](#step6).

## Prerequisites {#requirements}

To run the examples, you will need:

* a running {{ ydb-short-name }} database — see [quick start](../../quickstart.md);
* the `enable_streaming_queries` flag enabled.

{% list tabs %}

- Docker

  ```bash
  docker run -d --rm --name ydb-local -h localhost \
    --platform linux/amd64 \
    -p 2135:2135 -p 2136:2136 -p 8765:8765 -p 9092:9092 \
    -v $(pwd)/ydb_certs:/ydb_certs \
    -e GRPC_TLS_PORT=2135 -e GRPC_PORT=2136 -e MON_PORT=8765 \
    -e YDB_FEATURE_FLAGS=enable_streaming_queries \
    ydbplatform/local-ydb:25.4
  ```

- local_ydb

  ```bash
  ./local_ydb deploy \
    --ydb-working-dir=/absolute/path/to/working/directory \
    --ydb-binary-path=/path/to/kikimr/driver \
    --enable-feature-flag=enable_streaming_queries
  ```

{% endlist %}

{% include [ydb-cli-profile](../../_includes/ydb-cli-profile.md) %}

## Step 1. Creating topics {#step1}

Create input and output [topics](../../concepts/datamodel/topic.md):


```sql
CREATE TOPIC input_topic;
CREATE TOPIC output_topic;
```


Verify that the topics are created:


```bash
./ydb --profile quickstart scheme ls
```


## Step 2. Creating a streaming query {#step2}

Create a [streaming query](../../concepts/streaming-query/streaming-query.md) using [CREATE STREAMING QUERY](../../yql/reference/syntax/create-streaming-query.md):


```sql
CREATE STREAMING QUERY query_example AS
DO BEGIN

$number_errors = SELECT
    Host,
    COUNT(*) AS ErrorCount,
    CAST(HOP_START() AS String) AS Ts  -- Start time of the window corresponding to the aggregation result
FROM
    input_topic
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
    HOP(CAST(Time AS Timestamp), "PT600S", "PT600S", "PT0S"),  -- Number of errors in non-overlapping windows of 10 minutes
    Host;

INSERT INTO
    output_topic
SELECT
    ToBytes(Unwrap(Yson::SerializeJson(Yson::From(TableRow()))))  -- Serialization of all columns to JSON
FROM
    $number_errors;

END DO
```


Details:

- Aggregation `GROUP BY HOP` and function `HOP_START` — [{#T}](../../yql/reference/syntax/select/group-by.md#group-by-hop).
- Writing data to a topic — [{#T}](../../dev/streaming-query/streaming-query-formats.md#write_formats).
- JSON serialization: [TableRow](../../yql/reference/builtins/basic#tablerow), [Yson::From](../../yql/reference/udf/list/yson#ysonfrom), [Yson::SerializeJson](../../yql/reference/udf/list/yson#ysonserializejson), [Unwrap](../../yql/reference/builtins/basic#unwrap), [ToBytes](../../yql/reference/builtins/basic#to-from-bytes).

## Step 3. Viewing the query status {#step3}

Check the query status via the [streaming_queries](../../dev/system-views.md#streaming_queries) system table:


```sql
SELECT
    Path,
    Status,
    Issues,
    Run
FROM
    `.sys/streaming_queries`
```


Make sure the `Status` field has the value `RUNNING`. Otherwise, check the `Issues` field.

If the query is in the `SUSPENDED` status or there are errors in the `Issues` field, refer to the error diagnostics section.

## Step 4. Populating the input topic with data {#step4}

Write test messages to the topic using the [{{ ydb-short-name }} CLI](../../reference/ydb-cli/index.md):


```bash
echo '{"Time": "2025-01-01T00:00:00.000000Z", "Level": "error", "Host": "host-1"}' | ./ydb --profile quickstart topic write input_topic
echo '{"Time": "2025-01-01T00:04:00.000000Z", "Level": "error", "Host": "host-2"}' | ./ydb --profile quickstart topic write input_topic
echo '{"Time": "2025-01-01T00:08:00.000000Z", "Level": "error", "Host": "host-1"}' | ./ydb --profile quickstart topic write input_topic
echo '{"Time": "2025-01-01T00:12:00.000000Z", "Level": "error", "Host": "host-2"}' | ./ydb --profile quickstart topic write input_topic
echo '{"Time": "2025-01-01T00:12:00.000000Z", "Level": "error", "Host": "host-1"}' | ./ydb --profile quickstart topic write input_topic
```


The result will appear in the output topic after the 10-minute aggregation window closes.

## Step 5. Checking the output topic contents {#step5}

Read data from the output topic:


```bash
./ydb --profile quickstart topic read output_topic --partition-ids 0 --start-offset 0 --limit 10 --format newline-delimited
```


Expected result:


```json
{"ErrorCount":1,"Host":"host-2","Ts":"2025-01-01T00:00:00Z"}
{"ErrorCount":2,"Host":"host-1","Ts":"2025-01-01T00:00:00Z"}
```


## Step 6. Deleting the query {#step6}

Delete the query using [DROP STREAMING QUERY](../../yql/reference/syntax/drop-streaming-query.md):


```sql
DROP STREAMING QUERY query_example;
```


## What's next {#next-steps}

- Explore the [data formats](../../dev/streaming-query/streaming-query-formats.md) supported in streaming queries.
- Learn how to [enrich data with a reference table](../../dev/streaming-query/enrichment.md) from a local table or from S3.
- Learn how to [write results to tables](../../dev/streaming-query/table-writing.md).

## See also

* [{#T}](../../concepts/streaming-query/streaming-query.md);
* [{#T}](../../dev/streaming-query/streaming-query-formats.md).
