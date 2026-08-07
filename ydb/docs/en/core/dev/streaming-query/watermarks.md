# Watermarks

Watermark is a monotonically increasing lower bound on event times in a stream (for more details on the concept, see [{#T}](../../concepts/streaming-query/watermarks.md)). This section describes the configuration of watermarks in streaming queries {{ ydb-short-name }}.

## Event time {#event-time}

In streaming processing, each event has a timestamp by which the system tracks the progress of time in the stream. In the current implementation, the only possible source of event time is the time the event was written to a [topic](../../concepts/datamodel/topic.md), available through the system column [`__ydb_write_time`](../../concepts/query_execution/topics.md#system-metadata).

{% note info %}

Support for arbitrary expressions to extract time from event data (for example, the `event.created_at` field) is planned in future versions.

{% endnote %}

## Usage {#usage}

Watermark is used by operations that depend on the progress of event time in the stream. In {{ ydb-short-name }}, such operations include window aggregation [HoppingWindow](../../yql/reference/syntax/select/group-by.md#group-by-hopping_window) — it defines sliding time windows that group events. When a watermark is received, `HoppingWindow` closes all windows that are fully covered by this value.


```mermaid
sequenceDiagram
    participant Топик
    participant Запрос as Потоковый запрос
    participant Приёмник

    Топик->>Запрос: событие, ts = 10с
    Note over Запрос: watermark = 5с
    Топик->>Запрос: событие, ts = 12с
    Note over Запрос: watermark = 7с
    Топик->>Запрос: событие, ts = 18с
    Note over Запрос: watermark = 13с<br/>Окно [0, 10) закрыто
    Запрос->>Приёмник: результат окна [0, 10)
    Топик->>Запрос: событие, ts = 8с
    Note over Запрос: ts=8 меньше watermark=13<br/>Событие будет отброшено
```


## Watermark computation {#watermark-computation}

When the system receives an event, it updates the watermark — **advances** it forward along the time axis. The watermark is computed as `maximum observed event time − delay`, where `delay` is the lag value specified in the expression [`WATERMARK`](#configuration) (for example, `Interval("PT5S")` in `WATERMARK = __ydb_write_time - Interval("PT5S")`; [`__ydb_write_time`](../../concepts/query_execution/topics.md#system-metadata) is the service column of the topic).

Events in a stream may arrive out of chronological order: an event with time 10:00:03 may be processed after an event with time 10:00:05. Reasons: clock skew in a distributed system, network delays, uneven load on topic [partitions](../../concepts/datamodel/topic.md#partitioning).

The `delay` parameter sets the allowed time 'buffer' for events arriving late. For example, with `delay` of 5 seconds, an event with time 00:00:48 will be accepted even if events with time 00:00:50 have already arrived: the watermark has not yet reached 00:00:48. If the same event arrives later, when the watermark has already advanced past 00:00:48, it will be considered late and discarded.

About the trade-off between accuracy and result latency: [{#T}](../../concepts/streaming-query/watermarks.md#tradeoff).

## Idle partitions {#idle-partitions}

If the input topic contains multiple [partitions](../../concepts/datamodel/topic.md#partitioning), each one advances the watermark independently. The query's overall watermark does not outpace the slowest partition: windows are not closed until at least one partition has reached the corresponding point in time.

If one of the partitions stops receiving data, its watermark stops advancing. Such a partition is called idle. As long as the idle partition is considered when computing the overall watermark, it also stops advancing, and results are not emitted despite data arriving from other partitions.

To avoid this blocking, the idle partition is excluded from the overall watermark computation after a configurable timeout period (the `WATERMARK_IDLE_TIMEOUT` parameter, see more in the [Settings](#configuration) section).

## Settings {#configuration}

Watermarks are enabled and configured in the WITH section when reading from a topic.

Configuration parameters:

{% include notitle [x](../../_includes/watermark_parameters.md) %}

{% note warning %}

When using [HoppingWindow](../../yql/reference/syntax/select/group-by.md#group-by-hopping_window), the first parameter (time extractor) and the time source in the WATERMARK expression must match. In the current implementation, both must use [`__ydb_write_time`](../../concepts/query_execution/topics.md#system-metadata).

{% endnote %}

## Example {#example}

Below is an example of a streaming query with watermark and window aggregation. The query reads events from a topic, filters them by the `pass` field, and aggregates `payload` values in windows of 10 seconds with a 5-second step. The watermark is configured with a 5-second lag.

### Input data


```json
{"pass": 1, "payload": "a"} // write time: 1970-01-01T00:00:40Z
{"pass": 1, "payload": "b"} // write time: 1970-01-01T00:00:42Z
{"pass": 0, "payload": "c"} // write time: 1970-01-01T00:00:50Z
{"pass": 1, "payload": "d"} // write time: 1970-01-01T00:00:40Z
```


### Query


```yql
CREATE STREAMING QUERY example AS
DO BEGIN
    $input = (
        SELECT
            t.*,
            __ydb_write_time AS ts
        FROM
            Input
        WITH (
            FORMAT = json_each_row,
            SCHEMA = (
                pass Int64,
                payload String
            ),
            WATERMARK = __ydb_write_time - Interval("PT5S")
        ) AS t
    );

    $output = (
        SELECT
            AGGREGATE_LIST(payload) AS result,
            CAST(HOP_END() AS String) AS ts
        FROM
            $input
        WHERE pass > 0
        GROUP BY
            HoppingWindow(ts, "PT5S", "PT10S")
    );

    INSERT INTO Output
    SELECT
        ToBytes(Unwrap(Yson2::SerializeJson(Yson::From(TableRow()))))
    FROM $output;
END DO;
```


Where:

- [`CREATE STREAMING QUERY`](../../yql/reference/syntax/create-streaming-query.md) — creates a named streaming query.
- [`__ydb_write_time`](../../concepts/query_execution/topics.md#system-metadata) — a system column containing the event write time to the [topic](../../concepts/datamodel/topic.md).
- `FORMAT = json_each_row` — [data format](streaming-query-formats.md) in the topic, each row contains a separate JSON object.
- `WATERMARK = __ydb_write_time - Interval("PT5S")` — watermark with a 5-second lag. `Interval("PT5S")` specifies the interval in [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601#Durations) format.
- [`AGGREGATE_LIST`](../../yql/reference/builtins/aggregation.md#agg-list) — an aggregate function that collects values into a list.
- [`HOP_END()`](../../yql/reference/syntax/select/group-by.md#group-by-hop) — returns the timestamp of the end of the current window.
- [`HoppingWindow(ts, "PT5S", "PT10S")`](../../yql/reference/syntax/select/group-by.md#group-by-hopping_window) — a window function with a 5-second step and a window size of 10 seconds.

### Result


```json
{"result": ["a", "b"], "ts": "1970-01-01T00:00:45.000000Z"}
```


### Explanation

1. The first event (`"a"`, write time 40s) passes the filter (`pass > 0`) and falls into windows `[35; 45)` and `[40; 50)`. The watermark advances to 35s and does not close any window.
2. The second event (`"b"`, write time 42s) similarly falls into windows `[35; 45)` and `[40; 50)`. The watermark advances to 37s.
3. The third event (`"c"`, write time 50s) is discarded by the filter (`pass = 0`). Despite this, the event still advances the watermark to 45s. The watermark closes window `[35; 45)` — result `["a", "b"]` is emitted.
4. The fourth event (`"d"`, write time 40s) does not advance the watermark: it is already at 45s. The event passes the filter but is discarded as late — its write time (40s) is less than the current watermark (45s). Although window `[40; 50)` is still open, the watermark has already promised that no events with time < 45s will come, so `d` is not included in any of its windows.

## See also

- [{#T}](../../yql/reference/syntax/select/group-by.md#group-by-hopping_window) — a window function that uses watermarks.
- with — the WITH section for configuring topic read parameters.
- [{#T}](guarantees.md) — data delivery guarantees.
- [{#T}](checkpoints.md) — checkpoint mechanism.
