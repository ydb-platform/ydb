# Watermarks

A watermark in stream processing ( [stream processing](https://en.wikipedia.org/wiki/Stream_processing)) is a monotonically increasing lower bound on the event times that may still arrive in the stream. When the watermark reaches a value X, the system declares that all events with time less than X have most likely been received.

{{ ydb-short-name }} implements the watermark mechanism in [streaming queries](streaming-query.md): they are used to correctly close time-based aggregation windows ([HoppingWindow](../../yql/reference/syntax/select/group-by.md#group-by-hopping_window)) and ensure that the window result is emitted only when the system has confirmed the completeness of input data for that period.

In stream processing, each event has two timestamps: **event time** — the moment when the event occurred in the real world, and **processing time** — the moment when the system received the event. Due to network delays, failures, and uneven load, these two values can differ significantly. This is precisely why the system needs a watermark mechanism: without it, the system does not know when a past time range can be considered sufficiently complete to emit a result.

## Accuracy vs. Latency Trade-off {#tradeoff}

In {{ ydb-short-name }}, this trade-off is explicitly controlled: the [`WATERMARK`](../../dev/streaming-query/watermarks.md#configuration) parameter in the `GROUP BY` clause specifies the expression for computing the watermark, including the amount of lag behind the last event time. This allows you to balance between result freshness and completeness of accounting for late events.

A watermark cannot simultaneously account for arbitrarily large event delays and advance quickly: the longer the system waits for late events, the later it emits results. This is a fundamental trade-off in stream processing.

Events that arrive after the watermark has passed the corresponding time range are considered late and are discarded. For more details on handling late events and configuring watermarks, see [{#T}](../../dev/streaming-query/watermarks.md).
