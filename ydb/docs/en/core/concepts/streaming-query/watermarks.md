# Watermarks

Watermark in stream processing ( [stream processing](https://en.wikipedia.org/wiki/Stream_processing)) is a monotonically increasing lower bound on event times that may still arrive in the stream. When the watermark reaches value X, the system declares that all events with timestamps less than X have been received with high probability.

In stream processing, each event has two timestamps: **event time** — the moment the event occurred in the real world, and **processing time** — the moment the system received the event. Due to network delays, failures, and uneven load, these two values can differ significantly. That is why the system needs a watermark mechanism: without it, it does not know when the past time window can be considered sufficiently complete to produce a result.

## Accuracy and latency trade-off {#tradeoff}

A watermark cannot simultaneously accommodate arbitrarily large event delays and advance quickly: the longer the system waits for late events, the later it emits results. This is a fundamental trade-off in stream processing.

Events that arrive after the watermark has passed the corresponding time window are considered late and are dropped. For more information about handling late events and configuring watermarks: [{#T}](../../dev/streaming-query/watermarks.md).
