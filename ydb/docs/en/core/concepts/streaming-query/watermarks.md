# Watermarks

A watermark in stream processing ( [stream processing](https://en.wikipedia.org/wiki/Stream_processing)) is a monotonically increasing lower bound on the event times that may still arrive in the stream. When the watermark reaches value X, the system declares that all events with a time less than X have most likely already been received.

In stream processing, each event has two timestamps: **event time** — the moment when the event occurred in the real world, and **processing time** — the moment when the system received the event. Due to network delays, failures, and uneven load, these two values can differ significantly. This is why the system needs a watermark mechanism: without it, the system does not know when a past time range can be considered sufficiently complete to produce a result.

## Accuracy and latency trade-off {#tradeoff}

A watermark cannot simultaneously account for arbitrarily large event delays and advance quickly: the longer the system waits for late events, the later it produces results. This is a fundamental trade-off in stream processing.

Events that arrive after the watermark has passed the corresponding time range are considered late and are discarded. For more details on handling late events and configuring watermarks, see [{#T}](../../dev/streaming-query/watermarks.md).
