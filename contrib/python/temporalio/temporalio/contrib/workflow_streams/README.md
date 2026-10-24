# Temporal Workflow Streams

> ⚠️ **This package is currently at an experimental release stage.** ⚠️

**Workflow Streams** is a Temporal Python SDK contrib library that gives a
Workflow a durable, offset-addressed event channel for keeping outside
observers updated on the progress of the Workflow and its Activities.
Typical uses include driving a UI for a long-running AI agent, surfacing
status during in-flight payment or order processing, and reporting progress
from data pipelines. It is not designed for ultra-low-latency applications
such as real-time voice; per-roundtrip latency is around 100ms, and cost
scales with durable batches rather than tokens.

Under the hood the stream is built directly on Temporal's existing
message-passing primitives: Signals carry publishes, Updates serve
long-poll subscriptions, and a Query exposes the current global offset.
The library packages the boilerplate that turns those primitives into
a usable stream: batching to amortize per-event overhead, deduplication
for exactly-once delivery, topic filtering, and continue-as-new helpers
that hand stream state across Workflow runs.

## Documentation

📖 **The full guide lives in the Temporal documentation site:**
**[Workflow Streams — Python SDK](https://docs.temporal.io/develop/python/workflows/workflow-streams)**

It covers installation, enabling streaming on a Workflow, publishing from
Workflows and Activities, subscribing, continue-as-new, delivery semantics,
codec and payload encoding, architecture, and caveats — with runnable code
snippets throughout.

For runnable end-to-end examples, see the
[Workflow Streams samples](https://github.com/temporalio/samples-python/tree/main/workflow-streams).
