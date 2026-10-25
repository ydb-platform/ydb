# Temporal Rust SDK Examples

These examples demonstrate common Temporal patterns using the Rust SDK. Each example is a self-contained directory with its own README, workflow definitions, a worker, and a starter.

## Prerequisites

A running Temporal server. The quickest way to get one locally:

```bash
temporal server start-dev
```

By default, examples connect to `http://localhost:7233` in the `default` namespace.
Override with `TEMPORAL_SERVICE_ADDRESS` and `TEMPORAL_NAMESPACE` environment variables.

## Running an Example

From the `crates/sdk/` directory, each example has a worker and a starter. Run the worker first, then the starter in a separate terminal:

```bash
# Terminal 1: start the worker
cargo run --features examples --example hello-world-worker

# Terminal 2: start the workflow
cargo run --features examples --example hello-world-starter
```

See each example's README for details and expected output.

## Examples

- [Hello World](hello_world/) — Basic workflow that calls a single activity
- [Activity Heartbeating](activity_heartbeating/) — Long-running activity with heartbeating and resume-on-retry
- [Activity Inbound Interceptor](activity_interceptor/) — Wrapping inbound activity execution and inspecting typed inputs/outputs
- [Timer Examples](timer_examples/) — Workflow timers, racing timers against activities, and timer cancellation
- [Message Passing](message_passing/) — Signals, queries, and updates on a workflow
- [Child Workflows](child_workflows/) — Starting and collecting results from child workflows
- [Continue-As-New](continue_as_new/) — Long-running workflows via continue-as-new
- [Saga](saga/) — Saga/compensation pattern for distributed transactions
- [Patching](patching/) — Workflow versioning with `ctx.patched()`
- [Local Activities](local_activities/) — Local vs remote activity execution
- [Search Attributes](search_attributes/) — Reading and upserting workflow search attributes
- [Updatable Timer](updatable_timer/) — Timer that can be rescheduled via signals
- [Polling](polling/) — Polling an external condition with activities and timers
- [Cancellation](cancellation/) — Workflow and activity cancellation with cleanup
- [Encryption](encryption/) — Custom `PayloadCodec` for payload encryption
- [Schedules](schedules/) — Creating and managing scheduled workflows
- [WASM Workflows](wasm_workflows/) — Component workflow used by WASM workflow integration tests
