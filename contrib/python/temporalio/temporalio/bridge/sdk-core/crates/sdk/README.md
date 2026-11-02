# Temporal Rust SDK

[![crates.io](https://img.shields.io/crates/v/temporalio-sdk.svg)](https://crates.io/crates/temporalio-sdk)
[![docs.rs](https://docs.rs/temporalio-sdk/badge.svg)](https://docs.rs/temporalio-sdk)

This crate contains a Public Preview Rust SDK. The SDK is built on top of
Core and provides a native Rust experience for writing Temporal workflows and activities.

⚠️ **The SDK is in Public Preview and under active development.** The API can and
will continue to evolve.

## Quick Start

### Activities

Activities are defined using the `#[activities]` and `#[activity]` macros:

```rust
use temporalio_macros::activities;
use temporalio_sdk::activities::{ActivityContext, ActivityError};
use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};

struct MyActivities {
    counter: AtomicUsize,
}

#[activities]
impl MyActivities {
    #[activity]
    pub async fn greet(_ctx: ActivityContext, name: String) -> Result<String, ActivityError> {
        Ok(format!("Hello, {}!", name))
    }

    // Activities can also use shared state via Arc<Self>
    #[activity]
    pub async fn increment(self: Arc<Self>, _ctx: ActivityContext) -> Result<u32, ActivityError> {
        Ok(self.counter.fetch_add(1, Ordering::Relaxed) as u32)
    }
}
```

### Workflows

Workflows are defined using the `#[workflow]` and `#[workflow_methods]` macros:

```rust
use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::{WorkflowContext, WorkflowContextView, WorkflowResult};
use std::time::Duration;

#[workflow]
pub struct GreetingWorkflow {
    name: String,
}

#[workflow_methods]
impl GreetingWorkflow {
    #[init]
    fn new(_ctx: &WorkflowContextView, name: String) -> Self {
        Self { name }
    }

    #[run]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<String> {
        let name = ctx.state(|s| s.name.clone());

        // Execute an activity
        let greeting = ctx.execute_activity(
            MyActivities::greet,
            name,
            ActivityOptions::start_to_close_timeout(Duration::from_secs(10))
        )?.await?;

        Ok(greeting)
    }
}
```

### Running a Worker

The simplest way to configure a connection is with environment variables and/or a `temporal.toml`
config file. See the [`envconfig` module docs](https://docs.rs/temporalio-client/latest/temporalio_client/envconfig/) for supported variables and
the TOML format.

```rust
use temporalio_client::{Client, ClientOptions, Connection, envconfig::LoadClientConfigProfileOptions};
use temporalio_sdk::{Runtime, Worker, WorkerOptions};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = Runtime::new_assume_tokio(Default::default())?;
    let (conn_options, client_options) = ClientOptions::load_from_config(
        LoadClientConfigProfileOptions::default()
    )?;
    let connection = Connection::connect(conn_options).await?;
    let client = Client::new(connection, client_options)?;

    let worker_options = WorkerOptions::new("my-task-queue")
        .register_activities(MyActivities { counter: Default::default() })
        .register_workflow::<GreetingWorkflow>()?
        .build();

    Worker::new(&runtime, client, worker_options)?.run().await?;
    Ok(())
}
```

## Crate Features

The SDK enables a few convenience integrations by default. Users who want a smaller dependency
graph can disable defaults and opt back into the integrations they use:

```toml
temporalio-sdk = { version = "0.3", default-features = false, features = ["envconfig"] }
```

- `envconfig` - enabled by default. Adds `ClientOptions::load_from_config` and related helpers for
  loading connection settings from environment variables and `temporal.toml` files.
- `prometheus` - enabled by default. Adds the Prometheus metrics exporter in
  `temporalio_common::telemetry` for serving SDK metrics from a HTTP endpoint.
- `otel` - optional. Adds the OpenTelemetry metrics exporter in `temporalio_common::telemetry` for
  sending SDK metrics to an OpenTelemetry collector.

## Workflows in detail

Workflows are the core abstraction in Temporal. They are defined as structs with associated methods:

- **`#[init]`** (optional) - Constructor that receives initial input
- **`#[run]`** (required) - Main workflow logic, must be async
- **`#[signal]`** - Handlers for external signals (sync or async)
- **`#[query]`** - Read-only handlers for querying workflow state (must be sync)
- **`#[update]`** - Handlers that can mutate state and return a result (sync or async)

`#[run]`, `#[signal]`, `#[query]`, and `#[update]` all accept an optional `name` parameter to specify the name of the method. If not specified, the name of the method will be used.

Sync signals and updates are able to mutate state directly. Async methods must mutate state through
the context with `state()` or `state_mut()`.

```rust
#[workflow]
pub struct MyWorkflow {
    values: Vec<u32>,
}

#[workflow_methods]
impl MyWorkflow {
    #[run]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<Vec<u32>> {
        // Wait until we have at least 3 values
        ctx.wait_condition(|s| s.values.len() >= 3)
            .await?;
        Ok(ctx.state(|s| s.values.clone()))
    }

    #[signal(name = "add_value")]
    fn push_value(&mut self, _ctx: &mut WorkflowContext<Self>, value: u32) {
        self.values.push(value);
    }

    #[query]
    fn get_values(&self, _ctx: &WorkflowContextView) -> Vec<u32> {
        self.values.clone()
    }

    #[update]
    async fn add_wait_return(ctx: &mut WorkflowContext<Self>, value: u32) -> Vec<u32> {
        ctx.state_mut(|s| s.values.push(value));
        ctx.timer(Duration::from_secs(1)).await;
        ctx.state(|s| s.values.clone())
    }
}
```

### Workflow Logic Constraints

Workflow code must be deterministic. This means:

- No direct I/O operations (use activities instead)
- No threading or random number generation
- No access to system time (use `ctx.workflow_time()` instead)
- No global mutable state
- **Do not use `tokio` or `futures` concurrency primitives directly in workflow code.** Many of them
  (e.g. `tokio::select!`, `tokio::spawn`, `futures::select!`) introduce nondeterministic behavior
  that will break workflow replay. Instead, use the deterministic wrappers provided in
  `temporalio_sdk::workflows`:
  - `select!` — deterministic select (polls in declaration order)
  - `join!` — deterministic join for a fixed number of futures
  - `join_all` — deterministic join for a dynamic collection of futures

### Runtime Nondeterminism Detection

The Rust SDK includes a runtime nondeterminism detector that monitors async wake sources inside
workflow code. It is **enabled by default** and can be disabled via
`WorkerOptions::detect_nondeterministic_futures(false)`.

**How it works:** The SDK tracks which async wake-ups originate from SDK-provided primitives (timers,
activities, child workflows, etc.) versus external sources. When a non-SDK wake is detected, the
workflow task is failed with a descriptive error.

**What it catches:**

- `tokio::time::sleep` / `tokio::time::interval` -- use `ctx.timer()` instead
- `tokio::net` / `tokio::fs` / any async IO -- perform IO in activities, not workflows
- `tokio::spawn` -- do not spawn tasks from workflow code
- `std::thread::spawn` with async channels -- all cross-thread wakes are flagged
- Direct use of `tokio::sync` channels (oneshot, mpsc, watch) -- use `ctx.state_mut()` +
  `ctx.wait_condition()` for inter-future coordination instead

**Detection timing:** Detection is based on observing non-SDK wake sources. Because these wakes fire
asynchronously (e.g., a tokio timer fires after the activation that started it), the failure is
typically reported on the *next* workflow task, not the one that introduced the nondeterministic
code. The workflow task that started the operation completes normally; the subsequent task fails
with the detection error. The server then retries from that point.

**What it does NOT catch:**

- `futures::select!` without `biased` -- randomizes poll order within a single poll. Use
  `workflows::select!` or `futures::select! { biased; ... }` for deterministic ordering
- Any combinator that only affects the order in which ready futures are polled
- Purely synchronous nondeterminism (e.g., `std::time::SystemTime::now()`, `rand::random()`)

**Disabling detection:** Set `detect_nondeterministic_futures(false)` on `WorkerOptions`. This may
be useful during migration or for advanced users who understand the determinism constraints and want
to use patterns that trigger false positives.

### Timers

```rust
// Wait for a duration
ctx.timer(Duration::from_secs(60)).await;
```

### Child Workflows

```rust
let started = ctx
    .start_child_workflow(
        MyChildWorkflow::run,
        "input",
        ChildWorkflowOptions::workflow_id("child-1".to_string()),
    )
    .await?;
let result = started.result().await?;
```

### Continue-As-New

```rust
// To continue as new, use the workflow context helper and propagate the termination
ctx.continue_as_new(&new_input, ContinueAsNewOptions::default())?;
```

### Patching (Versioning)

Use patching to safely evolve workflow logic:

```rust
if ctx.patched("my-patch-id") {
    // New code path
} else {
    // Old code path (for existing workflows)
}
```

### Nexus Operations

The SDK supports starting Nexus operations from a workflow:

```rust
let started = ctx.start_nexus_operation(NexusOperationOptions {
    endpoint: "my-endpoint".to_string(),
    service: "my-service".to_string(),
    operation: "my-operation".to_string(),
    input: Some(payload),
    ..Default::default()
}).await?;
```

Defining Nexus handlers will be added later.

## Activities in detail

Use Activities to perform side effects like I/O operations, API calls, or any non-deterministic
work.

### Error Handling

Activities return `Result<T, ActivityError>` with the following error types:

- **`ActivityError::Application`** - Application failure metadata is carried by `ApplicationFailure`
- **`ActivityError::Cancelled`** - Activity was cancelled
- **`ActivityError::WillCompleteAsync`** - Activity will complete asynchronously

### Local Activities

For short-lived activities that you want to run on the same worker as the workflow:

```rust
ctx.execute_local_activity(
    MyActivities::quick_operation,
    input,
    LocalActivityOptions {
        schedule_to_close_timeout: Some(Duration::from_secs(5)),
        ..Default::default()
    }
)?.await?;
```

## Cancellation

Workflow operations inherit the workflow's root cancellation token by default. This includes
timers, activities, local activities, child workflows, signals, Nexus operations, and wait
conditions. Long-running activities should heartbeat with `ctx.record_heartbeat(...)` to ensure they
receive cancellation notifications and report progress.

```rust
// Condition waits inherit workflow cancellation.
ctx.wait_condition(|state| state.ready).await?;

// A child token cancels a related group of operations together.
let group = ctx.cancellation_token().child_token();
let timer = ctx.timer(TimerOptions {
    duration: Duration::from_secs(60),
    cancellation_token: Some(group.clone()),
    ..Default::default()
});
group.cancel_with_reason("no longer needed");

// A newly-created token is detached, which is useful for cleanup after workflow cancellation.
let cleanup_token = WorkflowCancellationToken::new();
let mut cleanup_options = ActivityOptions::start_to_close_timeout(Duration::from_secs(10));
cleanup_options.cancellation_token = Some(cleanup_token);
ctx.execute_activity(MyActivities::cleanup, (), cleanup_options).await?;
```

## Worker Configuration

Workers can be configured with various options:

```rust
let worker_options = WorkerOptions::new("task-queue")
    .max_cached_workflows(1000)           // Workflow cache size
    .workflow_task_poller_behavior(...)   // Polling configuration
    .activity_task_poller_behavior(...)
    .graceful_shutdown_period(Duration::from_secs(30))
    .register_activities(my_activities)
    .register_workflow::<MyWorkflow>()?
    .build();
```

## Workflow Replay

`WorkflowReplayer` checks whether workflow code remains compatible with recorded workflow
histories. Histories can come from directly from a workflow handle or from JSON:

```rust
use temporalio_client::WorkflowHistory;
use temporalio_sdk::workflow_replayer::{WorkflowReplayer, WorkflowReplayerOptions};

let replayer = WorkflowReplayer::new(
    WorkflowReplayerOptions::new()
        .register_workflow::<MyWorkflow>()?
        .build(),
)?;
let saved_history = std::fs::read("workflow-history.json")?;
let history = WorkflowHistory::from_json(&saved_history)?;

replayer.replay_workflow(history).await?;
```


## Using the Client

The `temporalio_client` crate provides a client for interacting with the Temporal service. You can
use it to start workflows and communicate with running workflows via signals, queries, and updates,
among other operations.

### Connecting and Starting Workflows

```rust
use temporalio_client::{
    Client, ClientOptions, Connection,
    envconfig::LoadClientConfigProfileOptions,
    WorkflowOptions, GetWorkflowResultOptions,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (conn_options, client_options) = ClientOptions::load_from_config(
        LoadClientConfigProfileOptions::default()
    )?;
    let connection = Connection::connect(conn_options).await?;
    let client = Client::new(connection, client_options);

    // Start a workflow
    let handle = client.start_workflow(
        GreetingWorkflow::run,
        "World".to_string(),
        WorkflowOptions::new("my-task-queue", "greeting-workflow-1").build()
    ).await?;

    // Wait for the result
    let result = handle.get_result(GetWorkflowResultOptions::default()).await?;

    Ok(())
}
```

### Signals, Queries, and Updates

Once you have a workflow handle, you can interact with the running workflow:

```rust
use temporalio_client::{
    SignalOptions, QueryOptions, UpdateOptions,
    StartUpdateOptions,
    UntypedSignal,
};
use temporalio_common::data_converters::{PayloadConverter, RawValue};

// Get a handle to an existing workflow (or use one from start_workflow)
let handle = client.get_workflow_handle::<MyWorkflow>("workflow-id");

// --- Signals (fire-and-forget messages) ---
handle.signal(MyWorkflow::push_value, 42, SignalOptions::default()).await?;

// --- Queries (read workflow state) ---
let values = handle
    .query(MyWorkflow::get_values, (), QueryOptions::default())
    .await?;

// --- Updates (modify state and get a result) ---
let values = handle
    .execute_update(MyWorkflow::add_wait_return, 100, UpdateOptions::default())
    .await?;

// Start an update and wait for acceptance only
let update_handle = handle
    .start_update(
        MyWorkflow::add_wait_return,
        50,
        StartUpdateOptions::default()
    )
    .await?;
update_handle.get_result().await?;

// --- Untyped interactions (when types aren't known at compile time) ---
let pc = PayloadConverter::serde_json();
handle
    .signal(
        UntypedSignal::new("increment"),
        RawValue::from_value(&25i32, &pc),
        SignalOptions::default(),
    )
    .await?;
// UntypedQuery and UntypedUpdate work similarly
```

### Cancelling and Terminating Workflows

```rust
use temporalio_client::{CancelWorkflowOptions, TerminateWorkflowOptions};

// Request cancellation (workflow can handle this gracefully)
handle.cancel(CancelWorkflowOptions::builder().reason("No longer needed").build()).await?;

// Terminate immediately (workflow cannot intercept this)
handle.terminate(TerminateWorkflowOptions::builder().reason("Emergency shutdown").build()).await?;
```

### Listing Workflows

```rust
use temporalio_client::ListWorkflowsOptions;
use futures_util::StreamExt;

let mut stream = client.list_workflows(
    "WorkflowType = 'GreetingWorkflow'",
    ListWorkflowsOptions::builder().limit(10).build()
);

while let Some(result) = stream.next().await {
    let execution = result?;
    println!("Workflow: {} ({})", execution.id(), execution.workflow_type());
}
```

## Failure Conversion and Error Wrapping

The default failure converter preserves Temporal failure types when errors cross workflow or
activity boundaries.

This matters when Rust error propagation wraps Temporal SDK error types, e.g. `anyhow::Error`.
When an `ApplicationFailure` is created from an error whose source is a known Temporal SDK error, the
converter skips the outer error for the failure cause and encodes the known Temporal error
directly. The application failure's own message and metadata are still preserved.

This keeps the Rust SDK's `Failure`s aligned with other Temporal SDKs: SDK error types
remain represented as Temporal failure types, while unknown Rust error types are encoded as
application failures.
