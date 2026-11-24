# Temporal Rust Client

[![crates.io](https://img.shields.io/crates/v/temporalio-client.svg)](https://crates.io/crates/temporalio-client)
[![docs.rs](https://docs.rs/temporalio-client/badge.svg)](https://docs.rs/temporalio-client)

This crate provides a Rust client for interacting with the Temporal service. It can be used
standalone to start and manage workflows, or together with the
[`temporalio-sdk`](https://crates.io/crates/temporalio-sdk) crate to run workers.

⚠️ **This crate is in Public Preview and under active development.** The API can and
will continue to evolve.

## Quick Start

### Connecting and Starting Workflows with Environment Configuration

The simplest way to connect is to load connection settings from environment variables and/or a
`temporal.toml` config file:

```rust
use temporalio_client::{
    Client, Connection, WorkflowOptions, GetWorkflowResultOptions,
    envconfig::LoadClientConfigProfileOptions,
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

This reads `TEMPORAL_ADDRESS`, `TEMPORAL_NAMESPACE`, `TEMPORAL_API_KEY`, TLS settings, and more
from the environment. See the [`envconfig` module docs](https://docs.rs/temporalio-client/latest/temporalio_client/envconfig/) for the full list of
supported variables and the TOML file format.

### Connecting and Starting Workflows with Explicit Options

```rust
use temporalio_client::{
    Client, ClientOptions, Connection, ConnectionOptions,
    WorkflowOptions, GetWorkflowResultOptions, Url,
};
use std::str::FromStr;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let connection = Connection::connect(
        ConnectionOptions::new(Url::from_str("http://localhost:7233")?)
            .identity("my-client".to_string())
            .build()
    ).await?;

    // "default" is your namespace
    let client = Client::new(connection, ClientOptions::new("default").build());

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

## Raw gRPC Access

For operations not covered by the high-level API, access the underlying gRPC service clients
directly:

```rust
let mut workflowf_service = client.connection().workflow_service();
let mut operator_service = client.connection().operator_service();
```
