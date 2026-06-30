# Example application in Rust

<!-- markdownlint-disable blanks-around-fences -->

This page provides a detailed breakdown of the code of the [test application](https://github.com/ydb-platform/ydb-rs-sdk/tree/master/ydb/examples/basic) that uses the YDB [Rust SDK](https://github.com/ydb-platform/ydb-rs-sdk).

## Getting and running {#download}

You need [Git](https://git-scm.com/downloads) and [Rust](https://www.rust-lang.org/tools/install) 1.85+. For SDK installation, see [{#T}](../../../reference/ydb-sdk/install.md).

Clone the repository:


```bash
git clone https://github.com/ydb-platform/ydb-rs-sdk.git
```


Running the example:

{% include [run_options.md](_includes/run_options.md) %}

{% include [init.md](../_includes/steps/01_init.md) %}

Importing and initializing the client:


```rust
use ydb::{ClientBuilder, YdbResult};

#[tokio::main]
async fn main() -> YdbResult<()> {
    let connection_string = std::env::var("YDB_CONNECTION_STRING")
        .unwrap_or_else(|_| "grpc://localhost:2136/local".to_string());

    let client = ClientBuilder::new_from_connection_string(connection_string)?.client()?;
    client.wait().await?;

    let mut qc = client.query_client().clone_with_idempotent_operations(true);
    // ...
    Ok(())
}
```


`ClientBuilder::new_from_connection_string` accepts a connection string (`grpc://host:port/database`). `client.wait()` waits for endpoint discovery. `query_client()` is the entry point to the Query Service API.

For a local Docker instance, anonymous authentication is used by default. For a token, use [`ClientBuilder::with_credentials`](https://docs.rs/ydb/latest/ydb/struct.ClientBuilder.html); see [authentication recipes](../../../recipes/ydb-sdk/auth.md).

## Query Service client {#query-client}

One-shot queries — awaitable builders on [`QueryClient`](https://docs.rs/ydb/latest/ydb/struct.QueryClient.html):

- `qc.exec(yql)` — without a result set (DDL, DML).
- `qc.query_row(yql)` — a single row.
- `qc.query(yql).await?` — streaming [`QueryStream`](https://docs.rs/ydb/latest/ydb/struct.QueryStream.html).

Retries on errors: `clone_with_idempotent_operations(true)` for idempotent reads and DDL.

{% include [steps/02_create_table.md](../_includes/steps/02_create_table.md) %}

Creating a table (implicit session, DDL without explicit tx_control):


```rust
qc.exec(format!(
    "CREATE TABLE IF NOT EXISTS `{}` (
        series_id Bytes,
        title Text,
        series_info Text,
        release_date Date,
        comment Text,
        PRIMARY KEY(series_id)
    )",
    "native/query/series"
))
.await?;
```


{% include [steps/03_write_queries.md](../_includes/steps/03_write_queries.md) %}

Batch upload via `AS_TABLE`:


```rust
use ydb::{Value, ydb_struct};

let rows: Vec<Value> = /* ... */;
let list = Value::list_from(example_row, rows)?;
qc.exec("UPSERT INTO ... FROM AS_TABLE($seriesData);")
    .param("$seriesData", list)
    .await?;
```


{% include [steps/04_query_processing.md](../_includes/steps/04_query_processing.md) %}

Reading a materialized result in snapshot read-only isolation mode:


```rust
use ydb::QueryTxMode;

let mut result = qc
    .query("SELECT series_id, title, release_date FROM `native/query/series`")
    .with_tx_mode(QueryTxMode::SnapshotReadOnly)
    .idempotent(true)
    .await?;

while let Some(result_set) = result.next_result_set().await? {
    for mut row in result_set {
        // extracting columns from row
    }
}
result.close().await?;
```


{% include [steps/06_param_queries.md](../_includes/steps/06_param_queries.md) %}

Parameters are set using the `.param(name, value)` function or the `ydb_params!` macro:


```rust
use ydb::ydb_params;

// one parameter at a time
qc.exec("UPSERT INTO `native/query/series` (series_id, title) VALUES ($id, $title)")
    .param("$id", b"series-1".to_vec())
    .param("$title", "Example title")
    .await?;

// multiple parameters via macro
qc.exec("UPSERT INTO `native/query/series` (series_id, title) VALUES ($id, $title)")
    .params(ydb_params!(
        "$id" => b"series-2".to_vec(),
        "$title" => "Another title",
    ))
    .await?;
```


{% include [steps/10_transaction_control.md](../_includes/steps/10_transaction_control.md) %}

In the Rust SDK, explicit transaction management (via `Begin` and `Commit`) is not available for client code. Instead, use `retry_transaction` with [`QueryTransactionOptions`](https://docs.rs/ydb/latest/ydb/struct.QueryTransactionOptions.html) for interactive transactions. For a single SQL query, the isolation mode is set via `.with_tx_mode(...)`.

A single query in snapshot read-only mode:


```rust
use ydb::QueryTxMode;

let mut row = qc
    .query_row("SELECT title FROM `native/query/series` WHERE series_id = $id")
    .param("$id", b"series-1".to_vec())
    .with_tx_mode(QueryTxMode::SnapshotReadOnly)
    .idempotent(true)
    .await?;
```


An interactive transaction with multiple operations:


```rust
use ydb::{QueryTransactionOptions, QueryTxMode};

let mut qc = qc.clone_with_transaction_options(
    QueryTransactionOptions::new().with_mode(QueryTxMode::SerializableReadWrite),
);

let title: String = qc
    .retry_transaction(async |tx| {
        let mut row = tx
            .query_row("SELECT title FROM `native/query/series` WHERE series_id = $id")
            .param("$id", b"series-1".to_vec())
            .await?;
        Ok(row.remove_field_by_name("title")?.try_into()?)
    })
    .await?;
```


By default, queries on the query client use the ImplicitTx mode; the actual isolation mode is determined by the server side {{ ydb-short-name }}.
