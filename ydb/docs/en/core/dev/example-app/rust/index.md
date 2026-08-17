# Example app in Rust

<!-- markdownlint-disable blanks-around-fences -->

This page provides a detailed description of the code for a [test app](https://github.com/ydb-platform/ydb-rs-sdk/tree/master/ydb/examples/basic) that uses the YDB [Rust SDK](https://github.com/ydb-platform/ydb-rs-sdk).

## Downloading and starting {#download}

[Git](https://git-scm.com/downloads) and [Rust](https://www.rust-lang.org/tools/install) 1.85+ are required. For SDK installation, see [{#T}](../../../reference/ydb-sdk/install.md).

Clone the repository:

```bash
git clone https://github.com/ydb-platform/ydb-rs-sdk.git
```

Start the example:

{% include [run_options.md](_includes/run_options.md) %}

{% include [init.md](../_includes/steps/01_init.md) %}

Import the crate and open a client:

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

`ClientBuilder::new_from_connection_string` accepts a {{ ydb-short-name }} connection string (`grpc://host:port/database`). `client.wait()` waits until the driver discovers cluster endpoints. `query_client()` is the entry point for the Query Service API.

By default, anonymous authentication is used for local Docker. For token auth, use [`ClientBuilder::with_credentials`](https://docs.rs/ydb/latest/ydb/struct.ClientBuilder.html) — see [authentication recipes](../../../recipes/ydb-sdk/auth.md).

## Query Service client {#query-client}

To run a single transactional SQL statement, use awaitable builders on [`QueryClient`](https://docs.rs/ydb/latest/ydb/struct.QueryClient.html):

- `qc.exec(yql)` — statement with no result set (DDL, DML).
- `qc.query_row(yql)` — exactly one row.
- `qc.query(yql).await?` — streaming [`QueryStream`](https://docs.rs/ydb/latest/ydb/struct.QueryStream.html) for large results.

Automatic retries are enabled via `clone_with_idempotent_operations(true)` for idempotent reads and DDL.

{% include [steps/02_create_table.md](../_includes/steps/02_create_table.md) %}

Table creation (no explicit transaction control — implicit session, server-side DDL):

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

Bulk load with `AS_TABLE` and typed parameters:

```rust
use ydb::{Value, ydb_struct};

let rows: Vec<Value> = /* ... */;
let list = Value::list_from(example_row, rows)?;
qc.exec("UPSERT INTO ... FROM AS_TABLE($seriesData);")
    .param("$seriesData", list)
    .await?;
```

{% include [steps/04_query_processing.md](../_includes/steps/04_query_processing.md) %}

Read materialized result with snapshot read-only isolation:

```rust
use ydb::QueryTxMode;

let mut result = qc
    .query("SELECT series_id, title, release_date FROM `native/query/series`")
    .with_tx_mode(QueryTxMode::SnapshotReadOnly)
    .idempotent(true)
    .await?;

while let Some(result_set) = result.next_result_set().await? {
    for mut row in result_set {
        // extract columns from row
    }
}
result.close().await?;
```

{% include [steps/06_param_queries.md](../_includes/steps/06_param_queries.md) %}

Per-call parameters use `.param(name, value)` or the `ydb_params!` macro:

```rust
use ydb::ydb_params;

// one parameter at a time
qc.exec("UPSERT INTO `native/query/series` (series_id, title) VALUES ($id, $title)")
    .param("$id", b"series-1".to_vec())
    .param("$title", "Example title")
    .await?;

// several parameters via macro
qc.exec("UPSERT INTO `native/query/series` (series_id, title) VALUES ($id, $title)")
    .params(ydb_params!(
        "$id" => b"series-2".to_vec(),
        "$title" => "Another title",
    ))
    .await?;
```

{% include [steps/10_transaction_control.md](../_includes/steps/10_transaction_control.md) %}

The Rust SDK does not expose explicit `Begin` / `Commit` to application code. Use `retry_transaction` with [`QueryTransactionOptions`](https://docs.rs/ydb/latest/ydb/struct.QueryTransactionOptions.html) for interactive transactions. For a single SQL statement, set isolation with `.with_tx_mode(...)`.

Single statement in snapshot read-only mode:

```rust
use ydb::QueryTxMode;

let mut row = qc
    .query_row("SELECT title FROM `native/query/series` WHERE series_id = $id")
    .param("$id", b"series-1".to_vec())
    .with_tx_mode(QueryTxMode::SnapshotReadOnly)
    .idempotent(true)
    .await?;
```

Interactive transaction with multiple operations:

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

By default, such calls use implicit transaction control — the server infers isolation from the SQL statement.
