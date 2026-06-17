# Example app in Rust

<!-- markdownlint-disable blanks-around-fences -->

This page provides a detailed description of the code for a [test app](https://github.com/ydb-platform/ydb-rs-sdk/tree/master/ydb/examples/basic) that uses the YDB [Rust SDK](https://github.com/ydb-platform/ydb-rs-sdk).

## Downloading and starting {#download}

The instructions below assume that [Git](https://git-scm.com/downloads) and [Rust](https://www.rust-lang.org/tools/install) (1.85+) are installed. Install the SDK from crates.io or clone the repository — see [Installing the SDK](../../../reference/ydb-sdk/install.md).

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

One-shot queries use awaitable builders on [`QueryClient`](https://docs.rs/ydb/latest/ydb/struct.QueryClient.html):

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
        title Utf8,
        series_info Utf8,
        release_date Date,
        comment Utf8,
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
qc.exec("DECLARE $seriesData AS List<Struct<...>>; REPLACE INTO ... FROM AS_TABLE($seriesData);")
    .param("$seriesData", list)
    .await?;
```

{% include [steps/04_query_processing.md](../_includes/steps/04_query_processing.md) %}

Streaming read with snapshot read-only isolation:

```rust
use ydb::QueryTxMode;

let mut stream = qc
    .query("SELECT series_id, title, release_date FROM `native/query/series`")
    .with_tx_mode(QueryTxMode::SnapshotReadOnly)
    .idempotent(true)
    .await?;

while let Some(result_set) = stream.next_result_set().await? {
    for mut row in result_set {
        // extract columns from row
    }
}
stream.close().await?;
```

{% include [steps/06_param_queries.md](../_includes/steps/06_param_queries.md) %}

Per-call parameters use `.param(name, value)` or the `ydb_params!` macro.

{% include [steps/10_transaction_control.md](../_includes/steps/10_transaction_control.md) %}

Explicit isolation modes are set with `.with_tx_mode(QueryTxMode::SnapshotReadOnly)` (one-shot) or [`QueryTransactionOptions`](https://docs.rs/ydb/latest/ydb/struct.QueryTransactionOptions.html) for interactive transactions. Default one-shot mode is implicit — the server infers isolation from the SQL statement.
