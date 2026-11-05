Retry sqlx operations.

```rust
use backon::Retryable;
use anyhow::Result;
use backon::ExponentialBuilder;

#[tokio::main]
async fn main() -> Result<()> {
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(5)
        .connect("sqlite::memory:")
        .await?;

    let row: (i64,) = (|| sqlx::query_as("SELECT $1").bind(150_i64).fetch_one(&pool))
        .retry(ExponentialBuilder::default())
        .await?;

    assert_eq!(row.0, 150);

    Ok(())
}
```
