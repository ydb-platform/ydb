Retry an async function.

```rust
use backon::ExponentialBuilder;
use backon::Retryable;
use anyhow::Result;

async fn fetch() -> Result<String> {
    Ok("Hello, World!".to_string())
}

#[tokio::main]
async fn main() -> Result<()> {
    let content = fetch.retry(ExponentialBuilder::default()).await?;

    println!("fetch succeeded: {}", content);
    Ok(())
}
```
