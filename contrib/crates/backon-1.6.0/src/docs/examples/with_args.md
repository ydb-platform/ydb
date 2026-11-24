Retry function with args.

It's a pity that rust doesn't allow us to implement `Retryable` for async function with args. So we have to use a workaround to make it work.

```rust
 use anyhow::Result;
 use backon::ExponentialBuilder;
 use backon::Retryable;

 async fn fetch(url: &str) -> Result<String> {
     Ok(reqwest::get(url).await?.text().await?)
 }

 #[tokio::main(flavor = "current_thread")]
 async fn main() -> Result<()> {
     let content = (|| async { fetch("https://www.rust-lang.org").await })
         .retry(ExponentialBuilder::default())
         .when(|e| e.to_string() == "retryable")
         .await?;

     println!("fetch succeeded: {}", content);
     Ok(())
 }
```
