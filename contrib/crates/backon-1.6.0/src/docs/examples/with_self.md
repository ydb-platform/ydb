Retry an async function which takes `&self` as receiver.

```rust
 use anyhow::Result;
 use backon::ExponentialBuilder;
 use backon::Retryable;

 struct Test;

 impl Test {
     async fn fetch(&self, url: &str) -> Result<String> {
         Ok(reqwest::get(url).await?.text().await?)
     }
 }


 #[tokio::main(flavor = "current_thread")]
 async fn main() -> Result<()> {
     let test = Test;
     let content = (|| async { test.fetch("https://www.rust-lang.org").await })
         .retry(ExponentialBuilder::default())
         .when(|e| e.to_string() == "retryable")
         .await?;

     println!("fetch succeeded: {}", content);
     Ok(())
 }
```
