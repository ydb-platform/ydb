Retry an async function inside `&mut self` functions.

```rust
 use anyhow::Result;
 use backon::ExponentialBuilder;
 use backon::Retryable;

 struct Test;

 impl Test {
     async fn fetch(&self, url: &str) -> Result<String> {
         Ok(reqwest::get(url).await?.text().await?)
     }

     async fn run(&mut self) -> Result<String> {
         let content = (|| async { self.fetch("https://www.rust-lang.org").await })
             .retry(ExponentialBuilder::default())
             .when(|e| e.to_string() == "retryable")
             .await?;
         Ok(content)
     }
 }
```
