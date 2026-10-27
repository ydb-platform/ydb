Retry an async function which takes `&mut self` as receiver.

This is a bit more complex since we need to capture the receiver in the closure with ownership. backon supports this use case by `RetryableWithContext`.

```rust
 use anyhow::Result;
 use backon::ExponentialBuilder;
 use backon::RetryableWithContext;

 struct Test;

 impl Test {
     async fn fetch(&mut self, url: &str) -> Result<String> {
         Ok(reqwest::get(url).await?.text().await?)
     }
 }

 #[tokio::main(flavor = "current_thread")]
 async fn main() -> Result<()> {
     let test = Test;

     let (_, result) = (|mut v: Test| async {
         let res = v.fetch("https://www.rust-lang.org").await;
         // Return input context back.
         (v, res)
     })
     .retry(ExponentialBuilder::default())
     // Passing context in.
     .context(test)
     .when(|e| e.to_string() == "retryable")
     .await;

     println!("fetch succeeded: {}", result.unwrap());
     Ok(())
 }
```
