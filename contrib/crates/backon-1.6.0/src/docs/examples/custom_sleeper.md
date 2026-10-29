Let's implement a custom async Sleeper, say you are using Monoio as your async
runtime, you may want to implement it with `monoio::time::sleep()`. If you want
to implement a custom blocking Sleeper, you will find it pretty similar.

```rust
use std::time::Duration;
use backon::Sleeper;

/// Sleeper implemented using `monoio::time::sleep()`.
struct MonoioSleeper;

impl Sleeper for MonoioSleeper {
    type Sleep = monoio::time::Sleep;

    fn sleep(&self, dur: Duration) -> Self::Sleep {
        monoio::time::sleep(dur)
    }
}
```

Then you can use it like:

```rust
use backon::ExponentialBuilder;
use backon::Retryable;
use anyhow::Result;

async fn fetch() -> Result<String> {
    Ok("Hello, World!".to_string())
}

#[monoio::main(timer_enabled = true)]
async fn main() -> Result<()> {
    let content = fetch
        .retry(ExponentialBuilder::default())
        .sleep(MonoioSleeper)
        .await?;

    println!("fetch succeeded: {}", content);
    Ok(())
}

```