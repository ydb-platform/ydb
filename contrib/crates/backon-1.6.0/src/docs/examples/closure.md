Retry an closure.

```rust
use backon::ExponentialBuilder;
use backon::Retryable;
use backon::BlockingRetryable;

fn main() -> anyhow::Result<()> {
    let var = 42;
    // `f` can use input variables
    let f = || Ok::<u32, anyhow::Error>(var);
    let result = f.retry(backon::ExponentialBuilder::default()).call()?;
    println!("var = {result}");

    Ok(())
}
```
