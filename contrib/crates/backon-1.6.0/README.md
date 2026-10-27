# BackON &emsp; [![Build Status]][actions] [![Latest Version]][crates.io] [![](https://img.shields.io/discord/1111711408875393035?logo=discord&label=discord)](https://discord.gg/8ARnvtJePD)

[Build Status]: https://img.shields.io/github/actions/workflow/status/Xuanwo/backon/ci.yml?branch=main
[actions]: https://github.com/Xuanwo/backon/actions?query=branch%3Amain
[Latest Version]: https://img.shields.io/crates/v/backon.svg
[crates.io]: https://crates.io/crates/backon

<img src="https://raw.githubusercontent.com/Xuanwo/backon/main/.github/assets/logo.jpeg" alt="BackON" width="38.2%"/>

Make **retry** like a built-in feature provided by Rust.

- **Simple API**: Native feel: `your_fn.retry(ExponentialBuilder::default()).await`.
- **Sync & Async**: Supports both blocking and async operations seamlessly.
- **Precise Control**: Define when to retry and get notified via [`when`](https://docs.rs/backon/latest/backon/struct.Retry.html#method.when) and [`notify`](https://docs.rs/backon/latest/backon/struct.Retry.html#method.notify).
- **Custom Strategies**: Use built-in backoff strategies (exponential, constant) or define custom ones. Also supports dynamic backoff, such as using the HTTP `Retry-After` header.
- **Cross-Platform**: Works everywhere Rust does, including `wasm` & `no-std`.

---

## Quick Start

For more examples, check out the [examples](https://docs.rs/backon/latest/backon/docs/examples/index.html).

### Retry an async function.

```rust
use anyhow::Result;
use backon::ExponentialBuilder;
use backon::Retryable;

async fn fetch() -> Result<String> {
    Ok("hello, world!".to_string())
}

#[tokio::main]
async fn main() -> Result<()> {
    let content = fetch
        // Retry with exponential backoff
        .retry(ExponentialBuilder::default())
        // Sleep implementation, required if no feature has been enabled
        .sleep(tokio::time::sleep)
        // When to retry
        .when(|e| e.to_string() == "EOF")
        // Notify when retrying
        .notify(|err: &anyhow::Error, dur: Duration| {
            println!("retrying {:?} after {:?}", err, dur);
        })
        .await?;
    println!("fetch succeeded: {}", content);

    Ok(())
}
```

### Retry a blocking function.

```rust
use anyhow::Result;
use backon::BlockingRetryable;
use backon::ExponentialBuilder;

fn fetch() -> Result<String> {
    Ok("hello, world!".to_string())
}

fn main() -> Result<()> {
    let content = fetch
        // Retry with exponential backoff
        .retry(ExponentialBuilder::default())
        // Sleep implementation, required if no feature has been enabled
        .sleep(std::thread::sleep)
        // When to retry
        .when(|e| e.to_string() == "EOF")
        // Notify when retrying
        .notify(|err: &anyhow::Error, dur: Duration| {
            println!("retrying {:?} after {:?}", err, dur);
        })
        .call()?;
    println!("fetch succeeded: {}", content);

    Ok(())
}
```

## Contributing

Check out the [CONTRIBUTING.md](./CONTRIBUTING.md) guide for more details on getting started with contributing to this
project.

## Getting help

Submit [issues](https://github.com/Xuanwo/backon/issues/new/choose) for bug report or asking questions
in [discussion](https://github.com/Xuanwo/backon/discussions/new?category=q-a).

## License

Licensed under <a href="./LICENSE">Apache License, Version 2.0</a>.
