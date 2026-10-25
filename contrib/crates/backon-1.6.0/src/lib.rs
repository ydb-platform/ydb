#![doc(
    html_logo_url = "https://raw.githubusercontent.com/Xuanwo/backon/main/.github/assets/logo.jpeg"
)]
#![cfg_attr(docsrs, feature(doc_cfg))]

//! [![Build Status]][actions] [![Latest Version]][crates.io] [![](https://img.shields.io/discord/1111711408875393035?logo=discord&label=discord)](https://discord.gg/8ARnvtJePD)
//!
//! [Build Status]: https://img.shields.io/github/actions/workflow/status/Xuanwo/backon/ci.yml?branch=main
//! [actions]: https://github.com/Xuanwo/backon/actions?query=branch%3Amain
//! [Latest Version]: https://img.shields.io/crates/v/backon.svg
//! [crates.io]: https://crates.io/crates/backon
//!
//! <img src="https://raw.githubusercontent.com/Xuanwo/backon/main/.github/assets/logo.jpeg" alt="BackON" width="38.2%"/>
//!
//! Make **retry** like a built-in feature provided by Rust.
//!
//! - **Simple**: Just like a built-in feature: `your_fn.retry(ExponentialBuilder::default()).await`.
//! - **Flexible**: Supports both blocking and async functions.
//! - **Powerful**: Allows control over retry behavior such as [`when`](https://docs.rs/backon/latest/backon/struct.Retry.html#method.when) and [`notify`](https://docs.rs/backon/latest/backon/struct.Retry.html#method.notify).
//! - **Customizable**: Supports custom retry strategies like [exponential](https://docs.rs/backon/latest/backon/struct.ExponentialBuilder.html), [constant](https://docs.rs/backon/latest/backon/struct.ConstantBuilder.html), etc.
//!
//! # Backoff
//!
//! Retry in BackON requires a backoff strategy. BackON will accept a [`BackoffBuilder`] which will generate a new [`Backoff`] for each retry. It also accepts any object that implements [`Backoff`]. You can therefore easily implement your own custom backoff strategy.
//!
//! BackON provides several backoff implementations with reasonable defaults:
//!
//! - [`ConstantBuilder`]: backoff with a constant delay, limited to a specific number of attempts.
//! - [`ExponentialBuilder`]: backoff with an exponential delay, also supports jitter.
//! - [`FibonacciBuilder`]: backoff with a fibonacci delay, also supports jitter.
//!
//! # Sleep
//!
//! Retry in BackON requires an implementation for sleeping, such an implementation
//! is called a Sleeper, it will implement [`Sleeper`] or [`BlockingSleeper`] depending
//! on if it is going to be used in an asynchronous context.
//!
//! ## Default Sleeper
//!
//! Currently, BackON has 3 built-in Sleeper implementations for different
//! environments, they are gated under their own features, which are enabled
//! by default:
//!
//! |      `Sleeper`          | feature             | Environment |  Asynchronous |
//! |-------------------------|---------------------|-------------|---------------|
//! | [`TokioSleeper`]        | tokio-sleep         | non-wasm32  |  Yes          |
//! | [`GlooTimersSleep`]     | gloo-timers-sleep   |   wasm32    |  Yes          |
//! | [`FuturesTimerSleeper`] | futures-timer-sleep |wasm/non-wasm|  Yes          |
//! | [`EmbassySleep`]        | embassy-sleep       |   no_std    |  Yes          |
//! | [`StdSleeper`]          | std-blocking-sleep  |    std      |  No           |
//!
//! ## Custom Sleeper
//!
//! If you do not want to use the built-in Sleeper, you CAN provide a custom
//! implementation, let's implement an asynchronous dummy Sleeper that does
//! not sleep at all. You will find it pretty similar when you implement a
//! blocking one.
//!
//! ```
//! use std::time::Duration;
//!
//! use backon::Sleeper;
//!
//! /// A dummy `Sleeper` impl that prints then becomes ready!
//! struct DummySleeper;
//!
//! impl Sleeper for DummySleeper {
//!     type Sleep = std::future::Ready<()>;
//!
//!     fn sleep(&self, dur: Duration) -> Self::Sleep {
//!         println!("Hello from DummySleeper!");
//!         std::future::ready(())
//!     }
//! }
//! ```
//!
//! ## The empty Sleeper
//!
//! If neither feature is enabled nor a custom implementation is provided, BackON
//! will fallback to the empty sleeper, in which case, a compile-time error that
//! `PleaseEnableAFeatureOrProvideACustomSleeper needs to implement Sleeper or
//! BlockingSleeper` will be raised to remind you to choose or bring a real Sleeper
//! implementation.
//!
//! # Retry
//!
//! For additional examples, please visit [`docs::examples`].
//!
//! ## Retry an async function
//!
//! ```rust
//! use anyhow::Result;
//! use backon::ExponentialBuilder;
//! use backon::Retryable;
//! use core::time::Duration;
//!
//! async fn fetch() -> Result<String> {
//!     Ok("hello, world!".to_string())
//! }
//!
//! #[tokio::main(flavor = "current_thread")]
//! async fn main() -> Result<()> {
//!     let content = fetch
//!         // Retry with exponential backoff
//!         .retry(ExponentialBuilder::default())
//!         // Sleep implementation, default to tokio::time::sleep if `tokio-sleep` has been enabled.
//!         .sleep(tokio::time::sleep)
//!         // When to retry
//!         .when(|e| e.to_string() == "EOF")
//!         // Notify when retrying
//!         .notify(|err: &anyhow::Error, dur: Duration| {
//!             println!("retrying {:?} after {:?}", err, dur);
//!         })
//!         .await?;
//!     println!("fetch succeeded: {}", content);
//!
//!     Ok(())
//! }
//! ```
//!
//! ## Retry a blocking function
//!
//! ```rust
//! use anyhow::Result;
//! use backon::BlockingRetryable;
//! use backon::ExponentialBuilder;
//! use core::time::Duration;
//!
//! fn fetch() -> Result<String> {
//!     Ok("hello, world!".to_string())
//! }
//!
//! fn main() -> Result<()> {
//!     let content = fetch
//!         // Retry with exponential backoff
//!         .retry(ExponentialBuilder::default())
//!         // Sleep implementation, default to std::thread::sleep if `std-blocking-sleep` has been enabled.
//!         .sleep(std::thread::sleep)
//!         // When to retry
//!         .when(|e| e.to_string() == "EOF")
//!         // Notify when retrying
//!         .notify(|err: &anyhow::Error, dur: Duration| {
//!             println!("retrying {:?} after {:?}", err, dur);
//!         })
//!         .call()?;
//!     println!("fetch succeeded: {}", content);
//!
//!     Ok(())
//! }
//! ```
//!
//! ## Retry an async function with context
//!
//! Sometimes users can meet the problem that the async function is needs to take `FnMut`:
//!
//! ```shell
//! error: captured variable cannot escape `FnMut` closure body
//!    --> src/retry.rs:404:27
//!     |
//! 400 |         let mut test = Test;
//!     |             -------- variable defined here
//! ...
//! 404 |         let result = { || async { test.hello().await } }
//!     |                         - ^^^^^^^^----^^^^^^^^^^^^^^^^
//!     |                         | |       |
//!     |                         | |       variable captured here
//!     |                         | returns an `async` block that contains a reference to a captured variable, which then escapes the closure body
//!     |                         inferred to be a `FnMut` closure
//!     |
//!     = note: `FnMut` closures only have access to their captured variables while they are executing...
//!     = note: ...therefore, they cannot allow references to captured variables to escape
//! ```
//!
//! `RetryableWithContext` is designed for this, it allows you to pass a context
//! to the retry function, and return it back after the retry is done.
//!
//! ```no_run
//! use anyhow::anyhow;
//! use anyhow::Result;
//! use backon::ExponentialBuilder;
//! use backon::RetryableWithContext;
//!
//! struct Test;
//!
//! impl Test {
//!     async fn hello(&mut self) -> Result<usize> {
//!         Err(anyhow!("not retryable"))
//!     }
//! }
//!
//! #[tokio::main(flavor = "current_thread")]
//! async fn main() -> Result<()> {
//!     let mut test = Test;
//!
//!     // (Test, Result<usize>)
//!     let (_, result) = {
//!         |mut v: Test| async {
//!             let res = v.hello().await;
//!             (v, res)
//!         }
//!     }
//!     .retry(ExponentialBuilder::default())
//!     .context(test)
//!     .await;
//!
//!     Ok(())
//! }
//! ```

#![deny(missing_docs)]
#![deny(unused_qualifications)]
#![no_std]

#[cfg(feature = "std-blocking-sleep")]
extern crate std;

mod backoff;
pub use backoff::*;

mod retry;
pub use retry::Retry;
pub use retry::Retryable;

mod retry_with_context;
pub use retry_with_context::RetryWithContext;
pub use retry_with_context::RetryableWithContext;

mod sleep;
pub use sleep::DefaultSleeper;
#[cfg(feature = "futures-timer-sleep")]
pub use sleep::FuturesTimerSleeper;
#[cfg(all(target_arch = "wasm32", feature = "gloo-timers-sleep"))]
pub use sleep::GlooTimersSleep;
pub use sleep::Sleeper;
#[cfg(all(not(target_arch = "wasm32"), feature = "tokio-sleep"))]
pub use sleep::TokioSleeper;

mod blocking_retry;
pub use blocking_retry::BlockingRetry;
pub use blocking_retry::BlockingRetryable;

mod blocking_retry_with_context;
pub use blocking_retry_with_context::BlockingRetryWithContext;
pub use blocking_retry_with_context::BlockingRetryableWithContext;

mod blocking_sleep;
pub use blocking_sleep::BlockingSleeper;
pub use blocking_sleep::DefaultBlockingSleeper;
#[cfg(feature = "std-blocking-sleep")]
pub use blocking_sleep::StdSleeper;

#[cfg(feature = "embassy-sleep")]
mod embassy_timer_sleep;
#[cfg(feature = "embassy-sleep")]
pub use embassy_timer_sleep::EmbassySleeper;

#[cfg(docsrs)]
pub mod docs;
