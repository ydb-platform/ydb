//! [![pipeline status](https://gitlab.com/mexus/futures-retry/badges/master/pipeline.svg)](https://gitlab.com/mexus/futures-retry/commits/master)
//! [![crates.io](https://img.shields.io/crates/v/futures-retry.svg)](https://crates.io/crates/futures-retry)
//! [![docs.rs](https://docs.rs/futures-retry/badge.svg)](https://docs.rs/futures-retry)
//!
//! [[Release docs]](https://docs.rs/futures-retry/)
//!
//! [[Master docs]](https://mexus.gitlab.io/futures-retry/futures_retry/)
//!
//! A tool that helps you retry your future :) Well, `Future`s and `Stream`s, to be precise.
//!
//! It's quite a common task when you need to repeat some action if you've got an error, be it a
//! connection timeout or some temporary OS error.
//!
//! When you do stuff in a synchronous manner it's quite easy to implement the attempts logic, but
//! when it comes to asynchronous programming you suddenly need to write a fool load of a
//! boilerplate code, introduce state machines and everything.
//!
//! This library aims to make your life easier and let you write more straightword and nice code,
//! concentrating on buisness logic rathen than on handling all the mess.
//!
//! I was inspired to write this library after coming over a [`hyper`
//! issue](https://github.com/hyperium/hyper/issues/1358), and I came to an understanding that the
//! problem is more common than I used to think.
//!
//! For examples have a look in the `examples/` folder in the git repo.
//!
//! Suggestions and critiques are welcome!
//!
//! ```rust
//! // ...
//! # use tokio::io;
//! # use tokio::net::{TcpListener, TcpStream};
//! # use std::time::Duration;
//! # use futures::{future::{ok, select}, TryStreamExt, TryFutureExt, FutureExt, stream};
//! use futures_retry::{RetryPolicy, StreamRetryExt};
//!
//! // In this example we use a free function to handle errors, while in your project you have
//! // more options: for simple cases a simple closure will do, for complex cases you might go
//! // as far as implementing an `ErrorHandler` trait for a custom type with some complex logic.
//! fn handle_error(e: io::Error) -> RetryPolicy<io::Error> {
//!   match e.kind() {
//!     io::ErrorKind::Interrupted => RetryPolicy::Repeat,
//!     io::ErrorKind::PermissionDenied => RetryPolicy::ForwardError(e),
//!     _ => RetryPolicy::WaitRetry(Duration::from_millis(5)),
//!   }
//! }
//!
//! async fn serve_connection(stream: TcpStream) {
//!   // ...
//! }
//!
//! #[tokio::main]
//! async fn main() {
//!   let addr = //...
//!   # "127.0.0.1:12345";
//!   let mut listener = TcpListener::bind(addr).await.unwrap();
//!   let server = stream::try_unfold(listener, |listener| async move {
//!     Ok(Some((listener.accept().await?.0, listener)))
//!   })
//!   .retry(handle_error) // Magic happens here
//!   .and_then(|(stream, _attempt)| {
//!     tokio::spawn(serve_connection(stream));
//!     ok(())
//!   })
//!   .try_for_each(|_| ok(()))
//!   .map_err(|(e, _attempt)| eprintln!("Caught an error {}", e));
//!   # // This nasty hack is required to exit immediately when running the doc tests.
//!   # futures::pin_mut!(server);
//!   # let server = select(ok::<_, ()>(()), server).map(|_| ());
//!   server.await
//! }
//! ```
//!
//! ## License
//!
//! Licensed under either of
//!
//!  * Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
//!  * MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)
//!
//! at your option.
//!
//! ### Contribution
//!
//! Unless you explicitly state otherwise, any contribution intentionally submitted
//! for inclusion in the work by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any
//! additional terms or conditions.

#![deny(missing_docs)]
#![allow(clippy::needless_doctest_main)]

use std::time::Duration;

mod error_handler;
mod future;
mod stream;

pub use crate::{
    error_handler::ErrorHandler,
    future::{FutureFactory, FutureRetry},
    stream::{StreamRetry, StreamRetryExt},
};

/// What to do when a future returns an error. Used in `FutureRetry::new` and `StreamRetry::new`.
#[derive(Debug, Eq, PartialEq)]
pub enum RetryPolicy<E> {
    /// Create and poll a new future immediately.
    ///
    /// # Be careful!
    ///
    /// Please be careful when using this variant since it might lead to a high (actually 100%) CPU
    /// usage in case a future instantly resolves into an error every time.
    Repeat,
    /// Wait for a given duration and make another attempt then.
    WaitRetry(Duration),
    /// Don't give it another try, just pass the error further to the user.
    ForwardError(E),
}
