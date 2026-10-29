use crate::{ErrorHandler, RetryPolicy};
use futures::{ready, Stream, TryStream};
use pin_project_lite::pin_project;
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::time;

pin_project! {
    /// Provides a way to handle errors during a `Stream` execution, i.e. it gives you an ability to
    /// poll for future stream's items with a delay.
    ///
    /// This type is similar to [`FutureRetry`](struct.FutureRetry.html), but with a different
    /// semantics. For example, if for [`FutureRetry`](struct.FutureRetry.html) we need a factory that
    /// creates `Future`s, we don't need one for `Stream`s, since `Stream` itself is a natural producer
    /// of new items, so we don't have to recreated it if an error is encountered.
    ///
    /// A typical usage might be recovering from connection errors while trying to accept a connection
    /// on a TCP server.
    ///
    /// A `tcp-listener` example is available in the `examples` folder.
    ///
    /// Also have a look at [`StreamRetryExt`](trait.StreamRetryExt.html) trait for a more convenient
    /// usage.
    pub struct StreamRetry<F, S> {
        error_action: F,
        #[pin]
        stream: S,
        attempt: usize,
        #[pin]
        state: RetryState,
    }
}

/// An extention trait for `Stream` which allows to use `StreamRetry` in a chain-like manner.
///
/// # Example
///
/// This magic trait allows you to handle errors on streams in a very neat manner:
///
/// ```
/// // ...
/// use futures_retry::{RetryPolicy, StreamRetryExt};
/// # use futures::{TryStreamExt, TryFutureExt, future::{ok, select}, FutureExt, stream};
/// # use std::io;
/// # use std::time::Duration;
/// # use tokio::net::{TcpListener, TcpStream};
///
/// fn handle_error(e: io::Error) -> RetryPolicy<io::Error> {
///   match e.kind() {
///     io::ErrorKind::Interrupted => RetryPolicy::Repeat,
///     io::ErrorKind::PermissionDenied => RetryPolicy::ForwardError(e),
///     _ => RetryPolicy::WaitRetry(Duration::from_millis(5)),
///   }
/// }
///
/// async fn serve_connection(stream: TcpStream) {
///   // ...
/// }
///
/// #[tokio::main]
/// async fn main() {
///   let mut listener: TcpListener = // ...
///   # TcpListener::bind("[::]:0").await.unwrap();
///   let server = stream::try_unfold(listener, |listener| async move {
///     Ok(Some((listener.accept().await?.0, listener)))
///   })
///   .retry(handle_error)
///   .and_then(|(stream, _attempt)| {
///     tokio::spawn(serve_connection(stream));
///     ok(())
///   })
///   .try_for_each(|_| ok(()))
///   .map_err(|(e, _attempt)| eprintln!("Caught an error {}", e));
///
///   # // This nasty hack is required to exit immediately when running the doc tests.
///   # futures::pin_mut!(server);
///   # let server = select(ok::<_, ()>(()), server).map(|_| ());
///   server.await
/// }
/// ```
pub trait StreamRetryExt: TryStream {
    /// Converts the stream into a **retry stream**. See `StreamRetry::new` for details.
    fn retry<F>(self, error_action: F) -> StreamRetry<F, Self>
    where
        Self: Sized,
    {
        StreamRetry::new(self, error_action)
    }
}

impl<S: ?Sized> StreamRetryExt for S where S: TryStream {}

pin_project! {
    #[project = RetryStateProj]
    enum RetryState {
        WaitingForStream,
        TimerActive { #[pin] delay: time::Sleep },
    }
}

impl<F, S> StreamRetry<F, S> {
    /// Creates a `StreamRetry` using a provided stream and an object of `ErrorHandler` type that
    /// decides on a retry-policy depending on an encountered error.
    ///
    /// Please refer to the `tcp-listener` example in the `examples` folder to have a look at a
    /// possible usage or to a very convenient extension trait
    /// [`StreamRetryExt`](trait.StreamRetryExt.html).
    ///
    /// # Arguments
    ///
    /// * `stream`: a stream of future items,
    /// * `error_action`: a type that handles an error and decides which route to take: simply
    ///                   try again, wait and then try, or give up (on a critical error for
    ///                   exapmle).
    pub fn new(stream: S, error_action: F) -> Self
    where
        S: TryStream,
    {
        Self::with_counter(stream, error_action, 1)
    }

    /// Like a `new` method, but a custom attempt counter initial value might be provided.
    pub fn with_counter(stream: S, error_action: F, attempt_counter: usize) -> Self {
        Self {
            error_action,
            stream,
            attempt: attempt_counter,
            state: RetryState::WaitingForStream,
        }
    }
}

impl<F, S> Stream for StreamRetry<F, S>
where
    S: TryStream,
    F: ErrorHandler<S::Error>,
{
    type Item = Result<(S::Ok, usize), (F::OutError, usize)>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        loop {
            let this = self.as_mut().project();
            let attempt = *this.attempt;
            let new_state = match this.state.project() {
                RetryStateProj::TimerActive { delay } => {
                    ready!(delay.poll(cx));
                    RetryState::WaitingForStream
                }
                RetryStateProj::WaitingForStream => match ready!(this.stream.try_poll_next(cx)) {
                    Some(Ok(x)) => {
                        *this.attempt = 1;
                        this.error_action.ok(attempt);
                        return Poll::Ready(Some(Ok((x, attempt))));
                    }
                    None => {
                        return Poll::Ready(None);
                    }
                    Some(Err(e)) => {
                        *this.attempt += 1;
                        match this.error_action.handle(attempt, e) {
                            RetryPolicy::ForwardError(e) => {
                                return Poll::Ready(Some(Err((e, attempt))))
                            }
                            RetryPolicy::Repeat => RetryState::WaitingForStream,
                            RetryPolicy::WaitRetry(duration) => RetryState::TimerActive {
                                delay: time::sleep(duration),
                            },
                        }
                    }
                },
            };
            self.as_mut().project().state.set(new_state);
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use futures::{pin_mut, prelude::*};
    use std::time::Duration;

    #[tokio::test]
    async fn naive() {
        let stream = stream::iter(vec![Ok::<_, u8>(17u8), Ok(19u8)]);
        let retry = StreamRetry::new(stream, |_| RetryPolicy::Repeat::<()>);
        assert_eq!(
            Ok(vec![(17, 1), (19, 1)]),
            retry.try_collect::<Vec<_>>().await,
        );
    }

    #[tokio::test]
    async fn repeat() {
        let stream = stream::iter(vec![Ok(1), Err(17), Ok(19)]);
        let retry = StreamRetry::new(stream, |_| RetryPolicy::Repeat::<()>);
        assert_eq!(
            Ok(vec![(1, 1), (19, 2)]),
            retry.try_collect::<Vec<_>>().await,
        );
    }

    #[tokio::test]
    async fn wait() {
        let stream = stream::iter(vec![Err(17), Ok(19)]);
        let retry = StreamRetry::new(stream, |_| {
            RetryPolicy::WaitRetry::<()>(Duration::from_millis(10))
        })
        .try_collect()
        .into_future();
        assert_eq!(Ok(vec!((19, 2))), retry.await);
    }

    #[tokio::test]
    async fn propagate() {
        let stream = stream::iter(vec![Err(17u8), Ok(19u16)]);
        let retry = StreamRetry::new(stream, RetryPolicy::ForwardError);
        pin_mut!(retry);
        assert_eq!(Some(Err((17u8, 1))), retry.next().await,);
    }
}
