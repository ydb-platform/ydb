use crate::{ErrorHandler, RetryPolicy};
use futures::{ready, TryFuture};
use pin_project_lite::pin_project;
use std::{
    future::Future,
    marker::Unpin,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::time;

/// A factory trait used to create futures.
///
/// We need a factory for the retry logic because when (and if) a future returns an error, its
/// internal state is undefined and we can't poll on it anymore. Hence we need to create a new one.
///
/// By the way, this trait is implemented for any closure that returns a `Future`, so you don't
/// have to write your own type and implement it to handle some simple cases.
pub trait FutureFactory {
    /// An future type that is created by the `new` method.
    type FutureItem: TryFuture;

    /// Creates a new future. We don't need the factory to be immutable so we pass `self` as a
    /// mutable reference.
    fn new(&mut self) -> Self::FutureItem;
}

impl<T, F> FutureFactory for T
where
    T: Unpin + FnMut() -> F,
    F: TryFuture,
{
    type FutureItem = F;

    #[allow(clippy::new_ret_no_self)]
    fn new(&mut self) -> F {
        (self)()
    }
}

pin_project! {
    /// A future that transparently launches an underlying future (created by a provided factory each
    /// time) as many times as needed to get things done.
    ///
    /// It is useful fot situations when you need to make several attempts, e.g. for establishing
    /// connections, RPC calls.
    ///
    /// There is also a type to handle `Stream` errors: [`StreamRetry`](struct.StreamRetry.html).#[pin_project]
    pub struct FutureRetry<F, R>
    where
        F: FutureFactory,
    {
        factory: F,
        error_action: R,
        attempt: usize,
        #[pin]
        state: RetryState<F::FutureItem>,
    }
}

pin_project! {
    #[project = RetryStateProj]
    enum RetryState<F> {
        NotStarted,
        WaitingForFuture { #[pin] future: F },
        TimerActive { #[pin] delay: time::Sleep },
    }
}

impl<F: FutureFactory, R> FutureRetry<F, R> {
    /// Creates a `FutureRetry` using a provided factory and an object of `ErrorHandler` type that
    /// decides on a retry-policy depending on an encountered error.
    ///
    /// Please refer to the `tcp-client` example in the `examples` folder to have a look at a
    /// possible usage.
    ///
    /// # Arguments
    ///
    /// * `factory`: a factory that creates futures,
    /// * `error_action`: a type that handles an error and decides which route to take: simply
    ///                   try again, wait and then try, or give up (on a critical error for
    ///                   exapmle).
    pub fn new(factory: F, error_action: R) -> Self {
        Self {
            factory,
            error_action,
            state: RetryState::NotStarted,
            attempt: 1,
        }
    }
}

impl<F: FutureFactory, R> Future for FutureRetry<F, R>
where
    R: ErrorHandler<<F::FutureItem as TryFuture>::Error>,
{
    type Output =
        Result<(<<F as FutureFactory>::FutureItem as TryFuture>::Ok, usize), (R::OutError, usize)>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        loop {
            let this = self.as_mut().project();
            let attempt = *this.attempt;
            let new_state = match this.state.project() {
                RetryStateProj::NotStarted => RetryState::WaitingForFuture {
                    future: this.factory.new(),
                },
                RetryStateProj::TimerActive { delay } => {
                    ready!(delay.poll(cx));
                    RetryState::WaitingForFuture {
                        future: this.factory.new(),
                    }
                }
                RetryStateProj::WaitingForFuture { future } => match ready!(future.try_poll(cx)) {
                    Ok(x) => {
                        this.error_action.ok(attempt);
                        *this.attempt = 1;
                        return Poll::Ready(Ok((x, attempt)));
                    }
                    Err(e) => {
                        *this.attempt += 1;
                        match this.error_action.handle(attempt, e) {
                            RetryPolicy::ForwardError(e) => return Poll::Ready(Err((e, attempt))),
                            RetryPolicy::Repeat => RetryState::WaitingForFuture {
                                future: this.factory.new(),
                            },
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
mod tests {
    use super::*;
    use futures::{
        future::{err, ok},
        TryFutureExt,
    };
    use std::time::Duration;

    /// Just a help type for the tests.
    struct FutureIterator<F>(F);

    impl<I, F> FutureFactory for FutureIterator<I>
    where
        I: Unpin + Iterator<Item = F>,
        F: TryFuture,
    {
        type FutureItem = F;

        /// # Warning
        ///
        /// Will panic if there is no *next* future.
        fn new(&mut self) -> Self::FutureItem {
            self.0.next().expect("No more futures!")
        }
    }

    #[tokio::test]
    async fn naive() {
        let f = FutureRetry::new(|| ok::<_, u8>(1u8), |_| RetryPolicy::Repeat::<u8>);
        assert_eq!(Ok((1u8, 1)), f.await);
    }

    #[tokio::test]
    async fn naive_error_forward() {
        let f = FutureRetry::new(|| err::<u8, _>(1u8), RetryPolicy::ForwardError);
        assert_eq!(Err((1u8, 1)), f.await);
    }

    #[tokio::test]
    async fn more_complicated_wait() {
        let f = FutureRetry::new(FutureIterator(vec![err(2u8), ok(3u8)].into_iter()), |_| {
            RetryPolicy::WaitRetry::<u8>(Duration::from_millis(10))
        })
        .into_future();
        assert_eq!(Ok((3, 2)), f.await);
    }

    #[tokio::test]
    async fn more_complicated_repeat() {
        let f = FutureRetry::new(FutureIterator(vec![err(2u8), ok(3u8)].into_iter()), |_| {
            RetryPolicy::Repeat::<u8>
        });
        assert_eq!(Ok((3u8, 2)), f.await);
    }

    #[tokio::test]
    async fn more_complicated_forward() {
        let f = FutureRetry::new(
            FutureIterator(vec![err(2u8), ok(3u8)].into_iter()),
            RetryPolicy::ForwardError,
        );
        assert_eq!(Err((2u8, 1)), f.await);
    }
}
