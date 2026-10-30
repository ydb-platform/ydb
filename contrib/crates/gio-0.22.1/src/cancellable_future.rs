// Take a look at the license at the top of the repository in the LICENSE file.

use std::{
    fmt::{Debug, Display},
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use pin_project_lite::pin_project;

use crate::{Cancellable, CancelledHandlerId, IOErrorEnum, prelude::*};

// rustdoc-stripper-ignore-next
/// Indicator that the [`CancellableFuture`] was cancelled.
pub struct Cancelled;

pin_project! {
    // rustdoc-stripper-ignore-next
    /// A future which can be cancelled via [`Cancellable`].
    ///
    /// # Examples
    ///
    /// ```
    /// # use futures::FutureExt;
    /// # use gio::prelude::*;
    /// # use gio::CancellableFuture;
    /// let l = glib::MainLoop::new(None, false);
    /// let c = gio::Cancellable::new();
    ///
    /// l.context().spawn_local(CancellableFuture::new(async { 42 }, c.clone()).map(|_| ()));
    /// c.cancel();
    ///
    /// ```
    pub struct CancellableFuture<F> {
        #[pin]
        future: F,

        #[pin]
        waker_handler_cb: Option<CancelledHandlerId>,

        cancellable: Cancellable,
    }
}

impl<F> CancellableFuture<F> {
    // rustdoc-stripper-ignore-next
    /// Creates a new `CancellableFuture` using a [`Cancellable`].
    ///
    /// When [`cancel`](CancellableExt::cancel) is called, the future will complete
    /// immediately without making any further progress. In such a case, an error
    /// will be returned by this future (i.e., [`Cancelled`]).
    pub fn new(future: F, cancellable: Cancellable) -> Self {
        Self {
            future,
            waker_handler_cb: None,
            cancellable,
        }
    }

    // rustdoc-stripper-ignore-next
    /// Checks whether the future has been cancelled.
    ///
    /// This is a shortcut for `self.cancellable().is_cancelled()`
    ///
    /// Note that all this method indicates is whether [`cancel`](CancellableExt::cancel)
    /// was called. This means that it will return true even if:
    ///   * `cancel` was called after the future had completed.
    ///   * `cancel` was called while the future was being polled.
    #[inline]
    pub fn is_cancelled(&self) -> bool {
        self.cancellable.is_cancelled()
    }

    // rustdoc-stripper-ignore-next
    /// Returns the inner [`Cancellable`] associated during creation.
    #[inline]
    pub fn cancellable(&self) -> &Cancellable {
        &self.cancellable
    }
}

impl<F> Future for CancellableFuture<F>
where
    F: Future,
{
    type Output = Result<<F as Future>::Output, Cancelled>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.is_cancelled() {
            return Poll::Ready(Err(Cancelled));
        }

        let mut this = self.as_mut().project();

        match this.future.poll(cx) {
            Poll::Ready(out) => Poll::Ready(Ok(out)),

            Poll::Pending => {
                if let Some(prev_handler) = this.waker_handler_cb.take() {
                    this.cancellable.disconnect_cancelled(prev_handler);
                }

                let canceller_handler_id = this.cancellable.connect_cancelled({
                    let w = cx.waker().clone();
                    move |_| w.wake()
                });

                match canceller_handler_id {
                    Some(canceller_handler_id) => {
                        *this.waker_handler_cb = Some(canceller_handler_id);
                        Poll::Pending
                    }

                    None => Poll::Ready(Err(Cancelled)),
                }
            }
        }
    }
}

impl From<Cancelled> for glib::Error {
    fn from(_: Cancelled) -> Self {
        glib::Error::new(IOErrorEnum::Cancelled, "Task cancelled")
    }
}

impl std::error::Error for Cancelled {}

impl Debug for Cancelled {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Task cancelled")
    }
}

impl Display for Cancelled {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(self, f)
    }
}

#[cfg(test)]
mod tests {
    use futures_channel::oneshot;

    use super::{Cancellable, CancellableFuture, Cancelled};
    use crate::prelude::*;

    #[test]
    fn cancellable_future_ok() {
        let ctx = glib::MainContext::new();
        let c = Cancellable::new();
        let (tx, rx) = oneshot::channel();

        {
            ctx.spawn_local(async {
                let cancellable_future = CancellableFuture::new(async { 42 }, c);
                assert!(!cancellable_future.is_cancelled());

                let result = cancellable_future.await;
                assert!(matches!(result, Ok(42)));

                tx.send(()).unwrap();
            });
        }

        ctx.block_on(rx).unwrap()
    }

    #[test]
    fn cancellable_future_cancel() {
        let ctx = glib::MainContext::new();
        let c = Cancellable::new();
        let (tx, rx) = oneshot::channel();

        {
            let c = c.clone();
            ctx.spawn_local(async move {
                let cancellable_future = CancellableFuture::new(std::future::pending::<()>(), c);

                let result = cancellable_future.await;
                assert!(matches!(result, Err(Cancelled)));

                tx.send(()).unwrap();
            });
        }

        std::thread::spawn(move || c.cancel()).join().unwrap();

        ctx.block_on(rx).unwrap();
    }
}
