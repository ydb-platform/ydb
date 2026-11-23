// Take a look at the license at the top of the repository in the LICENSE file.

use std::{pin::Pin, task::ready};

use futures_core::{
    Future,
    stream::Stream,
    task::{Context, Poll},
};

use crate::{SocketConnection, SocketListener, prelude::SocketListenerExt};
use glib::{Error, Object, prelude::*};

pub struct Incoming {
    listener: SocketListener,
    fut: Option<Pin<Box<dyn Future<Output = Result<(SocketConnection, Option<Object>), Error>>>>>,
}

impl Stream for Incoming {
    type Item = Result<(SocketConnection, Option<Object>), Error>;

    fn poll_next(mut self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.fut.is_none() {
            self.fut = Some(Box::pin(self.listener.accept_future()));
        }

        let fut = self.fut.as_mut().unwrap();
        let res = ready!(Pin::new(fut).poll(ctx));
        self.fut.take();

        Poll::Ready(Some(res))
    }
}

pub trait SocketListenerExtManual: SocketListenerExt {
    // rustdoc-stripper-ignore-next
    /// Returns a stream of incoming connections
    ///
    /// Iterating over this stream is equivalent to calling [`SocketListenerExt::accept_future`] in a
    /// loop. The stream of connections is infinite, i.e awaiting the next
    /// connection will never result in [`None`].
    fn incoming(
        &self,
    ) -> Pin<Box<dyn Stream<Item = Result<(SocketConnection, Option<Object>), Error>>>>;
}

impl<O: IsA<SocketListener>> SocketListenerExtManual for O {
    fn incoming(
        &self,
    ) -> Pin<Box<dyn Stream<Item = Result<(SocketConnection, Option<Object>), Error>>>> {
        Box::pin(Incoming {
            listener: self.as_ref().clone(),
            fut: None,
        })
    }
}
