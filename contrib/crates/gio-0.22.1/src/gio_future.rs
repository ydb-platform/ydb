// Take a look at the license at the top of the repository in the LICENSE file.

use std::{
    future::Future,
    pin::{self, Pin},
};

use futures_channel::oneshot;
use futures_core::{
    FusedFuture,
    task::{Context, Poll},
};

use crate::{Cancellable, prelude::*};

pub struct GioFuture<F, O, T> {
    obj: O,
    schedule_operation: Option<F>,
    cancellable: Option<Cancellable>,
    receiver: Option<oneshot::Receiver<T>>,
}

pub struct GioFutureResult<T> {
    sender: oneshot::Sender<T>,
}

impl<T> GioFutureResult<T> {
    pub fn resolve(self, res: T) {
        let _ = self.sender.send(res);
    }
}

impl<F, O, T: 'static> GioFuture<F, O, T>
where
    O: Clone + 'static,
    F: FnOnce(&O, &Cancellable, GioFutureResult<T>) + 'static,
{
    pub fn new(obj: &O, schedule_operation: F) -> GioFuture<F, O, T> {
        GioFuture {
            obj: obj.clone(),
            schedule_operation: Some(schedule_operation),
            cancellable: Some(Cancellable::new()),
            receiver: None,
        }
    }
}

impl<F, O, T> Future for GioFuture<F, O, T>
where
    O: Clone + 'static,
    F: FnOnce(&O, &Cancellable, GioFutureResult<T>) + 'static,
{
    type Output = T;

    fn poll(mut self: pin::Pin<&mut Self>, ctx: &mut Context) -> Poll<T> {
        let GioFuture {
            ref obj,
            ref mut schedule_operation,
            ref mut cancellable,
            ref mut receiver,
            ..
        } = *self;

        if let Some(schedule_operation) = schedule_operation.take() {
            let main_context = glib::MainContext::ref_thread_default();
            assert!(
                main_context.is_owner(),
                "Spawning futures only allowed if the thread is owning the MainContext"
            );

            // Channel for sending back the GIO async operation
            // result to our future here.
            //
            // In theory, we could directly continue polling the
            // corresponding task from the GIO async operation
            // callback, however this would break at the very
            // least the g_main_current_source() API.
            let (send, recv) = oneshot::channel();

            schedule_operation(
                obj,
                cancellable.as_ref().unwrap(),
                GioFutureResult { sender: send },
            );

            *receiver = Some(recv);
        }

        // At this point we must have a receiver
        let res = {
            let receiver = receiver.as_mut().unwrap();
            Pin::new(receiver).poll(ctx)
        };

        match res {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(_)) => panic!("Async operation sender was unexpectedly closed"),
            Poll::Ready(Ok(v)) => {
                // Get rid of the reference to the cancellable and receiver
                let _ = cancellable.take();
                let _ = receiver.take();
                Poll::Ready(v)
            }
        }
    }
}

impl<F, O, T> FusedFuture for GioFuture<F, O, T>
where
    O: Clone + 'static,
    F: FnOnce(&O, &Cancellable, GioFutureResult<T>) + 'static,
{
    fn is_terminated(&self) -> bool {
        self.schedule_operation.is_none()
            && self
                .receiver
                .as_ref()
                .is_none_or(|receiver| receiver.is_terminated())
    }
}

impl<F, O, T> Drop for GioFuture<F, O, T> {
    fn drop(&mut self) {
        if let Some(cancellable) = self.cancellable.take() {
            cancellable.cancel();
        }
        let _ = self.receiver.take();
    }
}

impl<F, O, T> Unpin for GioFuture<F, O, T> {}
