use crate::runtime::SdkWakeGuard;
use futures_util::{FutureExt, future::FusedFuture};
use std::{
    cell::{Cell, RefCell},
    collections::BTreeMap,
    future,
    rc::{Rc, Weak},
    task::{Poll, Waker},
};

type CancellationCallback = Rc<dyn Fn(Option<String>)>;

#[derive(derive_more::Debug, Default)]
struct WorkflowCancellationState {
    cancelled: Cell<bool>,
    reason: RefCell<Option<String>>,
    wakers: RefCell<Vec<Waker>>,
    next_callback_id: Cell<u64>,
    #[debug(skip)]
    callbacks: RefCell<BTreeMap<u64, CancellationCallback>>,
}

impl WorkflowCancellationState {
    fn cancel(&self, reason: Option<String>) {
        if self.cancelled.replace(true) {
            return;
        }
        *self.reason.borrow_mut() = reason.clone();

        let _guard = SdkWakeGuard::new();
        for waker in self.wakers.borrow_mut().drain(..) {
            waker.wake();
        }
        let callbacks = std::mem::take(&mut *self.callbacks.borrow_mut());
        for callback in callbacks.into_values() {
            callback(reason.clone());
        }
    }
}

/// A deterministic cancellation token for workflow operations.
///
/// Tokens created with [`WorkflowCancellationToken::new`] are detached from workflow
/// cancellation. Use [`WorkflowCancellationToken::child_token`] to create a token that is
/// cancelled when its parent is cancelled.
#[derive(Clone, Debug)]
pub struct WorkflowCancellationToken {
    inner: Rc<WorkflowCancellationState>,
}

impl Default for WorkflowCancellationToken {
    fn default() -> Self {
        Self::new()
    }
}

impl WorkflowCancellationToken {
    /// Create a detached cancellation token.
    pub fn new() -> Self {
        Self {
            inner: Default::default(),
        }
    }

    /// Create a token that is cancelled when this token is cancelled.
    pub fn child_token(&self) -> Self {
        let child = Self::new();
        let weak_child = Rc::downgrade(&child.inner);
        self.register(move |reason| {
            if let Some(child) = weak_child.upgrade() {
                child.cancel(reason);
            }
        });
        child
    }

    /// Cancel this token without a reason.
    pub fn cancel(&self) {
        self.inner.cancel(None);
    }

    /// Cancel this token with a reason.
    pub fn cancel_with_reason(&self, reason: impl Into<String>) {
        self.inner.cancel(Some(reason.into()));
    }

    /// Return whether this token has been cancelled.
    pub fn is_cancelled(&self) -> bool {
        self.inner.cancelled.get()
    }

    /// Return the first cancellation reason, if one was provided.
    pub fn reason(&self) -> Option<String> {
        self.inner.reason.borrow().clone()
    }

    /// Return a future that resolves when this token is cancelled.
    pub fn cancelled(&self) -> impl FusedFuture<Output = ()> + '_ {
        future::poll_fn(move |cx| {
            if self.is_cancelled() {
                Poll::Ready(())
            } else {
                self.inner.wakers.borrow_mut().push(cx.waker().clone());
                Poll::Pending
            }
        })
        .fuse()
    }

    pub(crate) fn register(
        &self,
        callback: impl Fn(Option<String>) + 'static,
    ) -> WorkflowCancellationRegistration {
        if self.is_cancelled() {
            callback(self.reason());
            return WorkflowCancellationRegistration::default();
        }

        let id = self.inner.next_callback_id.get();
        self.inner.next_callback_id.set(id + 1);
        self.inner
            .callbacks
            .borrow_mut()
            .insert(id, Rc::new(callback));

        WorkflowCancellationRegistration {
            token: Rc::downgrade(&self.inner),
            callback_id: Some(id),
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct WorkflowCancellationRegistration {
    token: Weak<WorkflowCancellationState>,
    callback_id: Option<u64>,
}

impl WorkflowCancellationRegistration {
    pub(crate) fn unregister(&mut self) {
        let Some(callback_id) = self.callback_id.take() else {
            return;
        };
        if let Some(token) = self.token.upgrade() {
            token.callbacks.borrow_mut().remove(&callback_id);
        }
    }
}

/// Returned when a cancellable workflow wait is cancelled.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[error("Workflow wait cancelled")]
pub struct WorkflowCancellationError {
    reason: Option<String>,
}

impl WorkflowCancellationError {
    pub(crate) fn new(reason: Option<String>) -> Self {
        Self { reason }
    }

    /// Return the cancellation reason, if one was provided.
    pub fn reason(&self) -> Option<&str> {
        self.reason.as_deref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn child_cancellation_is_downward_and_first_reason_wins() {
        let parent = WorkflowCancellationToken::new();
        let child = parent.child_token();

        child.cancel_with_reason("child");
        parent.cancel_with_reason("parent");

        assert_eq!(child.reason().as_deref(), Some("child"));
        assert_eq!(parent.reason().as_deref(), Some("parent"));
    }

    #[test]
    fn child_inherits_reason() {
        let parent = WorkflowCancellationToken::new();
        let child = parent.child_token();

        parent.cancel_with_reason("parent");

        assert_eq!(child.reason().as_deref(), Some("parent"));
        assert_eq!(parent.reason().as_deref(), Some("parent"));
    }
    #[test]
    fn parent_cancellation_ignores_dropped_child_with_callback() {
        let parent = WorkflowCancellationToken::new();
        let callback_called = Rc::new(Cell::new(false));
        let child = parent.child_token();
        let callback_called_ref = callback_called.clone();
        child.register(move |_| callback_called_ref.set(true));
        drop(child);

        parent.cancel_with_reason("parent");

        assert!(parent.is_cancelled());
        assert_eq!(parent.reason().as_deref(), Some("parent"));
        assert!(!callback_called.get());
    }

    #[test]
    fn detached_token_does_not_follow_an_unrelated_token() {
        let root = WorkflowCancellationToken::new();
        let detached = WorkflowCancellationToken::new();

        root.cancel();

        assert!(root.is_cancelled());
        assert!(!detached.is_cancelled());
    }
}
