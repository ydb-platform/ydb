//! Functionality related to defining and interacting with workflows
//!
//! This module contains traits and types for implementing workflows using the
//! `#[workflow]` and `#[workflow_methods]` macros.
//!
//! Example usage:
//! ```
//! use temporalio_macros::{workflow, workflow_methods};
//! use temporalio_workflow::{
//!     SyncWorkflowContext, WorkflowContext, WorkflowContextView, WorkflowResult,
//! };
//!
//! #[workflow]
//! pub struct MyWorkflow {
//!     counter: u32,
//! }
//!
//! #[workflow_methods]
//! impl MyWorkflow {
//!     #[init]
//!     pub fn new(ctx: &WorkflowContextView, input: String) -> Self {
//!         Self { counter: 0 }
//!     }
//!
//!     // Async run method uses ctx.state() for reading
//!     #[run]
//!     pub async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<String> {
//!         let counter = ctx.state(|s| s.counter);
//!         Ok(format!("Done with counter: {}", counter))
//!     }
//!
//!     // Sync signals use &mut self for direct mutations
//!     #[signal]
//!     pub fn increment(&mut self, ctx: &mut SyncWorkflowContext<Self>, amount: u32) {
//!         self.counter += amount;
//!     }
//!
//!     // Queries use &self with read-only context
//!     #[query]
//!     pub fn get_counter(&self, ctx: &WorkflowContextView) -> u32 {
//!         self.counter
//!     }
//! }
//! ```

/// Deterministic `select!` for use in Temporal workflows.
///
/// Polls branches in declaration order (top to bottom), ensuring deterministic
/// behavior across workflow replays. Delegates to [`futures_util::select_biased!`].
///
/// All workflow futures (timers, activities, child workflows, etc.) implement
/// `FusedFuture`, so they can be stored in variables and passed to `select!`
/// without needing `.fuse()`.
///
/// # Example
///
/// ```ignore
/// use temporalio_sdk::workflows::select;
/// use temporalio_sdk::WorkflowContext;
/// use std::time::Duration;
///
/// # async fn hidden(ctx: &mut WorkflowContext<()>) {
/// select! {
///     _ = ctx.timer(Duration::from_secs(60)) => { /* timer fired */ }
///     reason = ctx.cancelled() => { /* cancelled */ }
/// };
/// # }
/// ```
#[doc(inline)]
pub use crate::__temporal_select as select;

/// Deterministic `join!` for use in Temporal workflows.
///
/// Polls all futures concurrently to completion in declaration order,
/// ensuring deterministic behavior across workflow replays. Delegates
/// to [`futures_util::join!`].
///
/// # Example
///
/// ```ignore
/// use temporalio_sdk::workflows::join;
///
/// # async fn hidden() {
/// let future_a = async { 1 };
/// let future_b = async { 2 };
/// let (a, b) = join!(future_a, future_b);
/// # }
/// ```
#[doc(inline)]
pub use crate::__temporal_join as join;

use crate::runtime::SdkGuardedFuture;
use futures_util::FutureExt;

pub use crate::runtime::entry::{
    ExecutableAsyncSignal, ExecutableAsyncUpdate, ExecutableQuery, ExecutableSyncSignal,
    ExecutableSyncUpdate, WorkflowError, WorkflowImplementation, serialize_result,
};

/// Deterministic `join_all` for use in Temporal workflows.
///
/// Polls a collection of futures concurrently to completion in declaration order,
/// returning a `Vec` of their results.
///
/// # Example
///
/// ```ignore
/// use temporalio_sdk::workflows::join_all;
/// use temporalio_sdk::WorkflowContext;
/// use std::time::Duration;
///
/// # async fn hidden(ctx: &mut WorkflowContext<()>) {
/// let timers = vec![
///     ctx.timer(Duration::from_secs(1)),
///     ctx.timer(Duration::from_secs(2)),
/// ];
/// let results = join_all(timers).await;
/// # }
/// ```
pub fn join_all<I>(iter: I) -> JoinAll<I::Item>
where
    I: IntoIterator,
    I::Item: std::future::Future,
{
    JoinAll(SdkGuardedFuture(futures_util::future::join_all(iter)))
}

/// Future returned by [`join_all`].
pub struct JoinAll<F: std::future::Future>(SdkGuardedFuture<futures_util::future::JoinAll<F>>);

impl<F: std::future::Future> std::future::Future for JoinAll<F> {
    type Output = Vec<F::Output>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        self.0.poll_unpin(cx)
    }
}
