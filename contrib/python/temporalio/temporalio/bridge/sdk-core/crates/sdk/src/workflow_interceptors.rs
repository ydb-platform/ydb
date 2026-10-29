//! APIs for intercepting calls into workflow code and commands issued by workflow code.
//!
//! Implement [`WorkflowInterceptor`] to wrap selected inbound or outbound operations, then create
//! a [`Vec`] of [`WorkflowInterceptorConstructor`] to register with
//! [`crate::WorkerOptions::register_workflow_interceptors`].
//!
//! See [`temporalio_workflow::workflow_interceptors`] for the full guide to call chaining,
//! interceptor lifecycle and ordering, async polling, determinism, and an implementation example.

pub use temporalio_workflow::workflow_interceptors::*;
