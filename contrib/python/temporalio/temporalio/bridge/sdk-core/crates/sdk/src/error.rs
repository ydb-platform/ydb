//! Shared SDK error re-exports.

pub use crate::workflow_registry::WorkflowRegistrationError;
use temporalio_client::PluginApplyError;
pub use temporalio_sdk_core::WorkerValidationError;

/// Errors that can occur while creating a worker.
///
/// **Experimental:** This API may change or be removed.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum WorkerCreateError {
    /// A plugin failed while configuring worker options.
    #[error(transparent)]
    Plugin(#[from] PluginApplyError),
    /// Worker initialization failed after plugin configuration completed.
    #[error("worker initialization failed: {0}")]
    Initialization(#[source] anyhow::Error),
}

/// Errors that can occur while running a worker.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum WorkerRunError {
    /// Worker validation failed before polling began.
    #[error("worker validation failed: {0}")]
    Validation(#[source] WorkerValidationError),
    /// An unrecoverable error occurred while running the worker.
    #[error("{message}")]
    Fatal {
        /// Worker operation that could not continue.
        message: String,
        /// Underlying cause.
        #[source]
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },
}

pub use temporalio_common::error::{
    ActivityExecutionError, ApplicationErrorCategory, ApplicationFailure,
    ChildWorkflowExecutionError, ChildWorkflowStartError, OutgoingActivityError, OutgoingError,
    OutgoingWorkflowError, RetryState, TimeoutType, WorkflowSignalError,
};
