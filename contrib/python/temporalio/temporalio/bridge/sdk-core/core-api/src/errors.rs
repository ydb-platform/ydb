//! Error types exposed by public APIs

use temporal_sdk_core_protos::coresdk::activity_result::ActivityExecutionResult;

/// Errors thrown by [crate::Worker::validate]
#[derive(thiserror::Error, Debug)]
pub enum WorkerValidationError {
    /// The namespace provided to the worker does not exist on the server.
    #[error("Namespace {namespace} was not found or otherwise could not be described: {source:?}")]
    NamespaceDescribeError {
        source: tonic::Status,
        namespace: String,
    },
}

/// Errors thrown by [crate::Worker] polling methods
#[derive(thiserror::Error, Debug)]
pub enum PollError {
    /// [crate::Worker::shutdown] was called, and there are no more tasks to be handled from this
    /// poll function. Lang must call [crate::Worker::complete_workflow_activation],
    /// [crate::Worker::complete_activity_task], or
    /// [crate::Worker::complete_nexus_task] for any remaining tasks, and then may exit.
    #[error("Core is shut down and there are no more tasks of this kind")]
    ShutDown,
    /// Unhandled error when calling the temporal server. Core will attempt to retry any non-fatal
    /// errors, so lang should consider this fatal.
    #[error("Unhandled grpc error when polling: {0:?}")]
    TonicError(#[from] tonic::Status),
}

/// Errors thrown by [crate::Worker::complete_workflow_activation]
#[derive(thiserror::Error, Debug)]
#[allow(clippy::large_enum_variant)]
pub enum CompleteWfError {
    /// Lang SDK sent us a malformed workflow completion. This likely means a bug in the lang sdk.
    #[error("Lang SDK sent us a malformed workflow completion for run ({run_id}): {reason}")]
    MalformedWorkflowCompletion {
        /// Reason the completion was malformed
        reason: String,
        /// The run associated with the completion
        run_id: String,
    },
}

/// Errors thrown by [crate::Worker::complete_activity_task]
#[derive(thiserror::Error, Debug)]
pub enum CompleteActivityError {
    /// Lang SDK sent us a malformed activity completion. This likely means a bug in the lang sdk.
    #[error("Lang SDK sent us a malformed activity completion ({reason}): {completion:?}")]
    MalformedActivityCompletion {
        /// Reason the completion was malformed
        reason: String,
        /// The completion, which may not be included to avoid unnecessary copies.
        completion: Option<ActivityExecutionResult>,
    },
}

/// Errors thrown by [crate::Worker::complete_nexus_task]
#[derive(thiserror::Error, Debug)]
pub enum CompleteNexusError {
    /// Lang SDK sent us a malformed nexus completion. This likely means a bug in the lang sdk.
    #[error("Lang SDK sent us a malformed nexus completion: {reason}")]
    MalformedNexusCompletion {
        /// Reason the completion was malformed
        reason: String,
    },
    /// Nexus has not been enabled on this worker. If a user registers any Nexus handlers, the
    #[error("Nexus is not enabled on this worker")]
    NexusNotEnabled,
}

/// Errors we can encounter during workflow processing which we may treat as either WFT failures
/// or whole-workflow failures depending on user preference.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum WorkflowErrorType {
    /// A nondeterminism error
    Nondeterminism,
}
