//! Runtime protocol and execution model types shared by workflow code and native hosts.

use crate::{
    WorkflowCancellationError,
    runtime::types::ContinueAsNewRequest,
    workflow_context::{
        ChildWfCommon, NexusUnblockData, PendingChildWorkflow, StartedNexusOperation,
    },
};
use temporalio_common_wasm::{
    WorkflowDefinition,
    data_converters::PayloadConversionError,
    error::{
        ActivityExecutionError, ApplicationFailure, ChildWorkflowExecutionError,
        ChildWorkflowStartError, WorkflowSignalError,
    },
    protos::{
        coresdk::{
            activity_result::ActivityResolution,
            child_workflow::ChildWorkflowResult,
            nexus::NexusOperationResult,
            workflow_activation::{
                resolve_child_workflow_execution_start::Status as ChildWorkflowStartStatus,
                resolve_nexus_operation_start,
            },
        },
        temporal::api::failure::v1::Failure,
    },
};

#[derive(Debug)]
pub enum UnblockEvent {
    Timer(u32, TimerResult),
    Activity(u32, Box<ActivityResolution>),
    WorkflowStart(u32, Box<ChildWorkflowStartStatus>),
    WorkflowComplete(u32, Box<ChildWorkflowResult>),
    SignalExternal(u32, Option<Failure>),
    CancelExternal(u32, Option<Failure>),
    NexusOperationStart(u32, Box<resolve_nexus_operation_start::Status>),
    NexusOperationComplete(u32, Box<NexusOperationResult>),
}

/// Result of awaiting on a timer
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum TimerResult {
    /// The timer was cancelled
    Cancelled,
    /// The timer elapsed and fired
    Fired,
}

/// Successful result of sending a signal to an external workflow
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SignalExternalOk;
/// Result of awaiting on sending a signal to an external workflow
pub type SignalExternalWfResult = Result<SignalExternalOk, Failure>;

/// Successful result of sending a cancel request to an external workflow
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CancelExternalOk;
/// Result of awaiting on sending a cancel request to an external workflow
pub type CancelExternalWfResult = Result<CancelExternalOk, Failure>;

pub(crate) trait Unblockable {
    type OtherDat;

    fn unblock(ue: UnblockEvent, od: Self::OtherDat) -> Self;
}

impl Unblockable for TimerResult {
    type OtherDat = ();

    fn unblock(ue: UnblockEvent, _: Self::OtherDat) -> Self {
        match ue {
            UnblockEvent::Timer(_, result) => result,
            _ => panic!("Invalid unblock event for timer"),
        }
    }
}

impl Unblockable for ActivityResolution {
    type OtherDat = ();

    fn unblock(ue: UnblockEvent, _: Self::OtherDat) -> Self {
        match ue {
            UnblockEvent::Activity(_, result) => *result,
            _ => panic!("Invalid unblock event for activity"),
        }
    }
}

impl<WD: WorkflowDefinition> Unblockable for PendingChildWorkflow<WD> {
    type OtherDat = ChildWfCommon;

    fn unblock(ue: UnblockEvent, od: Self::OtherDat) -> Self {
        match ue {
            UnblockEvent::WorkflowStart(_, result) => Self {
                status: *result,
                common: od,
                _phantom: std::marker::PhantomData,
            },
            _ => panic!("Invalid unblock event for child workflow start"),
        }
    }
}

impl Unblockable for ChildWorkflowResult {
    type OtherDat = ();

    fn unblock(ue: UnblockEvent, _: Self::OtherDat) -> Self {
        match ue {
            UnblockEvent::WorkflowComplete(_, result) => *result,
            _ => panic!("Invalid unblock event for child workflow complete"),
        }
    }
}

impl Unblockable for SignalExternalWfResult {
    type OtherDat = ();

    fn unblock(ue: UnblockEvent, _: Self::OtherDat) -> Self {
        match ue {
            UnblockEvent::SignalExternal(_, maybefail) => {
                maybefail.map_or(Ok(SignalExternalOk), Err)
            }
            _ => panic!("Invalid unblock event for signal external workflow result"),
        }
    }
}

impl Unblockable for CancelExternalWfResult {
    type OtherDat = ();

    fn unblock(ue: UnblockEvent, _: Self::OtherDat) -> Self {
        match ue {
            UnblockEvent::CancelExternal(_, maybefail) => {
                maybefail.map_or(Ok(CancelExternalOk), Err)
            }
            _ => panic!("Invalid unblock event for cancel external workflow result"),
        }
    }
}

pub(crate) type NexusStartResult = Result<StartedNexusOperation, Failure>;

impl Unblockable for NexusStartResult {
    type OtherDat = NexusUnblockData;

    fn unblock(ue: UnblockEvent, od: Self::OtherDat) -> Self {
        let NexusUnblockData {
            result_future,
            schedule_seq,
            base_ctx,
        } = od;
        match ue {
            UnblockEvent::NexusOperationStart(_, result) => match *result {
                resolve_nexus_operation_start::Status::OperationToken(op_token) => {
                    Ok(StartedNexusOperation {
                        operation_token: Some(op_token),
                        result_future,
                        schedule_seq,
                        base_ctx,
                    })
                }
                resolve_nexus_operation_start::Status::StartedSync(_) => {
                    Ok(StartedNexusOperation {
                        operation_token: None,
                        result_future,
                        schedule_seq,
                        base_ctx,
                    })
                }
                resolve_nexus_operation_start::Status::Failed(f) => Err(f),
            },
            _ => panic!("Invalid unblock event for nexus operation"),
        }
    }
}

impl Unblockable for NexusOperationResult {
    type OtherDat = ();

    fn unblock(ue: UnblockEvent, _: Self::OtherDat) -> Self {
        match ue {
            UnblockEvent::NexusOperationComplete(_, result) => *result,
            _ => panic!("Invalid unblock event for nexus operation complete"),
        }
    }
}

#[derive(Debug, Clone)]
pub enum CancellableID {
    Timer(u32),
    Activity(u32),
    LocalActivity(u32),
    ChildWorkflow { seqnum: u32, reason: String },
    SignalExternalWorkflow(u32),
    NexusOp(u32),
}

impl CancellableID {
    pub(crate) fn with_reason(self, reason: String) -> Self {
        match self {
            CancellableID::ChildWorkflow { seqnum, .. } => {
                CancellableID::ChildWorkflow { seqnum, reason }
            }
            other => other,
        }
    }
}

/// The result of running a workflow.
pub type WorkflowResult<T> = Result<T, WorkflowTermination>;

/// Represents ways a workflow can terminate without producing a normal result.
///
/// Payload conversion errors returned by workflow operations propagated directly into `WorkflowTermination`, such as with `?`, will fail
/// the current Workflow Task so it can be retried.
///
/// Wrap an error in an [`ApplicationFailure`] to explicitly fail the Workflow Execution.
#[derive(Debug, thiserror::Error)]
pub enum WorkflowTermination {
    #[error("Workflow cancelled")]
    Cancelled,
    #[error("Workflow evicted from cache")]
    Evicted,
    #[error("Continue as new")]
    ContinueAsNew(Box<ContinueAsNewRequest>),
    #[error("Workflow failed: {0}")]
    Failed(#[source] temporalio_common_wasm::error::OutgoingWorkflowError),
}

impl WorkflowTermination {
    pub fn continue_as_new(can: ContinueAsNewRequest) -> Self {
        Self::ContinueAsNew(Box::new(can))
    }

    /// Construct a [`WorkflowTermination::Failed`] from an [`ApplicationFailure`].
    pub fn failed_application(err: ApplicationFailure) -> Self {
        Self::Failed(err.into())
    }
}

impl From<WorkflowCancellationError> for WorkflowTermination {
    fn from(_value: WorkflowCancellationError) -> Self {
        Self::Cancelled
    }
}

impl From<ApplicationFailure> for WorkflowTermination {
    fn from(value: ApplicationFailure) -> Self {
        Self::Failed(value.into())
    }
}

impl From<PayloadConversionError> for WorkflowTermination {
    fn from(value: PayloadConversionError) -> Self {
        Self::Failed(value.into())
    }
}

impl From<ActivityExecutionError> for WorkflowTermination {
    fn from(value: ActivityExecutionError) -> Self {
        Self::Failed(value.into())
    }
}

impl From<ChildWorkflowExecutionError> for WorkflowTermination {
    fn from(value: ChildWorkflowExecutionError) -> Self {
        Self::Failed(value.into())
    }
}

impl From<WorkflowSignalError> for WorkflowTermination {
    fn from(value: WorkflowSignalError) -> Self {
        Self::Failed(value.into())
    }
}

impl From<ChildWorkflowStartError> for WorkflowTermination {
    fn from(value: ChildWorkflowStartError) -> Self {
        Self::Failed(value.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;
    use temporalio_common_wasm::error::OutgoingWorkflowError;

    fn conversion_error() -> PayloadConversionError {
        PayloadConversionError::EncodingError(std::io::Error::other("test conversion error").into())
    }

    #[rstest]
    #[case::payload(conversion_error())]
    #[case::activity(ActivityExecutionError::Serialization(conversion_error()))]
    #[case::child_start(ChildWorkflowStartError::Serialization(conversion_error()))]
    #[case::child_execution(ChildWorkflowExecutionError::Serialization(conversion_error()))]
    #[case::signal(WorkflowSignalError::Serialization(conversion_error()))]
    fn conversion_error_is_preserved_in_workflow_termination<T: Into<WorkflowTermination>>(
        #[case] error: T,
    ) {
        let termination = error.into();
        let WorkflowTermination::Failed(OutgoingWorkflowError::PayloadConversion(err)) =
            termination
        else {
            panic!("expected a payload conversion failure");
        };
        assert_eq!(err.to_string(), "Encoding error: test conversion error");
    }

    #[test]
    fn explicitly_wrapped_conversion_error_remains_an_application_failure() {
        let termination = WorkflowTermination::from(ApplicationFailure::new(conversion_error()));

        assert!(matches!(
            termination,
            WorkflowTermination::Failed(OutgoingWorkflowError::Application(_))
        ));
    }
}
