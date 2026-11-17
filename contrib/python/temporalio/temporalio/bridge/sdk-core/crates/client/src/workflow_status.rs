use temporalio_common::protos::temporal::api::enums::v1::WorkflowExecutionStatus as ProtoWorkflowExecutionStatus;

/// The lifecycle status of a workflow execution.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum WorkflowExecutionStatus {
    /// No status was specified.
    Unspecified,
    /// The workflow is running.
    Running,
    /// The workflow completed successfully.
    Completed,
    /// The workflow failed.
    Failed,
    /// The workflow was canceled.
    Canceled,
    /// The workflow was terminated.
    Terminated,
    /// The workflow continued as new.
    ContinuedAsNew,
    /// The workflow timed out.
    TimedOut,
    /// The workflow is paused.
    Paused,
    /// A status introduced by a newer server or API version.
    Unknown,
}

impl WorkflowExecutionStatus {
    pub(crate) fn from_raw(value: i32) -> Self {
        match ProtoWorkflowExecutionStatus::try_from(value) {
            Ok(ProtoWorkflowExecutionStatus::Unspecified) => Self::Unspecified,
            Ok(ProtoWorkflowExecutionStatus::Running) => Self::Running,
            Ok(ProtoWorkflowExecutionStatus::Completed) => Self::Completed,
            Ok(ProtoWorkflowExecutionStatus::Failed) => Self::Failed,
            Ok(ProtoWorkflowExecutionStatus::Canceled) => Self::Canceled,
            Ok(ProtoWorkflowExecutionStatus::Terminated) => Self::Terminated,
            Ok(ProtoWorkflowExecutionStatus::ContinuedAsNew) => Self::ContinuedAsNew,
            Ok(ProtoWorkflowExecutionStatus::TimedOut) => Self::TimedOut,
            Ok(ProtoWorkflowExecutionStatus::Paused) => Self::Paused,
            Err(_) => Self::Unknown,
        }
    }
}

impl From<ProtoWorkflowExecutionStatus> for WorkflowExecutionStatus {
    fn from(value: ProtoWorkflowExecutionStatus) -> Self {
        Self::from_raw(value as i32)
    }
}
