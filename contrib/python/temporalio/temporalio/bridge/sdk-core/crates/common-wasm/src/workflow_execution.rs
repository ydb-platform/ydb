use crate::protos::temporal::api::common::v1::WorkflowExecution as ProtoWorkflowExecution;

/// Identifies a workflow execution by workflow and run ID.
#[derive(Clone, Debug, Default, PartialEq)]
#[non_exhaustive]
pub struct WorkflowExecution {
    raw: ProtoWorkflowExecution,
}

#[bon::bon]
impl WorkflowExecution {
    /// Create a workflow execution identifier.
    #[builder(on(String, into), state_mod(vis = "pub"))]
    pub fn new(workflow_id: String, run_id: String) -> Self {
        let mut execution = Self::default();
        execution.set_workflow_id(workflow_id).set_run_id(run_id);
        execution
    }

    /// The workflow ID.
    pub fn workflow_id(&self) -> &str {
        &self.raw.workflow_id
    }

    /// Set the workflow ID.
    pub fn set_workflow_id(&mut self, workflow_id: impl Into<String>) -> &mut Self {
        self.raw.workflow_id = workflow_id.into();
        self
    }

    /// The run ID.
    pub fn run_id(&self) -> &str {
        &self.raw.run_id
    }

    /// Set the run ID.
    pub fn set_run_id(&mut self, run_id: impl Into<String>) -> &mut Self {
        self.raw.run_id = run_id.into();
        self
    }

    /// Access the underlying workflow execution protobuf.
    pub fn raw(&self) -> &ProtoWorkflowExecution {
        &self.raw
    }

    /// Consume this wrapper and return the underlying workflow execution protobuf.
    pub fn into_raw(self) -> ProtoWorkflowExecution {
        self.raw
    }
}

impl From<ProtoWorkflowExecution> for WorkflowExecution {
    fn from(value: ProtoWorkflowExecution) -> Self {
        Self { raw: value }
    }
}

impl From<WorkflowExecution> for ProtoWorkflowExecution {
    fn from(value: WorkflowExecution) -> Self {
        value.raw
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builder_and_proto_round_trip() {
        let execution = WorkflowExecution::builder()
            .workflow_id("workflow-id")
            .run_id("run-id")
            .build();

        assert_eq!(execution.workflow_id(), "workflow-id");
        assert_eq!(execution.run_id(), "run-id");
        assert_eq!(execution.raw().workflow_id, "workflow-id");
        assert_eq!(execution.raw().run_id, "run-id");

        assert_eq!(
            WorkflowExecution::from(ProtoWorkflowExecution::from(execution.clone())),
            execution
        );
    }

    #[test]
    fn setters_update_raw_proto() {
        let mut execution = WorkflowExecution::default();
        execution
            .set_workflow_id("workflow-id")
            .set_run_id("run-id");

        assert_eq!(execution.workflow_id(), "workflow-id");
        assert_eq!(execution.run_id(), "run-id");
        assert_eq!(execution.raw().workflow_id, "workflow-id");
        assert_eq!(execution.raw().run_id, "run-id");
    }
}
