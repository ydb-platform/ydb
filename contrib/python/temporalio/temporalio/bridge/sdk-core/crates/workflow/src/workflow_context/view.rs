use std::time::{Duration, SystemTime};

use temporalio_common_wasm::{
    Memo, Priority, RetryPolicy, WorkflowExecution,
    data_converters::{PayloadConverter, SerializationContextData},
    protos::coresdk::{
        common::NamespacedWorkflowExecution, workflow_activation::InitializeWorkflow,
    },
    search_attributes::SearchAttributes,
};

/// Read-only view of workflow context for use in init and query handlers.
///
/// This provides access to workflow information but cannot issue commands.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct WorkflowContextView {
    raw: InitializeWorkflow,
    namespace: String,
    task_queue: String,
    run_id: String,
    payload_converter: PayloadConverter,
}

impl WorkflowContextView {
    /// Create a new view from workflow initialization data.
    pub(crate) fn new(
        namespace: String,
        task_queue: String,
        run_id: String,
        raw: InitializeWorkflow,
        payload_converter: PayloadConverter,
    ) -> Self {
        Self {
            raw,
            namespace,
            task_queue,
            run_id,
            payload_converter,
        }
    }

    pub(super) fn into_parts(self) -> (String, String, String, InitializeWorkflow) {
        (self.namespace, self.task_queue, self.run_id, self.raw)
    }

    /// Returns the workflow's unique identifier.
    pub fn workflow_id(&self) -> &str {
        &self.raw.workflow_id
    }

    /// Returns the run ID of this workflow execution.
    pub fn run_id(&self) -> &str {
        &self.run_id
    }

    /// Returns the workflow type name.
    pub fn workflow_type(&self) -> &str {
        &self.raw.workflow_type
    }

    /// Returns the task queue this workflow is executing on.
    pub fn task_queue(&self) -> &str {
        &self.task_queue
    }

    /// Returns the namespace this workflow is executing in.
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// Returns the current attempt number, starting from one.
    pub fn attempt(&self) -> u32 {
        self.raw.attempt as u32
    }

    /// Returns the run ID of the first execution in the chain.
    pub fn first_execution_run_id(&self) -> &str {
        &self.raw.first_execution_run_id
    }

    /// Returns the run ID of the previous execution when this is a continuation.
    pub fn continued_from_run_id(&self) -> Option<&str> {
        (!self.raw.continued_from_execution_run_id.is_empty())
            .then_some(self.raw.continued_from_execution_run_id.as_str())
    }

    /// Returns when the workflow execution started.
    pub fn start_time(&self) -> Option<SystemTime> {
        self.raw.start_time.and_then(|time| time.try_into().ok())
    }

    /// Returns the total workflow execution timeout, including retries and continue-as-new.
    pub fn execution_timeout(&self) -> Option<Duration> {
        self.raw
            .workflow_execution_timeout
            .and_then(|timeout| timeout.try_into().ok())
    }

    /// Returns the timeout of a single workflow run.
    pub fn run_timeout(&self) -> Option<Duration> {
        self.raw
            .workflow_run_timeout
            .and_then(|timeout| timeout.try_into().ok())
    }

    /// Returns the timeout of a single workflow task.
    pub fn task_timeout(&self) -> Option<Duration> {
        self.raw
            .workflow_task_timeout
            .and_then(|timeout| timeout.try_into().ok())
    }

    /// Returns information about the parent workflow when this is a child workflow.
    pub fn parent(&self) -> Option<NamespacedWorkflowInfo> {
        self.raw
            .parent_workflow_info
            .clone()
            .map(NamespacedWorkflowInfo::from_raw)
    }

    /// Returns information about the root workflow in the execution chain.
    pub fn root(&self) -> Option<WorkflowExecution> {
        self.raw.root_workflow.clone().map(Into::into)
    }

    /// Returns the workflow's retry policy.
    pub fn retry_policy(&self) -> Option<RetryPolicy> {
        self.raw.retry_policy.clone().map(Into::into)
    }

    /// Returns the cron schedule when this workflow runs on one.
    pub fn cron_schedule(&self) -> Option<&str> {
        (!self.raw.cron_schedule.is_empty()).then_some(self.raw.cron_schedule.as_str())
    }

    /// Returns priority and fairness configuration for this workflow execution.
    pub fn priority(&self) -> Priority {
        self.raw.priority.clone().unwrap_or_default().into()
    }

    /// Returns user-defined memo values.
    pub fn memo(&self) -> Memo {
        Memo::from_raw(
            self.raw.memo.clone(),
            self.payload_converter.clone(),
            SerializationContextData::Workflow,
        )
    }

    /// Returns initial search attributes as a typed collection.
    pub fn search_attributes(&self) -> Option<SearchAttributes> {
        self.raw
            .search_attributes
            .as_ref()
            .map(SearchAttributes::from_proto)
    }

    /// Accesses the underlying workflow initialization protobuf.
    pub fn raw(&self) -> &InitializeWorkflow {
        &self.raw
    }

    /// Consumes this view and returns the underlying workflow initialization protobuf.
    pub fn into_raw(self) -> InitializeWorkflow {
        self.raw
    }
}

/// Information about a parent workflow.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct NamespacedWorkflowInfo {
    raw: NamespacedWorkflowExecution,
}

impl NamespacedWorkflowInfo {
    fn from_raw(raw: NamespacedWorkflowExecution) -> Self {
        Self { raw }
    }

    /// Returns the parent workflow's unique identifier.
    pub fn workflow_id(&self) -> &str {
        &self.raw.workflow_id
    }

    /// Returns the parent workflow's run ID.
    pub fn run_id(&self) -> &str {
        &self.raw.run_id
    }

    /// Returns the parent workflow's namespace.
    pub fn namespace(&self) -> &str {
        &self.raw.namespace
    }

    /// Accesses the underlying parent workflow protobuf.
    pub fn raw(&self) -> &NamespacedWorkflowExecution {
        &self.raw
    }

    /// Consumes this wrapper and returns the underlying parent workflow protobuf.
    pub fn into_raw(self) -> NamespacedWorkflowExecution {
        self.raw
    }
}
