//! Interceptors for high-level client operations.

use crate::{
    ActivityHeartbeatResponse, ActivityIdentifier, WorkflowCancelOptions, WorkflowCountOptions,
    WorkflowDescribeOptions, WorkflowFetchHistoryOptions, WorkflowQueryOptions,
    WorkflowSignalOptions, WorkflowStartError, WorkflowStartOptions, WorkflowStartUpdateOptions,
    WorkflowTerminateOptions,
    errors::{
        AsyncActivityError, ClientError, WorkflowInteractionError, WorkflowQueryError,
        WorkflowUpdateError,
    },
    schedules::{
        CreateScheduleOptions, ScheduleBackfill, ScheduleError, ScheduleOverlapPolicy,
        ScheduleUpdate,
    },
};
use futures_util::future::BoxFuture;
use std::{any::Any, sync::Arc};
use temporalio_common::{
    data_converters::{
        GenericPayloadConverter, PayloadConversionError, SerializationContext, TemporalSerializable,
    },
    protos::temporal::api::{
        common::v1::Payload,
        history::v1::HistoryEvent,
        schedule::v1::ScheduleListEntry,
        update::v1::Outcome,
        workflow::v1::WorkflowExecutionInfo,
        workflowservice::v1::{
            CountWorkflowExecutionsResponse, DescribeScheduleResponse,
            DescribeWorkflowExecutionResponse, QueryWorkflowResponse,
        },
    },
};

mod temporal_client_value {
    use super::*;

    pub trait Sealed {
        fn serialize_client_payloads(
            &self,
            context: &SerializationContext<'_>,
        ) -> Result<Vec<Payload>, PayloadConversionError>;
    }

    impl<T> Sealed for T
    where
        T: Any + TemporalSerializable + Send,
    {
        fn serialize_client_payloads(
            &self,
            context: &SerializationContext<'_>,
        ) -> Result<Vec<Payload>, PayloadConversionError> {
            context.converter.to_payloads(context, self)
        }
    }
}

/// Type-erased input carried through the client interceptor chain.
pub trait TemporalClientValue: Any + Send + temporal_client_value::Sealed {
    /// Access this value as [`Any`] for type-specific inspection.
    fn as_any(&self) -> &dyn Any;

    /// Access this value as mutable [`Any`] for type-specific mutation.
    fn as_any_mut(&mut self) -> &mut dyn Any;
}

impl<T> TemporalClientValue for T
where
    T: Any + TemporalSerializable + Send,
{
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

impl dyn TemporalClientValue {
    pub(crate) fn serialize_payloads(
        &self,
        context: &SerializationContext<'_>,
    ) -> Result<Vec<Payload>, PayloadConversionError> {
        temporal_client_value::Sealed::serialize_client_payloads(self, context)
    }
}

/// Provides access to the arguments carried by a client interceptor input.
pub trait HasArgs {
    /// Attempt to access the arguments as a concrete type.
    fn args_ref<T: Any>(&self) -> Option<&T>;

    /// Attempt to mutably access the arguments as a concrete type.
    fn args_mut<T: Any>(&mut self) -> Option<&mut T>;

    /// Replace the arguments with another serializable value.
    fn replace_args<T>(&mut self, args: T)
    where
        T: TemporalSerializable + Send + 'static;
}

macro_rules! impl_with_args {
    ($input:ty) => {
        impl HasArgs for $input {
            fn args_ref<T: Any>(&self) -> Option<&T> {
                self.args.as_any().downcast_ref()
            }

            fn args_mut<T: Any>(&mut self) -> Option<&mut T> {
                self.args.as_any_mut().downcast_mut()
            }

            fn replace_args<T>(&mut self, args: T)
            where
                T: TemporalSerializable + Send + 'static,
            {
                self.args = Box::new(args);
            }
        }
    };
}

/// Continuation for an intercepted client operation.
///
/// A continuation can be invoked at most once because [`run`](Self::run) consumes it.
pub struct Next<'a, I, O> {
    inner: Box<dyn FnOnce(I) -> O + Send + 'a>,
}

impl<'a, I, O> Next<'a, I, O> {
    pub(crate) fn new(f: impl FnOnce(I) -> O + Send + 'a) -> Self {
        Self { inner: Box::new(f) }
    }

    /// Continue the interceptor chain with the provided input.
    pub fn run(self, input: I) -> O {
        (self.inner)(input)
    }
}

/// Input to [`ClientInterceptor::start_workflow`].
#[non_exhaustive]
#[derive(derive_more::Debug)]
pub struct StartWorkflowInput {
    /// The workflow type sent to the server.
    pub workflow_type: String,
    /// Options for the workflow start.
    pub options: WorkflowStartOptions,
    /// Controls for the start RPC.
    pub rpc_options: crate::RpcOptions,
    #[debug(skip)]
    args: Box<dyn TemporalClientValue>,
}

impl StartWorkflowInput {
    pub(crate) fn new<T>(workflow_type: String, args: T, mut options: WorkflowStartOptions) -> Self
    where
        T: TemporalSerializable + Send + 'static,
    {
        let rpc_options = std::mem::take(&mut options.rpc_options);
        Self {
            workflow_type,
            options,
            rpc_options,
            args: Box::new(args),
        }
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        String,
        Box<dyn TemporalClientValue>,
        WorkflowStartOptions,
        crate::RpcOptions,
    ) {
        (
            self.workflow_type,
            self.args,
            self.options,
            self.rpc_options,
        )
    }
}

impl_with_args!(StartWorkflowInput);

/// Result of a successful intercepted workflow start.
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StartWorkflowOutput {
    /// The workflow ID used by the start operation.
    pub workflow_id: String,
    /// The run ID returned by the service or a short-circuiting interceptor.
    pub run_id: String,
}

impl StartWorkflowOutput {
    pub(crate) fn new(workflow_id: impl Into<String>, run_id: impl Into<String>) -> Self {
        Self {
            workflow_id: workflow_id.into(),
            run_id: run_id.into(),
        }
    }
}

/// Input to [`ClientInterceptor::list_workflows_page`].
#[non_exhaustive]
#[derive(Clone, Debug)]
pub struct ListWorkflowsPageInput {
    /// Visibility query used to select workflows.
    pub query: String,
    /// Token identifying the page to retrieve, or empty for the first page.
    pub next_page_token: Vec<u8>,
    /// Controls for this page RPC.
    pub rpc_options: crate::RpcOptions,
}

/// Result of one intercepted workflow-list page.
#[non_exhaustive]
#[derive(Clone, Debug)]
pub struct ListWorkflowsPageOutput {
    /// Workflow executions returned by the service.
    pub executions: Vec<WorkflowExecutionInfo>,
    /// Token identifying the next page, or empty when no pages remain.
    pub next_page_token: Vec<u8>,
}

impl ListWorkflowsPageOutput {
    pub(crate) fn new(executions: Vec<WorkflowExecutionInfo>, next_page_token: Vec<u8>) -> Self {
        Self {
            executions,
            next_page_token,
        }
    }
}

/// Input to [`ClientInterceptor::count_workflows`].
#[non_exhaustive]
#[derive(Clone, Debug)]
pub struct CountWorkflowsInput {
    /// Visibility query used to count workflows.
    pub query: String,
    /// Count options, including per-call RPC controls.
    pub options: WorkflowCountOptions,
}

/// Result of an intercepted workflow count.
#[non_exhaustive]
#[derive(Clone, Debug)]
pub struct CountWorkflowsOutput {
    /// Raw service response used to assemble the high-level count result.
    pub response: CountWorkflowExecutionsResponse,
}

impl CountWorkflowsOutput {
    pub(crate) fn new(response: CountWorkflowExecutionsResponse) -> Self {
        Self { response }
    }
}

/// Input to [`ClientInterceptor::describe_workflow`].
#[non_exhaustive]
#[derive(Clone, Debug)]
pub struct DescribeWorkflowInput {
    /// Workflow ID to describe.
    pub workflow_id: String,
    /// Run ID to describe, or empty for the latest run.
    pub run_id: String,
    /// Describe options, including per-call RPC controls.
    pub options: WorkflowDescribeOptions,
}

/// Result of an intercepted workflow describe.
#[non_exhaustive]
#[derive(Clone, Debug)]
pub struct DescribeWorkflowOutput {
    /// Raw service response decoded after interceptor dispatch.
    pub response: DescribeWorkflowExecutionResponse,
}

impl DescribeWorkflowOutput {
    pub(crate) fn new(response: DescribeWorkflowExecutionResponse) -> Self {
        Self { response }
    }
}

/// Input to [`ClientInterceptor::fetch_workflow_history_page`].
#[non_exhaustive]
#[derive(Clone, Debug)]
pub struct FetchWorkflowHistoryPageInput {
    /// Workflow ID whose history is being retrieved.
    pub workflow_id: String,
    /// Run ID whose history is being retrieved.
    pub run_id: String,
    /// Token identifying the page to retrieve, or empty for the first page.
    pub next_page_token: Vec<u8>,
    /// History options, including per-page RPC controls.
    pub options: WorkflowFetchHistoryOptions,
}

/// Result of one intercepted workflow-history page.
#[non_exhaustive]
#[derive(Clone, Debug)]
pub struct FetchWorkflowHistoryPageOutput {
    /// History events returned on this page.
    pub events: Vec<HistoryEvent>,
    /// Token identifying the next page, or empty when no pages remain.
    pub next_page_token: Vec<u8>,
}

impl FetchWorkflowHistoryPageOutput {
    pub(crate) fn new(events: Vec<HistoryEvent>, next_page_token: Vec<u8>) -> Self {
        Self {
            events,
            next_page_token,
        }
    }
}

/// Input to [`ClientInterceptor::signal_workflow`].
#[non_exhaustive]
#[derive(derive_more::Debug)]
pub struct SignalWorkflowInput {
    /// Workflow ID to signal.
    pub workflow_id: String,
    /// Run ID to signal, or empty for the latest run.
    pub run_id: String,
    /// Signal name sent to the workflow.
    pub signal_name: String,
    /// Signal options, including per-call RPC controls.
    pub options: WorkflowSignalOptions,
    #[debug(skip)]
    args: Box<dyn TemporalClientValue>,
}

impl SignalWorkflowInput {
    pub(crate) fn new<T>(
        workflow_id: String,
        run_id: String,
        signal_name: String,
        args: T,
        options: WorkflowSignalOptions,
    ) -> Self
    where
        T: TemporalSerializable + Send + 'static,
    {
        Self {
            workflow_id,
            run_id,
            signal_name,
            options,
            args: Box::new(args),
        }
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        String,
        String,
        String,
        Box<dyn TemporalClientValue>,
        WorkflowSignalOptions,
    ) {
        (
            self.workflow_id,
            self.run_id,
            self.signal_name,
            self.args,
            self.options,
        )
    }
}

impl_with_args!(SignalWorkflowInput);

/// Input to [`ClientInterceptor::query_workflow`].
#[non_exhaustive]
#[derive(derive_more::Debug)]
pub struct QueryWorkflowInput {
    /// Workflow ID to query.
    pub workflow_id: String,
    /// Run ID to query, or empty for the latest run.
    pub run_id: String,
    /// Query name sent to the workflow.
    pub query_name: String,
    /// Query options, including per-call RPC controls.
    pub options: WorkflowQueryOptions,
    #[debug(skip)]
    args: Box<dyn TemporalClientValue>,
}

impl QueryWorkflowInput {
    pub(crate) fn new<T>(
        workflow_id: String,
        run_id: String,
        query_name: String,
        args: T,
        options: WorkflowQueryOptions,
    ) -> Self
    where
        T: TemporalSerializable + Send + 'static,
    {
        Self {
            workflow_id,
            run_id,
            query_name,
            options,
            args: Box::new(args),
        }
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        String,
        String,
        String,
        Box<dyn TemporalClientValue>,
        WorkflowQueryOptions,
    ) {
        (
            self.workflow_id,
            self.run_id,
            self.query_name,
            self.args,
            self.options,
        )
    }
}

impl_with_args!(QueryWorkflowInput);

/// Result of an intercepted workflow query before typed result conversion.
#[non_exhaustive]
#[derive(Clone, Debug)]
pub struct QueryWorkflowOutput {
    /// Raw service response decoded after interceptor dispatch.
    pub response: QueryWorkflowResponse,
}

impl QueryWorkflowOutput {
    pub(crate) fn new(response: QueryWorkflowResponse) -> Self {
        Self { response }
    }
}

/// Input to [`ClientInterceptor::start_workflow_update`].
#[non_exhaustive]
#[derive(derive_more::Debug)]
pub struct StartWorkflowUpdateInput {
    /// Workflow ID to update.
    pub workflow_id: String,
    /// Run ID to update, or empty for the latest run.
    pub run_id: String,
    /// Update name sent to the workflow.
    pub update_name: String,
    /// Update options, including per-call RPC controls.
    pub options: WorkflowStartUpdateOptions,
    #[debug(skip)]
    args: Box<dyn TemporalClientValue>,
}

impl StartWorkflowUpdateInput {
    pub(crate) fn new<T>(
        workflow_id: String,
        run_id: String,
        update_name: String,
        args: T,
        options: WorkflowStartUpdateOptions,
    ) -> Self
    where
        T: TemporalSerializable + Send + 'static,
    {
        Self {
            workflow_id,
            run_id,
            update_name,
            options,
            args: Box::new(args),
        }
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        String,
        String,
        String,
        Box<dyn TemporalClientValue>,
        WorkflowStartUpdateOptions,
    ) {
        (
            self.workflow_id,
            self.run_id,
            self.update_name,
            self.args,
            self.options,
        )
    }
}

impl_with_args!(StartWorkflowUpdateInput);

/// Result of an intercepted workflow-update start.
#[non_exhaustive]
#[derive(Clone, Debug)]
pub struct StartWorkflowUpdateOutput {
    /// Update ID used by the operation.
    pub update_id: String,
    /// Workflow ID associated with the update.
    pub workflow_id: String,
    /// Run ID returned by the service, when available.
    pub run_id: Option<String>,
    /// Outcome returned when the requested wait stage completed the update.
    pub known_outcome: Option<Outcome>,
}

impl StartWorkflowUpdateOutput {
    pub(crate) fn new(
        update_id: impl Into<String>,
        workflow_id: impl Into<String>,
        run_id: Option<String>,
        known_outcome: Option<Outcome>,
    ) -> Self {
        Self {
            update_id: update_id.into(),
            workflow_id: workflow_id.into(),
            run_id,
            known_outcome,
        }
    }
}

/// Input to [`ClientInterceptor::poll_workflow_update`].
#[non_exhaustive]
#[derive(Clone, Debug)]
pub struct PollWorkflowUpdateInput {
    /// Update ID being polled.
    pub update_id: String,
    /// Workflow ID associated with the update.
    pub workflow_id: String,
    /// Run ID associated with the update, or empty for the latest run.
    pub run_id: String,
    /// Controls for every RPC in the polling loop.
    pub rpc_options: crate::RpcOptions,
}

/// Result of an intercepted workflow-update poll.
#[non_exhaustive]
#[derive(Clone, Debug)]
pub struct PollWorkflowUpdateOutput {
    /// Completed update outcome.
    pub outcome: Outcome,
}

impl PollWorkflowUpdateOutput {
    pub(crate) fn new(outcome: Outcome) -> Self {
        Self { outcome }
    }
}

/// Input to [`ClientInterceptor::cancel_workflow`].
#[non_exhaustive]
#[derive(Clone, Debug)]
pub struct CancelWorkflowInput {
    /// Workflow ID to cancel.
    pub workflow_id: String,
    /// Run ID to cancel, or empty for the latest run.
    pub run_id: String,
    /// First execution run ID used to constrain the cancellation.
    pub first_execution_run_id: String,
    /// Cancellation options, including per-call RPC controls.
    pub options: WorkflowCancelOptions,
}

/// Input to [`ClientInterceptor::terminate_workflow`].
#[non_exhaustive]
#[derive(Clone, Debug)]
pub struct TerminateWorkflowInput {
    /// Workflow ID to terminate.
    pub workflow_id: String,
    /// Run ID to terminate, or empty for the latest run.
    pub run_id: String,
    /// First execution run ID used to constrain the termination.
    pub first_execution_run_id: String,
    /// Termination options, including per-call RPC controls.
    pub options: WorkflowTerminateOptions,
}

/// Input to [`ClientInterceptor::create_schedule`].
#[non_exhaustive]
#[derive(Debug)]
pub struct CreateScheduleInput {
    /// Schedule ID to create.
    pub schedule_id: String,
    /// Schedule definition and per-call RPC controls.
    pub options: CreateScheduleOptions,
}

/// Result of an intercepted schedule create.
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateScheduleOutput {
    /// Schedule ID associated with the created handle.
    pub schedule_id: String,
}

impl CreateScheduleOutput {
    pub(crate) fn new(schedule_id: impl Into<String>) -> Self {
        Self {
            schedule_id: schedule_id.into(),
        }
    }
}

/// Input to [`ClientInterceptor::list_schedules_page`].
#[non_exhaustive]
#[derive(Clone, Debug)]
pub struct ListSchedulesPageInput {
    /// Maximum number of results requested from the service.
    pub maximum_page_size: i32,
    /// Visibility query used to select schedules.
    pub query: String,
    /// Token identifying the page to retrieve, or empty for the first page.
    pub next_page_token: Vec<u8>,
    /// Controls for this page RPC.
    pub rpc_options: crate::RpcOptions,
}

/// Result of one intercepted schedule-list page.
#[non_exhaustive]
#[derive(Clone, Debug)]
pub struct ListSchedulesPageOutput {
    /// Schedule entries returned by the service.
    pub schedules: Vec<ScheduleListEntry>,
    /// Token identifying the next page, or empty when no pages remain.
    pub next_page_token: Vec<u8>,
}

impl ListSchedulesPageOutput {
    pub(crate) fn new(schedules: Vec<ScheduleListEntry>, next_page_token: Vec<u8>) -> Self {
        Self {
            schedules,
            next_page_token,
        }
    }
}

/// Input to [`ClientInterceptor::describe_schedule`].
#[non_exhaustive]
#[derive(Clone, Debug)]
pub struct DescribeScheduleInput {
    /// Schedule ID to describe.
    pub schedule_id: String,
    /// Controls for the describe RPC.
    pub rpc_options: crate::RpcOptions,
}

/// Result of an intercepted schedule describe.
#[non_exhaustive]
#[derive(Clone, Debug)]
pub struct DescribeScheduleOutput {
    /// Raw service response decoded after interceptor dispatch.
    pub response: DescribeScheduleResponse,
}

impl DescribeScheduleOutput {
    pub(crate) fn new(response: DescribeScheduleResponse) -> Self {
        Self { response }
    }
}

/// Input to [`ClientInterceptor::update_schedule`].
#[non_exhaustive]
#[derive(Clone, Debug)]
pub struct UpdateScheduleInput {
    /// Schedule ID to update.
    pub schedule_id: String,
    /// Controls shared by the describe and update RPCs.
    pub rpc_options: crate::RpcOptions,
}

/// Input to [`ClientInterceptor::send_schedule_update`].
#[non_exhaustive]
#[derive(Clone, Debug)]
pub struct SendScheduleUpdateInput {
    /// Schedule ID to update.
    pub schedule_id: String,
    /// Pre-built schedule update.
    pub update: ScheduleUpdate,
    /// Controls for the update RPC.
    pub rpc_options: crate::RpcOptions,
}

/// Input to [`ClientInterceptor::delete_schedule`].
#[non_exhaustive]
#[derive(Clone, Debug)]
pub struct DeleteScheduleInput {
    /// Schedule ID to delete.
    pub schedule_id: String,
    /// Controls for the delete RPC.
    pub rpc_options: crate::RpcOptions,
}

/// Input to [`ClientInterceptor::pause_schedule`].
#[non_exhaustive]
#[derive(Clone, Debug)]
pub struct PauseScheduleInput {
    /// Schedule ID to pause.
    pub schedule_id: String,
    /// Note attached to the pause operation.
    pub note: String,
    /// Controls for the patch RPC.
    pub rpc_options: crate::RpcOptions,
}

/// Input to [`ClientInterceptor::unpause_schedule`].
#[non_exhaustive]
#[derive(Clone, Debug)]
pub struct UnpauseScheduleInput {
    /// Schedule ID to unpause.
    pub schedule_id: String,
    /// Note attached to the unpause operation.
    pub note: String,
    /// Controls for the patch RPC.
    pub rpc_options: crate::RpcOptions,
}

/// Input to [`ClientInterceptor::trigger_schedule`].
#[non_exhaustive]
#[derive(Clone, Debug)]
pub struct TriggerScheduleInput {
    /// Schedule ID to trigger.
    pub schedule_id: String,
    /// Overlap policy for the immediate action.
    pub overlap_policy: ScheduleOverlapPolicy,
    /// Controls for the patch RPC.
    pub rpc_options: crate::RpcOptions,
}

/// Input to [`ClientInterceptor::backfill_schedule`].
#[non_exhaustive]
#[derive(Clone, Debug)]
pub struct BackfillScheduleInput {
    /// Schedule ID to backfill.
    pub schedule_id: String,
    /// Backfill ranges requested by the caller.
    pub backfills: Vec<ScheduleBackfill>,
    /// Controls for the patch RPC.
    pub rpc_options: crate::RpcOptions,
}

/// Input to [`ClientInterceptor::complete_async_activity`].
#[non_exhaustive]
#[derive(derive_more::Debug)]
pub struct CompleteAsyncActivityInput {
    /// Activity being completed.
    pub identifier: ActivityIdentifier,
    #[debug(skip)]
    result: Option<Box<dyn TemporalClientValue>>,
    /// Controls for the completion RPC.
    pub rpc_options: crate::RpcOptions,
}

impl CompleteAsyncActivityInput {
    pub(crate) fn new<T>(
        identifier: ActivityIdentifier,
        result: Option<T>,
        rpc_options: crate::RpcOptions,
    ) -> Self
    where
        T: TemporalSerializable + Send + 'static,
    {
        Self {
            identifier,
            result: result.map(|value| Box::new(value) as Box<dyn TemporalClientValue>),
            rpc_options,
        }
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        ActivityIdentifier,
        Option<Box<dyn TemporalClientValue>>,
        crate::RpcOptions,
    ) {
        (self.identifier, self.result, self.rpc_options)
    }

    /// Attempt to access the activity result as a concrete type.
    pub fn result_ref<T: Any>(&self) -> Option<&T> {
        self.result
            .as_ref()
            .and_then(|result| result.as_any().downcast_ref())
    }

    /// Attempt to mutably access the activity result as a concrete type.
    pub fn result_mut<T: Any>(&mut self) -> Option<&mut T> {
        self.result
            .as_mut()
            .and_then(|result| result.as_any_mut().downcast_mut())
    }

    /// Replace or clear the activity result.
    pub fn replace_result<T>(&mut self, result: Option<T>)
    where
        T: TemporalSerializable + Send + 'static,
    {
        self.result = result.map(|value| Box::new(value) as Box<dyn TemporalClientValue>);
    }
}

/// Input to [`ClientInterceptor::fail_async_activity`].
#[non_exhaustive]
#[derive(derive_more::Debug)]
pub struct FailAsyncActivityInput {
    /// Activity being failed.
    pub identifier: ActivityIdentifier,
    /// Application failure reported for the activity.
    pub failure: temporalio_common::error::ApplicationFailure,
    #[debug(skip)]
    last_heartbeat_details: Option<Box<dyn TemporalClientValue>>,
    /// Controls for the failure RPC.
    pub rpc_options: crate::RpcOptions,
}

impl FailAsyncActivityInput {
    pub(crate) fn new<T>(
        identifier: ActivityIdentifier,
        failure: temporalio_common::error::ApplicationFailure,
        last_heartbeat_details: Option<T>,
        rpc_options: crate::RpcOptions,
    ) -> Self
    where
        T: TemporalSerializable + Send + 'static,
    {
        Self {
            identifier,
            failure,
            last_heartbeat_details: last_heartbeat_details
                .map(|value| Box::new(value) as Box<dyn TemporalClientValue>),
            rpc_options,
        }
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        ActivityIdentifier,
        temporalio_common::error::ApplicationFailure,
        Option<Box<dyn TemporalClientValue>>,
        crate::RpcOptions,
    ) {
        (
            self.identifier,
            self.failure,
            self.last_heartbeat_details,
            self.rpc_options,
        )
    }

    /// Attempt to access the last heartbeat details as a concrete type.
    pub fn last_heartbeat_details_ref<T: Any>(&self) -> Option<&T> {
        self.last_heartbeat_details
            .as_ref()
            .and_then(|details| details.as_any().downcast_ref())
    }

    /// Attempt to mutably access the last heartbeat details as a concrete type.
    pub fn last_heartbeat_details_mut<T: Any>(&mut self) -> Option<&mut T> {
        self.last_heartbeat_details
            .as_mut()
            .and_then(|details| details.as_any_mut().downcast_mut())
    }

    /// Replace or clear the last heartbeat details.
    pub fn replace_last_heartbeat_details<T>(&mut self, details: Option<T>)
    where
        T: TemporalSerializable + Send + 'static,
    {
        self.last_heartbeat_details =
            details.map(|value| Box::new(value) as Box<dyn TemporalClientValue>);
    }
}

/// Input to [`ClientInterceptor::report_async_activity_cancellation`].
#[non_exhaustive]
#[derive(derive_more::Debug)]
pub struct ReportAsyncActivityCancellationInput {
    /// Activity being reported as cancelled.
    pub identifier: ActivityIdentifier,
    #[debug(skip)]
    details: Option<Box<dyn TemporalClientValue>>,
    /// Controls for the cancellation RPC.
    pub rpc_options: crate::RpcOptions,
}

impl ReportAsyncActivityCancellationInput {
    pub(crate) fn new<T>(
        identifier: ActivityIdentifier,
        details: Option<T>,
        rpc_options: crate::RpcOptions,
    ) -> Self
    where
        T: TemporalSerializable + Send + 'static,
    {
        Self {
            identifier,
            details: details.map(|value| Box::new(value) as Box<dyn TemporalClientValue>),
            rpc_options,
        }
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        ActivityIdentifier,
        Option<Box<dyn TemporalClientValue>>,
        crate::RpcOptions,
    ) {
        (self.identifier, self.details, self.rpc_options)
    }

    /// Attempt to access the cancellation details as a concrete type.
    pub fn details_ref<T: Any>(&self) -> Option<&T> {
        self.details
            .as_ref()
            .and_then(|details| details.as_any().downcast_ref())
    }

    /// Attempt to mutably access the cancellation details as a concrete type.
    pub fn details_mut<T: Any>(&mut self) -> Option<&mut T> {
        self.details
            .as_mut()
            .and_then(|details| details.as_any_mut().downcast_mut())
    }

    /// Replace or clear the cancellation details.
    pub fn replace_details<T>(&mut self, details: Option<T>)
    where
        T: TemporalSerializable + Send + 'static,
    {
        self.details = details.map(|value| Box::new(value) as Box<dyn TemporalClientValue>);
    }
}

/// Input to [`ClientInterceptor::heartbeat_async_activity`].
#[non_exhaustive]
#[derive(derive_more::Debug)]
pub struct HeartbeatAsyncActivityInput {
    /// Activity being heartbeated.
    pub identifier: ActivityIdentifier,
    #[debug(skip)]
    details: Option<Box<dyn TemporalClientValue>>,
    /// Controls for the heartbeat RPC.
    pub rpc_options: crate::RpcOptions,
}

impl HeartbeatAsyncActivityInput {
    pub(crate) fn new<T>(
        identifier: ActivityIdentifier,
        details: Option<T>,
        rpc_options: crate::RpcOptions,
    ) -> Self
    where
        T: TemporalSerializable + Send + 'static,
    {
        Self {
            identifier,
            details: details.map(|value| Box::new(value) as Box<dyn TemporalClientValue>),
            rpc_options,
        }
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        ActivityIdentifier,
        Option<Box<dyn TemporalClientValue>>,
        crate::RpcOptions,
    ) {
        (self.identifier, self.details, self.rpc_options)
    }

    /// Attempt to access the heartbeat details as a concrete type.
    pub fn details_ref<T: Any>(&self) -> Option<&T> {
        self.details
            .as_ref()
            .and_then(|details| details.as_any().downcast_ref())
    }

    /// Attempt to mutably access the heartbeat details as a concrete type.
    pub fn details_mut<T: Any>(&mut self) -> Option<&mut T> {
        self.details
            .as_mut()
            .and_then(|details| details.as_any_mut().downcast_mut())
    }

    /// Replace or clear the heartbeat details.
    pub fn replace_details<T>(&mut self, details: Option<T>)
    where
        T: TemporalSerializable + Send + 'static,
    {
        self.details = details.map(|value| Box::new(value) as Box<dyn TemporalClientValue>);
    }
}

/// Intercepts high-level client operations.
///
/// The first interceptor configured on a client is the outermost interceptor. An interceptor can
/// do asynchronous work before and after calling `next`, mutate or replace typed input, or return
/// without calling `next` to short-circuit the operation.
///
/// ```
/// use futures_util::future::BoxFuture;
/// use std::{sync::Arc, time::Duration};
/// use temporalio_client::{
///     ClientInterceptor, ClientOptions, Next, StartWorkflowInput, StartWorkflowOutput,
///     errors::WorkflowStartError,
/// };
///
/// struct StartTimeout;
///
/// impl ClientInterceptor for StartTimeout {
///     fn start_workflow<'a>(
///         &'a self,
///         mut input: StartWorkflowInput,
///         next: Next<
///             'a,
///             StartWorkflowInput,
///             BoxFuture<'a, Result<StartWorkflowOutput, WorkflowStartError>>,
///         >,
///     ) -> BoxFuture<'a, Result<StartWorkflowOutput, WorkflowStartError>> {
///         Box::pin(async move {
///             input.rpc_options.timeout = Some(Duration::from_secs(10));
///             let output = next.run(input).await?;
///             Ok(output)
///         })
///     }
/// }
///
/// let _options = ClientOptions::new("my-namespace")
///     .client_interceptors(vec![Arc::new(StartTimeout)])
///     .build();
/// ```
pub trait ClientInterceptor: Send + Sync + 'static {
    /// Intercept a `start_workflow` operation.
    fn start_workflow<'a>(
        &'a self,
        input: StartWorkflowInput,
        next: Next<
            'a,
            StartWorkflowInput,
            BoxFuture<'a, Result<StartWorkflowOutput, WorkflowStartError>>,
        >,
    ) -> BoxFuture<'a, Result<StartWorkflowOutput, WorkflowStartError>> {
        next.run(input)
    }

    /// Intercept a `list_workflows_page` operation.
    fn list_workflows_page<'a>(
        &'a self,
        input: ListWorkflowsPageInput,
        next: Next<
            'a,
            ListWorkflowsPageInput,
            BoxFuture<'a, Result<ListWorkflowsPageOutput, ClientError>>,
        >,
    ) -> BoxFuture<'a, Result<ListWorkflowsPageOutput, ClientError>> {
        next.run(input)
    }

    /// Intercept a `count_workflows` operation.
    fn count_workflows<'a>(
        &'a self,
        input: CountWorkflowsInput,
        next: Next<
            'a,
            CountWorkflowsInput,
            BoxFuture<'a, Result<CountWorkflowsOutput, ClientError>>,
        >,
    ) -> BoxFuture<'a, Result<CountWorkflowsOutput, ClientError>> {
        next.run(input)
    }

    /// Intercept a `describe_workflow` operation.
    fn describe_workflow<'a>(
        &'a self,
        input: DescribeWorkflowInput,
        next: Next<
            'a,
            DescribeWorkflowInput,
            BoxFuture<'a, Result<DescribeWorkflowOutput, WorkflowInteractionError>>,
        >,
    ) -> BoxFuture<'a, Result<DescribeWorkflowOutput, WorkflowInteractionError>> {
        next.run(input)
    }

    /// Intercept a `fetch_workflow_history_page` operation.
    fn fetch_workflow_history_page<'a>(
        &'a self,
        input: FetchWorkflowHistoryPageInput,
        next: Next<
            'a,
            FetchWorkflowHistoryPageInput,
            BoxFuture<'a, Result<FetchWorkflowHistoryPageOutput, WorkflowInteractionError>>,
        >,
    ) -> BoxFuture<'a, Result<FetchWorkflowHistoryPageOutput, WorkflowInteractionError>> {
        next.run(input)
    }

    /// Intercept a `signal_workflow` operation.
    fn signal_workflow<'a>(
        &'a self,
        input: SignalWorkflowInput,
        next: Next<'a, SignalWorkflowInput, BoxFuture<'a, Result<(), WorkflowInteractionError>>>,
    ) -> BoxFuture<'a, Result<(), WorkflowInteractionError>> {
        next.run(input)
    }

    /// Intercept a `query_workflow` operation.
    fn query_workflow<'a>(
        &'a self,
        input: QueryWorkflowInput,
        next: Next<
            'a,
            QueryWorkflowInput,
            BoxFuture<'a, Result<QueryWorkflowOutput, WorkflowQueryError>>,
        >,
    ) -> BoxFuture<'a, Result<QueryWorkflowOutput, WorkflowQueryError>> {
        next.run(input)
    }

    /// Intercept a `start_workflow_update` operation.
    fn start_workflow_update<'a>(
        &'a self,
        input: StartWorkflowUpdateInput,
        next: Next<
            'a,
            StartWorkflowUpdateInput,
            BoxFuture<'a, Result<StartWorkflowUpdateOutput, WorkflowUpdateError>>,
        >,
    ) -> BoxFuture<'a, Result<StartWorkflowUpdateOutput, WorkflowUpdateError>> {
        next.run(input)
    }

    /// Intercept a `poll_workflow_update` operation.
    fn poll_workflow_update<'a>(
        &'a self,
        input: PollWorkflowUpdateInput,
        next: Next<
            'a,
            PollWorkflowUpdateInput,
            BoxFuture<'a, Result<PollWorkflowUpdateOutput, WorkflowUpdateError>>,
        >,
    ) -> BoxFuture<'a, Result<PollWorkflowUpdateOutput, WorkflowUpdateError>> {
        next.run(input)
    }

    /// Intercept a `cancel_workflow` operation.
    fn cancel_workflow<'a>(
        &'a self,
        input: CancelWorkflowInput,
        next: Next<'a, CancelWorkflowInput, BoxFuture<'a, Result<(), WorkflowInteractionError>>>,
    ) -> BoxFuture<'a, Result<(), WorkflowInteractionError>> {
        next.run(input)
    }

    /// Intercept a `terminate_workflow` operation.
    fn terminate_workflow<'a>(
        &'a self,
        input: TerminateWorkflowInput,
        next: Next<'a, TerminateWorkflowInput, BoxFuture<'a, Result<(), WorkflowInteractionError>>>,
    ) -> BoxFuture<'a, Result<(), WorkflowInteractionError>> {
        next.run(input)
    }

    /// Intercept a `create_schedule` operation.
    fn create_schedule<'a>(
        &'a self,
        input: CreateScheduleInput,
        next: Next<
            'a,
            CreateScheduleInput,
            BoxFuture<'a, Result<CreateScheduleOutput, ScheduleError>>,
        >,
    ) -> BoxFuture<'a, Result<CreateScheduleOutput, ScheduleError>> {
        next.run(input)
    }

    /// Intercept a `list_schedules_page` operation.
    fn list_schedules_page<'a>(
        &'a self,
        input: ListSchedulesPageInput,
        next: Next<
            'a,
            ListSchedulesPageInput,
            BoxFuture<'a, Result<ListSchedulesPageOutput, ScheduleError>>,
        >,
    ) -> BoxFuture<'a, Result<ListSchedulesPageOutput, ScheduleError>> {
        next.run(input)
    }

    /// Intercept a `describe_schedule` operation.
    fn describe_schedule<'a>(
        &'a self,
        input: DescribeScheduleInput,
        next: Next<
            'a,
            DescribeScheduleInput,
            BoxFuture<'a, Result<DescribeScheduleOutput, ScheduleError>>,
        >,
    ) -> BoxFuture<'a, Result<DescribeScheduleOutput, ScheduleError>> {
        next.run(input)
    }

    /// Intercept an `update_schedule` operation.
    fn update_schedule<'a>(
        &'a self,
        input: UpdateScheduleInput,
        next: Next<'a, UpdateScheduleInput, BoxFuture<'a, Result<(), ScheduleError>>>,
    ) -> BoxFuture<'a, Result<(), ScheduleError>> {
        next.run(input)
    }

    /// Intercept a `send_schedule_update` operation.
    fn send_schedule_update<'a>(
        &'a self,
        input: SendScheduleUpdateInput,
        next: Next<'a, SendScheduleUpdateInput, BoxFuture<'a, Result<(), ScheduleError>>>,
    ) -> BoxFuture<'a, Result<(), ScheduleError>> {
        next.run(input)
    }

    /// Intercept a `delete_schedule` operation.
    fn delete_schedule<'a>(
        &'a self,
        input: DeleteScheduleInput,
        next: Next<'a, DeleteScheduleInput, BoxFuture<'a, Result<(), ScheduleError>>>,
    ) -> BoxFuture<'a, Result<(), ScheduleError>> {
        next.run(input)
    }

    /// Intercept a `pause_schedule` operation.
    fn pause_schedule<'a>(
        &'a self,
        input: PauseScheduleInput,
        next: Next<'a, PauseScheduleInput, BoxFuture<'a, Result<(), ScheduleError>>>,
    ) -> BoxFuture<'a, Result<(), ScheduleError>> {
        next.run(input)
    }

    /// Intercept an `unpause_schedule` operation.
    fn unpause_schedule<'a>(
        &'a self,
        input: UnpauseScheduleInput,
        next: Next<'a, UnpauseScheduleInput, BoxFuture<'a, Result<(), ScheduleError>>>,
    ) -> BoxFuture<'a, Result<(), ScheduleError>> {
        next.run(input)
    }

    /// Intercept a `trigger_schedule` operation.
    fn trigger_schedule<'a>(
        &'a self,
        input: TriggerScheduleInput,
        next: Next<'a, TriggerScheduleInput, BoxFuture<'a, Result<(), ScheduleError>>>,
    ) -> BoxFuture<'a, Result<(), ScheduleError>> {
        next.run(input)
    }

    /// Intercept a `backfill_schedule` operation.
    fn backfill_schedule<'a>(
        &'a self,
        input: BackfillScheduleInput,
        next: Next<'a, BackfillScheduleInput, BoxFuture<'a, Result<(), ScheduleError>>>,
    ) -> BoxFuture<'a, Result<(), ScheduleError>> {
        next.run(input)
    }

    /// Intercept a `complete_async_activity` operation.
    fn complete_async_activity<'a>(
        &'a self,
        input: CompleteAsyncActivityInput,
        next: Next<'a, CompleteAsyncActivityInput, BoxFuture<'a, Result<(), AsyncActivityError>>>,
    ) -> BoxFuture<'a, Result<(), AsyncActivityError>> {
        next.run(input)
    }

    /// Intercept a `fail_async_activity` operation.
    fn fail_async_activity<'a>(
        &'a self,
        input: FailAsyncActivityInput,
        next: Next<'a, FailAsyncActivityInput, BoxFuture<'a, Result<(), AsyncActivityError>>>,
    ) -> BoxFuture<'a, Result<(), AsyncActivityError>> {
        next.run(input)
    }

    /// Intercept a `report_async_activity_cancellation` operation.
    fn report_async_activity_cancellation<'a>(
        &'a self,
        input: ReportAsyncActivityCancellationInput,
        next: Next<
            'a,
            ReportAsyncActivityCancellationInput,
            BoxFuture<'a, Result<(), AsyncActivityError>>,
        >,
    ) -> BoxFuture<'a, Result<(), AsyncActivityError>> {
        next.run(input)
    }

    /// Intercept a `heartbeat_async_activity` operation.
    fn heartbeat_async_activity<'a>(
        &'a self,
        input: HeartbeatAsyncActivityInput,
        next: Next<
            'a,
            HeartbeatAsyncActivityInput,
            BoxFuture<'a, Result<ActivityHeartbeatResponse, AsyncActivityError>>,
        >,
    ) -> BoxFuture<'a, Result<ActivityHeartbeatResponse, AsyncActivityError>> {
        next.run(input)
    }
}

macro_rules! interceptor_chain {
    ($fn_name:ident, $method:ident, $input:ty, $output:ty) => {
        pub(crate) fn $fn_name<'a>(
            interceptors: &'a [Arc<dyn ClientInterceptor>],
            input: $input,
            terminal: Next<'a, $input, $output>,
        ) -> $output {
            if let Some((interceptor, remaining)) = interceptors.split_first() {
                let next = Next::new(move |input| $fn_name(remaining, input, terminal));
                interceptor.$method(input, next)
            } else {
                terminal.run(input)
            }
        }
    };
}

interceptor_chain!(
    call_start_workflow,
    start_workflow,
    StartWorkflowInput,
    BoxFuture<'a, Result<StartWorkflowOutput, WorkflowStartError>>
);

interceptor_chain!(
    call_list_workflows_page,
    list_workflows_page,
    ListWorkflowsPageInput,
    BoxFuture<'a, Result<ListWorkflowsPageOutput, ClientError>>
);

interceptor_chain!(
    call_count_workflows,
    count_workflows,
    CountWorkflowsInput,
    BoxFuture<'a, Result<CountWorkflowsOutput, ClientError>>
);

interceptor_chain!(
    call_describe_workflow,
    describe_workflow,
    DescribeWorkflowInput,
    BoxFuture<'a, Result<DescribeWorkflowOutput, WorkflowInteractionError>>
);

interceptor_chain!(
    call_fetch_workflow_history_page,
    fetch_workflow_history_page,
    FetchWorkflowHistoryPageInput,
    BoxFuture<'a, Result<FetchWorkflowHistoryPageOutput, WorkflowInteractionError>>
);

interceptor_chain!(
    call_signal_workflow,
    signal_workflow,
    SignalWorkflowInput,
    BoxFuture<'a, Result<(), WorkflowInteractionError>>
);

interceptor_chain!(
    call_query_workflow,
    query_workflow,
    QueryWorkflowInput,
    BoxFuture<'a, Result<QueryWorkflowOutput, WorkflowQueryError>>
);

interceptor_chain!(
    call_start_workflow_update,
    start_workflow_update,
    StartWorkflowUpdateInput,
    BoxFuture<'a, Result<StartWorkflowUpdateOutput, WorkflowUpdateError>>
);

interceptor_chain!(
    call_poll_workflow_update,
    poll_workflow_update,
    PollWorkflowUpdateInput,
    BoxFuture<'a, Result<PollWorkflowUpdateOutput, WorkflowUpdateError>>
);

interceptor_chain!(
    call_cancel_workflow,
    cancel_workflow,
    CancelWorkflowInput,
    BoxFuture<'a, Result<(), WorkflowInteractionError>>
);

interceptor_chain!(
    call_terminate_workflow,
    terminate_workflow,
    TerminateWorkflowInput,
    BoxFuture<'a, Result<(), WorkflowInteractionError>>
);

interceptor_chain!(
    call_create_schedule,
    create_schedule,
    CreateScheduleInput,
    BoxFuture<'a, Result<CreateScheduleOutput, ScheduleError>>
);

interceptor_chain!(
    call_list_schedules_page,
    list_schedules_page,
    ListSchedulesPageInput,
    BoxFuture<'a, Result<ListSchedulesPageOutput, ScheduleError>>
);

interceptor_chain!(
    call_describe_schedule,
    describe_schedule,
    DescribeScheduleInput,
    BoxFuture<'a, Result<DescribeScheduleOutput, ScheduleError>>
);

interceptor_chain!(
    call_update_schedule,
    update_schedule,
    UpdateScheduleInput,
    BoxFuture<'a, Result<(), ScheduleError>>
);

interceptor_chain!(
    call_send_schedule_update,
    send_schedule_update,
    SendScheduleUpdateInput,
    BoxFuture<'a, Result<(), ScheduleError>>
);

interceptor_chain!(
    call_delete_schedule,
    delete_schedule,
    DeleteScheduleInput,
    BoxFuture<'a, Result<(), ScheduleError>>
);

interceptor_chain!(
    call_pause_schedule,
    pause_schedule,
    PauseScheduleInput,
    BoxFuture<'a, Result<(), ScheduleError>>
);

interceptor_chain!(
    call_unpause_schedule,
    unpause_schedule,
    UnpauseScheduleInput,
    BoxFuture<'a, Result<(), ScheduleError>>
);

interceptor_chain!(
    call_trigger_schedule,
    trigger_schedule,
    TriggerScheduleInput,
    BoxFuture<'a, Result<(), ScheduleError>>
);

interceptor_chain!(
    call_backfill_schedule,
    backfill_schedule,
    BackfillScheduleInput,
    BoxFuture<'a, Result<(), ScheduleError>>
);

interceptor_chain!(
    call_complete_async_activity,
    complete_async_activity,
    CompleteAsyncActivityInput,
    BoxFuture<'a, Result<(), AsyncActivityError>>
);

interceptor_chain!(
    call_fail_async_activity,
    fail_async_activity,
    FailAsyncActivityInput,
    BoxFuture<'a, Result<(), AsyncActivityError>>
);

interceptor_chain!(
    call_report_async_activity_cancellation,
    report_async_activity_cancellation,
    ReportAsyncActivityCancellationInput,
    BoxFuture<'a, Result<(), AsyncActivityError>>
);

interceptor_chain!(
    call_heartbeat_async_activity,
    heartbeat_async_activity,
    HeartbeatAsyncActivityInput,
    BoxFuture<'a, Result<ActivityHeartbeatResponse, AsyncActivityError>>
);
