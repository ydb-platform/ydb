//! User-definable interceptors are defined in this module

use crate::{
    Worker, WorkerRunError,
    activities::{ActivityContext, ActivityError, ActivityInfo},
};
use anyhow::bail;
use futures_util::future::{BoxFuture, LocalBoxFuture};
use std::{
    any::Any,
    collections::HashMap,
    sync::{Arc, OnceLock},
};
use temporalio_common::{
    data_converters::{
        GenericPayloadConverter, PayloadConversionError, SerializationContext, TemporalSerializable,
    },
    protos::{
        coresdk::{
            workflow_activation::{WorkflowActivation, remove_from_cache::EvictionReason},
            workflow_completion::WorkflowActivationCompletion,
        },
        temporal::api::common::v1::Payload,
    },
};

mod activity_execution_value {
    use super::*;

    pub trait Sealed {
        fn to_activity_payload(
            &self,
            context: &SerializationContext<'_>,
        ) -> Result<Payload, PayloadConversionError>;
    }

    impl<T> Sealed for T
    where
        T: Any + TemporalSerializable + Send + Sync,
    {
        fn to_activity_payload(
            &self,
            context: &SerializationContext<'_>,
        ) -> Result<Payload, PayloadConversionError> {
            context.converter.to_payload(context, self)
        }
    }
}

/// Implementors can intercept certain actions that happen within the Worker.
///
/// Advanced usage only.
/// **Experimental:** This API may change or be removed.
#[async_trait::async_trait(?Send)]
pub trait WorkerInterceptor: Send + Sync {
    /// Intercept the running of a worker.
    fn run_worker<'a>(
        &'a self,
        input: RunWorkerInput<'a>,
        next: Next<'a, RunWorkerInput<'a>, LocalBoxFuture<'a, Result<(), WorkerRunError>>>,
    ) -> LocalBoxFuture<'a, Result<(), WorkerRunError>> {
        next.run(input)
    }

    /// Intercept the running of a worker created for workflow replay.
    fn with_workflow_replay_worker<'a>(
        &'a self,
        input: WithWorkflowReplayWorkerInput<'a>,
        next: Next<
            'a,
            WithWorkflowReplayWorkerInput<'a>,
            LocalBoxFuture<'a, Result<(), WorkerRunError>>,
        >,
    ) -> LocalBoxFuture<'a, Result<(), WorkerRunError>> {
        next.run(input)
    }

    /// Called every time a workflow activation completes (just before sending the completion to
    /// core).
    async fn on_workflow_activation_completion(&self, _completion: &WorkflowActivationCompletion) {}
    /// Called after the worker has initiated shutdown and the workflow/activity polling loops
    /// have exited, but just before waiting for the inner core worker shutdown
    fn on_shutdown(&self, _sdk_worker: &Worker) {}
    /// Called every time a workflow is about to be activated
    async fn on_workflow_activation(
        &self,
        _activation: &WorkflowActivation,
    ) -> Result<(), anyhow::Error> {
        Ok(())
    }
}

/// Continuation for an interceptor operation.
///
/// Interceptor implementations call [`Next::run`] to invoke the next step of the chain.
pub struct Next<'a, I, O> {
    inner: Box<dyn FnOnce(I) -> O + Send + 'a>,
}

/// Input to [`WorkerInterceptor::run_worker`].
#[derive(Debug)]
#[non_exhaustive]
pub struct RunWorkerInput<'a> {
    /// The worker being run.
    pub worker: &'a mut Worker,
}

impl<'a> RunWorkerInput<'a> {
    pub(crate) fn new(worker: &'a mut Worker) -> Self {
        Self { worker }
    }
}

/// Input to [`WorkerInterceptor::with_workflow_replay_worker`].
#[derive(Debug)]
#[non_exhaustive]
pub struct WithWorkflowReplayWorkerInput<'a> {
    /// The worker created for this replay operation.
    pub worker: &'a mut Worker,
}

impl<'a> WithWorkflowReplayWorkerInput<'a> {
    pub(crate) fn new(worker: &'a mut Worker) -> Self {
        Self { worker }
    }
}

pub(crate) fn call_run_worker<'a>(
    interceptors: &'a [Arc<dyn WorkerInterceptor>],
    input: RunWorkerInput<'a>,
    terminal: Next<'a, RunWorkerInput<'a>, LocalBoxFuture<'a, Result<(), WorkerRunError>>>,
) -> LocalBoxFuture<'a, Result<(), WorkerRunError>> {
    if let Some((interceptor, remaining)) = interceptors.split_first() {
        let next = Next::new(move |input| call_run_worker(remaining, input, terminal));
        interceptor.run_worker(input, next)
    } else {
        terminal.run(input)
    }
}

pub(crate) fn call_with_workflow_replay_worker<'a>(
    interceptors: &'a [Arc<dyn WorkerInterceptor>],
    input: WithWorkflowReplayWorkerInput<'a>,
    terminal: Next<
        'a,
        WithWorkflowReplayWorkerInput<'a>,
        LocalBoxFuture<'a, Result<(), WorkerRunError>>,
    >,
) -> LocalBoxFuture<'a, Result<(), WorkerRunError>> {
    if let Some((interceptor, remaining)) = interceptors.split_first() {
        let next =
            Next::new(move |input| call_with_workflow_replay_worker(remaining, input, terminal));
        interceptor.with_workflow_replay_worker(input, next)
    } else {
        terminal.run(input)
    }
}

impl<'a, I, O> Next<'a, I, O> {
    pub(crate) fn new(f: impl FnOnce(I) -> O + Send + 'a) -> Self {
        Self { inner: Box::new(f) }
    }

    /// Continue the call chain with the provided input.
    pub fn run(self, input: I) -> O {
        (self.inner)(input)
    }
}

/// Activity execution data passed to [`ActivityInboundInterceptor::execute_activity`].
#[non_exhaustive]
pub struct ExecuteActivityInput {
    context: ActivityContext,
    args: Box<dyn Any + Send + Sync>,
}

impl ExecuteActivityInput {
    pub(crate) fn new(context: ActivityContext, args: Box<dyn Any + Send + Sync>) -> Self {
        Self { context, args }
    }

    pub(crate) fn into_parts(self) -> (ActivityContext, Box<dyn Any + Send + Sync>) {
        (self.context, self.args)
    }

    /// Information about the activity execution.
    pub fn activity_info(&self) -> &ActivityInfo {
        self.context.info()
    }

    /// Headers attached to this activity.
    pub fn headers(&self) -> &HashMap<String, Payload> {
        self.context.headers()
    }

    /// Mutably access headers attached to this activity.
    pub fn headers_mut(&mut self) -> &mut HashMap<String, Payload> {
        self.context.headers_mut()
    }

    /// Attempt to access the decoded activity arguments as a concrete type.
    pub fn args_ref<T: Any>(&self) -> Option<&T> {
        self.args.downcast_ref()
    }

    /// Attempt to mutably access the decoded activity arguments as a concrete type.
    pub fn args_mut<T: Any>(&mut self) -> Option<&mut T> {
        self.args.downcast_mut()
    }
}

/// Type-erased activity output carried through the activity interceptor chain.
pub trait ActivityExecutionValue:
    Any + TemporalSerializable + Send + Sync + activity_execution_value::Sealed
{
    /// Access this value as [`Any`] for type-specific inspection.
    fn as_any(&self) -> &dyn Any;
}

impl<T> ActivityExecutionValue for T
where
    T: Any + TemporalSerializable + Send + Sync,
{
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl dyn ActivityExecutionValue {
    /// Attempt to access the activity output as a concrete type.
    pub fn downcast_ref<T: Any>(&self) -> Option<&T> {
        self.as_any().downcast_ref()
    }

    pub(crate) fn serialize_payload(
        &self,
        context: &SerializationContext<'_>,
    ) -> Result<Payload, PayloadConversionError> {
        self.to_activity_payload(context)
    }
}

/// Result of an activity execution carried through the interceptor chain.
pub type ExecuteActivityResult = Result<Box<dyn ActivityExecutionValue>, ActivityError>;

/// Future produced by activity inbound interceptors.
pub type ExecuteActivityOutput<'a> = BoxFuture<'a, ExecuteActivityResult>;

/// Inbound interceptor for activity calls coming from the server.
///
/// Must be implemented by inbound activity interceptors.
pub trait ActivityInboundInterceptor: Send + Sync + 'static {
    /// Called to invoke the activity.
    fn execute_activity<'a>(
        &'a self,
        input: ExecuteActivityInput,
        next: Next<'a, ExecuteActivityInput, ExecuteActivityOutput<'a>>,
    ) -> ExecuteActivityOutput<'a> {
        next.run(input)
    }
}

/// An interceptor which causes the worker's run function to exit early if nondeterminism errors are
/// encountered
pub struct FailOnNondeterminismInterceptor {}
#[async_trait::async_trait(?Send)]
impl WorkerInterceptor for FailOnNondeterminismInterceptor {
    async fn on_workflow_activation(
        &self,
        activation: &WorkflowActivation,
    ) -> Result<(), anyhow::Error> {
        if matches!(
            activation.eviction_reason(),
            Some(EvictionReason::Nondeterminism)
        ) {
            bail!("Workflow is being evicted because of nondeterminism! {activation}");
        }
        Ok(())
    }
}

/// An interceptor that allows you to fetch the exit value of the workflow if and when it is set
#[derive(Default)]
pub struct ReturnWorkflowExitValueInterceptor {
    result_value: Arc<OnceLock<Payload>>,
}

impl ReturnWorkflowExitValueInterceptor {
    /// Can be used to fetch the workflow result if/when it is determined
    pub fn result_handle(&self) -> Arc<OnceLock<Payload>> {
        self.result_value.clone()
    }
}

#[async_trait::async_trait(?Send)]
impl WorkerInterceptor for ReturnWorkflowExitValueInterceptor {
    async fn on_workflow_activation_completion(&self, c: &WorkflowActivationCompletion) {
        if let Some(v) = c.complete_workflow_execution_value() {
            let _ = self.result_value.set(v.clone());
        }
    }
}
