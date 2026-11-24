//! Intercept inbound and outbound calls made during workflow execution.
//!
//! Workflow interceptors allow observing, transforming, or short-circuit workflow
//! operations without putting that behavior in each workflow implementation.
//!
//! [`WorkflowInterceptor`] has two groups of methods:
//!
//! - Inbound methods wrap calls into workflow code, such as executing the workflow or handling
//!   a signal, query, or update.
//! - Outbound methods wrap commands issued by workflow code, such as scheduling an activity,
//!   starting a timer, or signaling another workflow.
//!
//! Each method receives a [`WorkflowNext`] continuation. An interceptor can change the input
//! before calling [`WorkflowNext::run`], inspect or change the returned value, or deliberately not
//! call `next` to short-circuit the operation. Most interceptors should call `next` exactly once.
//!
//! Async operation interceptors return [`WorkflowInterceptorFuture`]. Wrap an `async` block with
//! [`WorkflowInterceptorFuture::new`] when work must happen after the next interceptor completes.
//! Synchronous methods, including queries and update validators, cannot await workflow operations.
//!
//! Workers register interceptors with `register_workflow_interceptors` on their
//! worker options. Interceptors are entered in insertion order for inbound calls and in reverse
//! insertion order for outbound calls.
//!
//! # Determinism
//!
//! Interceptors execute as part of the workflow and are replayed with it. They must follow the same
//! determinism rules as workflow code: do not read wall-clock time, perform network or filesystem
//! I/O, use nondeterministic randomness, or await arbitrary futures. Use values from the
//! interceptor context and SDK-provided workflow futures instead. [`WorkflowInterceptorFuture`]
//! identifies a future for the workflow scheduler; it does not make an arbitrary future
//! deterministic.
//!
//! [`WorkflowInterceptorContext::is_replaying`] and
//! [`WorkflowInterceptorContext::is_replaying_history_events`] can be used to suppress duplicate external
//! observability during replay, but replay state must not change commands or results that affect
//! workflow behavior.
//!
//! # Example
//!
//! This interceptor wraps workflow execution and transforms string outputs after the workflow has
//! completed. The constructor is passed to the worker during worker setup.
//!
//! ```
//! # use temporalio_workflow::{
//! #     WorkflowContextView,
//! #     workflow_interceptors::{
//! #         ExecuteWorkflowInput, ExecuteWorkflowResult, WorkflowInterceptor,
//! #         WorkflowInterceptorConstructor, WorkflowInterceptorContext, WorkflowInterceptorFuture,
//! #         WorkflowNext, WorkflowOutputValue,
//! #     },
//! # };
//!
//! struct UppercaseStringOutput;
//!
//! impl WorkflowInterceptor for UppercaseStringOutput {
//!     fn execute<'a>(
//!         &'a self,
//!         _ctx: WorkflowInterceptorContext,
//!         input: ExecuteWorkflowInput,
//!         next: WorkflowNext<
//!             'a,
//!             ExecuteWorkflowInput,
//!             WorkflowInterceptorFuture<'a, ExecuteWorkflowResult>,
//!         >,
//!     ) -> WorkflowInterceptorFuture<'a, ExecuteWorkflowResult> {
//!         WorkflowInterceptorFuture::new(async move {
//!             let output = next.run(input).await?;
//!             if let Some(value) = output.downcast_ref::<String>() {
//!                 return Ok(Box::new(value.to_uppercase()) as Box<dyn WorkflowOutputValue>);
//!             }
//!             Ok(output)
//!         })
//!     }
//! }
//!
//! fn interceptor_constructor() -> WorkflowInterceptorConstructor {
//!     WorkflowInterceptorConstructor::new(|_ctx: &WorkflowContextView| UppercaseStringOutput)
//! }
//!
//! # let _ = interceptor_constructor();
//! ```

use crate::{
    ActivityOptions, BaseWorkflowContext, CancellableFuture, CancellableFutureWithReason,
    ChildWorkflowOptions, ContinueAsNewOptions, ExternalWorkflowHandle, LocalActivityOptions,
    NexusOperationOptions, SignalWorkflowOptions, StartChildWorkflowOutput, StartedChildWorkflow,
    StartedNexusOperation, TimerOptions, WorkflowCancellationToken, WorkflowContextView,
    cancellation::WorkflowCancellationRegistration,
    runtime::{
        entry::WorkflowError,
        model::{
            CancelExternalWfResult, NexusStartResult, TimerResult, WorkflowResult,
            WorkflowTermination,
        },
    },
};
use futures_util::{
    FutureExt,
    future::{Fuse, FusedFuture, LocalBoxFuture},
};
use std::{
    any::Any,
    collections::HashMap,
    convert::Infallible,
    future::Future,
    pin::Pin,
    rc::Rc,
    sync::Arc,
    task::{Context, Poll},
    time::SystemTime,
};
use temporalio_common_wasm::{
    ActivityDefinition, WorkflowDefinition,
    data_converters::{
        GenericPayloadConverter, PayloadConversionError, PayloadConverter, SerializationContext,
        SerializationContextData, TemporalDeserializable, TemporalSerializable,
    },
    error::{
        ActivityExecutionError, ChildWorkflowExecutionError, ChildWorkflowStartError,
        WorkflowSignalError,
    },
    protos::temporal::api::{common::v1::Payload, failure::v1::Failure},
    search_attributes::SearchAttributes,
};

mod workflow_output_value {
    use super::*;

    pub trait Sealed {
        fn to_workflow_payload(
            &self,
            context: &SerializationContext<'_>,
        ) -> Result<Payload, PayloadConversionError>;
    }

    impl<T> Sealed for T
    where
        T: Any + TemporalSerializable,
    {
        fn to_workflow_payload(
            &self,
            context: &SerializationContext<'_>,
        ) -> Result<Payload, PayloadConversionError> {
            context.converter.to_payload(context, self)
        }
    }
}

/// Type-erased workflow output carried through the workflow interceptor chain.
pub trait WorkflowOutputValue: Any + TemporalSerializable + workflow_output_value::Sealed {
    /// Access this value as [`Any`] for type-specific inspection.
    fn as_any(&self) -> &dyn Any;
}

impl<T> WorkflowOutputValue for T
where
    T: Any + TemporalSerializable,
{
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl dyn WorkflowOutputValue {
    /// Attempt to access the workflow output as a concrete type.
    pub fn downcast_ref<T: Any>(&self) -> Option<&T> {
        self.as_any().downcast_ref()
    }

    pub(crate) fn serialize_payload(
        &self,
        context: &SerializationContext<'_>,
    ) -> Result<Payload, PayloadConversionError> {
        self.to_workflow_payload(context)
    }
}

pub(crate) fn serialize_workflow_output(
    output: &dyn WorkflowOutputValue,
    converter: &PayloadConverter,
) -> Result<Payload, PayloadConversionError> {
    let ctx = SerializationContext {
        data: &SerializationContextData::Workflow,
        converter,
    };
    output.serialize_payload(&ctx)
}

/// Result of an intercepted workflow execution.
pub type ExecuteWorkflowResult = WorkflowResult<Box<dyn WorkflowOutputValue>>;

/// Result of an intercepted signal handler.
pub type HandleSignalResult = Result<(), WorkflowError>;

/// Result of an intercepted update handler.
pub type HandleUpdateResult = Result<Box<dyn WorkflowOutputValue>, WorkflowError>;

/// Result of an intercepted query handler.
pub type HandleQueryResult = Result<Box<dyn WorkflowOutputValue>, WorkflowError>;

/// Result of an intercepted update validator.
pub type ValidateUpdateResult = Result<(), WorkflowError>;

/// Future produced by workflow interceptors.
///
/// The SDK polls a newly created interceptor future once while processing its activation. This
/// runs synchronous interceptor and synchronous handler work through the first genuine
/// [`Poll::Pending`]. Async workflow and handler bodies are not entered
/// until normal routine polling. Awaiting a pending workflow future before calling
/// [`WorkflowNext::run`] intentionally delays the underlying handler.
///
/// This type identifies futures that are polled inside workflow execution. It does not make
/// arbitrary Rust futures deterministic. Interceptor implementations must only await workflow
/// scheduler primitives or SDK-provided workflow futures.
pub struct WorkflowInterceptorFuture<'a, T>(LocalBoxFuture<'a, T>);

impl<'a, T> WorkflowInterceptorFuture<'a, T> {
    /// Create a workflow interceptor future from a local future.
    pub fn new(fut: impl Future<Output = T> + 'a) -> Self {
        Self(fut.boxed_local())
    }
}

impl<'a, T> Unpin for WorkflowInterceptorFuture<'a, T> {}

impl<T> Future for WorkflowInterceptorFuture<'_, T> {
    type Output = T;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.0.as_mut().poll(cx)
    }
}

/// Continuation for a workflow interceptor operation.
pub struct WorkflowNext<'a, I, O> {
    inner: Box<dyn FnOnce(I) -> O + 'a>,
}

impl<'a, I, O> WorkflowNext<'a, I, O> {
    pub(crate) fn new(f: impl FnOnce(I) -> O + 'a) -> Self {
        Self { inner: Box::new(f) }
    }

    /// Continue the call chain with the provided input.
    pub fn run(self, input: I) -> O {
        (self.inner)(input)
    }
}

/// Workflow execution context available to async-capable inbound interceptors.
#[derive(Clone)]
pub struct WorkflowInterceptorContext {
    base: BaseWorkflowContext,
}

impl WorkflowInterceptorContext {
    pub(crate) fn new(base: BaseWorkflowContext) -> Self {
        Self { base }
    }

    /// Return the workflow's unique identifier.
    pub fn workflow_id(&self) -> &str {
        self.base.workflow_id()
    }

    /// Return the run id of this workflow execution.
    pub fn run_id(&self) -> &str {
        self.base.run_id()
    }

    /// Return the namespace the workflow is executing in.
    pub fn namespace(&self) -> &str {
        self.base.namespace()
    }

    /// Return the task queue the workflow is executing in.
    pub fn task_queue(&self) -> &str {
        self.base.task_queue()
    }

    /// Return the workflow type name.
    pub fn workflow_type(&self) -> &str {
        self.base.workflow_type()
    }

    /// Return the current time according to the workflow.
    pub fn workflow_time(&self) -> Option<SystemTime> {
        self.base.workflow_time()
    }

    /// Return the length of history so far at this point in the workflow.
    pub fn history_length(&self) -> u32 {
        self.base.history_length()
    }

    /// Return current values for workflow search attributes.
    pub fn search_attributes(&self) -> SearchAttributes {
        self.base.search_attributes()
    }

    /// Returns true if the workflow is replaying (including during queries and update validators), false otherwise.
    pub fn is_replaying(&self) -> bool {
        self.base.is_replaying()
    }

    /// Return true if the workflow is replaying history events (excluding queries and update validators), false otherwise.
    pub fn is_replaying_history_events(&self) -> bool {
        self.base.is_replaying_history_events()
    }

    /// Returns the payload converter used by the worker running this workflow.
    pub fn payload_converter(&self) -> &PayloadConverter {
        self.base.payload_converter()
    }

    /// Return the workflow's root cancellation token.
    pub fn cancellation_token(&self) -> WorkflowCancellationToken {
        self.base.cancellation_token()
    }

    /// Request to create a timer through the workflow outbound interceptor chain.
    pub fn timer<T: Into<TimerOptions>>(
        &self,
        opts: T,
    ) -> impl CancellableFuture<Output = TimerResult> + use<T> {
        self.base.timer(opts)
    }

    /// Request to run an activity through the workflow outbound interceptor chain.
    pub fn execute_activity<AD: ActivityDefinition>(
        &self,
        activity: AD,
        input: impl Into<AD::Input>,
        opts: ActivityOptions,
    ) -> impl CancellableFuture<Output = Result<AD::Output, ActivityExecutionError>>
    where
        AD::Output: TemporalDeserializable,
    {
        self.base.execute_activity(activity, input, opts)
    }

    /// Request to run a local activity through the workflow outbound interceptor chain.
    pub fn execute_local_activity<AD: ActivityDefinition>(
        &self,
        activity: AD,
        input: impl Into<AD::Input>,
        opts: LocalActivityOptions,
    ) -> impl CancellableFuture<Output = Result<AD::Output, ActivityExecutionError>>
    where
        AD::Output: TemporalDeserializable,
    {
        self.base.execute_local_activity(activity, input, opts)
    }

    /// Start a child workflow through the workflow outbound interceptor chain.
    pub fn start_child_workflow<WD: WorkflowDefinition + 'static>(
        &self,
        workflow: WD,
        input: impl Into<WD::Input>,
        opts: ChildWorkflowOptions,
    ) -> impl CancellableFutureWithReason<
        Output = Result<StartedChildWorkflow<WD>, ChildWorkflowStartError>,
    >
    where
        WD::Output: TemporalDeserializable,
    {
        self.base.start_child_workflow(workflow, input, opts)
    }

    /// Get a handle to an external workflow for signaling or requesting cancellation.
    pub fn external_workflow(
        &self,
        workflow_id: impl Into<String>,
        run_id: Option<String>,
    ) -> ExternalWorkflowHandle {
        self.base.external_workflow(workflow_id, run_id)
    }

    /// Start a Nexus operation through the workflow outbound interceptor chain.
    pub fn start_nexus_operation(
        &self,
        opts: NexusOperationOptions,
    ) -> impl CancellableFuture<Output = NexusStartResult> {
        self.base.start_nexus_operation(opts)
    }
}

/// Workflow execution context available to sync-only inbound interceptors.
#[derive(Clone)]
pub struct SyncWorkflowInterceptorContext {
    base: BaseWorkflowContext,
}

impl SyncWorkflowInterceptorContext {
    pub(crate) fn new(base: BaseWorkflowContext) -> Self {
        Self { base }
    }

    /// Return the workflow's unique identifier.
    pub fn workflow_id(&self) -> &str {
        self.base.workflow_id()
    }

    /// Return the run id of this workflow execution.
    pub fn run_id(&self) -> &str {
        self.base.run_id()
    }

    /// Return the namespace the workflow is executing in.
    pub fn namespace(&self) -> &str {
        self.base.namespace()
    }

    /// Return the task queue the workflow is executing in.
    pub fn task_queue(&self) -> &str {
        self.base.task_queue()
    }

    /// Return the workflow type name.
    pub fn workflow_type(&self) -> &str {
        self.base.workflow_type()
    }

    /// Return the workflow's root cancellation token.
    pub fn cancellation_token(&self) -> WorkflowCancellationToken {
        self.base.cancellation_token()
    }

    /// Return the current time according to the workflow.
    pub fn workflow_time(&self) -> Option<SystemTime> {
        self.base.workflow_time()
    }

    /// Return the length of history so far at this point in the workflow.
    pub fn history_length(&self) -> u32 {
        self.base.history_length()
    }

    /// Return current values for workflow search attributes.
    pub fn search_attributes(&self) -> SearchAttributes {
        self.base.search_attributes()
    }

    /// Returns true if the current workflow task is happening under replay.
    pub fn is_replaying(&self) -> bool {
        self.base.is_replaying()
    }

    /// Returns true if the current work is replaying history events.
    pub fn is_replaying_history_events(&self) -> bool {
        self.base.is_replaying_history_events()
    }

    /// Returns the payload converter used by the worker running this workflow.
    pub fn payload_converter(&self) -> &PayloadConverter {
        self.base.payload_converter()
    }
}

struct DecodedInput {
    value: Option<Box<dyn Any>>,
    headers: HashMap<String, Payload>,
}

impl DecodedInput {
    fn new(value: Option<Box<dyn Any>>, headers: HashMap<String, Payload>) -> Self {
        Self { value, headers }
    }

    fn input_ref<T: Any>(&self) -> Option<&T> {
        self.value.as_ref()?.downcast_ref()
    }

    fn input_mut<T: Any>(&mut self) -> Option<&mut T> {
        self.value.as_mut()?.downcast_mut()
    }

    fn headers(&self) -> &HashMap<String, Payload> {
        &self.headers
    }

    fn headers_mut(&mut self) -> &mut HashMap<String, Payload> {
        &mut self.headers
    }

    fn into_parts(self) -> (Option<Box<dyn Any>>, HashMap<String, Payload>) {
        (self.value, self.headers)
    }
}

/// Input passed to [`WorkflowInterceptor::initialize_workflow`].
///
/// The decoded input provided to workflow's `#[init]` method.
/// If a workflow has no `#[init]`, inputs are instead passed to [`WorkflowInterceptor::execute`].
#[non_exhaustive]
pub struct InitializeWorkflowInput {
    decoded: DecodedInput,
}

impl InitializeWorkflowInput {
    pub(crate) fn new(value: Option<Box<dyn Any>>, headers: HashMap<String, Payload>) -> Self {
        Self {
            decoded: DecodedInput::new(value, headers),
        }
    }

    pub(crate) fn into_parts(self) -> (Option<Box<dyn Any>>, HashMap<String, Payload>) {
        self.decoded.into_parts()
    }

    /// Attempt to access the decoded workflow input as a concrete type.
    pub fn input_ref<T: Any>(&self) -> Option<&T> {
        self.decoded.input_ref()
    }

    /// Attempt to mutably access the decoded workflow input as a concrete type.
    pub fn input_mut<T: Any>(&mut self) -> Option<&mut T> {
        self.decoded.input_mut()
    }

    /// Headers attached to the workflow execution.
    pub fn headers(&self) -> &HashMap<String, Payload> {
        self.decoded.headers()
    }

    /// Mutably access headers attached to the workflow execution.
    pub fn headers_mut(&mut self) -> &mut HashMap<String, Payload> {
        self.decoded.headers_mut()
    }
}

/// Result of workflow initialization.
pub struct InitializeWorkflowOutput {
    _private: (),
}

impl InitializeWorkflowOutput {
    pub(crate) fn new() -> Self {
        Self { _private: () }
    }
}

/// Input passed to [`WorkflowInterceptor::execute`].
///
/// The decoded input provided to workflow's `#[run]` method.
/// Inputs consumed by `#[init]` are instead passed to [`WorkflowInterceptor::initialize_workflow`].
#[non_exhaustive]
pub struct ExecuteWorkflowInput {
    decoded: DecodedInput,
}

impl ExecuteWorkflowInput {
    pub(crate) fn new(value: Option<Box<dyn Any>>, headers: HashMap<String, Payload>) -> Self {
        Self {
            decoded: DecodedInput::new(value, headers),
        }
    }

    pub(crate) fn into_parts(self) -> (Option<Box<dyn Any>>, HashMap<String, Payload>) {
        self.decoded.into_parts()
    }

    /// Attempt to access the decoded workflow input as a concrete type.
    pub fn input_ref<T: Any>(&self) -> Option<&T> {
        self.decoded.input_ref()
    }

    /// Attempt to mutably access the decoded workflow input as a concrete type.
    pub fn input_mut<T: Any>(&mut self) -> Option<&mut T> {
        self.decoded.input_mut()
    }

    /// Headers attached to the workflow execution.
    pub fn headers(&self) -> &HashMap<String, Payload> {
        self.decoded.headers()
    }

    /// Mutably access headers attached to the workflow execution.
    pub fn headers_mut(&mut self) -> &mut HashMap<String, Payload> {
        self.decoded.headers_mut()
    }
}

macro_rules! handler_input {
    ($name:ident, $doc:literal, $field:ident, $field_doc:literal $(, $id_field:ident, $id_doc:literal)?) => {
        #[doc = $doc]
        #[non_exhaustive]
        pub struct $name {
            $($id_field: String,)?
            $field: String,
            decoded: DecodedInput,
        }

        impl $name {
            pub(crate) fn new(
                $($id_field: String,)?
                $field: String,
                value: Box<dyn Any>,
                headers: HashMap<String, Payload>,
            ) -> Self {
                Self {
                    $($id_field,)?
                    $field,
                    decoded: DecodedInput::new(Some(value), headers),
                }
            }

            pub(crate) fn into_parts(self) -> (String, Box<dyn Any>, HashMap<String, Payload>) {
                let (value, headers) = self.decoded.into_parts();
                (
                    self.$field,
                    value.expect("handler input must exist after typed decode"),
                    headers,
                )
            }

            #[doc = $field_doc]
            pub fn name(&self) -> &str {
                &self.$field
            }

            $(
                #[doc = $id_doc]
                pub fn id(&self) -> &str {
                    &self.$id_field
                }
            )?

            /// Attempt to access the decoded input as a concrete type.
            pub fn input_ref<T: Any>(&self) -> Option<&T> {
                self.decoded.input_ref()
            }

            /// Attempt to mutably access the decoded input as a concrete type.
            pub fn input_mut<T: Any>(&mut self) -> Option<&mut T> {
                self.decoded.input_mut()
            }

            /// Headers attached to this handler invocation.
            pub fn headers(&self) -> &HashMap<String, Payload> {
                self.decoded.headers()
            }

            /// Mutably access headers attached to this handler invocation.
            pub fn headers_mut(&mut self) -> &mut HashMap<String, Payload> {
                self.decoded.headers_mut()
            }
        }
    };
}

handler_input!(
    HandleSignalInput,
    "Input passed to [`WorkflowInterceptor::handle_signal`].",
    signal_name,
    "Return the signal name."
);

handler_input!(
    HandleUpdateInput,
    "Input passed to [`WorkflowInterceptor::handle_update`].",
    update_name,
    "Return the update name.",
    update_id,
    "Return the update ID."
);

handler_input!(
    HandleQueryInput,
    "Input passed to [`WorkflowInterceptor::handle_query`].",
    query_name,
    "Return the query name.",
    query_id,
    "Return the query ID."
);

/// Input passed to [`WorkflowInterceptor::validate_update`].
#[non_exhaustive]
pub struct ValidateUpdateInput {
    update_id: String,
    update_name: String,
    decoded: DecodedInput,
}

impl ValidateUpdateInput {
    pub(crate) fn new(
        update_id: String,
        update_name: String,
        value: Box<dyn Any>,
        headers: HashMap<String, Payload>,
    ) -> Self {
        Self {
            update_id,
            update_name,
            decoded: DecodedInput::new(Some(value), headers),
        }
    }

    pub(crate) fn into_parts(self) -> (String, Box<dyn Any>, HashMap<String, Payload>) {
        let (value, headers) = self.decoded.into_parts();
        (
            self.update_name,
            value.expect("update validation input must exist after typed decode"),
            headers,
        )
    }

    /// Return the update name.
    pub fn name(&self) -> &str {
        &self.update_name
    }

    /// Return the update ID.
    pub fn id(&self) -> &str {
        &self.update_id
    }

    /// Attempt to access the decoded input as a concrete type.
    pub fn input_ref<T: Any>(&self) -> Option<&T> {
        self.decoded.input_ref()
    }

    /// Attempt to mutably access the decoded input as a concrete type.
    pub fn input_mut<T: Any>(&mut self) -> Option<&mut T> {
        self.decoded.input_mut()
    }

    /// Headers attached to this update invocation.
    pub fn headers(&self) -> &HashMap<String, Payload> {
        self.decoded.headers()
    }

    /// Mutably access headers attached to this update invocation.
    pub fn headers_mut(&mut self) -> &mut HashMap<String, Payload> {
        self.decoded.headers_mut()
    }
}

/// Type-erased output returned by an intercepted outbound workflow call.
pub trait WorkflowOutboundValue: Any {
    /// Access the concrete value through [`Any`].
    fn as_any(&self) -> &dyn Any;

    /// Convert this value into [`Any`] for a consuming downcast.
    fn into_any(self: Box<Self>) -> Box<dyn Any>;
}

impl<T: Any> WorkflowOutboundValue for T {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

impl dyn WorkflowOutboundValue {
    /// Attempt to access the output as a concrete type.
    pub fn downcast_ref<T: Any>(&self) -> Option<&T> {
        self.as_any().downcast_ref()
    }

    /// Attempt to convert the output into a concrete type.
    pub fn downcast<T: Any>(self: Box<Self>) -> Result<Box<T>, Box<dyn Any>> {
        self.into_any().downcast()
    }
}

/// Future returned by a non-cancellable outbound interceptor operation.
pub struct WorkflowOutboundFuture<T> {
    state: WorkflowOutboundFutureState<T>,
}

enum WorkflowOutboundFutureState<T> {
    Running(Fuse<LocalBoxFuture<'static, T>>),
    Prefetched(Option<T>),
    Terminated,
}

impl<T> WorkflowOutboundFuture<T> {
    /// Create an outbound future.
    pub fn new(future: impl Future<Output = T> + 'static) -> Self {
        Self {
            state: WorkflowOutboundFutureState::Running(future.boxed_local().fuse()),
        }
    }

    /// Create an immediately ready outbound future.
    pub fn ready(value: T) -> Self
    where
        T: 'static,
    {
        Self::new(async move { value })
    }

    /// Transform the result of this future.
    pub fn map<U>(self, map: impl FnOnce(T) -> U + 'static) -> WorkflowOutboundFuture<U>
    where
        T: 'static,
        U: 'static,
    {
        WorkflowOutboundFuture::new(async move { map(self.await) })
    }

    pub(crate) fn poll_for_construction(&mut self, cx: &mut Context<'_>) {
        let WorkflowOutboundFutureState::Running(future) = &mut self.state else {
            return;
        };
        if let Poll::Ready(value) = future.poll_unpin(cx) {
            self.state = WorkflowOutboundFutureState::Prefetched(Some(value));
        }
    }
}

impl<T> Unpin for WorkflowOutboundFuture<T> {}

impl<T> Future for WorkflowOutboundFuture<T> {
    type Output = T;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match &mut self.state {
            WorkflowOutboundFutureState::Running(future) => {
                let result = future.poll_unpin(cx);
                if result.is_ready() {
                    self.state = WorkflowOutboundFutureState::Terminated;
                }
                result
            }
            WorkflowOutboundFutureState::Prefetched(value) => {
                let value = value
                    .take()
                    .expect("outbound future polled after completion");
                self.state = WorkflowOutboundFutureState::Terminated;
                Poll::Ready(value)
            }
            WorkflowOutboundFutureState::Terminated => {
                panic!("outbound future polled after completion")
            }
        }
    }
}

impl<T> FusedFuture for WorkflowOutboundFuture<T> {
    fn is_terminated(&self) -> bool {
        matches!(self.state, WorkflowOutboundFutureState::Terminated)
    }
}

/// Cancellation callback retained when an interceptor wraps an operation future.
#[derive(Clone)]
pub struct WorkflowCancellationHandle {
    cancel: Rc<dyn Fn(Option<String>)>,
}

impl WorkflowCancellationHandle {
    /// Create a cancellation handle.
    pub fn new(cancel: impl Fn(Option<String>) + 'static) -> Self {
        Self {
            cancel: Rc::new(cancel),
        }
    }

    pub(crate) fn noop() -> Self {
        Self::new(|_| {})
    }

    /// Cancel with an optional reason.
    pub fn cancel(&self, reason: Option<String>) {
        (self.cancel)(reason);
    }
}

/// Future returned by a cancellable outbound interceptor operation.
pub struct CancellableWorkflowOutboundFuture<T> {
    inner: WorkflowOutboundFuture<T>,
    cancellation: WorkflowCancellationHandle,
    cancellation_registration: Option<WorkflowCancellationRegistration>,
}

impl<T> CancellableWorkflowOutboundFuture<T> {
    /// Create a cancellable outbound future.
    pub fn new(
        future: impl Future<Output = T> + 'static,
        cancellation: WorkflowCancellationHandle,
    ) -> Self {
        Self {
            inner: WorkflowOutboundFuture::new(future),
            cancellation,
            cancellation_registration: None,
        }
    }

    pub(crate) fn with_cancellation_token(mut self, token: WorkflowCancellationToken) -> Self {
        let cancellation = self.cancellation.clone();
        self.cancellation_registration = Some(token.register(move |reason| {
            cancellation.cancel(reason);
        }));
        self
    }

    pub(crate) fn unregister_cancellation(&mut self) {
        if let Some(registration) = &mut self.cancellation_registration {
            registration.unregister();
        }
    }

    /// Return the operation's cancellation handle.
    pub fn cancellation_handle(&self) -> WorkflowCancellationHandle {
        self.cancellation.clone()
    }

    /// Transform the result while retaining cancellation behavior.
    pub fn map<U>(self, map: impl FnOnce(T) -> U + 'static) -> CancellableWorkflowOutboundFuture<U>
    where
        T: 'static,
        U: 'static,
    {
        let cancellation = self.cancellation.clone();
        CancellableWorkflowOutboundFuture::new(async move { map(self.await) }, cancellation)
    }

    pub(crate) fn poll_for_construction(&mut self, cx: &mut Context<'_>) {
        self.inner.poll_for_construction(cx);
    }
}

impl<T> Unpin for CancellableWorkflowOutboundFuture<T> {}

impl<T> Future for CancellableWorkflowOutboundFuture<T> {
    type Output = T;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let result = Pin::new(&mut self.inner).poll(cx);
        if result.is_ready() {
            self.unregister_cancellation();
        }
        result
    }
}

impl<T> FusedFuture for CancellableWorkflowOutboundFuture<T> {
    fn is_terminated(&self) -> bool {
        self.inner.is_terminated()
    }
}

impl<T> CancellableFuture for CancellableWorkflowOutboundFuture<T> {
    fn cancel(&self) {
        if !self.inner.is_terminated() {
            self.cancellation.cancel(None);
        }
    }
}

impl<T> CancellableFutureWithReason for CancellableWorkflowOutboundFuture<T> {
    fn cancel_with_reason(&self, reason: String) {
        if !self.inner.is_terminated() {
            self.cancellation.cancel(Some(reason));
        }
    }
}

macro_rules! typed_outbound_input {
    ($name:ident) => {
        impl $name {
            /// Attempt to access the decoded input as a concrete type.
            pub fn input_ref<T: Any>(&self) -> Option<&T> {
                self.decoded.input_ref()
            }

            /// Attempt to mutably access the decoded input as a concrete type.
            pub fn input_mut<T: Any>(&mut self) -> Option<&mut T> {
                self.decoded.input_mut()
            }

            /// Headers attached to this outbound call.
            pub fn headers(&self) -> &HashMap<String, Payload> {
                self.decoded.headers()
            }

            /// Mutably access headers attached to this outbound call.
            pub fn headers_mut(&mut self) -> &mut HashMap<String, Payload> {
                self.decoded.headers_mut()
            }
        }
    };
}

/// Input passed to [`WorkflowInterceptor::start_timer`].
#[non_exhaustive]
pub struct StartTimerInput {
    options: TimerOptions,
}

impl StartTimerInput {
    pub(crate) fn new(options: TimerOptions) -> Self {
        Self { options }
    }

    pub(crate) fn into_options(self) -> TimerOptions {
        self.options
    }

    /// Timer options.
    pub fn options(&self) -> &TimerOptions {
        &self.options
    }

    /// Mutably access timer options.
    pub fn options_mut(&mut self) -> &mut TimerOptions {
        &mut self.options
    }
}

/// Input passed to [`WorkflowInterceptor::schedule_activity`].
#[non_exhaustive]
pub struct ScheduleActivityInput {
    activity_type: String,
    decoded: DecodedInput,
    options: ActivityOptions,
}

impl ScheduleActivityInput {
    pub(crate) fn new(
        activity_type: String,
        input: Box<dyn Any>,
        options: ActivityOptions,
    ) -> Self {
        Self {
            activity_type,
            decoded: DecodedInput::new(Some(input), HashMap::new()),
            options,
        }
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        String,
        Box<dyn Any>,
        HashMap<String, Payload>,
        ActivityOptions,
    ) {
        let (input, headers) = self.decoded.into_parts();
        (
            self.activity_type,
            input.expect("activity input must exist"),
            headers,
            self.options,
        )
    }

    /// Activity type.
    pub fn activity_type(&self) -> &str {
        &self.activity_type
    }

    /// Mutably access the activity type.
    pub fn activity_type_mut(&mut self) -> &mut String {
        &mut self.activity_type
    }

    /// Activity options.
    pub fn options(&self) -> &ActivityOptions {
        &self.options
    }

    /// Mutably access activity options.
    pub fn options_mut(&mut self) -> &mut ActivityOptions {
        &mut self.options
    }
}

typed_outbound_input!(ScheduleActivityInput);

/// Input passed to [`WorkflowInterceptor::schedule_local_activity`].
#[non_exhaustive]
pub struct ScheduleLocalActivityInput {
    activity_type: String,
    decoded: DecodedInput,
    options: LocalActivityOptions,
}

impl ScheduleLocalActivityInput {
    pub(crate) fn new(
        activity_type: String,
        input: Box<dyn Any>,
        options: LocalActivityOptions,
    ) -> Self {
        Self {
            activity_type,
            decoded: DecodedInput::new(Some(input), HashMap::new()),
            options,
        }
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        String,
        Box<dyn Any>,
        HashMap<String, Payload>,
        LocalActivityOptions,
    ) {
        let (input, headers) = self.decoded.into_parts();
        (
            self.activity_type,
            input.expect("local activity input must exist"),
            headers,
            self.options,
        )
    }

    /// Activity type.
    pub fn activity_type(&self) -> &str {
        &self.activity_type
    }

    /// Mutably access the activity type.
    pub fn activity_type_mut(&mut self) -> &mut String {
        &mut self.activity_type
    }

    /// Local activity options.
    pub fn options(&self) -> &LocalActivityOptions {
        &self.options
    }

    /// Mutably access local activity options.
    pub fn options_mut(&mut self) -> &mut LocalActivityOptions {
        &mut self.options
    }
}

typed_outbound_input!(ScheduleLocalActivityInput);

/// Input passed to [`WorkflowInterceptor::start_child_workflow`].
#[non_exhaustive]
pub struct StartChildWorkflowInput {
    workflow_type: String,
    decoded: DecodedInput,
    options: ChildWorkflowOptions,
}

impl StartChildWorkflowInput {
    pub(crate) fn new(
        workflow_type: String,
        input: Box<dyn Any>,
        options: ChildWorkflowOptions,
    ) -> Self {
        Self {
            workflow_type,
            decoded: DecodedInput::new(Some(input), HashMap::new()),
            options,
        }
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        String,
        Box<dyn Any>,
        HashMap<String, Payload>,
        ChildWorkflowOptions,
    ) {
        let (input, headers) = self.decoded.into_parts();
        (
            self.workflow_type,
            input.expect("child workflow input must exist"),
            headers,
            self.options,
        )
    }

    /// Workflow type.
    pub fn workflow_type(&self) -> &str {
        &self.workflow_type
    }

    /// Mutably access the workflow type.
    pub fn workflow_type_mut(&mut self) -> &mut String {
        &mut self.workflow_type
    }

    /// Child workflow options.
    pub fn options(&self) -> &ChildWorkflowOptions {
        &self.options
    }

    /// Mutably access child workflow options.
    pub fn options_mut(&mut self) -> &mut ChildWorkflowOptions {
        &mut self.options
    }
}

typed_outbound_input!(StartChildWorkflowInput);

/// Workflow targeted by an outbound signal.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum SignalWorkflowTarget {
    /// A child workflow identified by workflow ID.
    Child {
        /// Child workflow ID.
        workflow_id: String,
    },
    /// An external workflow execution.
    External {
        /// Target namespace.
        namespace: String,
        /// Target workflow ID.
        workflow_id: String,
        /// Target run ID, or the latest run when absent.
        run_id: Option<String>,
    },
}

/// Input passed to [`WorkflowInterceptor::signal_workflow`].
#[non_exhaustive]
pub struct SignalWorkflowInput {
    signal_name: String,
    target: SignalWorkflowTarget,
    decoded: DecodedInput,
    options: SignalWorkflowOptions,
}

impl SignalWorkflowInput {
    pub(crate) fn new(
        signal_name: String,
        target: SignalWorkflowTarget,
        input: Box<dyn Any>,
        options: SignalWorkflowOptions,
    ) -> Self {
        Self {
            signal_name,
            target,
            decoded: DecodedInput::new(Some(input), HashMap::new()),
            options,
        }
    }

    #[allow(clippy::type_complexity)]
    pub(crate) fn into_parts(
        self,
    ) -> (
        String,
        SignalWorkflowTarget,
        Box<dyn Any>,
        HashMap<String, Payload>,
        SignalWorkflowOptions,
    ) {
        let (input, headers) = self.decoded.into_parts();
        (
            self.signal_name,
            self.target,
            input.expect("signal input must exist"),
            headers,
            self.options,
        )
    }

    /// Signal name.
    pub fn signal_name(&self) -> &str {
        &self.signal_name
    }

    /// Mutably access the signal name.
    pub fn signal_name_mut(&mut self) -> &mut String {
        &mut self.signal_name
    }

    /// Signal target.
    pub fn target(&self) -> &SignalWorkflowTarget {
        &self.target
    }

    /// Mutably access the signal target.
    pub fn target_mut(&mut self) -> &mut SignalWorkflowTarget {
        &mut self.target
    }

    /// Signal options.
    pub fn options(&self) -> &SignalWorkflowOptions {
        &self.options
    }

    /// Mutably access signal options.
    pub fn options_mut(&mut self) -> &mut SignalWorkflowOptions {
        &mut self.options
    }
}

typed_outbound_input!(SignalWorkflowInput);

/// Input passed to [`WorkflowInterceptor::cancel_external_workflow`].
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct CancelExternalWorkflowInput {
    /// Target workflow ID.
    pub workflow_id: String,
    /// Target run ID, or the latest run when absent.
    pub run_id: Option<String>,
    /// Cancellation reason.
    pub reason: Option<String>,
}

/// Input passed to [`WorkflowInterceptor::continue_as_new`].
#[non_exhaustive]
pub struct ContinueAsNewInput {
    decoded: DecodedInput,
    options: ContinueAsNewOptions,
}

impl ContinueAsNewInput {
    pub(crate) fn new(input: Box<dyn Any>, options: ContinueAsNewOptions) -> Self {
        Self {
            decoded: DecodedInput::new(Some(input), HashMap::new()),
            options,
        }
    }

    pub(crate) fn into_parts(
        self,
    ) -> (Box<dyn Any>, HashMap<String, Payload>, ContinueAsNewOptions) {
        let (input, headers) = self.decoded.into_parts();
        (
            input.expect("continue-as-new input must exist"),
            headers,
            self.options,
        )
    }

    /// Continue-as-new options.
    pub fn options(&self) -> &ContinueAsNewOptions {
        &self.options
    }

    /// Mutably access continue-as-new options.
    pub fn options_mut(&mut self) -> &mut ContinueAsNewOptions {
        &mut self.options
    }
}

typed_outbound_input!(ContinueAsNewInput);

/// Input passed to [`WorkflowInterceptor::start_nexus_operation`].
#[non_exhaustive]
pub struct StartNexusOperationInput {
    options: NexusOperationOptions,
}

impl StartNexusOperationInput {
    pub(crate) fn new(options: NexusOperationOptions) -> Self {
        Self { options }
    }

    pub(crate) fn into_options(self) -> NexusOperationOptions {
        self.options
    }

    /// Nexus operation options.
    pub fn options(&self) -> &NexusOperationOptions {
        &self.options
    }

    /// Mutably access Nexus operation options.
    pub fn options_mut(&mut self) -> &mut NexusOperationOptions {
        &mut self.options
    }
}

/// Result of an intercepted activity call.
pub type ScheduleActivityResult = Result<Box<dyn WorkflowOutboundValue>, ActivityExecutionError>;

/// Result of an intercepted child workflow completion.
pub type ChildWorkflowOutboundResult =
    Result<Box<dyn WorkflowOutboundValue>, ChildWorkflowExecutionError>;

/// Result of an intercepted signal call.
pub type SignalWorkflowResult = Result<(), WorkflowSignalError>;

/// Result of an intercepted child workflow start.
pub type StartChildWorkflowResult = Result<StartChildWorkflowOutput, ChildWorkflowStartError>;

/// Result of an intercepted Nexus operation start.
pub type StartNexusOperationResult = Result<StartedNexusOperation, Failure>;

/// Result of an intercepted continue-as-new call.
pub type ContinueAsNewResult = Result<Infallible, WorkflowTermination>;

/// Interceptor for calls into workflow code and commands issued by workflow code.
///
/// Implement this trait for behavior that should wrap workflow operations. Inbound
/// methods intercept operations such as workflow execution and handler dispatch;
/// outbound methods intercept timers, activities, child workflows, external workflow calls,
/// continue-as-new, and Nexus operations.
///
/// Implementations normally calls [`WorkflowNext::run`] exactly once. It may transform the input first,
/// then inspect or transform the result. Not calling `next` short-circuits the operation, so it
/// should only be done if intentionally skipping the operation.
///
/// The async inbound methods return [`WorkflowInterceptorFuture`]. Use
/// [`WorkflowInterceptorFuture::new`] to wrap an `async` block around the downstream future:
/// call `next.run(input).await` to call the next interceptor. See the [module-level guide](self)
/// for a complete example and the determinism requirements.
///
/// Interceptors run as workflow code and are recreated when an evicted workflow is rebuilt. Their
/// behavior and any instance-local state must remain deterministic under replay. Async methods may
/// await only workflow scheduler primitives or SDK-provided workflow futures.
///
/// A [`WorkflowInterceptorConstructor`] creates one interceptor for each in-memory workflow
/// instance. The same interceptor object handles inbound and outbound calls for that instance and
/// is recreated if the workflow is evicted and rebuilt.
/// Inbound interceptors are called in regsitration order and outbound interceptors are called in reverse order.
pub trait WorkflowInterceptor: 'static {
    /// Called to invoke the workflow's `#[init]` method.
    ///
    /// It is only called for workflows that define `#[init]`, before the workflow instance exists.
    fn initialize_workflow(
        &self,
        _ctx: WorkflowContextView,
        input: InitializeWorkflowInput,
        next: WorkflowNext<'_, InitializeWorkflowInput, InitializeWorkflowOutput>,
    ) -> InitializeWorkflowOutput {
        next.run(input)
    }

    /// Called to invoke the workflow run method.
    ///
    /// Inputs consumed by `#[init]` are instead passed to
    /// [`WorkflowInterceptor::initialize_workflow`].
    fn execute<'a>(
        &'a self,
        _ctx: WorkflowInterceptorContext,
        input: ExecuteWorkflowInput,
        next: WorkflowNext<
            'a,
            ExecuteWorkflowInput,
            WorkflowInterceptorFuture<'a, ExecuteWorkflowResult>,
        >,
    ) -> WorkflowInterceptorFuture<'a, ExecuteWorkflowResult> {
        next.run(input)
    }

    /// Called to invoke a signal handler.
    fn handle_signal<'a>(
        &'a self,
        _ctx: WorkflowInterceptorContext,
        input: HandleSignalInput,
        next: WorkflowNext<
            'a,
            HandleSignalInput,
            WorkflowInterceptorFuture<'a, HandleSignalResult>,
        >,
    ) -> WorkflowInterceptorFuture<'a, HandleSignalResult> {
        next.run(input)
    }

    /// Called to invoke an update handler.
    fn handle_update<'a>(
        &'a self,
        _ctx: WorkflowInterceptorContext,
        input: HandleUpdateInput,
        next: WorkflowNext<
            'a,
            HandleUpdateInput,
            WorkflowInterceptorFuture<'a, HandleUpdateResult>,
        >,
    ) -> WorkflowInterceptorFuture<'a, HandleUpdateResult> {
        next.run(input)
    }

    /// Called to invoke a query handler.
    fn handle_query(
        &self,
        _ctx: SyncWorkflowInterceptorContext,
        input: HandleQueryInput,
        next: WorkflowNext<'_, HandleQueryInput, HandleQueryResult>,
    ) -> HandleQueryResult {
        next.run(input)
    }

    /// Called to validate an update.
    fn validate_update(
        &self,
        _ctx: SyncWorkflowInterceptorContext,
        input: ValidateUpdateInput,
        next: WorkflowNext<'_, ValidateUpdateInput, ValidateUpdateResult>,
    ) -> ValidateUpdateResult {
        next.run(input)
    }

    /// Called when the workflow starts a timer.
    fn start_timer(
        &self,
        _ctx: WorkflowInterceptorContext,
        input: StartTimerInput,
        next: WorkflowNext<
            'static,
            StartTimerInput,
            CancellableWorkflowOutboundFuture<TimerResult>,
        >,
    ) -> CancellableWorkflowOutboundFuture<TimerResult> {
        next.run(input)
    }

    /// Called when the workflow schedules an activity.
    fn schedule_activity(
        &self,
        _ctx: WorkflowInterceptorContext,
        input: ScheduleActivityInput,
        next: WorkflowNext<
            'static,
            ScheduleActivityInput,
            CancellableWorkflowOutboundFuture<ScheduleActivityResult>,
        >,
    ) -> CancellableWorkflowOutboundFuture<ScheduleActivityResult> {
        next.run(input)
    }

    /// Called when the workflow schedules a local activity.
    fn schedule_local_activity(
        &self,
        _ctx: WorkflowInterceptorContext,
        input: ScheduleLocalActivityInput,
        next: WorkflowNext<
            'static,
            ScheduleLocalActivityInput,
            CancellableWorkflowOutboundFuture<ScheduleActivityResult>,
        >,
    ) -> CancellableWorkflowOutboundFuture<ScheduleActivityResult> {
        next.run(input)
    }

    /// Called when the workflow starts a child workflow.
    fn start_child_workflow(
        &self,
        _ctx: WorkflowInterceptorContext,
        input: StartChildWorkflowInput,
        next: WorkflowNext<
            'static,
            StartChildWorkflowInput,
            CancellableWorkflowOutboundFuture<StartChildWorkflowResult>,
        >,
    ) -> CancellableWorkflowOutboundFuture<StartChildWorkflowResult> {
        next.run(input)
    }

    /// Called when the workflow signals a child or external workflow.
    fn signal_workflow(
        &self,
        _ctx: WorkflowInterceptorContext,
        input: SignalWorkflowInput,
        next: WorkflowNext<
            'static,
            SignalWorkflowInput,
            CancellableWorkflowOutboundFuture<SignalWorkflowResult>,
        >,
    ) -> CancellableWorkflowOutboundFuture<SignalWorkflowResult> {
        next.run(input)
    }

    /// Called when the workflow requests cancellation of an external workflow.
    fn cancel_external_workflow(
        &self,
        _ctx: WorkflowInterceptorContext,
        input: CancelExternalWorkflowInput,
        next: WorkflowNext<
            'static,
            CancelExternalWorkflowInput,
            WorkflowOutboundFuture<CancelExternalWfResult>,
        >,
    ) -> WorkflowOutboundFuture<CancelExternalWfResult> {
        next.run(input)
    }

    /// Called when the workflow continues as new.
    fn continue_as_new(
        &self,
        _ctx: SyncWorkflowInterceptorContext,
        input: ContinueAsNewInput,
        next: WorkflowNext<'static, ContinueAsNewInput, ContinueAsNewResult>,
    ) -> ContinueAsNewResult {
        next.run(input)
    }

    /// Called when the workflow starts a Nexus operation.
    fn start_nexus_operation(
        &self,
        _ctx: WorkflowInterceptorContext,
        input: StartNexusOperationInput,
        next: WorkflowNext<
            'static,
            StartNexusOperationInput,
            CancellableWorkflowOutboundFuture<StartNexusOperationResult>,
        >,
    ) -> CancellableWorkflowOutboundFuture<StartNexusOperationResult> {
        next.run(input)
    }
}

macro_rules! outbound_chain {
    ($fn_name:ident, $method:ident, $context:ty, $input:ty, $output:ty) => {
        pub(crate) fn $fn_name(
            interceptors: Rc<[Arc<dyn WorkflowInterceptor>]>,
            ctx: $context,
            input: $input,
            next: WorkflowNext<'static, $input, $output>,
        ) -> $output {
            fn call(
                interceptors: Rc<[Arc<dyn WorkflowInterceptor>]>,
                interceptor_count: usize,
                ctx: $context,
                input: $input,
                next: WorkflowNext<'static, $input, $output>,
            ) -> $output {
                if let Some(interceptor_index) = interceptor_count.checked_sub(1) {
                    let interceptor = interceptors[interceptor_index].clone();
                    let next_ctx = ctx.clone();
                    let downstream = WorkflowNext::new(move |input| {
                        call(interceptors, interceptor_index, next_ctx, input, next)
                    });
                    interceptor.$method(ctx, input, downstream)
                } else {
                    next.run(input)
                }
            }

            let interceptor_count = interceptors.len();
            call(interceptors, interceptor_count, ctx, input, next)
        }
    };
}

outbound_chain!(
    call_start_timer,
    start_timer,
    WorkflowInterceptorContext,
    StartTimerInput,
    CancellableWorkflowOutboundFuture<TimerResult>
);
outbound_chain!(
    call_schedule_activity,
    schedule_activity,
    WorkflowInterceptorContext,
    ScheduleActivityInput,
    CancellableWorkflowOutboundFuture<ScheduleActivityResult>
);
outbound_chain!(
    call_schedule_local_activity,
    schedule_local_activity,
    WorkflowInterceptorContext,
    ScheduleLocalActivityInput,
    CancellableWorkflowOutboundFuture<ScheduleActivityResult>
);
outbound_chain!(
    call_start_child_workflow,
    start_child_workflow,
    WorkflowInterceptorContext,
    StartChildWorkflowInput,
    CancellableWorkflowOutboundFuture<StartChildWorkflowResult>
);
outbound_chain!(
    call_signal_workflow,
    signal_workflow,
    WorkflowInterceptorContext,
    SignalWorkflowInput,
    CancellableWorkflowOutboundFuture<SignalWorkflowResult>
);
outbound_chain!(
    call_cancel_external_workflow,
    cancel_external_workflow,
    WorkflowInterceptorContext,
    CancelExternalWorkflowInput,
    WorkflowOutboundFuture<CancelExternalWfResult>
);
outbound_chain!(
    call_continue_as_new,
    continue_as_new,
    SyncWorkflowInterceptorContext,
    ContinueAsNewInput,
    ContinueAsNewResult
);
outbound_chain!(
    call_start_nexus_operation,
    start_nexus_operation,
    WorkflowInterceptorContext,
    StartNexusOperationInput,
    CancellableWorkflowOutboundFuture<StartNexusOperationResult>
);

type WorkflowInterceptorConstructorFn =
    dyn Fn(&WorkflowContextView) -> Arc<dyn WorkflowInterceptor> + Send + Sync + 'static;

/// Creates one interceptor for each in-memory workflow instance.
///
/// The constructor receives a read-only view of the workflow's initialization context. It may use
/// that context to initialize interceptor state but must remain deterministic because it runs again
/// when an evicted workflow is rebuilt.
#[derive(Clone)]
pub struct WorkflowInterceptorConstructor {
    constructor: Arc<WorkflowInterceptorConstructorFn>,
}

impl WorkflowInterceptorConstructor {
    /// Create a workflow interceptor constructor.
    pub fn new<F, I>(constructor: F) -> Self
    where
        F: Fn(&WorkflowContextView) -> I + Send + Sync + 'static,
        I: WorkflowInterceptor,
    {
        Self {
            constructor: Arc::new(move |ctx| Arc::new(constructor(ctx))),
        }
    }

    pub(crate) fn construct(&self, ctx: &WorkflowContextView) -> Arc<dyn WorkflowInterceptor> {
        (self.constructor)(ctx)
    }
}

pub(crate) fn wrong_workflow_input_type(type_name: &'static str) -> WorkflowTermination {
    WorkflowTermination::failed_application(temporalio_common_wasm::error::ApplicationFailure::new(
        anyhow::anyhow!(
            "Workflow inbound interceptor returned arguments with wrong concrete type for workflow {type_name}"
        ),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    fn cancellable_future<T: 'static>(
        future: impl Future<Output = T> + 'static,
        token: &WorkflowCancellationToken,
        cancellation_count: &Rc<Cell<usize>>,
    ) -> CancellableWorkflowOutboundFuture<T> {
        let cancellation_count = cancellation_count.clone();
        CancellableWorkflowOutboundFuture::new(
            future,
            WorkflowCancellationHandle::new(move |_| {
                cancellation_count.set(cancellation_count.get() + 1);
            }),
        )
        .with_cancellation_token(token.clone())
    }

    #[test]
    fn pending_cancellable_future_observes_token_cancellation() {
        let token = WorkflowCancellationToken::new();
        let cancellation_count = Rc::new(Cell::new(0));
        let _future = cancellable_future(std::future::pending::<()>(), &token, &cancellation_count);

        token.cancel();

        assert_eq!(cancellation_count.get(), 1);
    }

    #[test]
    fn completed_cancellable_future_unregisters_from_token() {
        let token = WorkflowCancellationToken::new();
        let cancellation_count = Rc::new(Cell::new(0));
        let future = cancellable_future(std::future::ready(()), &token, &cancellation_count);

        assert_eq!(future.now_or_never(), Some(()));
        token.cancel();

        assert_eq!(cancellation_count.get(), 0);
    }

    #[test]
    fn mapped_cancellable_future_unregisters_from_token() {
        let token = WorkflowCancellationToken::new();
        let cancellation_count = Rc::new(Cell::new(0));
        let future = cancellable_future(std::future::ready(1), &token, &cancellation_count)
            .map(|value| value + 1);

        assert_eq!(future.now_or_never(), Some(2));
        token.cancel();

        assert_eq!(cancellation_count.get(), 0);
    }

    #[test]
    fn shared_cancellable_future_unregisters_from_token() {
        let token = WorkflowCancellationToken::new();
        let cancellation_count = Rc::new(Cell::new(0));
        let future =
            cancellable_future(std::future::ready(1), &token, &cancellation_count).shared();

        assert_eq!(future.clone().now_or_never(), Some(1));
        token.cancel();

        assert_eq!(cancellation_count.get(), 0);
    }
}
