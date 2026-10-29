//! Guest-side workflow execution implementation used by native and future WASM hosts.

use crate::{
    BaseWorkflowContext, WorkflowContext, WorkflowContextView,
    runtime::{
        InterceptedFuturePollGuard, InterceptedFuturePollKind, InterceptedFutureStatus,
        entry::{WorkflowError, WorkflowImplementation},
        guest::WorkflowInstance,
        model::{TimerResult, UnblockEvent, WorkflowTermination},
        types::{
            ActivationJobResult, ActivationResult, MAIN_ROUTINE_ID, MainRoutineCompletion,
            QueryResponse, RoutineCompletion, RoutineId, RoutineKind, RoutinePendingState,
            RoutinePollResult, StartedRoutine, TaskFailure, TerminalOutcome,
            UpdateRoutineCompletion, UpdateRoutineKind, WorkflowActivation, WorkflowFailure,
        },
    },
    workflow_interceptors::{
        ExecuteWorkflowInput, ExecuteWorkflowResult, HandleQueryInput, HandleQueryResult,
        HandleSignalInput, HandleSignalResult, HandleUpdateInput, HandleUpdateResult,
        InitializeWorkflowInput, InitializeWorkflowOutput, SyncWorkflowInterceptorContext,
        ValidateUpdateInput, ValidateUpdateResult, WorkflowInterceptor, WorkflowInterceptorContext,
        WorkflowInterceptorFuture, WorkflowNext, serialize_workflow_output,
        wrong_workflow_input_type,
    },
};
use futures_util::{
    FutureExt,
    future::{Fuse, LocalBoxFuture},
};
use std::{
    any::{Any, TypeId},
    cell::RefCell,
    collections::HashMap,
    fmt::{Display, Formatter},
    future::{Future, ready},
    panic::AssertUnwindSafe,
    pin::Pin,
    rc::Rc,
    sync::Arc,
    task::{Context, Poll, Waker},
};
use temporalio_common_wasm::{
    WorkflowDefinition,
    data_converters::{
        GenericPayloadConverter, PayloadConversionError, PayloadConverter, SerializationContext,
        SerializationContextData,
    },
    error::{ApplicationFailure, OutgoingError, OutgoingWorkflowError},
    protos::{
        coresdk::workflow_activation::{
            DoUpdate, QueryWorkflow, SignalWorkflow,
            workflow_activation_job::Variant as ActivationVariant,
        },
        temporal::api::{
            common::v1::{Payload, Payloads},
            failure::v1::Failure,
        },
    },
};

pub struct GuestWorkflowInstance<W: WorkflowImplementation> {
    base_ctx: BaseWorkflowContext,
    ctx: WorkflowContext<W>,
    run_future: InterceptedFuture<ExecuteWorkflowResult>,
    interceptors: Rc<[Arc<dyn WorkflowInterceptor>]>,
    main_construction_polled: bool,
    next_routine_id: RoutineId,
    routines: HashMap<RoutineId, GuestRoutine>,
}

enum GuestRoutine {
    Signal {
        future: InterceptedFuture<HandleSignalResult>,
    },
    Update {
        protocol_instance_id: String,
        future: InterceptedFuture<HandleUpdateResult>,
    },
}

struct InterceptedFuture<T> {
    inner: Fuse<LocalBoxFuture<'static, T>>,
    status: InterceptedFutureStatus,
}

impl<T> InterceptedFuture<T> {
    fn new(inner: LocalBoxFuture<'static, T>, status: InterceptedFutureStatus) -> Self {
        Self {
            inner: inner.fuse(),
            status,
        }
    }

    fn ready(value: T) -> Self
    where
        T: 'static,
    {
        Self::new(ready(value).boxed_local(), InterceptedFutureStatus::new())
    }

    fn pending_state(&self) -> RoutinePendingState {
        self.status.state()
    }

    fn poll_for_construction(&mut self, cx: &mut Context<'_>) -> Poll<T> {
        self.poll_with_kind(cx, InterceptedFuturePollKind::Construction)
    }

    fn poll_with_kind(
        &mut self,
        cx: &mut Context<'_>,
        poll_kind: InterceptedFuturePollKind,
    ) -> Poll<T> {
        self.status.reset_for_poll();
        let _guard = InterceptedFuturePollGuard::new(self.status.clone(), poll_kind);
        self.inner.poll_unpin(cx)
    }
}

impl<T> Future for InterceptedFuture<T> {
    type Output = T;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.poll_with_kind(cx, InterceptedFuturePollKind::Routine)
    }
}

struct HandlerBoundaryFuture<F> {
    inner: F,
    status: InterceptedFutureStatus,
}

impl<F> HandlerBoundaryFuture<F> {
    fn new(inner: F, status: InterceptedFutureStatus) -> Self {
        Self { inner, status }
    }
}

impl<F: Future + Unpin> Future for HandlerBoundaryFuture<F> {
    type Output = F::Output;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.status.enter_handler() {
            return Poll::Pending;
        }
        Pin::new(&mut self.inner).poll(cx)
    }
}

enum ConstructionPoll<T> {
    Ready(T),
    Pending,
}

enum RoutinePollState<T> {
    Ready {
        result: T,
        made_progress: bool,
    },
    ForcedFailure {
        failure: WorkflowFailure,
        made_progress: bool,
    },
    Stalled {
        made_progress: bool,
        pending_state: RoutinePendingState,
    },
}

fn expect_resolution<T>(value: Option<T>) -> T {
    value.expect("resolution expected payload")
}

// Macro for defining a function that drives forward an interceptor chain
macro_rules! call_workflow_interceptor {
    (
        $function:ident<$lifetime:lifetime>,
        $method:ident,
        $ctx:ty,
        $input:ty,
        $output:ty $(,)?
    ) => {
        fn $function<$lifetime>(
            interceptors: &$lifetime [Arc<dyn WorkflowInterceptor>],
            ctx: $ctx,
            input: $input,
            next: WorkflowNext<$lifetime, $input, $output>,
        ) -> $output {
            if let Some((first, rest)) = interceptors.split_first() {
                let next_ctx = ctx.clone();
                first.$method(
                    ctx,
                    input,
                    WorkflowNext::new(move |input| $function(rest, next_ctx, input, next)),
                )
            } else {
                next.run(input)
            }
        }
    };
}

call_workflow_interceptor!(
    call_initialize_workflow<'a>,
    initialize_workflow,
    WorkflowContextView,
    InitializeWorkflowInput,
    InitializeWorkflowOutput,
);

call_workflow_interceptor!(
    call_execute_workflow<'a>,
    execute,
    WorkflowInterceptorContext,
    ExecuteWorkflowInput,
    WorkflowInterceptorFuture<'a, ExecuteWorkflowResult>,
);

call_workflow_interceptor!(
    call_handle_signal<'a>,
    handle_signal,
    WorkflowInterceptorContext,
    HandleSignalInput,
    WorkflowInterceptorFuture<'a, HandleSignalResult>,
);

call_workflow_interceptor!(
    call_handle_update<'a>,
    handle_update,
    WorkflowInterceptorContext,
    HandleUpdateInput,
    WorkflowInterceptorFuture<'a, HandleUpdateResult>,
);

call_workflow_interceptor!(
    call_handle_query<'a>,
    handle_query,
    SyncWorkflowInterceptorContext,
    HandleQueryInput,
    HandleQueryResult,
);

call_workflow_interceptor!(
    call_validate_update<'a>,
    validate_update,
    SyncWorkflowInterceptorContext,
    ValidateUpdateInput,
    ValidateUpdateResult,
);

fn intercepted_execute_future<W>(
    ctx: WorkflowContext<W>,
    base_ctx: BaseWorkflowContext,
    run_input: Option<<W::Run as WorkflowDefinition>::Input>,
    headers: HashMap<String, Payload>,
    interceptors: Rc<[Arc<dyn WorkflowInterceptor>]>,
) -> InterceptedFuture<ExecuteWorkflowResult>
where
    W: WorkflowImplementation,
    <W::Run as WorkflowDefinition>::Input: Send,
{
    let status = InterceptedFutureStatus::new();
    let handler_status = status.clone();
    let future = async move {
        let input = ExecuteWorkflowInput::new(
            run_input.map(|input| Box::new(input) as Box<dyn Any>),
            headers,
        );
        let interceptor_ctx = WorkflowInterceptorContext::new(base_ctx);
        let next = WorkflowNext::new(move |input: ExecuteWorkflowInput| {
            let (input, headers) = input.into_parts();
            let handler: LocalBoxFuture<'static, ExecuteWorkflowResult> = match input {
                Some(input) => match input.downcast::<<W::Run as WorkflowDefinition>::Input>() {
                    Ok(input) => W::run(ctx.with_headers(headers), Some(*input)),
                    Err(_) => {
                        handler_status.mark_handler_result_ready();
                        ready(Err(wrong_workflow_input_type(W::name()))).boxed_local()
                    }
                },
                None => W::run(ctx.with_headers(headers), None),
            };
            WorkflowInterceptorFuture::new(HandlerBoundaryFuture::new(handler, handler_status))
        });
        call_execute_workflow(&interceptors, interceptor_ctx, input, next).await
    }
    .boxed_local();
    InterceptedFuture::new(future, status)
}

fn intercepted_signal_future<W>(
    ctx: WorkflowContext<W>,
    base_ctx: BaseWorkflowContext,
    interceptors: Rc<[Arc<dyn WorkflowInterceptor>]>,
    input: HandleSignalInput,
) -> InterceptedFuture<HandleSignalResult>
where
    W: WorkflowImplementation,
{
    let status = InterceptedFutureStatus::new();
    let handler_status = status.clone();
    let future = async move {
        let interceptor_ctx = WorkflowInterceptorContext::new(base_ctx);
        let next = WorkflowNext::new(move |input: HandleSignalInput| {
            let (name, input, headers) = input.into_parts();
            WorkflowInterceptorFuture::new(HandlerBoundaryFuture::new(
                W::dispatch_signal(ctx.with_headers(headers), &name, input),
                handler_status,
            ))
        });
        call_handle_signal(&interceptors, interceptor_ctx, input, next).await
    }
    .boxed_local();
    InterceptedFuture::new(future, status)
}

fn intercepted_update_future<W>(
    ctx: WorkflowContext<W>,
    base_ctx: BaseWorkflowContext,
    interceptors: Rc<[Arc<dyn WorkflowInterceptor>]>,
    input: HandleUpdateInput,
) -> InterceptedFuture<HandleUpdateResult>
where
    W: WorkflowImplementation,
{
    let status = InterceptedFutureStatus::new();
    let handler_status = status.clone();
    let future = async move {
        let interceptor_ctx = WorkflowInterceptorContext::new(base_ctx);
        let next = WorkflowNext::new(move |input: HandleUpdateInput| {
            let (name, input, headers) = input.into_parts();
            WorkflowInterceptorFuture::new(HandlerBoundaryFuture::new(
                W::dispatch_update(ctx.with_headers(headers), &name, input),
                handler_status,
            ))
        });
        call_handle_update(&interceptors, interceptor_ctx, input, next).await
    }
    .boxed_local();
    InterceptedFuture::new(future, status)
}

impl<W: WorkflowImplementation> GuestWorkflowInstance<W>
where
    <W::Run as WorkflowDefinition>::Input: Send,
{
    pub fn instantiate(
        payloads: Vec<Payload>,
        converter: PayloadConverter,
        base_ctx: BaseWorkflowContext,
    ) -> Result<Box<dyn WorkflowInstance>, PayloadConversionError> {
        let view = base_ctx.view();
        let interceptors = base_ctx.workflow_interceptors();
        let ser_ctx = SerializationContext {
            data: &SerializationContextData::Workflow,
            converter: &converter,
        };
        let input = converter.from_payloads(&ser_ctx, payloads)?;
        let (init_input, run_input) = if W::INIT_TAKES_INPUT {
            (Some(input), None)
        } else {
            (None, Some(input))
        };
        let (workflow, headers) = if W::HAS_INIT {
            let input = InitializeWorkflowInput::new(
                init_input.map(|input| Box::new(input) as Box<dyn Any>),
                base_ctx.initial_headers(),
            );
            let initialized = RefCell::new(None);
            let initialized_ref = &initialized;
            let init_view = view.clone();
            let next = WorkflowNext::new(move |input: InitializeWorkflowInput| {
                let (input, headers) = input.into_parts();
                let input = input.map(|input| {
                    *input
                        .downcast::<<W::Run as WorkflowDefinition>::Input>()
                        .unwrap_or_else(|_| {
                            panic!("workflow initialization received the wrong concrete input type")
                        })
                });
                initialized_ref.replace(Some((W::init(init_view, input), headers)));
                InitializeWorkflowOutput::new()
            });
            let _ = call_initialize_workflow(&interceptors, view, input, next);
            initialized
                .into_inner()
                .expect("workflow initialization interceptor must call next")
        } else {
            (W::init(view, init_input), base_ctx.initial_headers())
        };
        Ok(Box::new(Self::new_with_workflow_interceptors_and_headers(
            workflow, base_ctx, run_input, headers,
        )))
    }

    pub fn new_with_workflow(
        workflow: W,
        base_ctx: BaseWorkflowContext,
        run_input: Option<<W::Run as WorkflowDefinition>::Input>,
    ) -> Self {
        let headers = base_ctx.initial_headers();
        Self::new_with_workflow_interceptors_and_headers(workflow, base_ctx, run_input, headers)
    }

    fn new_with_workflow_interceptors_and_headers(
        workflow: W,
        base_ctx: BaseWorkflowContext,
        run_input: Option<<W::Run as WorkflowDefinition>::Input>,
        headers: HashMap<String, Payload>,
    ) -> Self {
        let interceptors = base_ctx.workflow_interceptors();
        let workflow = Rc::new(RefCell::new(workflow));
        let ctx = WorkflowContext::from_base(base_ctx.clone(), workflow);
        let run_future = intercepted_execute_future::<W>(
            ctx.clone(),
            base_ctx.clone(),
            run_input,
            headers,
            interceptors.clone(),
        );
        Self {
            base_ctx,
            ctx,
            run_future,
            interceptors,
            main_construction_polled: false,
            next_routine_id: MAIN_ROUTINE_ID + 1,
            routines: HashMap::new(),
        }
    }

    fn query_metadata(&self) -> QueryResponse {
        #[derive(serde::Serialize)]
        struct WorkflowMetadataJson {
            #[serde(rename = "currentDetails", skip_serializing_if = "String::is_empty")]
            current_details: String,
        }

        let converter = PayloadConverter::default();
        let ctx = SerializationContext {
            data: &SerializationContextData::Workflow,
            converter: &converter,
        };
        QueryResponse {
            result: converter
                .to_payload(
                    &ctx,
                    &WorkflowMetadataJson {
                        current_details: self.base_ctx.current_details(),
                    },
                )
                .map_err(|err| Failure {
                    message: err.to_string(),
                    ..Default::default()
                }),
        }
    }

    fn rejection_for_missing_update_handler(&self, name: String) -> ActivationJobResult {
        ActivationJobResult::UpdateRejected(Box::new(self.message_to_failure(format!(
            "No update handler registered for update name {name}"
        ))))
    }

    fn workflow_error_to_failure(&self, err: WorkflowError) -> Failure {
        let outgoing: OutgoingWorkflowError = match err {
            WorkflowError::PayloadConversion(err) => OutgoingWorkflowError::from(err),
            WorkflowError::Execution(err) => {
                OutgoingWorkflowError::Application(Box::new(ApplicationFailure::new(err)))
            }
        };
        self.base_ctx.data_converter().to_failure(
            &SerializationContextData::Workflow,
            OutgoingError::Workflow(outgoing),
        )
    }

    fn message_to_failure(&self, message: String) -> Failure {
        self.base_ctx.data_converter().to_failure(
            &SerializationContextData::Workflow,
            OutgoingError::Workflow(OutgoingWorkflowError::Application(Box::new(
                ApplicationFailure::new(message),
            ))),
        )
    }

    fn next_routine_id(&mut self) -> RoutineId {
        let id = self.next_routine_id;
        self.next_routine_id += 1;
        id
    }

    fn poll_for_construction<T>(
        base_ctx: &BaseWorkflowContext,
        future: &mut InterceptedFuture<T>,
    ) -> Result<ConstructionPoll<T>, WorkflowFailure> {
        if let Some(failure) = Self::take_forced_wft_failure(base_ctx) {
            return Err(failure);
        }

        let waker = base_ctx.construction_waker();
        let mut cx = Context::from_waker(&waker);
        let poll = future.poll_for_construction(&mut cx);

        if let Some(failure) = Self::take_forced_wft_failure(base_ctx) {
            return Err(failure);
        }

        match poll {
            Poll::Ready(result) => Ok(ConstructionPoll::Ready(result)),
            Poll::Pending => Ok(ConstructionPoll::Pending),
        }
    }

    fn take_forced_wft_failure(base_ctx: &BaseWorkflowContext) -> Option<WorkflowFailure> {
        base_ctx.take_forced_wft_failure().map(|err| {
            Box::new(Failure {
                message: err.to_string(),
                ..Default::default()
            })
        })
    }

    fn start_signal_routine(
        &mut self,
        signal: SignalWorkflow,
    ) -> Result<ActivationJobResult, WorkflowFailure> {
        let name = signal.signal_name;
        let payloads = Payloads {
            payloads: signal.input,
        };
        let converter = self.ctx.payload_converter();
        let future = match W::decode_signal_input(&name, payloads, converter) {
            Ok(Some(input)) => {
                let input = HandleSignalInput::new(name.clone(), input, signal.headers);
                let mut future = intercepted_signal_future::<W>(
                    self.ctx.clone(),
                    self.base_ctx.clone(),
                    self.interceptors.clone(),
                    input,
                );
                if let ConstructionPoll::Ready(result) =
                    Self::poll_for_construction(&self.base_ctx, &mut future)?
                {
                    future = InterceptedFuture::ready(result);
                }
                future
            }
            Err(err) => InterceptedFuture::ready(Err(err)),
            Ok(None) => return Ok(ActivationJobResult::None),
        };
        let routine_id = self.next_routine_id();
        self.routines
            .insert(routine_id, GuestRoutine::Signal { future });
        Ok(ActivationJobResult::StartedRoutine(StartedRoutine {
            routine_id,
            kind: RoutineKind::Signal(name),
        }))
    }

    fn start_update_routine(
        &mut self,
        update: DoUpdate,
    ) -> Result<ActivationJobResult, WorkflowFailure> {
        let DoUpdate {
            id,
            protocol_instance_id,
            name,
            input,
            headers,
            run_validator,
            ..
        } = update;
        let has_validator = match W::definition()
            .updates
            .into_iter()
            .find(|update| update.name.as_str() == name)
            .map(|update| update.has_validator)
        {
            Some(has_validator) => has_validator,
            None => return Ok(self.rejection_for_missing_update_handler(name)),
        };

        if run_validator && has_validator {
            let payloads = Payloads {
                payloads: input.clone(),
            };
            let converter = self.ctx.payload_converter();
            let decoded_input = match W::decode_update_input(&name, payloads, converter) {
                Ok(Some(input)) => input,
                Err(err) => {
                    return Ok(ActivationJobResult::UpdateRejected(Box::new(
                        self.workflow_error_to_failure(err),
                    )));
                }
                Ok(None) => {
                    return Ok(self.rejection_for_missing_update_handler(name));
                }
            };
            let validation_input =
                ValidateUpdateInput::new(id.clone(), name.clone(), decoded_input, headers.clone());
            let validation_ctx = SyncWorkflowInterceptorContext::new(self.base_ctx.clone());
            let workflow_ctx = self.ctx.clone();
            let validation_next = WorkflowNext::new(move |input: ValidateUpdateInput| {
                let (name, input, _headers) = input.into_parts();
                let view = workflow_ctx.view();
                workflow_ctx.state(|wf| wf.validate_update(view, &name, input))
            });
            let validation = std::panic::catch_unwind(AssertUnwindSafe(|| {
                call_validate_update(
                    &self.interceptors,
                    validation_ctx,
                    validation_input,
                    validation_next,
                )
            }))
            .unwrap_or_else(|panic| {
                Err(WorkflowError::Execution(
                    anyhow::anyhow!("Update validator panicked: {}", panic_formatter(panic)).into(),
                ))
            });
            match validation {
                Ok(()) => {}
                Err(e) => {
                    return Ok(ActivationJobResult::UpdateRejected(Box::new(
                        self.workflow_error_to_failure(e),
                    )));
                }
            }
        }

        let payloads = Payloads { payloads: input };
        let converter = self.ctx.payload_converter();
        let future = match W::decode_update_input(&name, payloads, converter) {
            Ok(Some(input)) => {
                let input = HandleUpdateInput::new(id.clone(), name.clone(), input, headers);
                let mut future = intercepted_update_future::<W>(
                    self.ctx.clone(),
                    self.base_ctx.clone(),
                    self.interceptors.clone(),
                    input,
                );
                if let ConstructionPoll::Ready(result) =
                    Self::poll_for_construction(&self.base_ctx, &mut future)?
                {
                    future = InterceptedFuture::ready(result);
                }
                future
            }
            Err(err) => InterceptedFuture::ready(Err(err)),
            Ok(None) => {
                return Ok(self.rejection_for_missing_update_handler(name));
            }
        };
        let routine_id = self.next_routine_id();
        self.routines.insert(
            routine_id,
            GuestRoutine::Update {
                protocol_instance_id: protocol_instance_id.clone(),
                future,
            },
        );
        Ok(ActivationJobResult::StartedRoutine(StartedRoutine {
            routine_id,
            kind: RoutineKind::Update(UpdateRoutineKind {
                name,
                update_id: id,
                protocol_instance_id,
            }),
        }))
    }

    fn query(&self, query: QueryWorkflow) -> QueryResponse {
        if query.query_type == "__temporal_workflow_metadata" {
            return self.query_metadata();
        }

        let payloads = Payloads {
            payloads: query.arguments,
        };
        let converter = self.ctx.payload_converter();
        let decoded_input = match W::decode_query_input(&query.query_type, &payloads, converter) {
            Ok(Some(input)) => input,
            Err(err) => {
                return QueryResponse {
                    result: Err(self.workflow_error_to_failure(err)),
                };
            }
            Ok(None) => {
                return QueryResponse {
                    result: Err(self.message_to_failure(format!(
                        "No query handler for '{}'",
                        query.query_type
                    ))),
                };
            }
        };
        let query_input = HandleQueryInput::new(
            query.query_id,
            query.query_type.clone(),
            decoded_input,
            query.headers,
        );
        let interceptor_ctx = SyncWorkflowInterceptorContext::new(self.base_ctx.clone());
        let workflow_ctx = self.ctx.clone();
        let query_next = WorkflowNext::new(move |input: HandleQueryInput| {
            let (name, input, _headers) = input.into_parts();
            let view = workflow_ctx.view();
            workflow_ctx.state(|wf| wf.dispatch_query(view, &name, input))
        });
        QueryResponse {
            result: call_handle_query(&self.interceptors, interceptor_ctx, query_input, query_next)
                .and_then(|output| {
                    serialize_workflow_output(output.as_ref(), converter)
                        .map_err(WorkflowError::from)
                })
                .map_err(|err| self.workflow_error_to_failure(err)),
        }
    }

    fn apply_resolution(&mut self, resolution: ActivationVariant) {
        let event = match resolution {
            ActivationVariant::FireTimer(event) => {
                UnblockEvent::Timer(event.seq, TimerResult::Fired)
            }
            ActivationVariant::ResolveActivity(event) => {
                UnblockEvent::Activity(event.seq, Box::new(expect_resolution(event.result)))
            }
            ActivationVariant::ResolveChildWorkflowExecutionStart(event) => {
                UnblockEvent::WorkflowStart(event.seq, Box::new(expect_resolution(event.status)))
            }
            ActivationVariant::ResolveChildWorkflowExecution(event) => {
                UnblockEvent::WorkflowComplete(event.seq, Box::new(expect_resolution(event.result)))
            }
            ActivationVariant::ResolveSignalExternalWorkflow(event) => {
                UnblockEvent::SignalExternal(event.seq, event.failure)
            }
            ActivationVariant::ResolveRequestCancelExternalWorkflow(event) => {
                UnblockEvent::CancelExternal(event.seq, event.failure)
            }
            ActivationVariant::ResolveNexusOperationStart(event) => {
                UnblockEvent::NexusOperationStart(
                    event.seq,
                    Box::new(expect_resolution(event.status)),
                )
            }
            ActivationVariant::ResolveNexusOperation(event) => {
                UnblockEvent::NexusOperationComplete(
                    event.seq,
                    Box::new(expect_resolution(event.result)),
                )
            }
            _ => unreachable!("only resolution jobs can be applied as resolutions"),
        };
        self.base_ctx
            .unblock(event)
            .expect("resolution must have a registered unblocker");
    }

    fn terminal_outcome_from_result(
        &self,
        result: ExecuteWorkflowResult,
    ) -> Result<TerminalOutcome, TaskFailure> {
        let result = result.and_then(|result| {
            serialize_workflow_output(result.as_ref(), self.ctx.payload_converter())
                .map_err(WorkflowTermination::from)
        });
        match result {
            Ok(result) => Ok(TerminalOutcome::Completed(result)),
            Err(WorkflowTermination::ContinueAsNew(req)) => Ok(TerminalOutcome::ContinueAsNew(req)),
            Err(WorkflowTermination::Cancelled) => Ok(TerminalOutcome::Cancelled),
            Err(WorkflowTermination::Evicted) => {
                panic!("workflow instances must not explicitly return eviction")
            }
            Err(WorkflowTermination::Failed(OutgoingWorkflowError::PayloadConversion(err))) => {
                Err(TaskFailure {
                    failure: Box::new(Failure {
                        message: format!("Workflow payload conversion failed: {err}"),
                        ..Default::default()
                    }),
                    force_cause: None,
                })
            }
            Err(WorkflowTermination::Failed(err)) => {
                if self.base_ctx.cancellation_token().is_cancelled() && err.as_cancelled().is_some()
                {
                    return Ok(TerminalOutcome::Cancelled);
                }
                let failure = self.base_ctx.data_converter().to_failure(
                    &SerializationContextData::Workflow,
                    temporalio_common_wasm::error::OutgoingError::Workflow(err),
                );
                Ok(TerminalOutcome::Failed(Box::new(failure)))
            }
        }
    }

    fn poll_routine_loop<T>(
        base_ctx: &BaseWorkflowContext,
        cx: &mut Context<'_>,
        future: &mut InterceptedFuture<T>,
    ) -> RoutinePollState<T> {
        base_ctx.take_state_mutated();
        base_ctx.take_runtime_progress();
        let mut made_progress = false;

        loop {
            if let Some(err) = base_ctx.take_forced_wft_failure() {
                return RoutinePollState::ForcedFailure {
                    failure: Box::new(Failure {
                        message: err.to_string(),
                        ..Default::default()
                    }),
                    made_progress,
                };
            }

            let poll = future.poll_unpin(cx);
            if let Some(err) = base_ctx.take_forced_wft_failure() {
                return RoutinePollState::ForcedFailure {
                    failure: Box::new(Failure {
                        message: err.to_string(),
                        ..Default::default()
                    }),
                    made_progress,
                };
            }

            match poll {
                Poll::Ready(result) => {
                    let state_mutated = base_ctx.take_state_mutated();
                    let runtime_progress = base_ctx.take_runtime_progress();
                    made_progress |= state_mutated || runtime_progress;
                    return RoutinePollState::Ready {
                        result,
                        made_progress,
                    };
                }
                Poll::Pending => {
                    let state_mutated = base_ctx.take_state_mutated();
                    let runtime_progress = base_ctx.take_runtime_progress();
                    made_progress |= state_mutated || runtime_progress;
                    if !(state_mutated || runtime_progress) {
                        return RoutinePollState::Stalled {
                            made_progress,
                            pending_state: future.pending_state(),
                        };
                    }
                }
            }
        }
    }

    fn poll_main_routine(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Result<RoutinePollResult, WorkflowFailure> {
        Ok(
            match Self::poll_routine_loop(&self.base_ctx, cx, &mut self.run_future) {
                RoutinePollState::Ready {
                    result,
                    made_progress,
                } => {
                    let completion = match self.terminal_outcome_from_result(result) {
                        Ok(outcome) => MainRoutineCompletion::Terminal(Box::new(outcome)),
                        Err(failure) => MainRoutineCompletion::TaskFailed(failure),
                    };
                    RoutinePollResult {
                        completion: Some(RoutineCompletion::Main(completion)),
                        made_progress,
                        pending_state: None,
                    }
                }
                RoutinePollState::ForcedFailure {
                    failure,
                    made_progress,
                } => RoutinePollResult {
                    completion: Some(RoutineCompletion::Main(MainRoutineCompletion::TaskFailed(
                        crate::runtime::types::TaskFailure {
                            failure,
                            force_cause: None,
                        },
                    ))),
                    made_progress,
                    pending_state: None,
                },
                RoutinePollState::Stalled {
                    made_progress,
                    pending_state,
                } => RoutinePollResult {
                    completion: Some(RoutineCompletion::Main(MainRoutineCompletion::Blocked)),
                    made_progress,
                    pending_state: Some(pending_state),
                },
            },
        )
    }

    fn poll_signal_routine(
        &mut self,
        routine_id: RoutineId,
        mut future: InterceptedFuture<HandleSignalResult>,
        cx: &mut Context<'_>,
    ) -> Result<RoutinePollResult, WorkflowFailure> {
        match Self::poll_routine_loop(&self.base_ctx, cx, &mut future) {
            RoutinePollState::Ready {
                result,
                made_progress,
            } => {
                let result = result.map_err(|err| Box::new(self.workflow_error_to_failure(err)));
                Ok(RoutinePollResult {
                    completion: Some(RoutineCompletion::Signal(result)),
                    made_progress,
                    pending_state: None,
                })
            }
            RoutinePollState::ForcedFailure { failure, .. } => Err(failure),
            RoutinePollState::Stalled {
                made_progress,
                pending_state,
            } => {
                self.routines
                    .insert(routine_id, GuestRoutine::Signal { future });
                Ok(RoutinePollResult {
                    completion: None,
                    made_progress,
                    pending_state: Some(pending_state),
                })
            }
        }
    }

    fn poll_update_routine(
        &mut self,
        routine_id: RoutineId,
        protocol_instance_id: String,
        mut future: InterceptedFuture<HandleUpdateResult>,
        cx: &mut Context<'_>,
    ) -> Result<RoutinePollResult, WorkflowFailure> {
        match Self::poll_routine_loop(&self.base_ctx, cx, &mut future) {
            RoutinePollState::Ready {
                result,
                made_progress,
            } => {
                let completion = match result {
                    Ok(result) => match serialize_workflow_output(
                        result.as_ref(),
                        self.ctx.payload_converter(),
                    )
                    .map_err(WorkflowError::from)
                    {
                        Ok(result) => UpdateRoutineCompletion::Completed {
                            protocol_instance_id,
                            result,
                        },
                        Err(err) => UpdateRoutineCompletion::Rejected {
                            protocol_instance_id,
                            failure: Box::new(self.workflow_error_to_failure(err)),
                        },
                    },
                    Err(err) => UpdateRoutineCompletion::Rejected {
                        protocol_instance_id,
                        failure: Box::new(self.workflow_error_to_failure(err)),
                    },
                };
                Ok(RoutinePollResult {
                    completion: Some(RoutineCompletion::Update(completion)),
                    made_progress,
                    pending_state: None,
                })
            }
            RoutinePollState::ForcedFailure { failure, .. } => Err(failure),
            RoutinePollState::Stalled {
                made_progress,
                pending_state,
            } => {
                self.routines.insert(
                    routine_id,
                    GuestRoutine::Update {
                        protocol_instance_id,
                        future,
                    },
                );
                Ok(RoutinePollResult {
                    completion: None,
                    made_progress,
                    pending_state: Some(pending_state),
                })
            }
        }
    }
}

impl<W: WorkflowImplementation> WorkflowInstance for GuestWorkflowInstance<W>
where
    <W::Run as WorkflowDefinition>::Input: Send,
{
    fn activate(
        &mut self,
        activation: WorkflowActivation,
        waker: &Waker,
    ) -> Result<ActivationResult, WorkflowFailure> {
        let base_ctx = self.base_ctx.clone();
        let _waker_guard = base_ctx.enter_runtime_poll(waker);
        let is_replaying_history_events = activation.is_replaying
            && activation
                .jobs
                .iter()
                .any(|job| !matches!(job.variant, Some(ActivationVariant::QueryWorkflow(_))));
        self.base_ctx
            .apply_activation_context(&activation, is_replaying_history_events);
        let mut job_results = Vec::with_capacity(activation.jobs.len());
        for job in activation.jobs {
            let result = match job.variant {
                Some(ActivationVariant::InitializeWorkflow(_)) => {
                    if !self.main_construction_polled {
                        if let ConstructionPoll::Ready(result) =
                            Self::poll_for_construction(&self.base_ctx, &mut self.run_future)?
                        {
                            self.run_future = InterceptedFuture::ready(result);
                        }
                        self.main_construction_polled = true;
                    }
                    ActivationJobResult::None
                }
                Some(ActivationVariant::UpdateRandomSeed(_)) => ActivationJobResult::None,
                Some(ActivationVariant::NotifyHasPatch(patch)) => {
                    self.base_ctx.notify_patch(patch.patch_id);
                    ActivationJobResult::None
                }
                Some(ActivationVariant::CancelWorkflow(cancel)) => {
                    self.base_ctx.notify_cancel(cancel.reason);
                    ActivationJobResult::None
                }
                Some(ActivationVariant::SignalWorkflow(signal)) => {
                    self.start_signal_routine(signal)?
                }
                Some(ActivationVariant::DoUpdate(update)) => self.start_update_routine(update)?,
                Some(ActivationVariant::QueryWorkflow(query)) => {
                    ActivationJobResult::QueryResponse(Box::new(self.query(query)))
                }
                Some(
                    resolution @ (ActivationVariant::FireTimer(_)
                    | ActivationVariant::ResolveActivity(_)
                    | ActivationVariant::ResolveChildWorkflowExecutionStart(_)
                    | ActivationVariant::ResolveChildWorkflowExecution(_)
                    | ActivationVariant::ResolveSignalExternalWorkflow(_)
                    | ActivationVariant::ResolveRequestCancelExternalWorkflow(_)
                    | ActivationVariant::ResolveNexusOperationStart(_)
                    | ActivationVariant::ResolveNexusOperation(_)),
                ) => {
                    self.apply_resolution(resolution);
                    ActivationJobResult::None
                }
                Some(ActivationVariant::RemoveFromCache(_)) => ActivationJobResult::None,
                None => {
                    return Err(Box::new(Failure {
                        message: "Activation job missing variant".to_string(),
                        ..Default::default()
                    }));
                }
            };
            job_results.push(result);
        }
        Ok(ActivationResult { job_results })
    }

    fn poll_routine(
        &mut self,
        routine_id: RoutineId,
        waker: &Waker,
    ) -> Result<RoutinePollResult, WorkflowFailure> {
        let base_ctx = self.base_ctx.clone();
        let _waker_guard = base_ctx.enter_runtime_poll(waker);
        let mut cx = Context::from_waker(waker);
        if routine_id == MAIN_ROUTINE_ID {
            return self.poll_main_routine(&mut cx);
        }

        let routine = self.routines.remove(&routine_id).ok_or_else(|| {
            Box::new(Failure {
                message: format!("No routine registered for id {routine_id}"),
                ..Default::default()
            })
        })?;

        match routine {
            GuestRoutine::Signal { future } => {
                self.poll_signal_routine(routine_id, future, &mut cx)
            }
            GuestRoutine::Update {
                protocol_instance_id,
                future,
            } => self.poll_update_routine(routine_id, protocol_instance_id, future, &mut cx),
        }
    }
}

pub fn instantiate_workflow<W: WorkflowImplementation>(
    payloads: Vec<Payload>,
    converter: PayloadConverter,
    base_ctx: BaseWorkflowContext,
) -> Result<Box<dyn WorkflowInstance>, PayloadConversionError>
where
    <W::Run as WorkflowDefinition>::Input: Send,
{
    GuestWorkflowInstance::<W>::instantiate(payloads, converter, base_ctx)
}

/// Attempts to turn caught panics into something printable
fn panic_formatter(panic: Box<dyn Any>) -> Box<dyn Display> {
    _panic_formatter::<&str>(panic)
}
fn _panic_formatter<T: 'static + PrintablePanicType>(panic: Box<dyn Any>) -> Box<dyn Display> {
    match panic.downcast::<T>() {
        Ok(d) => d,
        Err(orig) => {
            if TypeId::of::<<T as PrintablePanicType>::NextType>()
                == TypeId::of::<EndPrintingAttempts>()
            {
                return Box::new("Couldn't turn panic into a string");
            }
            _panic_formatter::<T::NextType>(orig)
        }
    }
}
trait PrintablePanicType: Display {
    type NextType: PrintablePanicType;
}

impl PrintablePanicType for &str {
    type NextType = String;
}
impl PrintablePanicType for String {
    type NextType = EndPrintingAttempts;
}
struct EndPrintingAttempts {}
impl Display for EndPrintingAttempts {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Will never be printed")
    }
}
impl PrintablePanicType for EndPrintingAttempts {
    type NextType = EndPrintingAttempts;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        runtime::{host::WorkflowHost, types::WorkflowInit},
        workflow_interceptors::WorkflowInterceptorConstructor,
    };
    use std::{
        cell::Cell,
        rc::Rc,
        sync::atomic::{AtomicUsize, Ordering},
        task::Waker,
    };
    use temporalio_common_wasm::{
        data_converters::{DataConverter, TemporalDeserializable, TemporalSerializable},
        protos::{
            coresdk::{
                workflow_activation::InitializeWorkflow, workflow_commands::WorkflowCommand,
            },
            temporal::api::common::v1::Payload,
        },
    };
    use temporalio_macros::{workflow, workflow_methods};

    struct NoopHost;

    impl WorkflowHost for NoopHost {
        fn set_current_details(&self, _details: String) {}

        fn push_command(&self, _command: WorkflowCommand) {}
    }

    #[workflow]
    #[derive(Default)]
    struct ForcedFailureWorkflow;

    #[workflow_methods]
    impl ForcedFailureWorkflow {
        #[run]
        async fn run(ctx: &mut WorkflowContext<Self>) -> crate::WorkflowResult<()> {
            ctx.force_task_fail(std::io::Error::other("forced failure"));
            Ok(())
        }
    }

    struct FailingInput;

    impl TemporalSerializable for FailingInput {}
    impl TemporalDeserializable for FailingInput {}

    #[workflow]
    #[derive(Default)]
    struct DecodeFailureWorkflow;

    #[workflow_methods]
    impl DecodeFailureWorkflow {
        #[run]
        async fn run(
            _ctx: &mut WorkflowContext<Self>,
            _input: FailingInput,
        ) -> crate::WorkflowResult<()> {
            unreachable!("workflow execution must not start when its input cannot be decoded")
        }
    }

    struct CountingExecuteInterceptor {
        calls: Arc<AtomicUsize>,
    }

    impl WorkflowInterceptor for CountingExecuteInterceptor {
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
            self.calls.fetch_add(1, Ordering::Relaxed);
            next.run(input)
        }
    }

    #[test]
    fn intercepted_future_reports_interceptor_pending() {
        let status = InterceptedFutureStatus::new();
        let mut future = InterceptedFuture::new(std::future::pending::<()>().boxed_local(), status);
        let mut cx = Context::from_waker(Waker::noop());

        assert!(future.poll_unpin(&mut cx).is_pending());
        assert_eq!(future.pending_state(), RoutinePendingState::Interceptor);
    }

    #[test]
    fn intercepted_future_reports_sdk_activation() {
        let status = InterceptedFutureStatus::new();
        let inner = std::future::poll_fn(|_| {
            crate::runtime::mark_intercepted_future_activation();
            Poll::<()>::Pending
        });
        let mut future = InterceptedFuture::new(inner.boxed_local(), status);
        let mut cx = Context::from_waker(Waker::noop());

        assert!(future.poll_unpin(&mut cx).is_pending());
        assert_eq!(
            future.pending_state(),
            RoutinePendingState::InterceptorWithActivation
        );
    }

    #[test]
    fn intercepted_future_reports_polled_handler_boundary() {
        let status = InterceptedFutureStatus::new();
        let boundary = HandlerBoundaryFuture::new(std::future::pending::<()>(), status.clone());
        let mut future = InterceptedFuture::new(boundary.boxed_local(), status);
        let mut cx = Context::from_waker(Waker::noop());

        assert!(future.poll_unpin(&mut cx).is_pending());
        assert_eq!(future.pending_state(), RoutinePendingState::Handler);
    }

    #[test]
    fn construction_poll_stops_at_async_handler_boundary() {
        let status = InterceptedFutureStatus::new();
        let polls = Rc::new(Cell::new(0));
        let poll_count = polls.clone();
        let inner = std::future::poll_fn(move |_| {
            poll_count.set(poll_count.get() + 1);
            Poll::Ready(42)
        });
        let boundary = HandlerBoundaryFuture::new(inner, status.clone());
        let mut future = InterceptedFuture::new(boundary.boxed_local(), status);
        let mut cx = Context::from_waker(Waker::noop());

        assert_eq!(future.poll_for_construction(&mut cx), Poll::Pending);
        assert_eq!(polls.get(), 0);
        assert_eq!(future.poll_unpin(&mut cx), Poll::Ready(42));
        assert_eq!(polls.get(), 1);
    }

    #[test]
    fn construction_poll_drives_ready_handler_result() {
        let status = InterceptedFutureStatus::new();
        let boundary_status = status.clone();
        let polls = Rc::new(Cell::new(0));
        let poll_count = polls.clone();
        let inner = async move {
            crate::runtime::mark_intercepted_handler_ready();
            HandlerBoundaryFuture::new(
                std::future::poll_fn(move |_| {
                    poll_count.set(poll_count.get() + 1);
                    Poll::Ready(42)
                }),
                boundary_status,
            )
            .await
        };
        let mut future = InterceptedFuture::new(inner.boxed_local(), status);
        let mut cx = Context::from_waker(Waker::noop());

        assert_eq!(future.poll_for_construction(&mut cx), Poll::Ready(42));
        assert_eq!(polls.get(), 1);
        assert_eq!(future.pending_state(), RoutinePendingState::Handler);
    }

    #[test]
    fn unpolled_ready_handler_still_reports_interceptor_pending() {
        let status = InterceptedFutureStatus::new();
        let boundary_status = status.clone();
        let inner = async move {
            crate::runtime::mark_intercepted_handler_ready();
            let boundary = HandlerBoundaryFuture::new(ready(42), boundary_status);
            std::future::pending::<()>().await;
            boundary.await
        };
        let mut future = InterceptedFuture::new(inner.boxed_local(), status);
        let mut cx = Context::from_waker(Waker::noop());

        assert_eq!(future.poll_for_construction(&mut cx), Poll::Pending);
        assert_eq!(future.pending_state(), RoutinePendingState::Interceptor);
    }

    #[test]
    fn handler_boundary_first_reached_during_routine_poll_does_not_block() {
        let status = InterceptedFutureStatus::new();
        let boundary_status = status.clone();
        let first_poll = Rc::new(Cell::new(true));
        let defer = first_poll.clone();
        let inner = async move {
            std::future::poll_fn(move |_| {
                if defer.replace(false) {
                    Poll::Pending
                } else {
                    Poll::Ready(())
                }
            })
            .await;
            HandlerBoundaryFuture::new(ready(42), boundary_status).await
        };
        let mut future = InterceptedFuture::new(inner.boxed_local(), status);
        let mut cx = Context::from_waker(Waker::noop());

        assert_eq!(future.poll_for_construction(&mut cx), Poll::Pending);
        assert_eq!(future.poll_unpin(&mut cx), Poll::Ready(42));
    }

    #[test]
    fn construction_poll_kind_is_restored_after_panic() {
        let status = InterceptedFutureStatus::new();
        let polled_status = status.clone();
        let inner = std::future::poll_fn(move |_| -> Poll<()> {
            assert_eq!(
                polled_status.poll_kind(),
                InterceptedFuturePollKind::Construction
            );
            panic!("test panic");
        });
        let mut future = InterceptedFuture::new(inner.boxed_local(), status.clone());
        let mut cx = Context::from_waker(Waker::noop());

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            future.poll_for_construction(&mut cx)
        }));

        assert!(result.is_err());
        assert_eq!(status.poll_kind(), InterceptedFuturePollKind::Routine);
    }

    #[test]
    fn forced_failure_set_during_ready_poll_wins_over_completion() {
        let init = WorkflowInit {
            namespace: "default".to_string(),
            task_queue: "task-queue".to_string(),
            run_id: "run-id".to_string(),
            initialize_workflow: InitializeWorkflow {
                workflow_type: ForcedFailureWorkflow::name().to_string(),
                ..Default::default()
            },
        };
        let base_ctx = BaseWorkflowContext::from_raw(
            init,
            DataConverter::default(),
            Rc::new(NoopHost),
            None,
            Vec::new(),
        );

        let mut instance = GuestWorkflowInstance::<ForcedFailureWorkflow>::new_with_workflow(
            ForcedFailureWorkflow,
            base_ctx,
            None,
        );

        let result = instance
            .poll_routine(MAIN_ROUTINE_ID, Waker::noop())
            .unwrap();

        let Some(RoutineCompletion::Main(MainRoutineCompletion::TaskFailed(failure))) =
            result.completion
        else {
            panic!("expected a workflow task failure, got {result:?}");
        };
        assert_eq!(failure.failure.message, "forced failure");
    }

    #[test]
    fn interceptor_constructors_run_before_workflow_input_decoding() {
        let constructor_calls = Arc::new(AtomicUsize::new(0));
        let execute_calls = Arc::new(AtomicUsize::new(0));
        let constructor_calls_ref = constructor_calls.clone();
        let execute_calls_ref = execute_calls.clone();
        let constructor = WorkflowInterceptorConstructor::new(move |ctx| {
            assert_eq!(ctx.namespace(), "default");
            assert_eq!(ctx.task_queue(), "task-queue");
            assert_eq!(ctx.run_id(), "run-id");
            assert_eq!(ctx.workflow_type(), DecodeFailureWorkflow::name());
            constructor_calls_ref.fetch_add(1, Ordering::Relaxed);
            CountingExecuteInterceptor {
                calls: execute_calls_ref.clone(),
            }
        });
        let init = WorkflowInit {
            namespace: "default".to_string(),
            task_queue: "task-queue".to_string(),
            run_id: "run-id".to_string(),
            initialize_workflow: InitializeWorkflow {
                workflow_type: DecodeFailureWorkflow::name().to_string(),
                ..Default::default()
            },
        };
        let base_ctx = BaseWorkflowContext::from_raw(
            init,
            DataConverter::default(),
            Rc::new(NoopHost),
            None,
            vec![constructor],
        );

        let result = GuestWorkflowInstance::<DecodeFailureWorkflow>::instantiate(
            vec![Payload::default()],
            PayloadConverter::default(),
            base_ctx,
        );

        assert!(result.is_err());
        assert_eq!(constructor_calls.load(Ordering::Relaxed), 1);
        assert_eq!(execute_calls.load(Ordering::Relaxed), 0);
    }
}
