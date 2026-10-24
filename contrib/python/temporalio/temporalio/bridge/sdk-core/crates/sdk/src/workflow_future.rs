use anyhow::{Context as AnyhowContext, Error, anyhow, bail};
use std::{
    cell::RefCell,
    future::Future,
    panic,
    panic::AssertUnwindSafe,
    pin::Pin,
    rc::Rc,
    task::{Context, Poll, Waker},
};
use temporalio_common::{
    data_converters::DataConverter,
    error::ApplicationFailure,
    protos::{
        coresdk::{
            workflow_activation::{
                InitializeWorkflow, WorkflowActivation as CoreWorkflowActivation,
                workflow_activation_job::Variant,
            },
            workflow_commands::{
                CancelWorkflowExecution, CompleteWorkflowExecution, FailWorkflowExecution,
                QueryResult, QuerySuccess, UpdateResponse, WorkflowCommand, query_result,
                update_response, workflow_command,
            },
            workflow_completion::{
                self, WorkflowActivationCompletion, workflow_activation_completion,
            },
        },
        temporal::api::{
            common::v1::Payload,
            enums::v1::{VersioningBehavior, WorkflowTaskFailedCause},
            failure::v1::Failure,
        },
    },
};
use temporalio_workflow::{
    PatchActivationCallback,
    runtime::{
        guest::WorkflowInstance,
        host::WorkflowHost,
        model::{WorkflowResult, WorkflowTermination},
        types::{
            ActivationJobResult, ActivationResult, MainRoutineCompletion, RoutineCompletion,
            RoutineId, RoutineKind, RoutinePendingState, RoutinePollResult, TerminalOutcome,
            UpdateRoutineCompletion, WorkflowActivation,
        },
    },
};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};

use crate::{
    panic_formatter,
    workflow_executor::WakeTracker,
    workflow_interceptors::WorkflowInterceptorConstructor,
    workflow_registry::{WorkflowExecutionFactory, WorkflowExecutionInput},
};

/// Start a workflow function, returning a future that will resolve when the workflow does,
/// and a channel that can be used to send it activations.
#[allow(clippy::too_many_arguments)]
pub(crate) fn start_workflow(
    factory: WorkflowExecutionFactory,
    namespace: String,
    task_queue: String,
    run_id: String,
    init_workflow_job: InitializeWorkflow,
    outgoing_completions: UnboundedSender<WorkflowActivationCompletion>,
    data_converter: DataConverter,
    detect_nondeterministic: bool,
    patch_activation_callback: Option<PatchActivationCallback>,
    workflow_interceptor_constructors: Vec<WorkflowInterceptorConstructor>,
) -> Result<
    (
        impl Future<Output = WorkflowResult<Payload>> + use<>,
        UnboundedSender<CoreWorkflowActivation>,
    ),
    anyhow::Error,
> {
    let span = info_span!(
        "RunWorkflow",
        "otel.name" = format!("RunWorkflow:{}", &init_workflow_job.workflow_type),
        "otel.kind" = "server"
    );

    let host = Rc::new(NativeWorkflowHost::default());

    let execution = factory(WorkflowExecutionInput {
        namespace,
        task_queue,
        run_id,
        init_workflow_job,
        data_converter: data_converter.clone(),
        host: host.clone(),
        patch_activation_callback,
        workflow_interceptor_constructors,
    })
    .context("Failed to create workflow execution")?;

    let wake_tracking = if detect_nondeterministic {
        Some(WakeTracker::new())
    } else {
        None
    };

    let (tx, incoming_activations) = unbounded_channel();
    Ok((
        WorkflowFuture {
            execution,
            host,
            span,
            outgoing_completions,
            incoming_activations,
            wake_tracking,
            active_routines: Vec::new(),
            in_flight_activation: None,
        },
        tx,
    ))
}

#[derive(Default)]
struct NativeWorkflowHost {
    activation_cmds: RefCell<Vec<WorkflowCommand>>,
}

impl NativeWorkflowHost {
    fn push_command_variant(&self, variant: workflow_command::Variant) {
        self.push_command(variant.into());
    }

    fn push_command(&self, command: WorkflowCommand) {
        self.activation_cmds.borrow_mut().push(command);
    }

    fn take_commands(&self) -> Vec<WorkflowCommand> {
        std::mem::take(&mut *self.activation_cmds.borrow_mut())
    }
}

impl WorkflowHost for NativeWorkflowHost {
    fn set_current_details(&self, _details: String) {}

    fn push_command(&self, command: WorkflowCommand) {
        NativeWorkflowHost::push_command(self, command);
    }
}

pub(crate) struct WorkflowFuture {
    /// The workflow execution instance
    execution: Box<dyn WorkflowInstance>,
    /// Native host adapter collecting commands emitted by guest workflow code.
    host: Rc<NativeWorkflowHost>,
    /// The tracing span for this workflow
    span: tracing::Span,
    /// Once blocked or the workflow has finished or errored out, the result is sent here
    outgoing_completions: UnboundedSender<WorkflowActivationCompletion>,
    /// Activations from core
    incoming_activations: UnboundedReceiver<CoreWorkflowActivation>,
    /// Nondeterminism detection tracker. When present, a tracking waker is used
    /// for polling user workflow code, and any non-SDK wake is flagged.
    wake_tracking: Option<WakeTracker>,
    /// Signal and update routines that are still live across activations.
    active_routines: Vec<RoutineId>,
    /// Activation retained while an interceptor is waiting for a wake that Core cannot provide.
    in_flight_activation: Option<InFlightActivation>,
}

struct InFlightActivation {
    run_id: String,
    commands: Vec<WorkflowCommand>,
}

enum RoutineDriveOutcome {
    Complete,
    AwaitWake,
    Failed,
}

#[derive(Debug)]
enum ActivationJobContext {
    Passive,
    Signal,
    Update { protocol_instance_id: String },
    Query { query_id: String },
}

impl WorkflowFuture {
    fn tracked_waker(&self, parent: &Waker) -> Option<Waker> {
        self.wake_tracking
            .as_ref()
            .map(|tracker| tracker.new_per_poll_waker(parent))
    }

    fn fail_on_non_sdk_wake(&self, run_id: &str) -> bool {
        if !self
            .wake_tracking
            .as_ref()
            .is_some_and(WakeTracker::take_non_sdk_wake)
        {
            return false;
        }
        self.fail_nondeterministic_future(
            run_id,
            "a waker was invoked by a non-SDK source. This usually means workflow code is using \
             nondeterministic operations like tokio async functions or channels, other async \
             libraries, or std::thread. Use SDK-provided alternatives (ctx.timer(), \
             ctx.state_mut() + ctx.wait_condition(), etc.) instead.",
        );
        true
    }

    fn fail_nondeterministic_future(&self, run_id: &str, message: &str) {
        self.fail_wft(
            run_id.to_owned(),
            anyhow!("[TMPRL1100] Nondeterministic future detected: {message}"),
            Some(WorkflowTaskFailedCause::NonDeterministicError),
        );
    }

    fn fail_wft(&self, run_id: String, fail: Error, cause: Option<WorkflowTaskFailedCause>) {
        self.fail_wft_with_failure(run_id, fail.into(), cause);
    }

    fn fail_wft_with_failure(
        &self,
        run_id: String,
        failure: Failure,
        cause: Option<WorkflowTaskFailedCause>,
    ) {
        warn!("Workflow task failed for {}: {}", run_id, failure);
        self.outgoing_completions
            .send(WorkflowActivationCompletion::fail(run_id, failure, cause))
            .expect("Completion channel intact");
    }

    fn send_completion(&self, run_id: String, activation_cmds: Vec<WorkflowCommand>) {
        self.outgoing_completions
            .send(WorkflowActivationCompletion {
                run_id,
                status: Some(workflow_activation_completion::Status::Successful(
                    workflow_completion::Success {
                        commands: activation_cmds,
                        used_internal_flags: vec![],
                        versioning_behavior: VersioningBehavior::Unspecified.into(),
                    },
                )),
                ..Default::default()
            })
            .expect("Completion channel intact");
    }

    fn translate_activation(
        &self,
        mut activation: CoreWorkflowActivation,
    ) -> Result<(WorkflowActivation, Vec<ActivationJobContext>, bool), Error> {
        let mut should_poll_routines = false;
        let mut job_contexts = Vec::with_capacity(activation.jobs.len());
        macro_rules! push_context {
            ($context:expr $(,)?) => {{
                job_contexts.push($context);
            }};
        }
        macro_rules! push_polled_context {
            ($context:expr $(,)?) => {{
                should_poll_routines = true;
                push_context!($context);
            }};
        }
        for job in &mut activation.jobs {
            match job
                .variant
                .as_mut()
                .context("Empty activation job variant")?
            {
                Variant::InitializeWorkflow(_) => {
                    push_polled_context!(ActivationJobContext::Passive);
                }
                Variant::FireTimer(_) => {
                    push_polled_context!(ActivationJobContext::Passive);
                }
                Variant::ResolveActivity(attrs) => {
                    attrs.result.as_ref().context("Activity must have result")?;
                    push_polled_context!(ActivationJobContext::Passive);
                }
                Variant::ResolveChildWorkflowExecutionStart(attrs) => {
                    attrs
                        .status
                        .as_ref()
                        .context("Workflow start must have status")?;
                    push_polled_context!(ActivationJobContext::Passive);
                }
                Variant::ResolveChildWorkflowExecution(attrs) => {
                    attrs
                        .result
                        .as_ref()
                        .context("Child Workflow execution must have a result")?;
                    push_polled_context!(ActivationJobContext::Passive);
                }
                Variant::UpdateRandomSeed(_) => {
                    push_polled_context!(ActivationJobContext::Passive);
                }
                Variant::QueryWorkflow(q) => {
                    debug!(query_type = %q.query_type, "Query received");
                    let query_id = q.query_id.clone();
                    push_context!(ActivationJobContext::Query { query_id });
                }
                Variant::CancelWorkflow(_) => {
                    push_polled_context!(ActivationJobContext::Passive);
                }
                Variant::SignalWorkflow(sig) => {
                    debug!(signal_name = %sig.signal_name, "Signal received");
                    push_polled_context!(ActivationJobContext::Signal);
                }
                Variant::NotifyHasPatch(_) => {
                    push_polled_context!(ActivationJobContext::Passive);
                }
                Variant::ResolveSignalExternalWorkflow(_) => {
                    push_polled_context!(ActivationJobContext::Passive);
                }
                Variant::ResolveRequestCancelExternalWorkflow(_) => {
                    push_polled_context!(ActivationJobContext::Passive);
                }
                Variant::DoUpdate(u) => {
                    let protocol_instance_id = u.protocol_instance_id.clone();
                    push_polled_context!(ActivationJobContext::Update {
                        protocol_instance_id,
                    });
                }
                Variant::ResolveNexusOperationStart(attrs) => {
                    attrs
                        .status
                        .as_ref()
                        .context("Nexus operation start must have status")?;
                    push_polled_context!(ActivationJobContext::Passive);
                }
                Variant::ResolveNexusOperation(attrs) => {
                    attrs
                        .result
                        .as_ref()
                        .context("Nexus operation must have result")?;
                    push_polled_context!(ActivationJobContext::Passive);
                }
                Variant::RemoveFromCache(_) => {
                    unreachable!("Cache removal should happen higher up");
                }
            }
        }

        Ok((activation, job_contexts, should_poll_routines))
    }

    fn process_activation_results(
        &mut self,
        job_contexts: Vec<ActivationJobContext>,
        activation_result: ActivationResult,
        outgoing_cmds: &mut Vec<WorkflowCommand>,
    ) -> Result<(), Error> {
        if job_contexts.len() != activation_result.job_results.len() {
            bail!(
                "Activation result count {} did not match job count {}",
                activation_result.job_results.len(),
                job_contexts.len()
            );
        }

        for (job_context, job_result) in job_contexts.into_iter().zip(activation_result.job_results)
        {
            match (job_context, job_result) {
                (ActivationJobContext::Passive, ActivationJobResult::None)
                | (ActivationJobContext::Signal, ActivationJobResult::None) => {}
                (
                    ActivationJobContext::Signal,
                    ActivationJobResult::StartedRoutine(started_routine),
                ) => match started_routine.kind {
                    RoutineKind::Signal(_) => self.active_routines.push(started_routine.routine_id),
                    other => bail!("Signal job started unexpected routine kind {other:?}"),
                },
                (
                    ActivationJobContext::Update {
                        protocol_instance_id,
                    },
                    ActivationJobResult::StartedRoutine(started_routine),
                ) => match started_routine.kind {
                    RoutineKind::Update(update_kind) => {
                        let started_id = update_kind.protocol_instance_id;
                        if started_id != protocol_instance_id {
                            bail!(
                                "Update routine protocol instance id {} did not match {}",
                                started_id,
                                protocol_instance_id
                            );
                        }
                        outgoing_cmds.push(
                            update_response(
                                protocol_instance_id,
                                update_response::Response::Accepted(()),
                            )
                            .into(),
                        );
                        self.active_routines.push(started_routine.routine_id);
                    }
                    other => bail!("Update job started unexpected routine kind {other:?}"),
                },
                (
                    ActivationJobContext::Update {
                        protocol_instance_id,
                    },
                    ActivationJobResult::UpdateRejected(failure),
                ) => {
                    outgoing_cmds.push(
                        update_response(
                            protocol_instance_id,
                            update_response::Response::Rejected(*failure),
                        )
                        .into(),
                    );
                }
                (
                    ActivationJobContext::Query { query_id },
                    ActivationJobResult::QueryResponse(query_response),
                ) => outgoing_cmds.push({
                    let response = *query_response;
                    workflow_command::Variant::RespondToQuery(QueryResult {
                        query_id,
                        variant: Some(match response.result {
                            Ok(payload) => query_result::Variant::Succeeded(QuerySuccess {
                                response: Some(payload),
                            }),
                            Err(failure) => query_result::Variant::Failed(failure),
                        }),
                    })
                    .into()
                }),
                (job_context, job_result) => {
                    bail!("Unexpected activation result {job_result:?} for job {job_context:?}");
                }
            }
        }

        Ok(())
    }

    fn poll_guest_routine(
        &mut self,
        routine_id: RoutineId,
        cx: &Context<'_>,
    ) -> Result<RoutinePollResult, Error> {
        let span = self.span.clone();
        let _guard = span.enter();
        let tracked_waker = self.tracked_waker(cx.waker());
        let waker = tracked_waker.as_ref().unwrap_or(cx.waker());
        match panic::catch_unwind(AssertUnwindSafe(|| {
            self.execution.poll_routine(routine_id, waker)
        })) {
            Ok(Ok(result)) => Ok(result),
            Ok(Err(err)) => Err(anyhow!(err.message)),
            Err(e) => bail!("Workflow function panicked: {}", panic_formatter(e)),
        }
    }

    fn drive_routines(
        &mut self,
        cx: &Context<'_>,
        run_id: &str,
        activation_cmds: &mut Vec<WorkflowCommand>,
    ) -> RoutineDriveOutcome {
        loop {
            let mut pass_made_progress = false;
            let mut interceptor_pending = false;
            let mut should_stop_polling = false;
            let mut still_active = Vec::with_capacity(self.active_routines.len());
            for routine_id in std::mem::take(&mut self.active_routines) {
                let poll_result = match self.poll_guest_routine(routine_id, cx) {
                    Ok(result) => result,
                    Err(e) => {
                        self.fail_wft(run_id.to_owned(), e, None);
                        return RoutineDriveOutcome::Failed;
                    }
                };
                if self.fail_on_non_sdk_wake(run_id) {
                    return RoutineDriveOutcome::Failed;
                }
                pass_made_progress |= poll_result.made_progress;
                interceptor_pending |= matches!(
                    poll_result.pending_state,
                    Some(RoutinePendingState::Interceptor)
                );
                match poll_result.completion {
                    None => still_active.push(routine_id),
                    Some(result) => match result {
                        RoutineCompletion::Signal(Ok(())) => {}
                        RoutineCompletion::Signal(Err(failure)) => {
                            self.fail_wft(run_id.to_owned(), anyhow!(failure.message), None);
                            return RoutineDriveOutcome::Failed;
                        }
                        RoutineCompletion::Update(UpdateRoutineCompletion::Completed {
                            protocol_instance_id,
                            result,
                        }) => activation_cmds.push(
                            update_response(
                                protocol_instance_id,
                                update_response::Response::Completed(result),
                            )
                            .into(),
                        ),
                        RoutineCompletion::Update(UpdateRoutineCompletion::Rejected {
                            protocol_instance_id,
                            failure,
                        }) => activation_cmds.push(
                            update_response(
                                protocol_instance_id,
                                update_response::Response::Rejected(*failure),
                            )
                            .into(),
                        ),
                        RoutineCompletion::Main(_) => {
                            self.fail_wft(
                                run_id.to_owned(),
                                anyhow!("non-main routine returned a main completion"),
                                None,
                            );
                            return RoutineDriveOutcome::Failed;
                        }
                    },
                }
            }
            self.active_routines = still_active;

            let main_poll_result = match self
                .poll_guest_routine(temporalio_workflow::runtime::types::MAIN_ROUTINE_ID, cx)
            {
                Ok(result) => result,
                Err(e) => {
                    self.fail_wft(run_id.to_owned(), e, None);
                    return RoutineDriveOutcome::Failed;
                }
            };
            if self.fail_on_non_sdk_wake(run_id) {
                return RoutineDriveOutcome::Failed;
            }
            pass_made_progress |= main_poll_result.made_progress;
            interceptor_pending |= matches!(
                main_poll_result.pending_state,
                Some(RoutinePendingState::Interceptor)
            );

            match main_poll_result.completion {
                None => {
                    self.fail_wft(
                        run_id.to_owned(),
                        anyhow!("main routine returned no completion"),
                        None,
                    );
                    return RoutineDriveOutcome::Failed;
                }
                Some(result) => match result {
                    RoutineCompletion::Main(MainRoutineCompletion::Blocked) => {}
                    RoutineCompletion::Main(MainRoutineCompletion::TaskFailed(task_failure)) => {
                        self.fail_wft_with_failure(
                            run_id.to_owned(),
                            *task_failure.failure,
                            workflow_task_failed_cause_from_wit(task_failure.force_cause),
                        );
                        return RoutineDriveOutcome::Failed;
                    }
                    RoutineCompletion::Main(MainRoutineCompletion::Terminal(outcome)) => {
                        let outcome = *outcome;
                        match outcome {
                            TerminalOutcome::Completed(result) => {
                                self.host.push_command_variant(
                                    workflow_command::Variant::CompleteWorkflowExecution(
                                        CompleteWorkflowExecution {
                                            result: Some(result),
                                        },
                                    ),
                                );
                            }
                            TerminalOutcome::Failed(failure) => {
                                self.host.push_command_variant(
                                    workflow_command::Variant::FailWorkflowExecution(
                                        FailWorkflowExecution {
                                            failure: Some(*failure),
                                        },
                                    ),
                                );
                            }
                            TerminalOutcome::Cancelled => {
                                self.host.push_command_variant(
                                    workflow_command::Variant::CancelWorkflowExecution(
                                        CancelWorkflowExecution {},
                                    ),
                                );
                            }
                            TerminalOutcome::ContinueAsNew(req) => {
                                self.host.push_command_variant(
                                    workflow_command::Variant::ContinueAsNewWorkflowExecution(*req),
                                );
                            }
                        }
                        should_stop_polling = true;
                    }
                    other => {
                        self.fail_wft(
                            run_id.to_owned(),
                            anyhow!("main routine returned unexpected completion {other:?}"),
                            None,
                        );
                        return RoutineDriveOutcome::Failed;
                    }
                },
            }

            if should_stop_polling {
                return RoutineDriveOutcome::Complete;
            }
            if !pass_made_progress {
                if !interceptor_pending {
                    return RoutineDriveOutcome::Complete;
                }
                if self.wake_tracking.is_some() {
                    self.fail_nondeterministic_future(
                        run_id,
                        "a workflow interceptor returned Poll::Pending without polling its \
                         handler or an SDK operation that will produce another workflow \
                         activation.",
                    );
                    return RoutineDriveOutcome::Failed;
                }
                return RoutineDriveOutcome::AwaitWake;
            }
        }
    }
}

impl Future for WorkflowFuture {
    type Output = WorkflowResult<Payload>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        'activations: loop {
            let mut in_flight = if let Some(in_flight) = self.in_flight_activation.take() {
                if self.fail_on_non_sdk_wake(&in_flight.run_id) {
                    continue 'activations;
                }
                in_flight
            } else {
                let activation = match self.incoming_activations.poll_recv(cx) {
                    Poll::Ready(a) => match a {
                        Some(act) => act,
                        None => {
                            return Poll::Ready(Err(WorkflowTermination::failed_application(
                                ApplicationFailure::new(
                                    "Workflow future's activation channel was lost!",
                                ),
                            )));
                        }
                    },
                    Poll::Pending => return Poll::Pending,
                };

                let run_id = activation.run_id.clone();
                if activation.is_only_eviction() {
                    self.outgoing_completions
                        .send(WorkflowActivationCompletion::from_cmds(run_id, vec![]))
                        .expect("Completion channel intact");
                    return Err(WorkflowTermination::Evicted).into();
                }
                if self.fail_on_non_sdk_wake(&run_id) {
                    continue 'activations;
                }

                let (guest_activation, job_contexts, should_poll_routines) =
                    match self.translate_activation(activation) {
                        Ok(translated) => translated,
                        Err(e) => {
                            self.fail_wft(run_id, e, None);
                            continue 'activations;
                        }
                    };

                let tracked_waker = self.tracked_waker(cx.waker());
                let waker = tracked_waker.as_ref().unwrap_or(cx.waker());
                let activation_result = match panic::catch_unwind(AssertUnwindSafe(|| {
                    self.execution.activate(guest_activation, waker)
                })) {
                    Ok(Ok(result)) => result,
                    Ok(Err(err)) => {
                        self.fail_wft(run_id.clone(), anyhow!(err.message), None);
                        continue 'activations;
                    }
                    Err(e) => {
                        self.fail_wft(
                            run_id.clone(),
                            anyhow!("Workflow function panicked: {}", panic_formatter(e)),
                            None,
                        );
                        continue 'activations;
                    }
                };
                if self.fail_on_non_sdk_wake(&run_id) {
                    continue 'activations;
                }

                let mut commands = Vec::new();
                if let Err(e) =
                    self.process_activation_results(job_contexts, activation_result, &mut commands)
                {
                    self.fail_wft(run_id.clone(), e, None);
                    continue 'activations;
                }

                if !should_poll_routines {
                    commands.extend(self.host.take_commands());
                    self.send_completion(run_id, commands);
                    continue 'activations;
                }

                InFlightActivation { run_id, commands }
            };

            match self.drive_routines(cx, &in_flight.run_id, &mut in_flight.commands) {
                RoutineDriveOutcome::Complete => {
                    in_flight.commands.extend(self.host.take_commands());
                    self.send_completion(in_flight.run_id, in_flight.commands);
                }
                RoutineDriveOutcome::AwaitWake => {
                    self.in_flight_activation = Some(in_flight);
                    return Poll::Pending;
                }
                RoutineDriveOutcome::Failed => continue 'activations,
            }
        }
    }
}

fn workflow_task_failed_cause_from_wit(cause: Option<u32>) -> Option<WorkflowTaskFailedCause> {
    cause
        .and_then(|cause| i32::try_from(cause).ok())
        .and_then(|cause| WorkflowTaskFailedCause::try_from(cause).ok())
}

fn update_response(
    instance_id: String,
    resp: update_response::Response,
) -> workflow_command::Variant {
    UpdateResponse {
        protocol_instance_id: instance_id,
        response: Some(resp),
    }
    .into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn workflow_task_failed_cause_from_wit_preserves_known_causes() {
        assert_eq!(
            workflow_task_failed_cause_from_wit(Some(
                WorkflowTaskFailedCause::NonDeterministicError as u32
            )),
            Some(WorkflowTaskFailedCause::NonDeterministicError)
        );
        assert_eq!(
            workflow_task_failed_cause_from_wit(Some(
                WorkflowTaskFailedCause::GrpcMessageTooLarge as u32
            )),
            Some(WorkflowTaskFailedCause::GrpcMessageTooLarge)
        );
    }

    #[test]
    fn workflow_task_failed_cause_from_wit_ignores_unknown_causes() {
        assert_eq!(workflow_task_failed_cause_from_wit(None), None);
        assert_eq!(workflow_task_failed_cause_from_wit(Some(u32::MAX)), None);
    }
}
