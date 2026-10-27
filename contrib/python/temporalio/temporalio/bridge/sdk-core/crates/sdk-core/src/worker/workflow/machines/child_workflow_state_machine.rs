use super::{
    EventInfo, MachineError, NewMachineWithCommand, OnEventWrapper, StateMachine, TransitionResult,
    WFMachinesAdapter, WFMachinesError, fsm, workflow_machines::MachineResponse,
};
use crate::{
    abstractions::dbg_panic,
    internal_flags::CoreInternalFlags,
    worker::workflow::{InternalFlagsRef, fatal, machines::HistEventData, nondeterminism},
};
use itertools::Itertools;
use std::{
    convert::{TryFrom, TryInto},
    string::ToString,
};
use temporalio_common::protos::{
    coresdk::{
        child_workflow::{
            self as wfr, ChildWorkflowCancellationType, ChildWorkflowResult,
            child_workflow_result::Status as ChildWorkflowStatus,
        },
        workflow_activation::{
            ResolveChildWorkflowExecution, ResolveChildWorkflowExecutionStart,
            ResolveChildWorkflowExecutionStartCancelled, ResolveChildWorkflowExecutionStartFailure,
            ResolveChildWorkflowExecutionStartSuccess, resolve_child_workflow_execution_start,
        },
        workflow_commands::StartChildWorkflowExecution,
    },
    temporal::api::{
        command::v1::{
            RequestCancelExternalWorkflowExecutionCommandAttributes,
            start_child_workflow_cmd_to_api,
        },
        common::v1::{Payload, Payloads, WorkflowExecution, WorkflowType},
        enums::v1::{
            CommandType, EventType, RetryState, StartChildWorkflowExecutionFailedCause, TimeoutType,
        },
        failure::v1::{self as failure, Failure, failure::FailureInfo},
        history::v1::{
            ChildWorkflowExecutionCompletedEventAttributes,
            ChildWorkflowExecutionFailedEventAttributes,
            ChildWorkflowExecutionStartedEventAttributes,
            StartChildWorkflowExecutionFailedEventAttributes, history_event,
        },
    },
};

fsm! {
    pub(super) name ChildWorkflowMachine;
    command ChildWorkflowCommand;
    error WFMachinesError;
    shared_state SharedState;

    Created --(Schedule, on_schedule) --> StartCommandCreated;
    StartCommandCreated --(CommandStartChildWorkflowExecution) --> StartCommandCreated;
    StartCommandCreated --(StartChildWorkflowExecutionInitiated(ChildWorkflowInitiatedData),
        shared on_start_child_workflow_execution_initiated) --> StartEventRecorded;
    StartCommandCreated --(Cancel(String), shared on_cancelled) --> Cancelled;

    StartEventRecorded --(ChildWorkflowExecutionStarted(ChildWorkflowExecutionStartedEvent),
        shared on_child_workflow_execution_started) --> Started;
    StartEventRecorded --(StartChildWorkflowExecutionFailed(StartChildWorkflowExecutionFailedCause),
        on_start_child_workflow_execution_failed) --> StartFailed;
    // Cancel may arrive before ChildWorkflowExecutionStarted if the parent workflow is cancelled
    // while the child start is still in flight.
    StartEventRecorded --(Cancel(String), shared on_cancelled) --> StartEventRecorded;
    StartEventRecorded --(Cancel(String), shared on_cancelled) --> Cancelled;
    StartEventRecorded --(CommandRequestCancelExternalWorkflowExecution) --> StartEventRecorded;

    Started --(ChildWorkflowExecutionCompleted(Option<Payloads>),
        on_child_workflow_execution_completed) --> Completed;
    Started --(ChildWorkflowExecutionFailed(ChildWorkflowExecutionFailedEventAttributes),
        shared on_child_workflow_execution_failed) --> Failed;
    Started --(ChildWorkflowExecutionTimedOut(RetryState),
        shared on_child_workflow_execution_timed_out) --> TimedOut;
    Started --(ChildWorkflowExecutionCancelled,
        on_child_workflow_execution_cancelled) --> Cancelled;
    Started --(ChildWorkflowExecutionTerminated,
        shared on_child_workflow_execution_terminated) --> Terminated;
    // If cancelled after started, we need to issue a cancel external workflow command, and then
    // the child workflow will resolve somehow, so we want to go back to started and wait for that
    // resolution.
    Started --(Cancel(String), shared on_cancelled) --> Started;
    // Abandon & try cancel modes may immediately move to cancelled
    Started --(Cancel(String), shared on_cancelled) --> Cancelled;
    Started --(CommandRequestCancelExternalWorkflowExecution) --> Started;

    // Ignore any spurious cancellations after resolution
    Cancelled --(Cancel(String)) --> Cancelled;
    // ChildWorkflowExecutionStarted may arrive after we cancelled from StartEventRecorded
    Cancelled --(ChildWorkflowExecutionStarted(ChildWorkflowExecutionStartedEvent)) --> Cancelled;
    Cancelled --(StartChildWorkflowExecutionFailed(StartChildWorkflowExecutionFailedCause)) --> Cancelled;
    Cancelled --(CommandRequestCancelExternalWorkflowExecution) --> Cancelled;
    Cancelled --(ChildWorkflowExecutionCancelled,
        on_child_workflow_execution_cancelled) --> Cancelled;
    // Late terminal events cannot affect children whose result was resolved on cancellation.
    Cancelled --(ChildWorkflowExecutionCompleted(Option<Payloads>),
        shared on_child_workflow_execution_completed) --> Cancelled;
    Cancelled --(ChildWorkflowExecutionFailed(ChildWorkflowExecutionFailedEventAttributes),
        shared on_child_workflow_execution_failed) --> Cancelled;
    Cancelled --(ChildWorkflowExecutionTimedOut(RetryState),
        shared on_child_workflow_execution_timed_out) --> Cancelled;
    Cancelled --(ChildWorkflowExecutionTerminated,
        shared on_child_workflow_execution_terminated) --> Cancelled;
    Failed --(Cancel(String)) --> Failed;
    StartFailed --(Cancel(String)) --> StartFailed;
    TimedOut --(Cancel(String)) --> TimedOut;
    Completed --(Cancel(String)) --> Completed;
}

pub(super) struct ChildWorkflowExecutionStartedEvent {
    workflow_execution: WorkflowExecution,
    started_event_id: i64,
}

#[derive(Debug, derive_more::Display)]
pub(super) enum ChildWorkflowCommand {
    #[display("Start")]
    Start(WorkflowExecution),
    #[display("Complete")]
    Complete(Option<Payloads>),
    #[display("Fail")]
    Fail(Failure),
    #[display("Cancel")]
    Cancel,
    #[display("StartFail")]
    StartFail(StartChildWorkflowExecutionFailedCause),
    #[display("StartCancel")]
    StartCancel(Failure),
    #[display("CancelAfterStarted")]
    IssueCancelAfterStarted { reason: String },
}

pub(super) struct ChildWorkflowInitiatedData {
    event_id: i64,
    wf_type: String,
    wf_id: String,
    last_task_in_history: bool,
}

#[derive(Default, Clone)]
pub(super) struct Cancelled {
    seen_cancelled_event: bool,
}

fn completion_after_non_immediate_cancel_err() -> WFMachinesError {
    nondeterminism!(
        "Child workflows which don't resolve immediately on cancellation cannot complete after \
         being cancelled."
    )
}

impl Cancelled {
    pub(super) fn on_child_workflow_execution_cancelled(
        self,
    ) -> ChildWorkflowMachineTransition<Cancelled> {
        if self.seen_cancelled_event {
            ChildWorkflowMachineTransition::Err(fatal!(
                "Child workflow has already seen a ChildWorkflowExecutionCanceledEvent, and now \
                 another is being applied! This is a bug, please report."
            ))
        } else {
            ChildWorkflowMachineTransition::ok(
                [],
                Cancelled {
                    seen_cancelled_event: true,
                },
            )
        }
    }

    pub(super) fn on_child_workflow_execution_completed(
        self,
        state: &mut SharedState,
        _: Option<Payloads>,
    ) -> ChildWorkflowMachineTransition<Cancelled> {
        if !state.resolves_immediately_on_cancel() {
            return ChildWorkflowMachineTransition::Err(completion_after_non_immediate_cancel_err());
        }
        ChildWorkflowMachineTransition::ok([], self)
    }

    pub(super) fn on_child_workflow_execution_failed(
        self,
        state: &mut SharedState,
        _: ChildWorkflowExecutionFailedEventAttributes,
    ) -> ChildWorkflowMachineTransition<Cancelled> {
        if !state.resolves_immediately_on_cancel() {
            return ChildWorkflowMachineTransition::Err(completion_after_non_immediate_cancel_err());
        }
        ChildWorkflowMachineTransition::ok([], self)
    }

    pub(super) fn on_child_workflow_execution_timed_out(
        self,
        state: &mut SharedState,
        _: RetryState,
    ) -> ChildWorkflowMachineTransition<Cancelled> {
        if !state.resolves_immediately_on_cancel() {
            return ChildWorkflowMachineTransition::Err(completion_after_non_immediate_cancel_err());
        }
        ChildWorkflowMachineTransition::ok([], self)
    }

    pub(super) fn on_child_workflow_execution_terminated(
        self,
        state: &mut SharedState,
    ) -> ChildWorkflowMachineTransition<Cancelled> {
        if !state.resolves_immediately_on_cancel() {
            return ChildWorkflowMachineTransition::Err(completion_after_non_immediate_cancel_err());
        }
        ChildWorkflowMachineTransition::ok([], self)
    }
}

#[derive(Default, Clone)]
pub(super) struct Completed {}

#[derive(Default, Clone)]
pub(super) struct Created {}

impl Created {
    pub(super) fn on_schedule(self) -> ChildWorkflowMachineTransition<StartCommandCreated> {
        TransitionResult::default()
    }
}

#[derive(Default, Clone)]
pub(super) struct Failed {}

#[derive(Default, Clone)]
pub(super) struct StartCommandCreated {}

impl StartCommandCreated {
    pub(super) fn on_start_child_workflow_execution_initiated(
        self,
        state: &mut SharedState,
        event_dat: ChildWorkflowInitiatedData,
    ) -> ChildWorkflowMachineTransition<StartEventRecorded> {
        if state.internal_flags.borrow_mut().try_use(
            CoreInternalFlags::IdAndTypeDeterminismChecks,
            event_dat.last_task_in_history,
        ) {
            if event_dat.wf_id != state.workflow_id {
                return TransitionResult::Err(nondeterminism!(
                    "Child workflow id of scheduled event '{}' does not \
                     match child workflow id of command '{}'",
                    event_dat.wf_id,
                    state.workflow_id
                ));
            }
            if event_dat.wf_type != state.workflow_type {
                return TransitionResult::Err(nondeterminism!(
                    "Child workflow type of scheduled event '{}' does not \
                     match child workflow type of command '{}'",
                    event_dat.wf_type,
                    state.workflow_type
                ));
            }
        }
        state.initiated_event_id = event_dat.event_id;
        ChildWorkflowMachineTransition::default()
    }

    pub(super) fn on_cancelled(
        self,
        state: &mut SharedState,
        reason: String,
    ) -> ChildWorkflowMachineTransition<Cancelled> {
        state.cancelled_before_sent = true;
        ChildWorkflowMachineTransition::commands(vec![ChildWorkflowCommand::StartCancel(Failure {
            message: format!("Child Workflow Execution cancelled before scheduled: {reason}"),
            cause: Some(Box::new(Failure {
                failure_info: Some(FailureInfo::CanceledFailureInfo(
                    failure::CanceledFailureInfo {
                        ..Default::default()
                    },
                )),
                ..Default::default()
            })),
            failure_info: failure_info_from_state(state, RetryState::NonRetryableFailure),
            ..Default::default()
        })])
    }
}

#[derive(Default, Clone)]
pub(super) struct StartEventRecorded {}

impl StartEventRecorded {
    pub(super) fn on_child_workflow_execution_started(
        self,
        state: &mut SharedState,
        event: ChildWorkflowExecutionStartedEvent,
    ) -> ChildWorkflowMachineTransition<Started> {
        state.started_event_id = event.started_event_id;
        state.run_id.clone_from(&event.workflow_execution.run_id);
        ChildWorkflowMachineTransition::commands(vec![ChildWorkflowCommand::Start(
            event.workflow_execution,
        )])
    }
    pub(super) fn on_start_child_workflow_execution_failed(
        self,
        cause: StartChildWorkflowExecutionFailedCause,
    ) -> ChildWorkflowMachineTransition<StartFailed> {
        ChildWorkflowMachineTransition::ok(
            vec![ChildWorkflowCommand::StartFail(cause)],
            StartFailed::default(),
        )
    }

    pub(super) fn on_cancelled(
        self,
        state: &mut SharedState,
        reason: String,
    ) -> ChildWorkflowMachineTransition<StartEventRecordedOrCancelled> {
        let dest = if state.resolves_immediately_on_cancel() {
            StartEventRecordedOrCancelled::Cancelled(Default::default())
        } else {
            StartEventRecordedOrCancelled::StartEventRecorded(Default::default())
        };
        TransitionResult::ok(
            [ChildWorkflowCommand::IssueCancelAfterStarted { reason }],
            dest,
        )
    }
}

#[derive(Default, Clone)]
pub(super) struct StartFailed {}

#[derive(Default, Clone)]
pub(super) struct Started {}

impl Started {
    fn on_child_workflow_execution_completed(
        self,
        result: Option<Payloads>,
    ) -> ChildWorkflowMachineTransition<Completed> {
        ChildWorkflowMachineTransition::ok(
            vec![ChildWorkflowCommand::Complete(result)],
            Completed::default(),
        )
    }
    fn on_child_workflow_execution_failed(
        self,
        state: &mut SharedState,
        attrs: ChildWorkflowExecutionFailedEventAttributes,
    ) -> ChildWorkflowMachineTransition<Failed> {
        ChildWorkflowMachineTransition::ok(
            vec![ChildWorkflowCommand::Fail(Failure {
                message: "Child Workflow execution failed".to_owned(),
                failure_info: failure_info_from_state(state, attrs.retry_state()),
                cause: attrs.failure.map(Box::new),
                ..Default::default()
            })],
            Failed::default(),
        )
    }
    fn on_child_workflow_execution_timed_out(
        self,
        state: &mut SharedState,
        retry_state: RetryState,
    ) -> ChildWorkflowMachineTransition<TimedOut> {
        ChildWorkflowMachineTransition::ok(
            vec![ChildWorkflowCommand::Fail(Failure {
                message: "Child Workflow execution timed out".to_owned(),
                cause: Some(Box::new(Failure {
                    message: "Timed out".to_owned(),
                    failure_info: Some(FailureInfo::TimeoutFailureInfo(
                        failure::TimeoutFailureInfo {
                            last_heartbeat_details: None,
                            timeout_type: TimeoutType::StartToClose as i32,
                        },
                    )),
                    ..Default::default()
                })),
                failure_info: failure_info_from_state(state, retry_state),
                ..Default::default()
            })],
            TimedOut::default(),
        )
    }
    fn on_child_workflow_execution_cancelled(self) -> ChildWorkflowMachineTransition<Cancelled> {
        ChildWorkflowMachineTransition::ok(
            vec![ChildWorkflowCommand::Cancel],
            Cancelled {
                seen_cancelled_event: true,
            },
        )
    }
    fn on_child_workflow_execution_terminated(
        self,
        state: &mut SharedState,
    ) -> ChildWorkflowMachineTransition<Terminated> {
        ChildWorkflowMachineTransition::ok(
            vec![ChildWorkflowCommand::Fail(Failure {
                message: "Child Workflow execution terminated".to_owned(),
                cause: Some(Box::new(Failure {
                    message: "Terminated".to_owned(),
                    failure_info: Some(FailureInfo::TerminatedFailureInfo(
                        failure::TerminatedFailureInfo {
                            identity: Default::default(),
                        },
                    )),
                    ..Default::default()
                })),
                failure_info: failure_info_from_state(state, RetryState::NonRetryableFailure),
                ..Default::default()
            })],
            Terminated::default(),
        )
    }
    fn on_cancelled(
        self,
        state: &mut SharedState,
        reason: String,
    ) -> ChildWorkflowMachineTransition<StartedOrCancelled> {
        let dest = if state.resolves_immediately_on_cancel() {
            StartedOrCancelled::Cancelled(Default::default())
        } else {
            StartedOrCancelled::Started(Default::default())
        };
        TransitionResult::ok(
            [ChildWorkflowCommand::IssueCancelAfterStarted { reason }],
            dest,
        )
    }
}

#[derive(Default, Clone)]
pub(super) struct Terminated {}

#[derive(Default, Clone)]
pub(super) struct TimedOut {}

#[derive(Clone, Debug)]
pub(super) struct SharedState {
    initiated_event_id: i64,
    started_event_id: i64,
    lang_sequence_number: u32,
    namespace: String,
    workflow_id: String,
    run_id: String,
    workflow_type: String,
    cancelled_before_sent: bool,
    cancel_type: ChildWorkflowCancellationType,
    internal_flags: InternalFlagsRef,
}

impl SharedState {
    fn resolves_immediately_on_cancel(&self) -> bool {
        matches!(
            self.cancel_type,
            ChildWorkflowCancellationType::Abandon | ChildWorkflowCancellationType::TryCancel
        )
    }
}

impl ChildWorkflowMachine {
    /// Create a new child workflow and immediately schedule it.
    pub(super) fn new_scheduled(
        attribs: StartChildWorkflowExecution,
        internal_flags: InternalFlagsRef,
        use_compatible_version: bool,
    ) -> NewMachineWithCommand {
        let mut s = Self::from_parts(
            Created {}.into(),
            SharedState {
                lang_sequence_number: attribs.seq,
                workflow_id: attribs.workflow_id.clone(),
                workflow_type: attribs.workflow_type.clone(),
                namespace: attribs.namespace.clone(),
                cancel_type: attribs.cancellation_type(),
                internal_flags,
                run_id: "".to_string(),
                initiated_event_id: 0,
                started_event_id: 0,
                cancelled_before_sent: false,
            },
        );
        OnEventWrapper::on_event_mut(&mut s, ChildWorkflowMachineEvents::Schedule)
            .expect("Scheduling child workflows doesn't fail");
        NewMachineWithCommand {
            command: start_child_workflow_cmd_to_api(attribs, use_compatible_version),
            machine: s.into(),
        }
    }

    fn resolve_cancelled_msg(&self) -> ResolveChildWorkflowExecution {
        let failure = Failure {
            message: "Child Workflow execution cancelled".to_owned(),
            cause: Some(Box::new(Failure {
                failure_info: Some(FailureInfo::CanceledFailureInfo(
                    failure::CanceledFailureInfo {
                        ..Default::default()
                    },
                )),
                ..Default::default()
            })),
            failure_info: failure_info_from_state(
                &self.shared_state,
                RetryState::NonRetryableFailure,
            ),
            ..Default::default()
        };
        ResolveChildWorkflowExecution {
            seq: self.shared_state.lang_sequence_number,
            result: Some(ChildWorkflowResult {
                status: Some(ChildWorkflowStatus::Cancelled(wfr::Cancellation {
                    failure: Some(failure),
                })),
            }),
        }
    }

    pub(super) fn cancel(
        &mut self,
        reason: String,
    ) -> Result<Vec<MachineResponse>, MachineError<WFMachinesError>> {
        let event = ChildWorkflowMachineEvents::Cancel(reason);
        let vec = OnEventWrapper::on_event_mut(self, event)?;
        let res = vec
            .into_iter()
            .map(|mc| match mc {
                c @ ChildWorkflowCommand::StartCancel(_)
                | c @ ChildWorkflowCommand::IssueCancelAfterStarted { .. } => {
                    self.adapt_response(c, None)
                }
                x => {
                    dbg_panic!("Invalid cancel event response {x:?}");
                    panic!("Invalid cancel event response {x:?}");
                }
            })
            .flatten_ok()
            .try_collect()?;
        Ok(res)
    }

    pub(super) fn was_cancelled_before_sent_to_server(&self) -> bool {
        self.shared_state.cancelled_before_sent
    }
}

impl TryFrom<HistEventData> for ChildWorkflowMachineEvents {
    type Error = WFMachinesError;

    fn try_from(e: HistEventData) -> Result<Self, Self::Error> {
        let last_task_in_history = e.current_task_is_last_in_history;
        let e = e.event;
        Ok(match EventType::try_from(e.event_type) {
            Ok(EventType::StartChildWorkflowExecutionInitiated) => {
                if let Some(
                    history_event::Attributes::StartChildWorkflowExecutionInitiatedEventAttributes(
                        attrs,
                    ),
                ) = e.attributes
                {
                    Self::StartChildWorkflowExecutionInitiated(ChildWorkflowInitiatedData {
                        event_id: e.event_id,
                        wf_type: attrs.workflow_type.unwrap_or_default().name,
                        wf_id: attrs.workflow_id,
                        last_task_in_history,
                    })
                } else {
                    return Err(fatal!(
                        "StartChildWorkflowExecutionInitiated attributes were unset"
                    ));
                }
            }
            Ok(EventType::StartChildWorkflowExecutionFailed) => {
                if let Some(
                    history_event::Attributes::StartChildWorkflowExecutionFailedEventAttributes(
                        StartChildWorkflowExecutionFailedEventAttributes { cause, .. },
                    ),
                ) = e.attributes
                {
                    Self::StartChildWorkflowExecutionFailed(
                        StartChildWorkflowExecutionFailedCause::try_from(cause).map_err(|_| {
                            fatal!("Invalid StartChildWorkflowExecutionFailedCause")
                        })?,
                    )
                } else {
                    return Err(fatal!(
                        "StartChildWorkflowExecutionFailed attributes were unset"
                    ));
                }
            }
            Ok(EventType::ChildWorkflowExecutionStarted) => {
                if let Some(
                    history_event::Attributes::ChildWorkflowExecutionStartedEventAttributes(
                        ChildWorkflowExecutionStartedEventAttributes {
                            workflow_execution: Some(we),
                            ..
                        },
                    ),
                ) = e.attributes
                {
                    Self::ChildWorkflowExecutionStarted(ChildWorkflowExecutionStartedEvent {
                        workflow_execution: we,
                        started_event_id: e.event_id,
                    })
                } else {
                    return Err(fatal!(
                        "ChildWorkflowExecutionStarted attributes were unset or malformed"
                    ));
                }
            }
            Ok(EventType::ChildWorkflowExecutionCompleted) => {
                if let Some(
                    history_event::Attributes::ChildWorkflowExecutionCompletedEventAttributes(
                        ChildWorkflowExecutionCompletedEventAttributes { result, .. },
                    ),
                ) = e.attributes
                {
                    Self::ChildWorkflowExecutionCompleted(result)
                } else {
                    return Err(fatal!(
                        "ChildWorkflowExecutionCompleted attributes were unset or malformed"
                    ));
                }
            }
            Ok(EventType::ChildWorkflowExecutionFailed) => {
                if let Some(
                    history_event::Attributes::ChildWorkflowExecutionFailedEventAttributes(attrs),
                ) = e.attributes
                {
                    Self::ChildWorkflowExecutionFailed(attrs)
                } else {
                    return Err(fatal!("ChildWorkflowExecutionFailed attributes were unset"));
                }
            }
            Ok(EventType::ChildWorkflowExecutionTimedOut) => {
                if let Some(
                    history_event::Attributes::ChildWorkflowExecutionTimedOutEventAttributes(atts),
                ) = e.attributes
                {
                    Self::ChildWorkflowExecutionTimedOut(atts.retry_state())
                } else {
                    return Err(fatal!(
                        "ChildWorkflowExecutionTimedOut attributes were unset or malformed"
                    ));
                }
            }
            Ok(EventType::ChildWorkflowExecutionTerminated) => {
                Self::ChildWorkflowExecutionTerminated
            }
            Ok(EventType::ChildWorkflowExecutionCanceled) => Self::ChildWorkflowExecutionCancelled,
            _ => {
                return Err(nondeterminism!(
                    "Child workflow machine does not handle this event: {e:?}"
                ));
            }
        })
    }
}

impl WFMachinesAdapter for ChildWorkflowMachine {
    fn adapt_response(
        &self,
        my_command: Self::Command,
        event_info: Option<EventInfo>,
    ) -> Result<Vec<MachineResponse>, WFMachinesError> {
        Ok(match my_command {
            ChildWorkflowCommand::Start(we) => {
                vec![
                    ResolveChildWorkflowExecutionStart {
                        seq: self.shared_state.lang_sequence_number,
                        status: Some(resolve_child_workflow_execution_start::Status::Succeeded(
                            ResolveChildWorkflowExecutionStartSuccess { run_id: we.run_id },
                        )),
                    }
                    .into(),
                ]
            }
            ChildWorkflowCommand::StartFail(cause) => {
                vec![
                    ResolveChildWorkflowExecutionStart {
                        seq: self.shared_state.lang_sequence_number,
                        status: Some(resolve_child_workflow_execution_start::Status::Failed(
                            ResolveChildWorkflowExecutionStartFailure {
                                workflow_id: self.shared_state.workflow_id.clone(),
                                workflow_type: self.shared_state.workflow_type.clone(),
                                cause: cause as i32,
                            },
                        )),
                    }
                    .into(),
                ]
            }
            ChildWorkflowCommand::StartCancel(failure) => {
                vec![
                    ResolveChildWorkflowExecutionStart {
                        seq: self.shared_state.lang_sequence_number,
                        status: Some(resolve_child_workflow_execution_start::Status::Cancelled(
                            ResolveChildWorkflowExecutionStartCancelled {
                                failure: Some(failure),
                            },
                        )),
                    }
                    .into(),
                ]
            }
            ChildWorkflowCommand::Complete(result) => {
                vec![
                    ResolveChildWorkflowExecution {
                        seq: self.shared_state.lang_sequence_number,
                        result: Some(ChildWorkflowResult {
                            status: Some(ChildWorkflowStatus::Completed(wfr::Success {
                                result: convert_payloads(event_info, result)?,
                            })),
                        }),
                    }
                    .into(),
                ]
            }
            ChildWorkflowCommand::Fail(failure) => {
                vec![
                    ResolveChildWorkflowExecution {
                        seq: self.shared_state.lang_sequence_number,
                        result: Some(ChildWorkflowResult {
                            status: Some(ChildWorkflowStatus::Failed(wfr::Failure {
                                failure: Some(failure),
                            })),
                        }),
                    }
                    .into(),
                ]
            }
            ChildWorkflowCommand::Cancel => {
                vec![self.resolve_cancelled_msg().into()]
            }
            ChildWorkflowCommand::IssueCancelAfterStarted { reason } => {
                let mut resps = vec![];
                if self.shared_state.cancel_type != ChildWorkflowCancellationType::Abandon {
                    #[allow(deprecated)]
                    resps.push(MachineResponse::NewCoreOriginatedCommand(
                        RequestCancelExternalWorkflowExecutionCommandAttributes {
                            namespace: self.shared_state.namespace.clone(),
                            workflow_id: self.shared_state.workflow_id.clone(),
                            run_id: self.shared_state.run_id.clone(),
                            child_workflow_only: true,
                            reason,
                            ..Default::default()
                        }
                        .into(),
                    ))
                }
                if self.shared_state.resolves_immediately_on_cancel() {
                    resps.push(self.resolve_cancelled_msg().into())
                }
                resps
            }
        })
    }
}

impl TryFrom<CommandType> for ChildWorkflowMachineEvents {
    type Error = ();

    fn try_from(c: CommandType) -> Result<Self, Self::Error> {
        Ok(match c {
            CommandType::StartChildWorkflowExecution => Self::CommandStartChildWorkflowExecution,
            CommandType::RequestCancelExternalWorkflowExecution => {
                Self::CommandRequestCancelExternalWorkflowExecution
            }
            _ => return Err(()),
        })
    }
}

fn failure_info_from_state(state: &SharedState, retry_state: RetryState) -> Option<FailureInfo> {
    Some(FailureInfo::ChildWorkflowExecutionFailureInfo(
        failure::ChildWorkflowExecutionFailureInfo {
            namespace: state.namespace.clone(),
            workflow_type: Some(WorkflowType {
                name: state.workflow_type.clone(),
            }),
            initiated_event_id: state.initiated_event_id,
            started_event_id: state.started_event_id,
            retry_state: retry_state as i32,
            workflow_execution: Some(WorkflowExecution {
                workflow_id: state.workflow_id.clone(),
                run_id: state.run_id.clone(),
            }),
        },
    ))
}

fn convert_payloads(
    event_info: Option<EventInfo>,
    result: Option<Payloads>,
) -> Result<Option<Payload>, WFMachinesError> {
    result.map(TryInto::try_into).transpose().map_err(|pe| {
        fatal!("Not exactly one payload in child workflow result ({pe}) for event: {event_info:?}")
    })
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::internal_flags::InternalFlags;
    use rstest::rstest;
    use std::{cell::RefCell, mem::discriminant, rc::Rc};

    #[test]
    fn cancels_ignored_terminal() {
        for state in [
            ChildWorkflowMachineState::Cancelled(Cancelled {
                seen_cancelled_event: false,
            }),
            Failed {}.into(),
            StartFailed {}.into(),
            TimedOut {}.into(),
            Completed {}.into(),
        ] {
            let mut s = ChildWorkflowMachine::from_parts(
                state.clone(),
                SharedState {
                    initiated_event_id: 0,
                    started_event_id: 0,
                    lang_sequence_number: 0,
                    namespace: "".to_string(),
                    workflow_id: "".to_string(),
                    run_id: "".to_string(),
                    workflow_type: "".to_string(),
                    cancelled_before_sent: false,
                    cancel_type: Default::default(),
                    internal_flags: Rc::new(RefCell::new(InternalFlags::default())),
                },
            );
            let cmds = s.cancel("cancel reason".to_string()).unwrap();
            assert_eq!(cmds.len(), 0);
            assert_eq!(discriminant(&state), discriminant(s.state()));
        }
    }

    #[rstest]
    #[case::abandon(ChildWorkflowCancellationType::Abandon)]
    #[case::try_cancel(ChildWorkflowCancellationType::TryCancel)]
    fn immediately_cancelled_ok_with_terminal_events(
        #[case] cancel_type: ChildWorkflowCancellationType,
    ) {
        let mut shared = SharedState {
            initiated_event_id: 0,
            started_event_id: 0,
            lang_sequence_number: 0,
            namespace: "".to_string(),
            workflow_id: "".to_string(),
            run_id: "".to_string(),
            workflow_type: "".to_string(),
            cancelled_before_sent: false,
            cancel_type,
            internal_flags: Rc::new(RefCell::new(InternalFlags::default())),
        };
        let state = Cancelled::default();
        let res = state.on_child_workflow_execution_completed(&mut shared, None);
        // Can't use assert_matches b/c not Debug.
        assert!(matches!(
            res,
            TransitionResult::Ok {
                commands,
                new_state: Cancelled { .. }
            }
            if commands.is_empty()
        ));
        let state = Cancelled::default();
        let res = state.on_child_workflow_execution_failed(&mut shared, Default::default());
        assert!(matches!(
            res,
            TransitionResult::Ok {
                commands,
                new_state: Cancelled { .. }
            }
            if commands.is_empty()
        ));
        let state = Cancelled::default();
        let res = state.on_child_workflow_execution_timed_out(&mut shared, Default::default());
        assert!(matches!(
            res,
            TransitionResult::Ok {
                commands,
                new_state: Cancelled { .. }
            }
            if commands.is_empty()
        ));
        let state = Cancelled::default();
        let res = state.on_child_workflow_execution_terminated(&mut shared);
        assert!(matches!(
            res,
            TransitionResult::Ok {
                commands,
                new_state: Cancelled { .. }
            }
            if commands.is_empty()
        ));
    }

    #[rstest]
    #[case::wait_cancellation_completed(
        ChildWorkflowCancellationType::WaitCancellationCompleted,
        discriminant(&ChildWorkflowMachineState::StartEventRecorded(Default::default()))
    )]
    #[case::try_cancel(
        ChildWorkflowCancellationType::TryCancel,
        discriminant(&ChildWorkflowMachineState::Cancelled(Default::default()))
    )]
    #[case::abandon(
        ChildWorkflowCancellationType::Abandon,
        discriminant(&ChildWorkflowMachineState::Cancelled(Default::default()))
    )]
    fn cancel_in_start_event_recorded(
        #[case] cancel_type: ChildWorkflowCancellationType,
        #[case] expected_state: std::mem::Discriminant<ChildWorkflowMachineState>,
    ) {
        let mut s = ChildWorkflowMachine::from_parts(
            StartEventRecorded {}.into(),
            SharedState {
                initiated_event_id: 1,
                started_event_id: 0,
                lang_sequence_number: 1,
                namespace: "".to_string(),
                workflow_id: "child-wf-id".to_string(),
                run_id: "".to_string(),
                workflow_type: "child".to_string(),
                cancelled_before_sent: false,
                cancel_type,
                internal_flags: Rc::new(RefCell::new(InternalFlags::default())),
            },
        );
        let cmds = s
            .cancel("parent cancelled".to_string())
            .expect("Cancel in StartEventRecorded should not fail");
        assert!(
            !cmds.is_empty(),
            "Should produce commands for {cancel_type:?}"
        );
        assert_eq!(
            discriminant(s.state()),
            expected_state,
            "Unexpected state for {cancel_type:?}"
        );
    }
}
