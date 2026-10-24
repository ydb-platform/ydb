use super::{MachineError, StateMachine, TransitionResult, fsm};
use crate::worker::workflow::{
    WFMachinesError,
    machines::{
        EventInfo, HistEventData, NewMachineWithCommand, OnEventWrapper, WFMachinesAdapter,
        workflow_machines::MachineResponse,
    },
    nondeterminism,
};
use itertools::Itertools;
use temporalio_common::protos::{
    coresdk::{
        nexus::{NexusOperationCancellationType, NexusOperationResult, nexus_operation_result},
        workflow_activation::{
            ResolveNexusOperation, ResolveNexusOperationStart, resolve_nexus_operation_start,
        },
        workflow_commands::ScheduleNexusOperation,
    },
    temporal::api::{
        command::v1::{RequestCancelNexusOperationCommandAttributes, command},
        common::v1::Payload,
        enums::v1::{CommandType, EventType},
        failure::v1::{self as failure, Failure, failure::FailureInfo},
        history::v1::{
            NexusOperationCancelRequestCompletedEventAttributes,
            NexusOperationCancelRequestFailedEventAttributes,
            NexusOperationCanceledEventAttributes, NexusOperationCompletedEventAttributes,
            NexusOperationFailedEventAttributes, NexusOperationStartedEventAttributes,
            NexusOperationTimedOutEventAttributes, history_event,
        },
    },
};

fsm! {
    pub(super) name NexusOperationMachine;
    command NexusOperationCommand;
    error WFMachinesError;
    shared_state SharedState;

    ScheduleCommandCreated --(CommandScheduleNexusOperation)--> ScheduleCommandCreated;
    ScheduleCommandCreated
      --(NexusOperationScheduled(NexusOpScheduledData), shared on_scheduled)--> ScheduledEventRecorded;
    ScheduleCommandCreated --(Cancel, shared on_cancelled)--> Cancelled;

    ScheduledEventRecorded --(Cancel, shared on_issue_cancel)--> ScheduledEventRecorded;
    ScheduledEventRecorded --(CommandRequestCancelNexusOperation)--> ScheduledEventRecorded;
    ScheduledEventRecorded --(NexusOperationCancelRequested)--> ScheduledEventRecorded;
    ScheduledEventRecorded
      --(NexusOperationCompleted(NexusOperationCompletedEventAttributes), on_completed)--> Completed;
    ScheduledEventRecorded
      --(NexusOperationFailed(NexusOperationFailedEventAttributes), on_failed)--> Failed;
    ScheduledEventRecorded
      --(NexusOperationCanceled(NexusOperationCanceledEventAttributes), on_canceled)--> Cancelled;
    ScheduledEventRecorded
      --(NexusOperationTimedOut(NexusOperationTimedOutEventAttributes), on_timed_out)--> TimedOut;
    ScheduledEventRecorded
      --(NexusOperationStarted(NexusOperationStartedEventAttributes), shared on_started)--> Started;

    Started --(Cancel, shared on_issue_cancel)--> Started;
    Started --(Cancel, shared on_issue_cancel)--> Cancelled;
    Started --(CommandRequestCancelNexusOperation)--> Started;
    Started --(NexusOperationCancelRequested)--> Started;
    Started
      --(NexusOperationCompleted(NexusOperationCompletedEventAttributes), on_completed)--> Completed;
    Started
      --(NexusOperationFailed(NexusOperationFailedEventAttributes), on_failed)--> Failed;
    Started
      --(NexusOperationCanceled(NexusOperationCanceledEventAttributes), on_canceled)--> Cancelled;


    Started --(NexusOperationCancelRequestCompleted(NexusOperationCancelRequestCompletedEventAttributes), shared on_cancel_request_completed)--> Started;
    Started --(NexusOperationCancelRequestCompleted(NexusOperationCancelRequestCompletedEventAttributes), shared on_cancel_request_completed)--> Cancelled;
    Started --(NexusOperationCancelRequestFailed(NexusOperationCancelRequestFailedEventAttributes), shared on_cancel_request_failed)--> Started;

    Started
      --(NexusOperationTimedOut(NexusOperationTimedOutEventAttributes), on_timed_out)--> TimedOut;

    Cancelled --(Cancel)--> Cancelled;
    Cancelled --(CommandRequestCancelNexusOperation)--> Cancelled;
    Cancelled --(NexusOperationCancelRequested)--> Cancelled;
    Cancelled --(NexusOperationCompleted(NexusOperationCompletedEventAttributes), shared on_completed)--> Cancelled;
    Cancelled --(NexusOperationFailed(NexusOperationFailedEventAttributes), shared on_failed)--> Cancelled;
    Cancelled --(NexusOperationTimedOut(NexusOperationTimedOutEventAttributes), shared on_timed_out)--> Cancelled;
    Cancelled --(NexusOperationCancelRequestCompleted(NexusOperationCancelRequestCompletedEventAttributes), on_cancel_request_completed)--> Cancelled;
    Cancelled --(NexusOperationCancelRequestFailed(NexusOperationCancelRequestFailedEventAttributes))--> Cancelled;
    Cancelled --(NexusOperationCanceled(NexusOperationCanceledEventAttributes))--> Cancelled;

    // Ignore cancels in all terminal states
    Completed --(Cancel)--> Completed;
    Failed --(Cancel)--> Failed;
    TimedOut --(Cancel)--> TimedOut;
    Completed --(NexusOperationCancelRequestCompleted(NexusOperationCancelRequestCompletedEventAttributes))--> Completed;
    Failed --(NexusOperationCancelRequestCompleted(NexusOperationCancelRequestCompletedEventAttributes))--> Failed;
    TimedOut --(NexusOperationCancelRequestCompleted(NexusOperationCancelRequestCompletedEventAttributes))--> TimedOut;
    Completed --(NexusOperationCancelRequestFailed(NexusOperationCancelRequestFailedEventAttributes))--> Completed;
    Failed --(NexusOperationCancelRequestFailed(NexusOperationCancelRequestFailedEventAttributes))--> Failed;
    TimedOut --(NexusOperationCancelRequestFailed(NexusOperationCancelRequestFailedEventAttributes))--> TimedOut;
}

#[derive(Debug, derive_more::Display)]
pub(super) enum NexusOperationCommand {
    #[display("Start")]
    Start { operation_token: String },
    #[display("StartSync")]
    StartSync,
    #[display("FailBeforeStart")]
    FailBeforeStart(Failure),
    #[display("Complete")]
    Complete(Option<Payload>),
    #[display("Fail")]
    Fail(Failure),
    #[display("Cancel")]
    Cancel(Failure),
    #[display("TimedOut")]
    TimedOut(Failure),
    #[display("IssueCancel")]
    IssueCancel,
}

#[derive(Clone, Debug)]
pub(super) struct SharedState {
    lang_seq_num: u32,
    pub(super) scheduled_event_id: i64,
    endpoint: String,
    service: String,
    operation: String,

    cancelled_before_sent: bool,
    cancel_sent: bool,
    cancel_type: NexusOperationCancellationType,
    operation_token: Option<String>,
}

impl NexusOperationMachine {
    pub(super) fn new_scheduled(attribs: ScheduleNexusOperation) -> NewMachineWithCommand {
        let s = Self::from_parts(
            ScheduleCommandCreated.into(),
            SharedState {
                lang_seq_num: attribs.seq,
                scheduled_event_id: 0,
                endpoint: attribs.endpoint.clone(),
                service: attribs.service.clone(),
                operation: attribs.operation.clone(),
                cancelled_before_sent: false,
                cancel_sent: false,
                cancel_type: attribs.cancellation_type(),
                operation_token: None,
            },
        );
        NewMachineWithCommand {
            command: attribs.into(),
            machine: s.into(),
        }
    }

    pub(super) fn cancel(&mut self) -> Result<Vec<MachineResponse>, MachineError<WFMachinesError>> {
        let event = NexusOperationMachineEvents::Cancel;
        let cmds = OnEventWrapper::on_event_mut(self, event)?;
        let mach_resps = cmds
            .into_iter()
            .map(|mc| self.adapt_response(mc, None))
            .flatten_ok()
            .try_collect()?;
        Ok(mach_resps)
    }

    pub(super) fn was_cancelled_before_sent_to_server(&self) -> bool {
        self.shared_state.cancelled_before_sent
    }
}

#[derive(Default, Clone)]
pub(super) struct ScheduleCommandCreated;

pub(super) struct NexusOpScheduledData {
    event_id: i64,
}

impl ScheduleCommandCreated {
    pub(super) fn on_scheduled(
        self,
        state: &mut SharedState,
        event_dat: NexusOpScheduledData,
    ) -> NexusOperationMachineTransition<ScheduledEventRecorded> {
        state.scheduled_event_id = event_dat.event_id;
        NexusOperationMachineTransition::default()
    }

    pub(super) fn on_cancelled(
        self,
        state: &mut SharedState,
    ) -> NexusOperationMachineTransition<Cancelled> {
        state.cancelled_before_sent = true;
        NexusOperationMachineTransition::commands([NexusOperationCommand::FailBeforeStart(
            state.cancelled_failure("Nexus Operation cancelled before scheduled".to_owned()),
        )])
    }
}

#[derive(Default, Clone)]
pub(super) struct ScheduledEventRecorded;

impl ScheduledEventRecorded {
    pub(crate) fn on_issue_cancel(
        &self,
        ss: &mut SharedState,
    ) -> NexusOperationMachineTransition<ScheduledEventRecorded> {
        if !ss.cancel_sent {
            ss.cancel_sent = true;
            NexusOperationMachineTransition::commands([NexusOperationCommand::IssueCancel])
        } else {
            NexusOperationMachineTransition::default()
        }
    }

    pub(super) fn on_completed(
        self,
        ca: NexusOperationCompletedEventAttributes,
    ) -> NexusOperationMachineTransition<Completed> {
        NexusOperationMachineTransition::commands([
            NexusOperationCommand::StartSync,
            NexusOperationCommand::Complete(ca.result),
        ])
    }

    pub(super) fn on_failed(
        self,
        fa: NexusOperationFailedEventAttributes,
    ) -> NexusOperationMachineTransition<Failed> {
        NexusOperationMachineTransition::commands([NexusOperationCommand::FailBeforeStart(
            fa.failure.unwrap_or_else(|| Failure {
                message: "Nexus operation failed but failure field was not populated".to_owned(),
                ..Default::default()
            }),
        )])
    }

    pub(super) fn on_canceled(
        self,
        ca: NexusOperationCanceledEventAttributes,
    ) -> NexusOperationMachineTransition<Cancelled> {
        NexusOperationMachineTransition::commands([NexusOperationCommand::FailBeforeStart(
            ca.failure.unwrap_or_else(|| Failure {
                message: "Nexus operation was cancelled but failure field was not populated"
                    .to_owned(),
                ..Default::default()
            }),
        )])
    }

    pub(super) fn on_timed_out(
        self,
        toa: NexusOperationTimedOutEventAttributes,
    ) -> NexusOperationMachineTransition<TimedOut> {
        NexusOperationMachineTransition::commands([NexusOperationCommand::FailBeforeStart(
            toa.failure.unwrap_or_else(|| Failure {
                message: "Nexus operation timed out but failure field was not populated".to_owned(),
                ..Default::default()
            }),
        )])
    }

    pub(super) fn on_started(
        self,
        ss: &mut SharedState,
        sa: NexusOperationStartedEventAttributes,
    ) -> NexusOperationMachineTransition<Started> {
        ss.operation_token = Some(sa.operation_token.clone());
        NexusOperationMachineTransition::commands([NexusOperationCommand::Start {
            operation_token: sa.operation_token,
        }])
    }
}

#[derive(Default, Clone)]
pub(super) struct Started;

impl Started {
    pub(crate) fn on_issue_cancel(
        &self,
        ss: &mut SharedState,
    ) -> NexusOperationMachineTransition<StartedOrCancelled> {
        if !ss.cancel_sent {
            ss.cancel_sent = true;
            let dest = if matches!(
                ss.cancel_type,
                NexusOperationCancellationType::Abandon | NexusOperationCancellationType::TryCancel
            ) {
                StartedOrCancelled::Cancelled(Default::default())
            } else {
                StartedOrCancelled::Started(Default::default())
            };
            TransitionResult::ok([NexusOperationCommand::IssueCancel], dest)
        } else {
            TransitionResult::ok([], StartedOrCancelled::Started(Default::default()))
        }
    }

    pub(super) fn on_completed(
        self,
        ca: NexusOperationCompletedEventAttributes,
    ) -> NexusOperationMachineTransition<Completed> {
        NexusOperationMachineTransition::commands([NexusOperationCommand::Complete(ca.result)])
    }

    pub(super) fn on_failed(
        self,
        fa: NexusOperationFailedEventAttributes,
    ) -> NexusOperationMachineTransition<Failed> {
        NexusOperationMachineTransition::commands([NexusOperationCommand::Fail(
            fa.failure.unwrap_or_else(|| Failure {
                message: "Nexus operation failed but failure field was not populated".to_owned(),
                ..Default::default()
            }),
        )])
    }

    pub(super) fn on_canceled(
        self,
        ca: NexusOperationCanceledEventAttributes,
    ) -> NexusOperationMachineTransition<Cancelled> {
        NexusOperationMachineTransition::commands([NexusOperationCommand::Cancel(
            ca.failure.unwrap_or_else(|| Failure {
                message: "Nexus operation was cancelled but failure field was not populated"
                    .to_owned(),
                ..Default::default()
            }),
        )])
    }

    pub(super) fn on_cancel_request_completed(
        self,
        ss: &mut SharedState,
        _: NexusOperationCancelRequestCompletedEventAttributes,
    ) -> NexusOperationMachineTransition<StartedOrCancelled> {
        if ss.cancel_type == NexusOperationCancellationType::WaitCancellationRequested {
            TransitionResult::ok(
                [NexusOperationCommand::Cancel(ss.cancelled_failure(
                    "Nexus operation cancellation request completed".to_owned(),
                ))],
                StartedOrCancelled::Cancelled(Default::default()),
            )
        } else {
            TransitionResult::ok([], StartedOrCancelled::Started(Default::default()))
        }
    }

    pub(super) fn on_cancel_request_failed(
        self,
        ss: &mut SharedState,
        fa: NexusOperationCancelRequestFailedEventAttributes,
    ) -> NexusOperationMachineTransition<Started> {
        if ss.cancel_type == NexusOperationCancellationType::WaitCancellationRequested {
            let message = "Nexus operation cancellation request failed".to_string();
            TransitionResult::ok(
                [NexusOperationCommand::Fail(ss.failure(
                    message.clone(),
                    fa.failure.unwrap_or_else(|| Failure {
                        message,
                        ..Default::default()
                    }),
                ))],
                self,
            )
        } else {
            TransitionResult::ok([], self)
        }
    }

    pub(super) fn on_timed_out(
        self,
        toa: NexusOperationTimedOutEventAttributes,
    ) -> NexusOperationMachineTransition<TimedOut> {
        NexusOperationMachineTransition::commands([NexusOperationCommand::TimedOut(
            toa.failure.unwrap_or_else(|| Failure {
                message: "Nexus operation timed out but failure field was not populated".to_owned(),
                ..Default::default()
            }),
        )])
    }
}

#[derive(Default, Clone)]
pub(super) struct Completed;

#[derive(Default, Clone)]
pub(super) struct Failed;

#[derive(Default, Clone)]
pub(super) struct TimedOut;

#[derive(Default, Clone)]
pub(super) struct Cancelled;

fn completion_of_not_abandoned_err() -> WFMachinesError {
    nondeterminism!(
        "Nexus operation which don't have the ABANDON cancellation type cannot complete after \
         being cancelled."
    )
}

impl Cancelled {
    pub(super) fn on_completed(
        self,
        ss: &mut SharedState,
        _: NexusOperationCompletedEventAttributes,
    ) -> NexusOperationMachineTransition<Cancelled> {
        if ss.cancel_type == NexusOperationCancellationType::Abandon {
            return NexusOperationMachineTransition::Err(completion_of_not_abandoned_err());
        }
        NexusOperationMachineTransition::ok([], self)
    }

    pub(super) fn on_cancel_request_completed(
        self,
        _: NexusOperationCancelRequestCompletedEventAttributes,
    ) -> NexusOperationMachineTransition<Cancelled> {
        NexusOperationMachineTransition::ok([], self)
    }

    pub(super) fn on_failed(
        self,
        ss: &mut SharedState,
        _: NexusOperationFailedEventAttributes,
    ) -> NexusOperationMachineTransition<Cancelled> {
        if ss.cancel_type == NexusOperationCancellationType::Abandon {
            return NexusOperationMachineTransition::Err(completion_of_not_abandoned_err());
        }
        NexusOperationMachineTransition::ok([], self)
    }

    pub(super) fn on_timed_out(
        self,
        ss: &mut SharedState,
        _: NexusOperationTimedOutEventAttributes,
    ) -> NexusOperationMachineTransition<Cancelled> {
        if ss.cancel_type == NexusOperationCancellationType::Abandon {
            return NexusOperationMachineTransition::Err(completion_of_not_abandoned_err());
        }
        NexusOperationMachineTransition::ok([], self)
    }
}

impl TryFrom<HistEventData> for NexusOperationMachineEvents {
    type Error = WFMachinesError;

    fn try_from(e: HistEventData) -> Result<Self, Self::Error> {
        let e = e.event;
        Ok(match EventType::try_from(e.event_type) {
            Ok(EventType::NexusOperationScheduled) => {
                if let Some(history_event::Attributes::NexusOperationScheduledEventAttributes(_)) =
                    e.attributes
                {
                    Self::NexusOperationScheduled(NexusOpScheduledData {
                        event_id: e.event_id,
                    })
                } else {
                    return Err(nondeterminism!(
                        "NexusOperationScheduled attributes were unset or malformed"
                    ));
                }
            }
            Ok(EventType::NexusOperationStarted) => {
                if let Some(history_event::Attributes::NexusOperationStartedEventAttributes(sa)) =
                    e.attributes
                {
                    Self::NexusOperationStarted(sa)
                } else {
                    return Err(nondeterminism!(
                        "NexusOperationStarted attributes were unset or malformed"
                    ));
                }
            }
            Ok(EventType::NexusOperationCompleted) => {
                if let Some(history_event::Attributes::NexusOperationCompletedEventAttributes(ca)) =
                    e.attributes
                {
                    Self::NexusOperationCompleted(ca)
                } else {
                    return Err(nondeterminism!(
                        "NexusOperationCompleted attributes were unset or malformed"
                    ));
                }
            }
            Ok(EventType::NexusOperationFailed) => {
                if let Some(history_event::Attributes::NexusOperationFailedEventAttributes(fa)) =
                    e.attributes
                {
                    Self::NexusOperationFailed(fa)
                } else {
                    return Err(nondeterminism!(
                        "NexusOperationFailed attributes were unset or malformed"
                    ));
                }
            }
            Ok(EventType::NexusOperationCanceled) => {
                if let Some(history_event::Attributes::NexusOperationCanceledEventAttributes(ca)) =
                    e.attributes
                {
                    Self::NexusOperationCanceled(ca)
                } else {
                    return Err(nondeterminism!(
                        "NexusOperationCanceled attributes were unset or malformed"
                    ));
                }
            }
            Ok(EventType::NexusOperationTimedOut) => {
                if let Some(history_event::Attributes::NexusOperationTimedOutEventAttributes(toa)) =
                    e.attributes
                {
                    Self::NexusOperationTimedOut(toa)
                } else {
                    return Err(nondeterminism!(
                        "NexusOperationTimedOut attributes were unset or malformed"
                    ));
                }
            }
            Ok(EventType::NexusOperationCancelRequested) => Self::NexusOperationCancelRequested,
            Ok(EventType::NexusOperationCancelRequestCompleted) => {
                if let Some(
                    history_event::Attributes::NexusOperationCancelRequestCompletedEventAttributes(
                        attrs,
                    ),
                ) = e.attributes
                {
                    Self::NexusOperationCancelRequestCompleted(attrs)
                } else {
                    return Err(nondeterminism!(
                        "NexusOperationCancelRequestCompleted attributes were unset or malformed"
                    ));
                }
            }
            Ok(EventType::NexusOperationCancelRequestFailed) => {
                if let Some(
                    history_event::Attributes::NexusOperationCancelRequestFailedEventAttributes(
                        attrs,
                    ),
                ) = e.attributes
                {
                    Self::NexusOperationCancelRequestFailed(attrs)
                } else {
                    return Err(nondeterminism!(
                        "NexusOperationCancelRequestFailed attributes were unset or malformed"
                    ));
                }
            }
            _ => {
                return Err(nondeterminism!(
                    "Nexus operation machine does not handle this event: {e:?}"
                ));
            }
        })
    }
}

impl WFMachinesAdapter for NexusOperationMachine {
    fn adapt_response(
        &self,
        my_command: Self::Command,
        _: Option<EventInfo>,
    ) -> Result<Vec<MachineResponse>, WFMachinesError> {
        Ok(match my_command {
            NexusOperationCommand::StartSync => {
                vec![
                    ResolveNexusOperationStart {
                        seq: self.shared_state.lang_seq_num,
                        status: Some(resolve_nexus_operation_start::Status::StartedSync(true)),
                    }
                    .into(),
                ]
            }
            NexusOperationCommand::Start { operation_token } => {
                vec![
                    ResolveNexusOperationStart {
                        seq: self.shared_state.lang_seq_num,
                        status: Some(resolve_nexus_operation_start::Status::OperationToken(
                            operation_token,
                        )),
                    }
                    .into(),
                ]
            }
            NexusOperationCommand::FailBeforeStart(failure) => {
                vec![
                    ResolveNexusOperationStart {
                        seq: self.shared_state.lang_seq_num,
                        status: Some(resolve_nexus_operation_start::Status::Failed(
                            failure.clone(),
                        )),
                    }
                    .into(),
                ]
            }
            NexusOperationCommand::Complete(c) => {
                vec![
                    ResolveNexusOperation {
                        seq: self.shared_state.lang_seq_num,
                        result: Some(NexusOperationResult {
                            status: Some(nexus_operation_result::Status::Completed(
                                c.unwrap_or_default(),
                            )),
                        }),
                    }
                    .into(),
                ]
            }
            NexusOperationCommand::Fail(f) => {
                vec![
                    ResolveNexusOperation {
                        seq: self.shared_state.lang_seq_num,
                        result: Some(NexusOperationResult {
                            status: Some(nexus_operation_result::Status::Failed(f)),
                        }),
                    }
                    .into(),
                ]
            }
            NexusOperationCommand::Cancel(f) => {
                vec![
                    ResolveNexusOperation {
                        seq: self.shared_state.lang_seq_num,
                        result: Some(NexusOperationResult {
                            status: Some(nexus_operation_result::Status::Cancelled(f)),
                        }),
                    }
                    .into(),
                ]
            }
            NexusOperationCommand::TimedOut(f) => {
                vec![
                    ResolveNexusOperation {
                        seq: self.shared_state.lang_seq_num,
                        result: Some(NexusOperationResult {
                            status: Some(nexus_operation_result::Status::TimedOut(f)),
                        }),
                    }
                    .into(),
                ]
            }
            NexusOperationCommand::IssueCancel => {
                let mut resps = vec![];
                if self.shared_state.cancel_type != NexusOperationCancellationType::Abandon {
                    resps.push(MachineResponse::IssueNewCommand(
                        command::Attributes::RequestCancelNexusOperationCommandAttributes(
                            RequestCancelNexusOperationCommandAttributes {
                                scheduled_event_id: self.shared_state.scheduled_event_id,
                            },
                        )
                        .into(),
                    ))
                }
                // Immediately resolve abandon/trycancel modes
                if matches!(
                    self.shared_state.cancel_type,
                    NexusOperationCancellationType::Abandon
                        | NexusOperationCancellationType::TryCancel
                ) {
                    resps.push(
                        ResolveNexusOperation {
                            seq: self.shared_state.lang_seq_num,
                            result: Some(NexusOperationResult {
                                status: Some(nexus_operation_result::Status::Cancelled(
                                    self.shared_state.cancelled_failure(
                                        "Nexus operation cancelled after starting".to_owned(),
                                    ),
                                )),
                            }),
                        }
                        .into(),
                    )
                }
                resps
            }
        })
    }
}

impl TryFrom<CommandType> for NexusOperationMachineEvents {
    type Error = ();

    fn try_from(c: CommandType) -> Result<Self, Self::Error> {
        Ok(match c {
            CommandType::ScheduleNexusOperation => Self::CommandScheduleNexusOperation,
            CommandType::RequestCancelNexusOperation => Self::CommandRequestCancelNexusOperation,
            _ => return Err(()),
        })
    }
}

impl SharedState {
    fn cancelled_failure(&self, message: String) -> Failure {
        self.failure(
            message,
            Failure {
                failure_info: Some(FailureInfo::CanceledFailureInfo(Default::default())),
                ..Default::default()
            },
        )
    }

    fn failure(&self, message: String, cause: Failure) -> Failure {
        Failure {
            message,
            cause: Some(Box::new(cause)),
            failure_info: Some(FailureInfo::NexusOperationExecutionFailureInfo(
                failure::NexusOperationFailureInfo {
                    scheduled_event_id: self.scheduled_event_id,
                    endpoint: self.endpoint.clone(),
                    service: self.service.clone(),
                    operation: self.operation.clone(),
                    operation_token: self.operation_token.clone().unwrap_or_default(),
                    ..Default::default()
                },
            )),
            ..Default::default()
        }
    }
}
