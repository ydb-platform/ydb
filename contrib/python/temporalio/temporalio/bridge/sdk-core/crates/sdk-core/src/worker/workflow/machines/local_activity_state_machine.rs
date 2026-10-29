use super::{
    EventInfo, MachineError, OnEventWrapper, StateMachine, TransitionResult, WFMachinesAdapter,
    WFMachinesError, fsm, workflow_machines::MachineResponse,
};
use crate::{
    internal_flags::CoreInternalFlags,
    protosext::{CompleteLocalActivityData, HistoryEventExt, ValidScheduleLA},
    worker::{
        LocalActivityExecutionResult,
        workflow::{
            InternalFlagsRef, fatal,
            machines::{HistEventData, activity_state_machine::activity_fail_info},
            nondeterminism,
        },
    },
};
use itertools::Itertools;
use std::{
    convert::TryFrom,
    time::{Duration, SystemTime},
};
use temporalio_common::protos::{
    constants::LOCAL_ACTIVITY_MARKER_NAME,
    coresdk::{
        activity_result::{
            ActivityResolution, Cancellation, DoBackoff, Failure as ActFail, Success,
        },
        common::build_local_activity_marker_details,
        external_data::LocalActivityMarkerData,
        workflow_activation::ResolveActivity,
        workflow_commands::ActivityCancellationType,
    },
    temporal::api::{
        command::v1::{Command as ProtoCommand, RecordMarkerCommandAttributes, command},
        enums::v1::{CommandType, EventType, RetryState},
        failure::v1::{Failure, failure::FailureInfo},
    },
    utilities::TryIntoOrNone,
};

fsm! {
    pub(super) name LocalActivityMachine;
    command LocalActivityCommand;
    error WFMachinesError;
    shared_state SharedState;

    // Machine is created in either executing or replaying (referring to whether or not the workflow
    // is replaying), and then immediately scheduled and transitions to either requesting that lang
    // execute the activity, or waiting for the marker from history.
    Executing --(Schedule, shared on_schedule) --> RequestSent;
    Replaying --(Schedule, on_schedule) --> WaitingResolveFromMarkerLookAhead;

    // Execution path =============================================================================
    RequestSent --(HandleResult(ResolveDat), on_handle_result) --> MarkerCommandCreated;
    // We loop back on RequestSent here because the LA needs to report its result
    RequestSent --(Cancel, on_cancel_requested) --> RequestSent;
    // No wait cancels skip waiting for the LA to report the result, but do generate a command
    // to record the cancel marker
    RequestSent --(NoWaitCancel(ActivityCancellationType), shared on_no_wait_cancel)
      --> MarkerCommandCreated;

    MarkerCommandCreated --(CommandRecordMarker, on_command_record_marker) --> ResultNotified;

    ResultNotified --(MarkerRecorded(CompleteLocalActivityData), shared on_marker_recorded)
      --> MarkerCommandRecorded;

    // Replay path ================================================================================
    WaitingResolveFromMarkerLookAhead --(HandleKnownResult(ResolveDat), on_handle_result)
      --> ResolvedFromMarkerLookAheadWaitingMarkerEvent;
    // If we are told to cancel while waiting for the marker, we still need to wait for the marker.
    WaitingResolveFromMarkerLookAhead --(Cancel, on_cancel_requested)
      --> WaitingResolveFromMarkerLookAhead;
    ResolvedFromMarkerLookAheadWaitingMarkerEvent --(Cancel, on_cancel_requested)
      --> ResolvedFromMarkerLookAheadWaitingMarkerEvent;

    // Because there could be non-heartbeat WFTs (ex: signals being received) between scheduling
    // the LA and the marker being recorded, peekahead might not always resolve the LA *before*
    // scheduling it. This transition accounts for that.
    WaitingResolveFromMarkerLookAhead --(NoWaitCancel(ActivityCancellationType),
                                         on_no_wait_cancel)
      --> WaitingResolveFromMarkerLookAhead;
    ResolvedFromMarkerLookAheadWaitingMarkerEvent --(NoWaitCancel(ActivityCancellationType),
                                                      on_no_wait_cancel)
      --> ResolvedFromMarkerLookAheadWaitingMarkerEvent;

    // LAs on the replay path always need to eventually see the marker
    ResolvedFromMarkerLookAheadWaitingMarkerEvent --(MarkerRecorded(CompleteLocalActivityData),
                                                      shared on_marker_recorded)
      --> MarkerCommandRecorded;

    // It is entirely possible to have started the LA while replaying, only to find that we have
    // reached a new WFT and there still was no marker. In such cases we need to execute the LA.
    // This can easily happen if upon first execution, the worker does WFT heartbeating but then
    // dies for some reason.
    WaitingResolveFromMarkerLookAhead --(StartedNonReplayWFT, shared on_started_non_replay_wft)
      --> RequestSent;

    // Ignore cancellation in final state
    MarkerCommandRecorded --(Cancel, on_cancel_requested) --> MarkerCommandRecorded;
    MarkerCommandRecorded --(NoWaitCancel(ActivityCancellationType),
                             on_no_wait_cancel) --> MarkerCommandRecorded;

    // LAs reporting status after they've handled their result can simply be ignored. We could
    // optimize this away higher up but that feels very overkill.
    MarkerCommandCreated --(HandleResult(ResolveDat)) --> MarkerCommandCreated;
    ResultNotified --(HandleResult(ResolveDat)) --> ResultNotified;
    MarkerCommandRecorded --(HandleResult(ResolveDat)) --> MarkerCommandRecorded;
}

#[derive(Debug, Clone)]
pub(super) struct ResolveDat {
    pub(super) result: LocalActivityExecutionResult,
    pub(super) complete_time: Option<SystemTime>,
    pub(super) attempt: u32,
    pub(super) backoff: Option<prost_types::Duration>,
    pub(super) original_schedule_time: Option<SystemTime>,
}

impl From<CompleteLocalActivityData> for ResolveDat {
    fn from(d: CompleteLocalActivityData) -> Self {
        ResolveDat {
            result: match d.result {
                Ok(res) => LocalActivityExecutionResult::Completed(Success { result: Some(res) }),
                Err(fail) => {
                    if matches!(fail.failure_info, Some(FailureInfo::CanceledFailureInfo(_)))
                        || matches!(
                            fail.cause.as_deref().and_then(|f| f.failure_info.as_ref()),
                            Some(FailureInfo::CanceledFailureInfo(_))
                        )
                    {
                        LocalActivityExecutionResult::Cancelled(Cancellation {
                            failure: Some(fail),
                        })
                    } else {
                        LocalActivityExecutionResult::Failed(ActFail {
                            failure: Some(fail),
                        })
                    }
                }
            },
            complete_time: d.marker_dat.complete_time.try_into_or_none(),
            attempt: d.marker_dat.attempt,
            backoff: d.marker_dat.backoff,
            original_schedule_time: d.marker_dat.original_schedule_time.try_into_or_none(),
        }
    }
}

/// Creates a new local activity state machine & immediately schedules the local activity for
/// execution. No command is produced immediately to be sent to the server, as the local activity
/// must resolve before we send a record marker command. A [MachineResponse] may be produced,
/// to queue the LA for execution if it needs to be.
pub(super) fn new_local_activity(
    mut attrs: ValidScheduleLA,
    replaying_when_invoked: bool,
    wf_time: Option<SystemTime>,
    internal_flags: InternalFlagsRef,
) -> Result<(LocalActivityMachine, Vec<MachineResponse>), WFMachinesError> {
    let initial_state = if replaying_when_invoked {
        Replaying {}.into()
    } else {
        Executing {}.into()
    };

    // If the scheduled LA doesn't already have an "original" schedule time, assign one.
    attrs
        .original_schedule_time
        .get_or_insert(SystemTime::now());

    let mut machine = LocalActivityMachine::from_parts(
        initial_state,
        SharedState {
            attrs,
            replaying_when_invoked,
            wf_time_when_started: wf_time,
            internal_flags,
        },
    );

    let mut res = OnEventWrapper::on_event_mut(&mut machine, LocalActivityMachineEvents::Schedule)
        .expect("Scheduling local activities doesn't fail");
    let mr = if let Some(res) = res.pop() {
        machine
            .adapt_response(res, None)
            .expect("Adapting LA schedule response doesn't fail")
    } else {
        vec![]
    };
    Ok((machine, mr))
}

impl LocalActivityMachine {
    /// Is called to check if, while handling the LA marker event, we should avoid doing normal
    /// command-event processing - instead simply applying the event to this machine and then
    /// skipping over the rest. If this machine is in the `ResultNotified` state, that means
    /// command handling should proceed as normal (ie: The command needs to be matched and removed).
    /// Attempting the check in any other state likely means a bug in the SDK.
    pub(super) fn marker_should_get_special_handling(&self) -> Result<bool, WFMachinesError> {
        match self.state() {
            LocalActivityMachineState::ResultNotified(_) => Ok(false),
            LocalActivityMachineState::ResolvedFromMarkerLookAheadWaitingMarkerEvent(_) => Ok(true),
            _ => Err(fatal!(
                "Attempted to check for LA marker handling in invalid state {}",
                self.state()
            )),
        }
    }

    /// Returns true if the machine will willingly accept data from a marker in its current state.
    /// IE: Calling [Self::try_resolve_with_dat] makes sense.
    pub(super) fn will_accept_resolve_marker(&self) -> bool {
        matches!(
            self.state(),
            LocalActivityMachineState::WaitingResolveFromMarkerLookAhead(_)
        )
    }

    /// Must be called if the workflow encounters a non-replay workflow task
    pub(super) fn encountered_non_replay_wft(
        &mut self,
    ) -> Result<Vec<MachineResponse>, WFMachinesError> {
        // This only applies to the waiting-for-marker state. It can safely be ignored in the others
        if !matches!(
            self.state(),
            LocalActivityMachineState::WaitingResolveFromMarkerLookAhead(_)
        ) {
            return Ok(vec![]);
        }

        let mut res =
            OnEventWrapper::on_event_mut(self, LocalActivityMachineEvents::StartedNonReplayWFT)
                .map_err(|e| match e {
                    MachineError::InvalidTransition => fatal!(
                        "Invalid transition while notifying local activity (seq {})\
                         of non-replay-wft-started in {}",
                        self.shared_state.attrs.seq,
                        self.state(),
                    ),
                    MachineError::Underlying(e) => e,
                })?;
        let res = res.pop().expect("Always produces one response");
        Ok(self
            .adapt_response(res, None)
            .expect("Adapting LA wft-non-replay response doesn't fail"))
    }

    /// Attempt to resolve the local activity with a result from execution (not from history)
    pub(super) fn try_resolve(
        &mut self,
        result: LocalActivityExecutionResult,
        runtime: Duration,
        attempt: u32,
        backoff: Option<prost_types::Duration>,
        original_schedule_time: Option<SystemTime>,
    ) -> Result<Vec<MachineResponse>, WFMachinesError> {
        self._try_resolve(
            ResolveDat {
                result,
                complete_time: self.shared_state.wf_time_when_started.map(|t| t + runtime),
                attempt,
                backoff,
                original_schedule_time,
            },
            false,
        )
    }

    /// Attempt to resolve the local activity with already known data, ex pre-resolved data
    pub(super) fn try_resolve_with_dat(
        &mut self,
        dat: ResolveDat,
    ) -> Result<Vec<MachineResponse>, WFMachinesError> {
        self._try_resolve(dat, true)
    }

    fn _try_resolve(
        &mut self,
        dat: ResolveDat,
        from_marker: bool,
    ) -> Result<Vec<MachineResponse>, WFMachinesError> {
        let evt = if from_marker {
            LocalActivityMachineEvents::HandleKnownResult(dat)
        } else {
            LocalActivityMachineEvents::HandleResult(dat)
        };
        let res = OnEventWrapper::on_event_mut(self, evt).map_err(|e| match e {
            MachineError::InvalidTransition => fatal!(
                "Invalid transition resolving local activity (seq {}, from marker: {}) in {}",
                self.shared_state.attrs.seq,
                from_marker,
                self.state(),
            ),
            MachineError::Underlying(e) => e,
        })?;

        Ok(res
            .into_iter()
            .flat_map(|res| {
                self.adapt_response(res, None)
                    .expect("Adapting LA resolve response doesn't fail")
            })
            .collect())
    }

    pub(super) fn cancel(&mut self) -> Result<Vec<MachineResponse>, MachineError<WFMachinesError>> {
        let event = match self.shared_state.attrs.cancellation_type {
            ct @ ActivityCancellationType::TryCancel | ct @ ActivityCancellationType::Abandon => {
                LocalActivityMachineEvents::NoWaitCancel(ct)
            }
            _ => LocalActivityMachineEvents::Cancel,
        };
        let cmds = OnEventWrapper::on_event_mut(self, event)?;
        let mach_resps = cmds
            .into_iter()
            .map(|mc| self.adapt_response(mc, None))
            .flatten_ok()
            .try_collect()?;
        Ok(mach_resps)
    }

    pub(super) fn was_cancelled_before_sent_to_server(&self) -> bool {
        // This needs to always be false because for the situation where we cancel in the same WFT,
        // no command of any kind is created and no LA request is queued. Otherwise, the command we
        // create to record a cancel marker *needs* to be sent to the server still, which returning
        // true here would prevent.
        false
    }
}

#[derive(Clone)]
pub(super) struct SharedState {
    attrs: ValidScheduleLA,
    replaying_when_invoked: bool,
    wf_time_when_started: Option<SystemTime>,
    internal_flags: InternalFlagsRef,
}

impl SharedState {
    fn produce_no_wait_cancel_resolve_dat(&self) -> ResolveDat {
        ResolveDat {
            result: LocalActivityExecutionResult::empty_cancel(),
            // Just don't provide a complete time, which means try-cancel/abandon cancels won't
            // advance the clock. Seems like that's fine, since you can only cancel after awaiting
            // some other command, which would have appropriately advanced the clock anyway.
            complete_time: None,
            attempt: self.attrs.attempt,
            backoff: None,
            original_schedule_time: self.attrs.original_schedule_time,
        }
    }
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, derive_more::Display)]
pub(super) enum LocalActivityCommand {
    RequestActivityExecution(ValidScheduleLA),
    #[display("Resolved")]
    Resolved(ResolveDat),
    /// Indicate we want to cancel an LA that is currently executing, or look up if we have
    /// processed a marker with resolution data since the machine was constructed.
    #[display("Cancel")]
    RequestCancel,
}

#[derive(Default, Clone)]
pub(super) struct Executing {}

impl Executing {
    pub(super) fn on_schedule(
        self,
        dat: &mut SharedState,
    ) -> LocalActivityMachineTransition<RequestSent> {
        TransitionResult::commands([LocalActivityCommand::RequestActivityExecution(
            dat.attrs.clone(),
        )])
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ResultType {
    Completed,
    Cancelled,
    Failed,
}
#[derive(Clone)]
pub(super) struct MarkerCommandCreated {
    result_type: ResultType,
}
impl From<MarkerCommandCreated> for ResultNotified {
    fn from(mc: MarkerCommandCreated) -> Self {
        Self {
            result_type: mc.result_type,
        }
    }
}

impl MarkerCommandCreated {
    pub(super) fn on_command_record_marker(self) -> LocalActivityMachineTransition<ResultNotified> {
        TransitionResult::from(self)
    }
}

#[derive(Default, Clone)]
pub(super) struct MarkerCommandRecorded {}
impl MarkerCommandRecorded {
    fn on_cancel_requested(self) -> LocalActivityMachineTransition<MarkerCommandRecorded> {
        // We still must issue a cancel request even if this command is resolved, because if it
        // failed and we are backing off locally, we must tell the LA dispatcher to quit retrying.
        TransitionResult::ok([LocalActivityCommand::RequestCancel], self)
    }

    fn on_no_wait_cancel(
        self,
        cancel_type: ActivityCancellationType,
    ) -> LocalActivityMachineTransition<MarkerCommandRecorded> {
        if matches!(cancel_type, ActivityCancellationType::TryCancel) {
            // We still must issue a cancel request even if this command is resolved, because if it
            // failed and we are backing off locally, we must tell the LA dispatcher to quit
            // retrying.
            TransitionResult::ok(
                [LocalActivityCommand::RequestCancel],
                MarkerCommandRecorded::default(),
            )
        } else {
            TransitionResult::default()
        }
    }
}

#[derive(Default, Clone)]
pub(super) struct Replaying {}
impl Replaying {
    pub(super) fn on_schedule(
        self,
    ) -> LocalActivityMachineTransition<WaitingResolveFromMarkerLookAhead> {
        TransitionResult::ok([], WaitingResolveFromMarkerLookAhead {})
    }
}

#[derive(Default, Clone)]
pub(super) struct RequestSent {}

impl RequestSent {
    fn on_handle_result(
        self,
        dat: ResolveDat,
    ) -> LocalActivityMachineTransition<MarkerCommandCreated> {
        let result_type = match &dat.result {
            LocalActivityExecutionResult::Completed(_) => ResultType::Completed,
            LocalActivityExecutionResult::Failed(_) => ResultType::Failed,
            LocalActivityExecutionResult::TimedOut(_) => ResultType::Failed,
            LocalActivityExecutionResult::Cancelled { .. } => ResultType::Cancelled,
        };
        let new_state = MarkerCommandCreated { result_type };
        TransitionResult::ok([LocalActivityCommand::Resolved(dat)], new_state)
    }

    fn on_cancel_requested(self) -> LocalActivityMachineTransition<RequestSent> {
        TransitionResult::ok([LocalActivityCommand::RequestCancel], self)
    }

    fn on_no_wait_cancel(
        self,
        shared: &mut SharedState,
        cancel_type: ActivityCancellationType,
    ) -> LocalActivityMachineTransition<MarkerCommandCreated> {
        let mut cmds = vec![];
        if matches!(cancel_type, ActivityCancellationType::TryCancel) {
            // For try-cancels also request the cancel
            cmds.push(LocalActivityCommand::RequestCancel);
        }
        // Immediately resolve
        cmds.push(LocalActivityCommand::Resolved(
            shared.produce_no_wait_cancel_resolve_dat(),
        ));
        TransitionResult::ok(
            cmds,
            MarkerCommandCreated {
                result_type: ResultType::Cancelled,
            },
        )
    }
}

macro_rules! verify_marker_dat {
    ($shared:expr, $dat:expr, $ok_expr:expr) => {
        if let Err(err) = verify_marker_data_matches($shared, $dat) {
            TransitionResult::Err(err)
        } else {
            $ok_expr
        }
    };
}

#[derive(Clone)]
pub(super) struct ResultNotified {
    result_type: ResultType,
}

impl ResultNotified {
    pub(super) fn on_marker_recorded(
        self,
        shared: &mut SharedState,
        dat: CompleteLocalActivityData,
    ) -> LocalActivityMachineTransition<MarkerCommandRecorded> {
        if self.result_type == ResultType::Completed && dat.result.is_err() {
            return TransitionResult::Err(nondeterminism!(
                "Local activity (seq {}) completed successfully locally, but history said \
                 it failed!",
                shared.attrs.seq
            ));
        } else if self.result_type == ResultType::Failed && dat.result.is_ok() {
            return TransitionResult::Err(nondeterminism!(
                "Local activity (seq {}) failed locally, but history said it completed!",
                shared.attrs.seq
            ));
        }
        verify_marker_dat!(shared, &dat, TransitionResult::default())
    }
}

#[derive(Default, Clone)]
pub(super) struct WaitingResolveFromMarkerLookAhead {}

impl WaitingResolveFromMarkerLookAhead {
    fn on_handle_result(
        self,
        dat: ResolveDat,
    ) -> LocalActivityMachineTransition<ResolvedFromMarkerLookAheadWaitingMarkerEvent> {
        TransitionResult::ok(
            [LocalActivityCommand::Resolved(dat)],
            ResolvedFromMarkerLookAheadWaitingMarkerEvent {},
        )
    }
    pub(super) fn on_started_non_replay_wft(
        self,
        dat: &mut SharedState,
    ) -> LocalActivityMachineTransition<RequestSent> {
        // We aren't really "replaying" anymore for our purposes, and want to record the marker.
        dat.replaying_when_invoked = false;
        TransitionResult::commands([LocalActivityCommand::RequestActivityExecution(
            dat.attrs.clone(),
        )])
    }

    fn on_cancel_requested(
        self,
    ) -> LocalActivityMachineTransition<WaitingResolveFromMarkerLookAhead> {
        // We still "request a cancel" even though we know the local activity should not be running
        // because the data might be in the pre-resolved list.
        TransitionResult::ok([LocalActivityCommand::RequestCancel], self)
    }

    fn on_no_wait_cancel(
        self,
        _: ActivityCancellationType,
    ) -> LocalActivityMachineTransition<WaitingResolveFromMarkerLookAhead> {
        // Markers are always recorded when cancelling, so this is the same as a normal cancel on
        // the replay path
        self.on_cancel_requested()
    }
}

#[derive(Default, Clone)]
pub(super) struct ResolvedFromMarkerLookAheadWaitingMarkerEvent {}
impl ResolvedFromMarkerLookAheadWaitingMarkerEvent {
    pub(super) fn on_marker_recorded(
        self,
        shared: &mut SharedState,
        dat: CompleteLocalActivityData,
    ) -> LocalActivityMachineTransition<MarkerCommandRecorded> {
        verify_marker_dat!(shared, &dat, TransitionResult::default())
    }

    fn on_cancel_requested(
        self,
    ) -> LocalActivityMachineTransition<ResolvedFromMarkerLookAheadWaitingMarkerEvent> {
        TransitionResult::ok([LocalActivityCommand::RequestCancel], self)
    }

    fn on_no_wait_cancel(
        self,
        _: ActivityCancellationType,
    ) -> LocalActivityMachineTransition<ResolvedFromMarkerLookAheadWaitingMarkerEvent> {
        self.on_cancel_requested()
    }
}

impl WFMachinesAdapter for LocalActivityMachine {
    fn adapt_response(
        &self,
        my_command: Self::Command,
        _event_info: Option<EventInfo>,
    ) -> Result<Vec<MachineResponse>, WFMachinesError> {
        match my_command {
            LocalActivityCommand::RequestActivityExecution(act) => {
                Ok(vec![MachineResponse::QueueLocalActivity(act)])
            }
            LocalActivityCommand::Resolved(ResolveDat {
                result,
                complete_time,
                attempt,
                backoff,
                original_schedule_time,
            }) => {
                let mut maybe_ok_result = None;
                let mut maybe_failure = None;
                // Only issue record marker commands if we weren't replaying
                let record_marker = !self.shared_state.replaying_when_invoked;
                let mut will_not_run_again = false;
                match result.clone() {
                    LocalActivityExecutionResult::Completed(suc) => {
                        maybe_ok_result = suc.result;
                    }
                    LocalActivityExecutionResult::Failed(fail) => {
                        maybe_failure = fail.failure;
                    }
                    LocalActivityExecutionResult::Cancelled(Cancellation { failure })
                    | LocalActivityExecutionResult::TimedOut(ActFail { failure }) => {
                        will_not_run_again = true;
                        maybe_failure = failure;
                    }
                };
                let resolution = if let Some(b) = backoff.as_ref() {
                    ActivityResolution {
                        status: Some(
                            DoBackoff {
                                attempt: attempt + 1,
                                backoff_duration: Some(*b),
                                original_schedule_time: original_schedule_time.map(Into::into),
                            }
                            .into(),
                        ),
                    }
                } else {
                    // Cancels and timeouts are to be wrapped with an activity failure
                    macro_rules! wrap_fail {
                        ($me:ident, $fail:ident, $msg:expr, $info:pat) => {
                            let mut fail = $fail.failure.take();
                            let fail_info = fail.as_ref().and_then(|f| f.failure_info.as_ref());
                            if matches!(fail_info, Some($info)) {
                                fail = Some(Failure {
                                    message: $msg,
                                    cause: fail.map(Box::new),
                                    failure_info: Some(activity_fail_info(
                                        $me.shared_state.attrs.activity_type.clone(),
                                        $me.shared_state.attrs.activity_id.clone(),
                                        None,
                                        RetryState::CancelRequested,
                                        0,
                                        0,
                                    )),
                                    ..Default::default()
                                });
                            }
                            $fail.failure = fail;
                        };
                    }
                    match result {
                        LocalActivityExecutionResult::Completed(c) => ActivityResolution {
                            status: Some(c.into()),
                        },
                        LocalActivityExecutionResult::Failed(f) => ActivityResolution {
                            status: Some(f.into()),
                        },
                        LocalActivityExecutionResult::TimedOut(mut failure) => {
                            wrap_fail!(
                                self,
                                failure,
                                "Local Activity timed out".to_string(),
                                FailureInfo::TimeoutFailureInfo(_)
                            );
                            ActivityResolution {
                                status: Some(failure.into()),
                            }
                        }
                        LocalActivityExecutionResult::Cancelled(mut cancel) => {
                            wrap_fail!(
                                self,
                                cancel,
                                "Local Activity cancelled".to_string(),
                                FailureInfo::CanceledFailureInfo(_)
                            );
                            ActivityResolution {
                                status: Some(cancel.into()),
                            }
                        }
                    }
                };

                let mut responses = vec![
                    ResolveActivity {
                        seq: self.shared_state.attrs.seq,
                        result: Some(resolution),
                        is_local: true,
                    }
                    .into(),
                    MachineResponse::UpdateWFTime(complete_time),
                ];

                // Cancel-resolves of abandoned activities must be explicitly dropped from tracking
                // to avoid unnecessary WFT heartbeating.
                if will_not_run_again
                    && matches!(
                        self.shared_state.attrs.cancellation_type,
                        ActivityCancellationType::Abandon
                    )
                {
                    responses.push(MachineResponse::AbandonLocalActivity(
                        self.shared_state.attrs.seq,
                    ));
                }

                if record_marker {
                    let marker_data = RecordMarkerCommandAttributes {
                        marker_name: LOCAL_ACTIVITY_MARKER_NAME.to_string(),
                        details: build_local_activity_marker_details(
                            LocalActivityMarkerData {
                                seq: self.shared_state.attrs.seq,
                                attempt,
                                activity_id: self.shared_state.attrs.activity_id.clone(),
                                activity_type: self.shared_state.attrs.activity_type.clone(),
                                complete_time: complete_time.map(Into::into),
                                backoff,
                                original_schedule_time: original_schedule_time.map(Into::into),
                            },
                            maybe_ok_result,
                        ),
                        header: None,
                        failure: maybe_failure,
                    };
                    let command = ProtoCommand {
                        user_metadata: self.shared_state.attrs.user_metadata.clone(),
                        event_group_markers: self.shared_state.attrs.event_group_markers.clone(),
                        ..command::Attributes::RecordMarkerCommandAttributes(marker_data).into()
                    };
                    responses.push(MachineResponse::IssueNewCommand(command));
                }
                Ok(responses)
            }
            LocalActivityCommand::RequestCancel => {
                Ok(vec![MachineResponse::RequestCancelLocalActivity(
                    self.shared_state.attrs.seq,
                )])
            }
        }
    }
}

impl TryFrom<CommandType> for LocalActivityMachineEvents {
    type Error = ();

    fn try_from(c: CommandType) -> Result<Self, Self::Error> {
        Ok(match c {
            CommandType::RecordMarker => Self::CommandRecordMarker,
            _ => return Err(()),
        })
    }
}

impl TryFrom<HistEventData> for LocalActivityMachineEvents {
    type Error = WFMachinesError;

    fn try_from(e: HistEventData) -> Result<Self, Self::Error> {
        let e = e.event;
        if e.event_type() != EventType::MarkerRecorded {
            return Err(nondeterminism!(
                "Local activity machine cannot handle this event: {e}"
            ));
        }

        match e.into_local_activity_marker_details() {
            Some(marker_dat) => Ok(LocalActivityMachineEvents::MarkerRecorded(marker_dat)),
            _ => Err(nondeterminism!(
                "Local activity machine encountered an unparsable marker"
            )),
        }
    }
}

fn verify_marker_data_matches(
    shared: &SharedState,
    dat: &CompleteLocalActivityData,
) -> Result<(), WFMachinesError> {
    if shared.attrs.seq != dat.marker_dat.seq {
        return Err(nondeterminism!(
            "Local activity marker data has sequence number {} but matched against LA \
            command with sequence number {}",
            dat.marker_dat.seq,
            shared.attrs.seq
        ));
    }
    // Here we use whether or not we were replaying when we _first invoked_ the LA, because we
    // are always replaying when we see the marker recorded event, and that would make this check
    // a bit pointless.
    if shared.internal_flags.borrow_mut().try_use(
        CoreInternalFlags::IdAndTypeDeterminismChecks,
        !shared.replaying_when_invoked,
    ) {
        if dat.marker_dat.activity_id != shared.attrs.activity_id {
            return Err(nondeterminism!(
                "Activity id of recorded marker '{}' does not \
                 match activity id of local activity command '{}'",
                dat.marker_dat.activity_id,
                shared.attrs.activity_id
            ));
        }
        if dat.marker_dat.activity_type != shared.attrs.activity_type {
            return Err(nondeterminism!(
                "Activity type of recorded marker '{}' does not \
                 match activity type of local activity command '{}'",
                dat.marker_dat.activity_type,
                shared.attrs.activity_type
            ));
        }
    }

    Ok(())
}
