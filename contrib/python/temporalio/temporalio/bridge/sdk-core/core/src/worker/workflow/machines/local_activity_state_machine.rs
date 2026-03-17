use super::{
    EventInfo, OnEventWrapper, WFMachinesAdapter, WFMachinesError,
    workflow_machines::MachineResponse,
};
use crate::{
    internal_flags::CoreInternalFlags,
    protosext::{CompleteLocalActivityData, HistoryEventExt, ValidScheduleLA},
    worker::{
        LocalActivityExecutionResult,
        workflow::{
            InternalFlagsRef,
            machines::{HistEventData, activity_state_machine::activity_fail_info},
        },
    },
};
use itertools::Itertools;
use rustfsm::{MachineError, StateMachine, TransitionResult, fsm};
use std::{
    convert::TryFrom,
    time::{Duration, SystemTime},
};
use temporal_sdk_core_protos::{
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
        command::v1::{RecordMarkerCommandAttributes, command},
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
    Replaying --(Schedule, on_schedule) --> WaitingMarkerEvent;
    ReplayingPreResolved --(Schedule, on_schedule) --> WaitingMarkerEventPreResolved;

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
    // LAs on the replay path always need to eventually see the marker
    WaitingMarkerEvent --(MarkerRecorded(CompleteLocalActivityData), shared on_marker_recorded)
      --> MarkerCommandRecorded;
    // If we are told to cancel while waiting for the marker, we still need to wait for the marker.
    WaitingMarkerEvent --(Cancel, on_cancel_requested) --> WaitingMarkerEvent;
    // Because there could be non-heartbeat WFTs (ex: signals being received) between scheduling
    // the LA and the marker being recorded, peekahead might not always resolve the LA *before*
    // scheduling it. This transition accounts for that.
    WaitingMarkerEvent --(HandleKnownResult(ResolveDat), on_handle_result) --> WaitingMarkerEvent;
    WaitingMarkerEvent --(NoWaitCancel(ActivityCancellationType),
                          on_no_wait_cancel) --> WaitingMarkerEvent;

    // It is entirely possible to have started the LA while replaying, only to find that we have
    // reached a new WFT and there still was no marker. In such cases we need to execute the LA.
    // This can easily happen if upon first execution, the worker does WFT heartbeating but then
    // dies for some reason.
    WaitingMarkerEvent --(StartedNonReplayWFT, shared on_started_non_replay_wft) --> RequestSent;

    // If the activity is pre resolved we still expect to see marker recorded event at some point,
    // even though we already resolved the activity.
    WaitingMarkerEventPreResolved --(MarkerRecorded(CompleteLocalActivityData),
                                     shared on_marker_recorded) --> MarkerCommandRecorded;
    // Ignore cancellations when waiting for the marker after being pre-resolved
    WaitingMarkerEventPreResolved --(Cancel) --> WaitingMarkerEventPreResolved;
    WaitingMarkerEventPreResolved --(NoWaitCancel(ActivityCancellationType))
                                     --> WaitingMarkerEventPreResolved;

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
    maybe_pre_resolved: Option<ResolveDat>,
    wf_time: Option<SystemTime>,
    internal_flags: InternalFlagsRef,
) -> Result<(LocalActivityMachine, Vec<MachineResponse>), WFMachinesError> {
    let initial_state = if replaying_when_invoked {
        if let Some(dat) = maybe_pre_resolved {
            ReplayingPreResolved { dat }.into()
        } else {
            Replaying {}.into()
        }
    } else {
        if maybe_pre_resolved.is_some() {
            return Err(WFMachinesError::Nondeterminism(
                "Local activity cannot be created as pre-resolved while not replaying".to_string(),
            ));
        }
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
    /// The other valid states to make this check in are the `WaitingMarkerEvent[PreResolved]`
    /// states, which will return true.
    ///
    /// Attempting the check in any other state likely means a bug in the SDK.
    pub(super) fn marker_should_get_special_handling(&self) -> Result<bool, WFMachinesError> {
        match self.state() {
            LocalActivityMachineState::ResultNotified(_) => Ok(false),
            LocalActivityMachineState::WaitingMarkerEvent(_) => Ok(true),
            LocalActivityMachineState::WaitingMarkerEventPreResolved(_) => Ok(true),
            _ => Err(WFMachinesError::Fatal(format!(
                "Attempted to check for LA marker handling in invalid state {}",
                self.state()
            ))),
        }
    }

    /// Returns true if the machine will willingly accept data from a marker in its current state.
    /// IE: Calling [Self::try_resolve_with_dat] makes sense.
    pub(super) fn will_accept_resolve_marker(&self) -> bool {
        matches!(
            self.state(),
            LocalActivityMachineState::WaitingMarkerEvent(_)
        )
    }

    /// Must be called if the workflow encounters a non-replay workflow task
    pub(super) fn encountered_non_replay_wft(
        &mut self,
    ) -> Result<Vec<MachineResponse>, WFMachinesError> {
        // This only applies to the waiting-for-marker state. It can safely be ignored in the others
        if !matches!(
            self.state(),
            LocalActivityMachineState::WaitingMarkerEvent(_)
        ) {
            return Ok(vec![]);
        }

        let mut res =
            OnEventWrapper::on_event_mut(self, LocalActivityMachineEvents::StartedNonReplayWFT)
                .map_err(|e| match e {
                    MachineError::InvalidTransition => WFMachinesError::Fatal(format!(
                        "Invalid transition while notifying local activity (seq {})\
                         of non-replay-wft-started in {}",
                        self.shared_state.attrs.seq,
                        self.state(),
                    )),
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
            MachineError::InvalidTransition => WFMachinesError::Fatal(format!(
                "Invalid transition resolving local activity (seq {}, from marker: {}) in {}",
                self.shared_state.attrs.seq,
                from_marker,
                self.state(),
            )),
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
    /// The fake marker is used to avoid special casing marker recorded event handling.
    /// If we didn't have the fake marker, there would be no "outgoing command" to match
    /// against the event. This way there is, but the command never will be issued to
    /// server because it is understood to be meaningless.
    #[display("FakeMarker")]
    FakeMarker,
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
    pub(super) fn on_schedule(self) -> LocalActivityMachineTransition<WaitingMarkerEvent> {
        TransitionResult::ok(
            [],
            WaitingMarkerEvent {
                already_resolved: false,
            },
        )
    }
}

#[derive(Clone)]
pub(super) struct ReplayingPreResolved {
    dat: ResolveDat,
}
impl ReplayingPreResolved {
    pub(super) fn on_schedule(
        self,
    ) -> LocalActivityMachineTransition<WaitingMarkerEventPreResolved> {
        TransitionResult::ok(
            [
                LocalActivityCommand::FakeMarker,
                LocalActivityCommand::Resolved(self.dat),
            ],
            WaitingMarkerEventPreResolved {},
        )
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
            return TransitionResult::Err(WFMachinesError::Nondeterminism(format!(
                "Local activity (seq {}) completed successfully locally, but history said \
                 it failed!",
                shared.attrs.seq
            )));
        } else if self.result_type == ResultType::Failed && dat.result.is_ok() {
            return TransitionResult::Err(WFMachinesError::Nondeterminism(format!(
                "Local activity (seq {}) failed locally, but history said it completed!",
                shared.attrs.seq
            )));
        }
        verify_marker_dat!(shared, &dat, TransitionResult::default())
    }
}

#[derive(Default, Clone)]
pub(super) struct WaitingMarkerEvent {
    already_resolved: bool,
}

impl WaitingMarkerEvent {
    pub(super) fn on_marker_recorded(
        self,
        shared: &mut SharedState,
        dat: CompleteLocalActivityData,
    ) -> LocalActivityMachineTransition<MarkerCommandRecorded> {
        verify_marker_dat!(
            shared,
            &dat,
            TransitionResult::commands(if self.already_resolved {
                vec![]
            } else {
                vec![LocalActivityCommand::Resolved(dat.into())]
            })
        )
    }
    fn on_handle_result(
        self,
        dat: ResolveDat,
    ) -> LocalActivityMachineTransition<WaitingMarkerEvent> {
        TransitionResult::ok(
            [LocalActivityCommand::Resolved(dat)],
            WaitingMarkerEvent {
                already_resolved: true,
            },
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

    fn on_cancel_requested(self) -> LocalActivityMachineTransition<WaitingMarkerEvent> {
        // We still "request a cancel" even though we know the local activity should not be running
        // because the data might be in the pre-resolved list.
        TransitionResult::ok([LocalActivityCommand::RequestCancel], self)
    }

    fn on_no_wait_cancel(
        self,
        _: ActivityCancellationType,
    ) -> LocalActivityMachineTransition<WaitingMarkerEvent> {
        // Markers are always recorded when cancelling, so this is the same as a normal cancel on
        // the replay path
        self.on_cancel_requested()
    }
}

#[derive(Default, Clone)]
pub(super) struct WaitingMarkerEventPreResolved {}
impl WaitingMarkerEventPreResolved {
    pub(super) fn on_marker_recorded(
        self,
        shared: &mut SharedState,
        dat: CompleteLocalActivityData,
    ) -> LocalActivityMachineTransition<MarkerCommandRecorded> {
        verify_marker_dat!(shared, &dat, TransitionResult::default())
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
                    responses.push(MachineResponse::IssueNewCommand(
                        command::Attributes::RecordMarkerCommandAttributes(marker_data).into(),
                    ));
                }
                Ok(responses)
            }
            LocalActivityCommand::FakeMarker => {
                // See docs for `FakeMarker` for more
                Ok(vec![MachineResponse::IssueFakeLocalActivityMarker(
                    self.shared_state.attrs.seq,
                )])
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
            return Err(WFMachinesError::Nondeterminism(format!(
                "Local activity machine cannot handle this event: {e}"
            )));
        }

        match e.into_local_activity_marker_details() {
            Some(marker_dat) => Ok(LocalActivityMachineEvents::MarkerRecorded(marker_dat)),
            _ => Err(WFMachinesError::Nondeterminism(
                "Local activity machine encountered an unparsable marker".to_string(),
            )),
        }
    }
}

fn verify_marker_data_matches(
    shared: &SharedState,
    dat: &CompleteLocalActivityData,
) -> Result<(), WFMachinesError> {
    if shared.attrs.seq != dat.marker_dat.seq {
        return Err(WFMachinesError::Nondeterminism(format!(
            "Local activity marker data has sequence number {} but matched against LA \
            command with sequence number {}",
            dat.marker_dat.seq, shared.attrs.seq
        )));
    }
    // Here we use whether or not we were replaying when we _first invoked_ the LA, because we
    // are always replaying when we see the marker recorded event, and that would make this check
    // a bit pointless.
    if shared.internal_flags.borrow_mut().try_use(
        CoreInternalFlags::IdAndTypeDeterminismChecks,
        !shared.replaying_when_invoked,
    ) {
        if dat.marker_dat.activity_id != shared.attrs.activity_id {
            return Err(WFMachinesError::Nondeterminism(format!(
                "Activity id of recorded marker '{}' does not \
                 match activity id of local activity command '{}'",
                dat.marker_dat.activity_id, shared.attrs.activity_id
            )));
        }
        if dat.marker_dat.activity_type != shared.attrs.activity_type {
            return Err(WFMachinesError::Nondeterminism(format!(
                "Activity type of recorded marker '{}' does not \
                 match activity type of local activity command '{}'",
                dat.marker_dat.activity_type, shared.attrs.activity_type
            )));
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        replay::TestHistoryBuilder,
        test_help::{MockPollCfg, ResponseType, build_fake_sdk, canned_histories},
    };
    use anyhow::anyhow;
    use rstest::rstest;
    use std::{
        sync::atomic::{AtomicI64, Ordering},
        time::Duration,
    };
    use temporal_sdk::{
        ActContext, ActivityError, CancellableFuture, LocalActivityOptions, WfContext,
        WorkflowResult,
    };
    use temporal_sdk_core_protos::{
        DEFAULT_ACTIVITY_TYPE, DEFAULT_WORKFLOW_TYPE,
        coresdk::{
            AsJsonPayloadExt,
            workflow_activation::{WorkflowActivationJob, workflow_activation_job},
        },
        temporal::api::{
            common::v1::RetryPolicy, enums::v1::WorkflowTaskFailedCause, failure::v1::Failure,
        },
    };
    use temporal_sdk_core_test_utils::interceptors::ActivationAssertionsInterceptor;
    use tokio_util::sync::CancellationToken;

    async fn la_wf(ctx: WfContext) -> WorkflowResult<()> {
        ctx.local_activity(LocalActivityOptions {
            activity_type: DEFAULT_ACTIVITY_TYPE.to_string(),
            input: ().as_json_payload().unwrap(),
            retry_policy: RetryPolicy {
                maximum_attempts: 1,
                ..Default::default()
            },
            ..Default::default()
        })
        .await;
        Ok(().into())
    }

    #[rstest]
    #[case::incremental(false, true)]
    #[case::replay(true, true)]
    #[case::incremental_fail(false, false)]
    #[case::replay_fail(true, false)]
    #[tokio::test]
    async fn one_la_success(#[case] replay: bool, #[case] completes_ok: bool) {
        let activity_id = "1";
        let mut t = TestHistoryBuilder::default();
        t.add_by_type(EventType::WorkflowExecutionStarted);
        t.add_full_wf_task();
        if completes_ok {
            t.add_local_activity_result_marker(1, activity_id, b"hi".into());
        } else {
            t.add_local_activity_fail_marker(
                1,
                activity_id,
                Failure::application_failure("I failed".to_string(), false),
            );
        }
        t.add_workflow_task_scheduled_and_started();

        let mut mock_cfg = if replay {
            MockPollCfg::from_resps(t, [ResponseType::AllHistory])
        } else {
            MockPollCfg::from_hist_builder(t)
        };
        mock_cfg.completion_asserts_from_expectations(|mut asserts| {
            asserts.then(move |wft| {
                let commands = &wft.commands;
                if !replay {
                    assert_eq!(commands.len(), 2);
                    assert_eq!(commands[0].command_type(), CommandType::RecordMarker);
                    if completes_ok {
                        assert_matches!(
                            commands[0].attributes.as_ref().unwrap(),
                            command::Attributes::RecordMarkerCommandAttributes(
                                RecordMarkerCommandAttributes { failure: None, .. }
                            )
                        );
                    } else {
                        assert_matches!(
                            commands[0].attributes.as_ref().unwrap(),
                            command::Attributes::RecordMarkerCommandAttributes(
                                RecordMarkerCommandAttributes {
                                    failure: Some(_),
                                    ..
                                }
                            )
                        );
                    }
                    assert_eq!(
                        commands[1].command_type(),
                        CommandType::CompleteWorkflowExecution
                    );
                } else {
                    assert_eq!(commands.len(), 1);
                    assert_matches!(
                        commands[0].command_type(),
                        CommandType::CompleteWorkflowExecution
                    );
                }
            });
        });

        let mut worker = build_fake_sdk(mock_cfg);
        worker.register_wf(DEFAULT_WORKFLOW_TYPE, la_wf);
        worker.register_activity(
            DEFAULT_ACTIVITY_TYPE,
            move |_ctx: ActContext, _: ()| async move {
                if replay {
                    panic!("Should not be invoked on replay");
                }
                if completes_ok {
                    Ok("hi")
                } else {
                    Err(anyhow!("Oh no I failed!").into())
                }
            },
        );
        worker.run().await.unwrap();
    }

    async fn two_la_wf(ctx: WfContext) -> WorkflowResult<()> {
        ctx.local_activity(LocalActivityOptions {
            activity_type: DEFAULT_ACTIVITY_TYPE.to_string(),
            input: ().as_json_payload().unwrap(),
            ..Default::default()
        })
        .await;
        ctx.local_activity(LocalActivityOptions {
            activity_type: DEFAULT_ACTIVITY_TYPE.to_string(),
            input: ().as_json_payload().unwrap(),
            ..Default::default()
        })
        .await;
        Ok(().into())
    }

    async fn two_la_wf_parallel(ctx: WfContext) -> WorkflowResult<()> {
        tokio::join!(
            ctx.local_activity(LocalActivityOptions {
                activity_type: DEFAULT_ACTIVITY_TYPE.to_string(),
                input: ().as_json_payload().unwrap(),
                ..Default::default()
            }),
            ctx.local_activity(LocalActivityOptions {
                activity_type: DEFAULT_ACTIVITY_TYPE.to_string(),
                input: ().as_json_payload().unwrap(),
                ..Default::default()
            })
        );
        Ok(().into())
    }

    #[rstest]
    #[tokio::test]
    async fn two_sequential_las(
        #[values(true, false)] replay: bool,
        #[values(true, false)] parallel: bool,
    ) {
        let t = canned_histories::two_local_activities_one_wft(parallel);
        let mut mock_cfg = if replay {
            MockPollCfg::from_resps(t, [ResponseType::AllHistory])
        } else {
            MockPollCfg::from_hist_builder(t)
        };

        let mut aai = ActivationAssertionsInterceptor::default();
        let first_act_ts_seconds: &'static _ = Box::leak(Box::new(AtomicI64::new(-1)));
        aai.then(|a| {
            first_act_ts_seconds.store(a.timestamp.as_ref().unwrap().seconds, Ordering::Relaxed)
        });
        // Verify LAs advance time (they take 1s as defined in the canned history)
        aai.then(move |a| {
            if !parallel {
                assert_matches!(
                    a.jobs.as_slice(),
                    [WorkflowActivationJob {
                        variant: Some(workflow_activation_job::Variant::ResolveActivity(ra))
                    }] => assert_eq!(ra.seq, 1)
                );
            } else {
                assert_matches!(
                    a.jobs.as_slice(),
                    [WorkflowActivationJob {
                        variant: Some(workflow_activation_job::Variant::ResolveActivity(ra))
                     }, WorkflowActivationJob {
                        variant: Some(workflow_activation_job::Variant::ResolveActivity(ra2))
                    }] => {assert_eq!(ra.seq, 1); assert_eq!(ra2.seq, 2)}
                );
            }
            if replay {
                assert!(
                    a.timestamp.as_ref().unwrap().seconds
                        > first_act_ts_seconds.load(Ordering::Relaxed)
                )
            }
        });
        if !parallel {
            aai.then(move |a| {
                assert_matches!(
                    a.jobs.as_slice(),
                    [WorkflowActivationJob {
                        variant: Some(workflow_activation_job::Variant::ResolveActivity(ra))
                    }] => assert_eq!(ra.seq, 2)
                );
                if replay {
                    assert!(
                        a.timestamp.as_ref().unwrap().seconds
                            >= first_act_ts_seconds.load(Ordering::Relaxed) + 2
                    )
                }
            });
        }

        mock_cfg.completion_asserts_from_expectations(|mut asserts| {
            asserts.then(move |wft| {
                let commands = &wft.commands;
                if !replay {
                    assert_eq!(commands.len(), 3);
                    assert_eq!(commands[0].command_type(), CommandType::RecordMarker);
                    assert_eq!(commands[1].command_type(), CommandType::RecordMarker);
                    assert_matches!(
                        commands[2].command_type(),
                        CommandType::CompleteWorkflowExecution
                    );
                } else {
                    assert_eq!(commands.len(), 1);
                    assert_matches!(
                        commands[0].command_type(),
                        CommandType::CompleteWorkflowExecution
                    );
                }
            });
        });

        let mut worker = build_fake_sdk(mock_cfg);
        worker.set_worker_interceptor(aai);
        if parallel {
            worker.register_wf(DEFAULT_WORKFLOW_TYPE, two_la_wf_parallel);
        } else {
            worker.register_wf(DEFAULT_WORKFLOW_TYPE, two_la_wf);
        }
        worker.register_activity(
            DEFAULT_ACTIVITY_TYPE,
            move |_ctx: ActContext, _: ()| async move { Ok("Resolved") },
        );
        worker.run().await.unwrap();
    }

    async fn la_timer_la(ctx: WfContext) -> WorkflowResult<()> {
        ctx.local_activity(LocalActivityOptions {
            activity_type: DEFAULT_ACTIVITY_TYPE.to_string(),
            input: ().as_json_payload().unwrap(),
            ..Default::default()
        })
        .await;
        ctx.timer(Duration::from_secs(5)).await;
        ctx.local_activity(LocalActivityOptions {
            activity_type: DEFAULT_ACTIVITY_TYPE.to_string(),
            input: ().as_json_payload().unwrap(),
            ..Default::default()
        })
        .await;
        Ok(().into())
    }

    #[rstest]
    #[case::incremental(false)]
    #[case::replay(true)]
    #[tokio::test]
    async fn las_separated_by_timer(#[case] replay: bool) {
        let mut t = TestHistoryBuilder::default();
        t.add_by_type(EventType::WorkflowExecutionStarted);
        t.add_full_wf_task();
        t.add_local_activity_result_marker(1, "1", b"hi".into());
        let timer_started_event_id = t.add_by_type(EventType::TimerStarted);
        t.add_timer_fired(timer_started_event_id, "1".to_string());
        t.add_full_wf_task();
        t.add_local_activity_result_marker(2, "2", b"hi2".into());
        t.add_workflow_task_scheduled_and_started();
        let mut mock_cfg = if replay {
            MockPollCfg::from_resps(t, [ResponseType::AllHistory])
        } else {
            MockPollCfg::from_hist_builder(t)
        };

        let mut aai = ActivationAssertionsInterceptor::default();
        aai.skip_one()
            .then(|a| {
                assert_matches!(
                    a.jobs.as_slice(),
                    [WorkflowActivationJob {
                        variant: Some(workflow_activation_job::Variant::ResolveActivity(ra))
                    }] => assert_eq!(ra.seq, 1)
                );
            })
            .then(|a| {
                assert_matches!(
                    a.jobs.as_slice(),
                    [WorkflowActivationJob {
                        variant: Some(workflow_activation_job::Variant::FireTimer(_))
                    }]
                );
            });

        mock_cfg.completion_asserts_from_expectations(|mut asserts| {
            if replay {
                asserts.then(|wft| {
                    assert_eq!(wft.commands.len(), 1);
                    assert_eq!(
                        wft.commands[0].command_type,
                        CommandType::CompleteWorkflowExecution as i32
                    );
                });
            } else {
                asserts
                    .then(|wft| {
                        let commands = &wft.commands;
                        assert_eq!(commands.len(), 2);
                        assert_eq!(commands[0].command_type, CommandType::RecordMarker as i32);
                        assert_eq!(commands[1].command_type, CommandType::StartTimer as i32);
                    })
                    .then(|wft| {
                        let commands = &wft.commands;
                        assert_eq!(commands.len(), 2);
                        assert_eq!(commands[0].command_type, CommandType::RecordMarker as i32);
                        assert_eq!(
                            commands[1].command_type,
                            CommandType::CompleteWorkflowExecution as i32
                        );
                    });
            }
        });

        let mut worker = build_fake_sdk(mock_cfg);
        worker.set_worker_interceptor(aai);
        worker.register_wf(DEFAULT_WORKFLOW_TYPE, la_timer_la);
        worker.register_activity(
            DEFAULT_ACTIVITY_TYPE,
            move |_ctx: ActContext, _: ()| async move { Ok("Resolved") },
        );
        worker.run().await.unwrap();
    }

    #[tokio::test]
    async fn one_la_heartbeating_wft_failure_still_executes() {
        let mut t = TestHistoryBuilder::default();
        t.add_by_type(EventType::WorkflowExecutionStarted);
        // Heartbeats
        t.add_full_wf_task();
        // fails a wft for some reason
        t.add_workflow_task_scheduled_and_started();
        t.add_workflow_task_failed_with_failure(
            WorkflowTaskFailedCause::NonDeterministicError,
            Default::default(),
        );
        t.add_workflow_task_scheduled_and_started();

        let mut mock_cfg = MockPollCfg::from_hist_builder(t);
        mock_cfg.completion_asserts_from_expectations(|mut asserts| {
            asserts.then(move |wft| {
                assert_eq!(wft.commands.len(), 2);
                assert_eq!(wft.commands[0].command_type(), CommandType::RecordMarker);
                assert_matches!(
                    wft.commands[1].command_type(),
                    CommandType::CompleteWorkflowExecution
                );
            });
        });

        let mut worker = build_fake_sdk(mock_cfg);
        worker.register_wf(DEFAULT_WORKFLOW_TYPE, la_wf);
        worker.register_activity(
            DEFAULT_ACTIVITY_TYPE,
            move |_ctx: ActContext, _: ()| async move { Ok("Resolved") },
        );
        worker.run().await.unwrap();
    }

    #[rstest]
    #[tokio::test]
    async fn immediate_cancel(
        #[values(
            ActivityCancellationType::WaitCancellationCompleted,
            ActivityCancellationType::TryCancel,
            ActivityCancellationType::Abandon
        )]
        cancel_type: ActivityCancellationType,
    ) {
        let mut t = TestHistoryBuilder::default();
        t.add_by_type(EventType::WorkflowExecutionStarted);
        t.add_full_wf_task();
        t.add_workflow_execution_completed();

        let mut mock_cfg = MockPollCfg::from_hist_builder(t);
        mock_cfg.completion_asserts_from_expectations(|mut asserts| {
            asserts.then(|wft| {
                assert_eq!(wft.commands.len(), 2);
                // We record the cancel marker
                assert_eq!(wft.commands[0].command_type(), CommandType::RecordMarker);
                assert_matches!(
                    wft.commands[1].command_type(),
                    CommandType::CompleteWorkflowExecution
                );
            });
        });

        let mut worker = build_fake_sdk(mock_cfg);
        worker.register_wf(DEFAULT_WORKFLOW_TYPE, move |ctx: WfContext| async move {
            let la = ctx.local_activity(LocalActivityOptions {
                cancel_type,
                ..Default::default()
            });
            la.cancel(&ctx);
            la.await;
            Ok(().into())
        });
        // Explicitly don't register an activity, since we shouldn't need to run one.
        worker.run().await.unwrap();
    }

    #[rstest]
    #[case::incremental(false)]
    #[case::replay(true)]
    #[tokio::test]
    async fn cancel_after_act_starts(
        #[case] replay: bool,
        #[values(
            ActivityCancellationType::WaitCancellationCompleted,
            ActivityCancellationType::TryCancel,
            ActivityCancellationType::Abandon
        )]
        cancel_type: ActivityCancellationType,
    ) {
        let mut t = TestHistoryBuilder::default();
        t.add_wfe_started_with_wft_timeout(Duration::from_millis(100));
        t.add_full_wf_task();
        let timer_started_event_id = t.add_by_type(EventType::TimerStarted);
        t.add_timer_fired(timer_started_event_id, "1".to_string());
        t.add_full_wf_task();
        // This extra workflow task serves to prevent looking ahead and pre-resolving during
        // wait-cancel.
        // TODO: including this on non wait-cancel seems to cause double-send of
        //   marker recorded cmd
        if cancel_type == ActivityCancellationType::WaitCancellationCompleted {
            t.add_full_wf_task();
        }
        if cancel_type != ActivityCancellationType::WaitCancellationCompleted {
            // With non-wait cancels, the cancel is immediate
            t.add_local_activity_cancel_marker(1, "1");
        }
        let timer_started_event_id = t.add_by_type(EventType::TimerStarted);
        if cancel_type == ActivityCancellationType::WaitCancellationCompleted {
            // With wait cancels, the cancel marker is not recorded until activity reports.
            t.add_local_activity_cancel_marker(1, "1");
        }
        t.add_timer_fired(timer_started_event_id, "2".to_string());
        t.add_full_wf_task();
        t.add_workflow_execution_completed();

        let mut mock_cfg = if replay {
            MockPollCfg::from_resps(t, [ResponseType::AllHistory])
        } else {
            MockPollCfg::from_hist_builder(t)
        };
        let allow_cancel_barr = CancellationToken::new();
        let allow_cancel_barr_clone = allow_cancel_barr.clone();

        if !replay {
            mock_cfg.completion_asserts_from_expectations(|mut asserts| {
                asserts
                    .then(move |wft| {
                        assert_eq!(wft.commands.len(), 1);
                        assert_eq!(wft.commands[0].command_type, CommandType::StartTimer as i32);
                    })
                    .then(move |wft| {
                        let commands = &wft.commands;
                        if cancel_type == ActivityCancellationType::WaitCancellationCompleted {
                            assert_eq!(commands.len(), 1);
                            assert_eq!(commands[0].command_type, CommandType::StartTimer as i32);
                        } else {
                            // Try-cancel/abandon immediately recordsmarker (when not replaying)
                            assert_eq!(commands.len(), 2);
                            assert_eq!(commands[0].command_type, CommandType::RecordMarker as i32);
                            assert_eq!(commands[1].command_type, CommandType::StartTimer as i32);
                        }
                        // Allow the wait-cancel to actually cancel
                        allow_cancel_barr.cancel();
                    })
                    .then(move |wft| {
                        let commands = &wft.commands;
                        if cancel_type == ActivityCancellationType::WaitCancellationCompleted {
                            assert_eq!(commands[0].command_type, CommandType::StartTimer as i32);
                            assert_eq!(commands[1].command_type, CommandType::RecordMarker as i32);
                        } else {
                            assert_eq!(
                                commands[0].command_type,
                                CommandType::CompleteWorkflowExecution as i32
                            );
                        }
                    });
            });
        }

        let mut worker = build_fake_sdk(mock_cfg);
        worker.register_wf(DEFAULT_WORKFLOW_TYPE, move |ctx: WfContext| async move {
            let la = ctx.local_activity(LocalActivityOptions {
                cancel_type,
                input: ().as_json_payload().unwrap(),
                activity_type: DEFAULT_ACTIVITY_TYPE.to_string(),
                ..Default::default()
            });
            ctx.timer(Duration::from_secs(1)).await;
            la.cancel(&ctx);
            // This extra timer is here to ensure the presence of another WF task doesn't mess up
            // resolving the LA with cancel on replay
            ctx.timer(Duration::from_secs(1)).await;
            let resolution = la.await;
            assert!(resolution.cancelled());
            let rfail = resolution.unwrap_failure();
            assert_matches!(
                rfail.failure_info,
                Some(FailureInfo::ActivityFailureInfo(_))
            );
            assert_matches!(
                rfail.cause.unwrap().failure_info,
                Some(FailureInfo::CanceledFailureInfo(_))
            );
            Ok(().into())
        });
        worker.register_activity(DEFAULT_ACTIVITY_TYPE, move |ctx: ActContext, _: ()| {
            let allow_cancel_barr_clone = allow_cancel_barr_clone.clone();
            async move {
                if cancel_type == ActivityCancellationType::WaitCancellationCompleted {
                    ctx.cancelled().await;
                }
                allow_cancel_barr_clone.cancelled().await;
                Result::<(), _>::Err(ActivityError::cancelled())
            }
        });
        worker.run().await.unwrap();
    }
}
