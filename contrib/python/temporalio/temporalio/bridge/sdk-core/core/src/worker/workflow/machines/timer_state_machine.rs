#![allow(clippy::large_enum_variant)]

use super::{
    EventInfo, NewMachineWithCommand, OnEventWrapper, WFMachinesAdapter,
    workflow_machines::MachineResponse,
};
use crate::worker::workflow::{WFMachinesError, machines::HistEventData};
use rustfsm::{MachineError, StateMachine, TransitionResult, fsm};
use std::convert::TryFrom;
use temporal_sdk_core_protos::{
    coresdk::{
        HistoryEventId,
        workflow_activation::FireTimer,
        workflow_commands::{CancelTimer, StartTimer},
    },
    temporal::api::{
        command::v1::command,
        enums::v1::{CommandType, EventType},
        history::v1::{TimerFiredEventAttributes, history_event},
    },
};

fsm! {
    pub(super) name TimerMachine;
    command TimerMachineCommand;
    error WFMachinesError;
    shared_state SharedState;

    Created --(Schedule, on_schedule) --> StartCommandCreated;

    StartCommandCreated --(CommandStartTimer) --> StartCommandCreated;
    StartCommandCreated --(TimerStarted(HistoryEventId), on_timer_started) --> StartCommandRecorded;
    StartCommandCreated --(Cancel, shared on_cancel) --> Canceled;

    StartCommandRecorded --(TimerFired(TimerFiredEventAttributes), shared on_timer_fired) --> Fired;
    StartCommandRecorded --(Cancel, shared on_cancel) --> CancelTimerCommandCreated;

    CancelTimerCommandCreated --(Cancel) --> CancelTimerCommandCreated;
    CancelTimerCommandCreated
        --(CommandCancelTimer, on_command_cancel_timer) --> CancelTimerCommandSent;

    CancelTimerCommandSent --(TimerCanceled) --> Canceled;

    // Ignore any spurious cancellations after resolution
    Canceled --(Cancel) --> Canceled;
    Fired --(Cancel) --> Fired;
}

#[derive(Debug, derive_more::Display)]
pub(super) enum TimerMachineCommand {
    Complete,
    IssueCancelCmd(command::Attributes),
    // We don't issue activations for timer cancellations. Lang SDK is expected to cancel
    // it's own timers when user calls cancel, and they cannot be cancelled by any other
    // means.
}

#[derive(Default, Clone)]
pub(super) struct SharedState {
    attrs: StartTimer,
    cancelled_before_sent: bool,
}

/// Creates a new, scheduled, timer as a [CancellableCommand]
pub(super) fn new_timer(attribs: StartTimer) -> NewMachineWithCommand {
    let (timer, add_cmd) = TimerMachine::new_scheduled(attribs);
    NewMachineWithCommand {
        command: add_cmd,
        machine: timer.into(),
    }
}

impl TimerMachine {
    /// Create a new timer and immediately schedule it
    fn new_scheduled(attribs: StartTimer) -> (Self, command::Attributes) {
        let mut s = Self::new(attribs);
        OnEventWrapper::on_event_mut(&mut s, TimerMachineEvents::Schedule)
            .expect("Scheduling timers doesn't fail");
        let cmd = s.shared_state().attrs.into();
        (s, cmd)
    }

    fn new(attribs: StartTimer) -> Self {
        Self::from_parts(
            Created {}.into(),
            SharedState {
                attrs: attribs,
                cancelled_before_sent: false,
            },
        )
    }

    pub(super) fn cancel(&mut self) -> Result<Vec<MachineResponse>, MachineError<WFMachinesError>> {
        Ok(
            match OnEventWrapper::on_event_mut(self, TimerMachineEvents::Cancel)?.pop() {
                Some(TimerMachineCommand::IssueCancelCmd(cmd)) => {
                    vec![MachineResponse::IssueNewCommand(cmd.into())]
                }
                None => vec![],
                x => panic!("Invalid cancel event response {x:?}"),
            },
        )
    }

    pub(super) fn was_cancelled_before_sent_to_server(&self) -> bool {
        self.shared_state().cancelled_before_sent
    }
}

impl TryFrom<HistEventData> for TimerMachineEvents {
    type Error = WFMachinesError;

    fn try_from(e: HistEventData) -> Result<Self, Self::Error> {
        let e = e.event;
        Ok(match e.event_type() {
            EventType::TimerStarted => Self::TimerStarted(e.event_id),
            EventType::TimerCanceled => Self::TimerCanceled,
            EventType::TimerFired => {
                if let Some(history_event::Attributes::TimerFiredEventAttributes(attrs)) =
                    e.attributes
                {
                    Self::TimerFired(attrs)
                } else {
                    return Err(WFMachinesError::Fatal(format!(
                        "Timer fired attribs were unset: {e}"
                    )));
                }
            }
            _ => {
                return Err(WFMachinesError::Nondeterminism(format!(
                    "Timer machine does not handle this event: {e}"
                )));
            }
        })
    }
}

impl TryFrom<CommandType> for TimerMachineEvents {
    type Error = ();

    fn try_from(c: CommandType) -> Result<Self, Self::Error> {
        Ok(match c {
            CommandType::StartTimer => Self::CommandStartTimer,
            CommandType::CancelTimer => Self::CommandCancelTimer,
            _ => return Err(()),
        })
    }
}

#[derive(Default, Clone)]
pub(super) struct Created {}

impl Created {
    pub(super) fn on_schedule(self) -> TimerMachineTransition<StartCommandCreated> {
        TransitionResult::default()
    }
}

#[derive(Default, Clone)]
pub(super) struct CancelTimerCommandCreated {}

impl CancelTimerCommandCreated {
    pub(super) fn on_command_cancel_timer(self) -> TimerMachineTransition<CancelTimerCommandSent> {
        TransitionResult::ok(vec![], CancelTimerCommandSent::default())
    }
}

#[derive(Default, Clone)]
pub(super) struct CancelTimerCommandSent {}

#[derive(Default, Clone)]
pub(super) struct Canceled {}

impl From<CancelTimerCommandSent> for Canceled {
    fn from(_: CancelTimerCommandSent) -> Self {
        Self::default()
    }
}

#[derive(Default, Clone)]
pub(super) struct Fired {}

#[derive(Default, Clone)]
pub(super) struct StartCommandCreated {}

impl StartCommandCreated {
    pub(super) fn on_timer_started(
        self,
        _id: HistoryEventId,
    ) -> TimerMachineTransition<StartCommandRecorded> {
        TransitionResult::default()
    }

    pub(super) fn on_cancel(self, dat: &mut SharedState) -> TimerMachineTransition<Canceled> {
        dat.cancelled_before_sent = true;
        TransitionResult::default()
    }
}

#[derive(Default, Clone)]
pub(super) struct StartCommandRecorded {}

impl StartCommandRecorded {
    pub(super) fn on_timer_fired(
        self,
        dat: &mut SharedState,
        attrs: TimerFiredEventAttributes,
    ) -> TimerMachineTransition<Fired> {
        if dat.attrs.seq.to_string() == attrs.timer_id {
            TransitionResult::ok(vec![TimerMachineCommand::Complete], Fired::default())
        } else {
            TransitionResult::Err(WFMachinesError::Nondeterminism(format!(
                "Timer fired event did not have expected timer id {}, it was {}!",
                dat.attrs.seq, attrs.timer_id
            )))
        }
    }

    pub(super) fn on_cancel(
        self,
        dat: &mut SharedState,
    ) -> TimerMachineTransition<CancelTimerCommandCreated> {
        TransitionResult::ok(
            vec![TimerMachineCommand::IssueCancelCmd(
                CancelTimer { seq: dat.attrs.seq }.into(),
            )],
            CancelTimerCommandCreated::default(),
        )
    }
}

impl WFMachinesAdapter for TimerMachine {
    fn adapt_response(
        &self,
        my_command: TimerMachineCommand,
        _event_info: Option<EventInfo>,
    ) -> Result<Vec<MachineResponse>, WFMachinesError> {
        Ok(match my_command {
            // Fire the completion
            TimerMachineCommand::Complete => vec![
                FireTimer {
                    seq: self.shared_state.attrs.seq,
                }
                .into(),
            ],
            TimerMachineCommand::IssueCancelCmd(c) => {
                vec![MachineResponse::IssueNewCommand(c.into())]
            }
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        replay::TestHistoryBuilder,
        test_help::{MockPollCfg, build_fake_sdk, canned_histories},
    };
    use std::{mem::discriminant, time::Duration};
    use temporal_sdk::{CancellableFuture, WfContext, WorkflowResult};
    use temporal_sdk_core_protos::{
        DEFAULT_WORKFLOW_TYPE,
        temporal::api::{enums::v1::WorkflowTaskFailedCause, failure::v1::Failure},
    };

    async fn happy_timer(ctx: WfContext) -> WorkflowResult<()> {
        ctx.timer(Duration::from_secs(5)).await;
        Ok(().into())
    }

    #[tokio::test]
    async fn test_fire_happy_path_inc() {
        let t = canned_histories::single_timer("1");
        let mut mock_cfg = MockPollCfg::from_hist_builder(t);
        mock_cfg.completion_asserts_from_expectations(|mut asserts| {
            asserts
                .then(move |wft| {
                    assert_eq!(wft.commands.len(), 1);
                    assert_eq!(wft.commands[0].command_type(), CommandType::StartTimer);
                })
                .then(move |wft| {
                    assert_eq!(wft.commands.len(), 1);
                    assert_eq!(
                        wft.commands[0].command_type(),
                        CommandType::CompleteWorkflowExecution
                    );
                });
        });

        let mut worker = build_fake_sdk(mock_cfg);
        worker.register_wf(DEFAULT_WORKFLOW_TYPE, happy_timer);
        worker.run().await.unwrap();
    }

    #[tokio::test]
    async fn mismatched_timer_ids_errors() {
        let t = canned_histories::single_timer("badid");
        let mut mock_cfg = MockPollCfg::from_hist_builder(t);
        mock_cfg.num_expected_fails = 1;
        mock_cfg.expect_fail_wft_matcher = Box::new(move |_, cause, f| {
            matches!(cause, WorkflowTaskFailedCause::NonDeterministicError)
                && matches!(f, Some(Failure {message, .. })
            if message.contains("Timer fired event did not have expected timer id 1"))
        });
        let mut worker = build_fake_sdk(mock_cfg);
        worker.register_wf(DEFAULT_WORKFLOW_TYPE, |ctx: WfContext| async move {
            ctx.timer(Duration::from_secs(5)).await;
            Ok(().into())
        });
        worker.run().await.unwrap();
    }

    async fn cancel_timer(ctx: WfContext) -> WorkflowResult<()> {
        let cancel_timer_fut = ctx.timer(Duration::from_secs(500));
        ctx.timer(Duration::from_secs(5)).await;
        // Cancel the first timer after having waited on the second
        cancel_timer_fut.cancel(&ctx);
        cancel_timer_fut.await;
        Ok(().into())
    }

    #[tokio::test]
    async fn incremental_cancellation() {
        let t = canned_histories::cancel_timer("2", "1");
        let mut mock_cfg = MockPollCfg::from_hist_builder(t);
        mock_cfg.completion_asserts_from_expectations(|mut asserts| {
            asserts
                .then(move |wft| {
                    assert_eq!(wft.commands.len(), 2);
                    assert_eq!(wft.commands[0].command_type(), CommandType::StartTimer);
                    assert_eq!(wft.commands[1].command_type(), CommandType::StartTimer);
                })
                .then(move |wft| {
                    assert_eq!(wft.commands.len(), 2);
                    assert_eq!(wft.commands[0].command_type(), CommandType::CancelTimer);
                    assert_eq!(
                        wft.commands[1].command_type(),
                        CommandType::CompleteWorkflowExecution
                    );
                });
        });

        let mut worker = build_fake_sdk(mock_cfg);
        worker.register_wf(DEFAULT_WORKFLOW_TYPE, cancel_timer);
        worker.run().await.unwrap();
    }

    #[tokio::test]
    async fn cancel_before_sent_to_server() {
        let mut t = TestHistoryBuilder::default();
        t.add_by_type(EventType::WorkflowExecutionStarted);
        t.add_full_wf_task();
        t.add_workflow_execution_completed();
        let mut mock_cfg = MockPollCfg::from_hist_builder(t);
        mock_cfg.completion_asserts_from_expectations(|mut asserts| {
            asserts.then(move |wft| {
                assert_eq!(wft.commands.len(), 1);
                assert_matches!(
                    wft.commands[0].command_type(),
                    CommandType::CompleteWorkflowExecution
                );
            });
        });
        let mut worker = build_fake_sdk(mock_cfg);
        worker.register_wf(DEFAULT_WORKFLOW_TYPE, |ctx: WfContext| async move {
            let cancel_timer_fut = ctx.timer(Duration::from_secs(500));
            // Immediately cancel the timer
            cancel_timer_fut.cancel(&ctx);
            cancel_timer_fut.await;
            Ok(().into())
        });
        worker.run().await.unwrap();
    }

    #[test]
    fn cancels_ignored_terminal() {
        for state in [TimerMachineState::Canceled(Canceled {}), Fired {}.into()] {
            let mut s = TimerMachine::from_parts(state.clone(), Default::default());
            let cmds = s.cancel().unwrap();
            assert_eq!(cmds.len(), 0);
            assert_eq!(discriminant(&state), discriminant(s.state()));
        }
    }
}
