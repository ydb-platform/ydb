use super::{
    EventInfo, MachineResponse, NewMachineWithCommand, OnEventWrapper, WFMachinesAdapter,
    WFMachinesError,
};
use crate::worker::workflow::machines::HistEventData;
use rustfsm::{StateMachine, TransitionResult, fsm};
use std::convert::TryFrom;
use temporal_sdk_core_protos::{
    coresdk::workflow_commands::ContinueAsNewWorkflowExecution,
    temporal::api::{
        command::v1::continue_as_new_cmd_to_api,
        enums::v1::{CommandType, EventType},
    },
};

fsm! {
    pub(super)
    name ContinueAsNewWorkflowMachine;
    command ContinueAsNewWorkflowCommand;
    error WFMachinesError;

    Created --(Schedule, on_schedule) --> ContinueAsNewWorkflowCommandCreated;

    ContinueAsNewWorkflowCommandCreated --(CommandContinueAsNewWorkflowExecution)
        --> ContinueAsNewWorkflowCommandCreated;
    ContinueAsNewWorkflowCommandCreated --(WorkflowExecutionContinuedAsNew)
        --> ContinueAsNewWorkflowCommandRecorded;
}

#[derive(Debug, derive_more::Display)]
pub(super) enum ContinueAsNewWorkflowCommand {}

pub(super) fn continue_as_new(
    attribs: ContinueAsNewWorkflowExecution,
    use_compatible_version: bool,
) -> NewMachineWithCommand {
    let mut machine = ContinueAsNewWorkflowMachine::from_parts(Created {}.into(), ());
    OnEventWrapper::on_event_mut(&mut machine, ContinueAsNewWorkflowMachineEvents::Schedule)
        .expect("Scheduling continue as new machine doesn't fail");
    NewMachineWithCommand {
        command: continue_as_new_cmd_to_api(attribs, use_compatible_version),
        machine: machine.into(),
    }
}

#[derive(Default, Clone)]
pub(super) struct ContinueAsNewWorkflowCommandCreated {}

#[derive(Default, Clone)]
pub(super) struct ContinueAsNewWorkflowCommandRecorded {}

#[derive(Default, Clone)]
pub(super) struct Created {}

impl Created {
    pub(super) fn on_schedule(
        self,
    ) -> ContinueAsNewWorkflowMachineTransition<ContinueAsNewWorkflowCommandCreated> {
        TransitionResult::default()
    }
}

impl From<ContinueAsNewWorkflowCommandCreated> for ContinueAsNewWorkflowCommandRecorded {
    fn from(_: ContinueAsNewWorkflowCommandCreated) -> Self {
        Self::default()
    }
}

impl TryFrom<HistEventData> for ContinueAsNewWorkflowMachineEvents {
    type Error = WFMachinesError;

    fn try_from(e: HistEventData) -> Result<Self, Self::Error> {
        let e = e.event;
        Ok(match e.event_type() {
            EventType::WorkflowExecutionContinuedAsNew => Self::WorkflowExecutionContinuedAsNew,
            _ => {
                return Err(WFMachinesError::Nondeterminism(format!(
                    "Continue as new workflow machine does not handle this event: {e}"
                )));
            }
        })
    }
}

impl TryFrom<CommandType> for ContinueAsNewWorkflowMachineEvents {
    type Error = ();

    fn try_from(c: CommandType) -> Result<Self, Self::Error> {
        Ok(match c {
            CommandType::ContinueAsNewWorkflowExecution => {
                Self::CommandContinueAsNewWorkflowExecution
            }
            _ => return Err(()),
        })
    }
}

impl WFMachinesAdapter for ContinueAsNewWorkflowMachine {
    fn adapt_response(
        &self,
        _my_command: Self::Command,
        _event_info: Option<EventInfo>,
    ) -> Result<Vec<MachineResponse>, WFMachinesError> {
        Ok(vec![])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_help::{MockPollCfg, build_fake_sdk, canned_histories};
    use std::time::Duration;
    use temporal_sdk::{WfContext, WfExitValue, WorkflowResult};
    use temporal_sdk_core_protos::DEFAULT_WORKFLOW_TYPE;

    async fn wf_with_timer(ctx: WfContext) -> WorkflowResult<()> {
        ctx.timer(Duration::from_millis(500)).await;
        Ok(WfExitValue::continue_as_new(
            ContinueAsNewWorkflowExecution {
                arguments: vec![[1].into()],
                ..Default::default()
            },
        ))
    }

    #[tokio::test]
    async fn wf_completing_with_continue_as_new() {
        let t = canned_histories::timer_then_continue_as_new("1");
        let mut mock_cfg = MockPollCfg::from_hist_builder(t);
        mock_cfg.completion_asserts_from_expectations(|mut asserts| {
            asserts
                .then(|wft| {
                    assert_eq!(wft.commands.len(), 1);
                    assert_matches!(wft.commands[0].command_type(), CommandType::StartTimer);
                })
                .then(move |wft| {
                    assert_eq!(wft.commands.len(), 1);
                    assert_matches!(
                        wft.commands[0].command_type(),
                        CommandType::ContinueAsNewWorkflowExecution
                    );
                });
        });

        let mut worker = build_fake_sdk(mock_cfg);
        worker.register_wf(DEFAULT_WORKFLOW_TYPE, wf_with_timer);
        worker.run().await.unwrap();
    }
}
