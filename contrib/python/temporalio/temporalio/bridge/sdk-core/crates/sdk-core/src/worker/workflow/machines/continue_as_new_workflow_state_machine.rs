use super::{
    EventInfo, MachineResponse, NewMachineWithCommand, OnEventWrapper, StateMachine,
    TransitionResult, WFMachinesAdapter, WFMachinesError, fsm,
};
use crate::worker::workflow::{machines::HistEventData, nondeterminism};
use std::convert::TryFrom;
use temporalio_common::protos::{
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
                return Err(nondeterminism!(
                    "Continue as new workflow machine does not handle this event: {e}"
                ));
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
