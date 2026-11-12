use super::{
    EventInfo, NewMachineWithCommand, OnEventWrapper, StateMachine, TransitionResult,
    WFMachinesAdapter, WFMachinesError, fsm, workflow_machines::MachineResponse,
};
use crate::worker::workflow::{machines::HistEventData, nondeterminism};
use std::convert::TryFrom;
use temporalio_common::protos::{
    coresdk::workflow_commands::CancelWorkflowExecution,
    temporal::api::enums::v1::{CommandType, EventType},
};

fsm! {
    pub(super)
    name CancelWorkflowMachine;
    command CancelWorkflowCommand;
    error WFMachinesError;

    Created --(Schedule, on_schedule) --> CancelWorkflowCommandCreated;

    CancelWorkflowCommandCreated --(CommandCancelWorkflowExecution)
        --> CancelWorkflowCommandCreated;
    CancelWorkflowCommandCreated --(WorkflowExecutionCanceled)
        --> CancelWorkflowCommandRecorded;
}

#[derive(Debug, derive_more::Display)]
pub(super) enum CancelWorkflowCommand {}

pub(super) fn cancel_workflow(attribs: CancelWorkflowExecution) -> NewMachineWithCommand {
    let mut machine = CancelWorkflowMachine::from_parts(Created {}.into(), ());
    OnEventWrapper::on_event_mut(&mut machine, CancelWorkflowMachineEvents::Schedule)
        .expect("Scheduling continue as new machine doesn't fail");
    NewMachineWithCommand {
        command: attribs.into(),
        machine: machine.into(),
    }
}

#[derive(Default, Clone)]
pub(super) struct CancelWorkflowCommandCreated {}

#[derive(Default, Clone)]
pub(super) struct CancelWorkflowCommandRecorded {}

impl From<CancelWorkflowCommandCreated> for CancelWorkflowCommandRecorded {
    fn from(_: CancelWorkflowCommandCreated) -> Self {
        Self::default()
    }
}

#[derive(Default, Clone)]
pub(super) struct Created {}

impl Created {
    pub(super) fn on_schedule(
        self,
    ) -> CancelWorkflowMachineTransition<CancelWorkflowCommandCreated> {
        TransitionResult::default()
    }
}

impl TryFrom<HistEventData> for CancelWorkflowMachineEvents {
    type Error = WFMachinesError;

    fn try_from(e: HistEventData) -> Result<Self, Self::Error> {
        let e = e.event;
        Ok(match EventType::try_from(e.event_type) {
            Ok(EventType::WorkflowExecutionCanceled) => Self::WorkflowExecutionCanceled,
            _ => {
                return Err(nondeterminism!(
                    "Cancel workflow machine does not handle this event: {e}"
                ));
            }
        })
    }
}

impl TryFrom<CommandType> for CancelWorkflowMachineEvents {
    type Error = ();

    fn try_from(c: CommandType) -> Result<Self, Self::Error> {
        Ok(match c {
            CommandType::CancelWorkflowExecution => Self::CommandCancelWorkflowExecution,
            _ => return Err(()),
        })
    }
}

impl WFMachinesAdapter for CancelWorkflowMachine {
    fn adapt_response(
        &self,
        _my_command: Self::Command,
        _event_info: Option<EventInfo>,
    ) -> Result<Vec<MachineResponse>, WFMachinesError> {
        Ok(vec![])
    }
}
