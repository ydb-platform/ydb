use super::{
    EventInfo, NewMachineWithCommand, OnEventWrapper, WFMachinesAdapter, WFMachinesError,
    workflow_machines::MachineResponse,
};
use crate::worker::workflow::machines::HistEventData;
use rustfsm::{StateMachine, TransitionResult, fsm};
use std::convert::TryFrom;
use temporal_sdk_core_protos::{
    coresdk::workflow_commands::CompleteWorkflowExecution,
    temporal::api::{
        command::v1::command,
        enums::v1::{CommandType, EventType},
    },
};

fsm! {
    pub(super)
    name CompleteWorkflowMachine;
    command CompleteWFCommand;
    error WFMachinesError;
    shared_state ();

    Created --(Schedule, on_schedule) --> CompleteWorkflowCommandCreated;

    CompleteWorkflowCommandCreated --(CommandCompleteWorkflowExecution)
        --> CompleteWorkflowCommandCreated;
    CompleteWorkflowCommandCreated --(WorkflowExecutionCompleted)
        --> CompleteWorkflowCommandRecorded;
}

#[derive(Debug, derive_more::Display)]
pub(super) enum CompleteWFCommand {
    AddCommand(command::Attributes),
}

/// Complete a workflow
pub(super) fn complete_workflow(attribs: CompleteWorkflowExecution) -> NewMachineWithCommand {
    let mut machine = CompleteWorkflowMachine::from_parts(Created { attribs }.into(), ());
    let add_cmd =
        match OnEventWrapper::on_event_mut(&mut machine, CompleteWorkflowMachineEvents::Schedule)
            .expect("Scheduling complete wf machines doesn't fail")
            .pop()
        {
            Some(CompleteWFCommand::AddCommand(c)) => c,
            _ => panic!("complete wf machine on_schedule must produce command"),
        };
    NewMachineWithCommand {
        command: add_cmd,
        machine: machine.into(),
    }
}

impl TryFrom<HistEventData> for CompleteWorkflowMachineEvents {
    type Error = WFMachinesError;

    fn try_from(e: HistEventData) -> Result<Self, Self::Error> {
        let e = e.event;
        Ok(match e.event_type() {
            EventType::WorkflowExecutionCompleted => Self::WorkflowExecutionCompleted,
            _ => {
                return Err(WFMachinesError::Nondeterminism(format!(
                    "Complete workflow machine does not handle this event: {e}"
                )));
            }
        })
    }
}

impl TryFrom<CommandType> for CompleteWorkflowMachineEvents {
    type Error = ();

    fn try_from(c: CommandType) -> Result<Self, Self::Error> {
        Ok(match c {
            CommandType::CompleteWorkflowExecution => Self::CommandCompleteWorkflowExecution,
            _ => return Err(()),
        })
    }
}

#[derive(Default, Clone)]
pub(super) struct Created {
    attribs: CompleteWorkflowExecution,
}

impl Created {
    pub(super) fn on_schedule(
        self,
    ) -> CompleteWorkflowMachineTransition<CompleteWorkflowCommandCreated> {
        TransitionResult::commands(vec![CompleteWFCommand::AddCommand(self.attribs.into())])
    }
}

#[derive(Default, Clone)]
pub(super) struct CompleteWorkflowCommandCreated {}

#[derive(Default, Clone)]
pub(super) struct CompleteWorkflowCommandRecorded {}

impl From<CompleteWorkflowCommandCreated> for CompleteWorkflowCommandRecorded {
    fn from(_: CompleteWorkflowCommandCreated) -> Self {
        Self::default()
    }
}

impl WFMachinesAdapter for CompleteWorkflowMachine {
    fn adapt_response(
        &self,
        _my_command: Self::Command,
        _event_info: Option<EventInfo>,
    ) -> Result<Vec<MachineResponse>, WFMachinesError> {
        Ok(vec![])
    }
}
