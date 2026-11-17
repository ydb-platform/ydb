use super::{
    NewMachineWithCommand, StateMachine, TransitionResult, fsm, workflow_machines::MachineResponse,
};
use crate::worker::workflow::{
    WFMachinesError,
    machines::{EventInfo, HistEventData, WFMachinesAdapter},
};
use temporalio_common::protos::{
    coresdk::workflow_commands::ModifyWorkflowProperties,
    temporal::api::enums::v1::{CommandType, EventType},
};

fsm! {
    pub(super) name ModifyWorkflowPropertiesMachine;
    command ModifyWorkflowPropertiesMachineCommand;
    error WFMachinesError;
    shared_state SharedState;

    Created --(CommandScheduled) --> CommandIssued;
    CommandIssued --(CommandRecorded) --> Done;
}

/// Instantiates an ModifyWorkflowPropertiesMachine and packs it together with the command to
/// be sent to server.
pub(super) fn modify_workflow_properties(
    lang_cmd: ModifyWorkflowProperties,
) -> NewMachineWithCommand {
    let sm = ModifyWorkflowPropertiesMachine::from_parts(Created {}.into(), ());
    NewMachineWithCommand {
        command: lang_cmd.into(),
        machine: sm.into(),
    }
}

type SharedState = ();

#[derive(Debug, derive_more::Display)]
pub(super) enum ModifyWorkflowPropertiesMachineCommand {}

#[derive(Debug, Default, Clone, derive_more::Display)]
pub(super) struct Created {}

#[derive(Debug, Default, Clone, derive_more::Display)]
pub(super) struct CommandIssued {}

#[derive(Debug, Default, Clone, derive_more::Display)]
pub(super) struct Done {}

impl WFMachinesAdapter for ModifyWorkflowPropertiesMachine {
    fn adapt_response(
        &self,
        _my_command: Self::Command,
        _event_info: Option<EventInfo>,
    ) -> Result<Vec<MachineResponse>, Self::Error> {
        Err(Self::Error::Nondeterminism(
            "ModifyWorkflowProperties does not use state machine commands".to_string(),
        ))
    }
}

impl TryFrom<HistEventData> for ModifyWorkflowPropertiesMachineEvents {
    type Error = WFMachinesError;

    fn try_from(e: HistEventData) -> Result<Self, Self::Error> {
        let e = e.event;
        match e.event_type() {
            EventType::WorkflowPropertiesModified => {
                Ok(ModifyWorkflowPropertiesMachineEvents::CommandRecorded)
            }
            _ => Err(Self::Error::Nondeterminism(format!(
                "ModifyWorkflowPropertiesMachine does not handle {e}"
            ))),
        }
    }
}

impl TryFrom<CommandType> for ModifyWorkflowPropertiesMachineEvents {
    type Error = WFMachinesError;

    fn try_from(c: CommandType) -> Result<Self, Self::Error> {
        match c {
            CommandType::ModifyWorkflowProperties => {
                Ok(ModifyWorkflowPropertiesMachineEvents::CommandScheduled)
            }
            _ => Err(Self::Error::Nondeterminism(format!(
                "ModifyWorkflowPropertiesMachine does not handle command type {c:?}"
            ))),
        }
    }
}

impl From<CommandIssued> for Done {
    fn from(_: CommandIssued) -> Self {
        Self {}
    }
}

impl From<Created> for CommandIssued {
    fn from(_: Created) -> Self {
        Self {}
    }
}
