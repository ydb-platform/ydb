use super::{
    workflow_machines::MachineResponse, Cancellable, EventInfo, NewMachineWithCommand,
    WFMachinesAdapter, WFMachinesError,
};
use crate::worker::workflow::machines::HistEventData;
use rustfsm::{fsm, StateMachine, TransitionResult};
use std::convert::TryFrom;
use temporal_sdk_core_protos::{
    coresdk::workflow_activation::ResolveCancelNexusOperation,
    temporal::api::{
        command::v1::{command, RequestCancelNexusOperationCommandAttributes},
        enums::v1::{CommandType, EventType},
    },
};

fsm! {
    pub(super)
    name CancelNexusOpMachine;
    command CancelNexusOpCommand;
    error WFMachinesError;
    shared_state SharedState;

    RequestCancelNexusOpCommandCreated --(CommandRequestCancelNexusOpWorkflowExecution)
      --> RequestCancelNexusOpCommandCreated;

    RequestCancelNexusOpCommandCreated --(NexusOpCancelRequested, on_cancel_requested)
      --> CancelRequested;
}

#[derive(Default, Clone)]
pub(super) struct SharedState {
    seq: u32,
}

#[derive(Debug, derive_more::Display)]
pub(super) enum CancelNexusOpCommand {
    Requested,
}

pub(super) fn new_nexus_op_cancel(
    seq: u32,
    nexus_op_scheduled_event_id: i64,
) -> NewMachineWithCommand {
    let s = CancelNexusOpMachine::from_parts(
        RequestCancelNexusOpCommandCreated {}.into(),
        SharedState { seq },
    );
    let cmd_attrs = command::Attributes::RequestCancelNexusOperationCommandAttributes(
        RequestCancelNexusOperationCommandAttributes {
            scheduled_event_id: nexus_op_scheduled_event_id,
        },
    );
    NewMachineWithCommand {
        command: cmd_attrs,
        machine: s.into(),
    }
}

#[derive(Default, Clone)]
pub(super) struct CancelRequested {}

#[derive(Default, Clone)]
pub(super) struct RequestCancelNexusOpCommandCreated {}

impl RequestCancelNexusOpCommandCreated {
    pub(super) fn on_cancel_requested(self) -> CancelNexusOpMachineTransition<CancelRequested> {
        TransitionResult::commands(vec![CancelNexusOpCommand::Requested])
    }
}

impl TryFrom<HistEventData> for CancelNexusOpMachineEvents {
    type Error = WFMachinesError;

    fn try_from(e: HistEventData) -> Result<Self, Self::Error> {
        let e = e.event;
        Ok(match e.event_type() {
            EventType::NexusOperationCancelRequested => Self::NexusOpCancelRequested,
            _ => {
                return Err(WFMachinesError::Nondeterminism(format!(
                    "Cancel external WF machine does not handle this event: {e}"
                )))
            }
        })
    }
}

impl TryFrom<CommandType> for CancelNexusOpMachineEvents {
    type Error = ();

    fn try_from(c: CommandType) -> Result<Self, Self::Error> {
        Ok(match c {
            CommandType::RequestCancelNexusOperation => {
                Self::CommandRequestCancelNexusOpWorkflowExecution
            }
            _ => return Err(()),
        })
    }
}

impl WFMachinesAdapter for CancelNexusOpMachine {
    fn adapt_response(
        &self,
        my_command: Self::Command,
        _event_info: Option<EventInfo>,
    ) -> Result<Vec<MachineResponse>, WFMachinesError> {
        Ok(match my_command {
            CancelNexusOpCommand::Requested => {
                vec![ResolveCancelNexusOperation {
                    seq: self.shared_state.seq,
                }
                .into()]
            }
        })
    }
}

impl Cancellable for CancelNexusOpMachine {}
