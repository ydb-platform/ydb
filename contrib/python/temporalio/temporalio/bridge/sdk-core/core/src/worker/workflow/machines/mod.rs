mod workflow_machines;

mod activity_state_machine;
mod cancel_external_state_machine;
// This machine is kept commented out until cancelling externally started nexus operations is
// supported
// mod cancel_nexus_op_state_machine;
mod cancel_workflow_state_machine;
mod child_workflow_state_machine;
mod complete_workflow_state_machine;
mod continue_as_new_workflow_state_machine;
mod fail_workflow_state_machine;
mod local_activity_state_machine;
mod modify_workflow_properties_state_machine;
mod nexus_operation_state_machine;
mod patch_state_machine;
mod signal_external_state_machine;
mod timer_state_machine;
mod update_state_machine;
mod upsert_search_attributes_state_machine;
mod workflow_task_state_machine;

#[cfg(test)]
mod transition_coverage;

pub(crate) use workflow_machines::{MachinesWFTResponseContent, WorkflowMachines};

use crate::{telemetry::VecDisplayer, worker::workflow::WFMachinesError};
use activity_state_machine::ActivityMachine;
use cancel_external_state_machine::CancelExternalMachine;
use cancel_workflow_state_machine::CancelWorkflowMachine;
use child_workflow_state_machine::ChildWorkflowMachine;
use complete_workflow_state_machine::CompleteWorkflowMachine;
use continue_as_new_workflow_state_machine::ContinueAsNewWorkflowMachine;
use fail_workflow_state_machine::FailWorkflowMachine;
use local_activity_state_machine::LocalActivityMachine;
use modify_workflow_properties_state_machine::ModifyWorkflowPropertiesMachine;
use nexus_operation_state_machine::NexusOperationMachine;
use patch_state_machine::PatchMachine;
use rustfsm::{MachineError, StateMachine};
use signal_external_state_machine::SignalExternalMachine;
use std::{
    convert::{TryFrom, TryInto},
    fmt::{Debug, Display},
};
use temporal_sdk_core_protos::temporal::api::{
    enums::v1::{CommandType, EventType},
    history::v1::HistoryEvent,
};
use timer_state_machine::TimerMachine;
use update_state_machine::UpdateMachine;
use upsert_search_attributes_state_machine::UpsertSearchAttributesMachine;
use workflow_machines::MachineResponse;
use workflow_task_state_machine::WorkflowTaskMachine;

#[cfg(test)]
use transition_coverage::add_coverage;

#[enum_dispatch::enum_dispatch]
#[allow(clippy::enum_variant_names, clippy::large_enum_variant)]
enum Machines {
    ActivityMachine,
    CancelExternalMachine,
    CancelWorkflowMachine,
    ChildWorkflowMachine,
    CompleteWorkflowMachine,
    ContinueAsNewWorkflowMachine,
    FailWorkflowMachine,
    LocalActivityMachine,
    PatchMachine,
    SignalExternalMachine,
    TimerMachine,
    WorkflowTaskMachine,
    UpsertSearchAttributesMachine,
    ModifyWorkflowPropertiesMachine,
    UpdateMachine,
    NexusOperationMachine,
}

/// Extends [rustfsm::StateMachine] with some functionality specific to the temporal SDK.
///
/// Formerly known as `EntityStateMachine` in Java.
#[enum_dispatch::enum_dispatch(Machines)]
trait TemporalStateMachine {
    fn handle_command(
        &mut self,
        command_type: CommandType,
    ) -> Result<Vec<MachineResponse>, WFMachinesError>;

    /// Tell the state machine to handle some event. Returns a list of responses that can be used
    /// to update the overall state of the workflow. EX: To issue outgoing WF activations.
    fn handle_event(
        &mut self,
        event: HistEventData,
    ) -> Result<Vec<MachineResponse>, WFMachinesError>;

    /// Returns true if the state machine is in a final state
    fn is_final_state(&self) -> bool;

    /// Returns a friendly name for the type of this machine
    fn name(&self) -> &str;
}

impl<SM> TemporalStateMachine for SM
where
    SM: StateMachine + WFMachinesAdapter + OnEventWrapper + Clone + 'static,
    <SM as StateMachine>::Event: TryFrom<HistEventData> + TryFrom<CommandType> + Display,
    WFMachinesError: From<<<SM as StateMachine>::Event as TryFrom<HistEventData>>::Error>,
    <SM as StateMachine>::Command: Debug + Display,
    <SM as StateMachine>::State: Display,
    <SM as StateMachine>::Error: Into<WFMachinesError> + 'static + Send + Sync,
{
    fn handle_command(
        &mut self,
        command_type: CommandType,
    ) -> Result<Vec<MachineResponse>, WFMachinesError> {
        debug!(
            command_type = ?command_type,
            machine_name = %self.name(),
            state = %self.state(),
            "handling command"
        );
        if let Ok(converted_command) = command_type.try_into() {
            match OnEventWrapper::on_event_mut(self, converted_command) {
                Ok(c) => process_machine_commands(self, c, None),
                Err(MachineError::InvalidTransition) => {
                    Err(WFMachinesError::Nondeterminism(format!(
                        "Unexpected command producing an invalid transition {:?} in state {}",
                        command_type,
                        self.state()
                    )))
                }
                Err(MachineError::Underlying(e)) => Err(e.into()),
            }
        } else {
            Err(WFMachinesError::Nondeterminism(format!(
                "Unexpected command {:?} generated by a {:?} machine",
                command_type,
                self.name()
            )))
        }
    }

    fn handle_event(
        &mut self,
        event_dat: HistEventData,
    ) -> Result<Vec<MachineResponse>, WFMachinesError> {
        trace!(
            event = %event_dat.event,
            machine_name = %self.name(),
            state = %self.state(),
            "handling event"
        );
        let event_info = EventInfo {
            event_id: event_dat.event.event_id,
            event_type: event_dat.event.event_type(),
        };
        let converted_event: <Self as StateMachine>::Event = event_dat.try_into()?;

        match OnEventWrapper::on_event_mut(self, converted_event) {
            Ok(c) => process_machine_commands(self, c, Some(event_info)),
            Err(MachineError::InvalidTransition) => Err(WFMachinesError::Fatal(format!(
                "{} in state {} says the transition is invalid during event {:?}",
                self.name(),
                self.state(),
                event_info
            ))),
            Err(MachineError::Underlying(e)) => Err(e.into()),
        }
    }

    fn is_final_state(&self) -> bool {
        self.has_reached_final_state()
    }

    fn name(&self) -> &str {
        self.name()
    }
}

impl Machines {
    /// Should return true if the command was cancelled before we sent it to the server. Always
    /// returns false for non-cancellable machines
    fn was_cancelled_before_sent_to_server(&self) -> bool {
        match self {
            Machines::ActivityMachine(m) => m.was_cancelled_before_sent_to_server(),
            Machines::ChildWorkflowMachine(m) => m.was_cancelled_before_sent_to_server(),
            Machines::LocalActivityMachine(m) => m.was_cancelled_before_sent_to_server(),
            Machines::SignalExternalMachine(m) => m.was_cancelled_before_sent_to_server(),
            Machines::TimerMachine(m) => m.was_cancelled_before_sent_to_server(),
            Machines::NexusOperationMachine(m) => m.was_cancelled_before_sent_to_server(),
            _ => false,
        }
    }
}

fn process_machine_commands<SM>(
    machine: &mut SM,
    commands: Vec<SM::Command>,
    event_info: Option<EventInfo>,
) -> Result<Vec<MachineResponse>, WFMachinesError>
where
    SM: TemporalStateMachine + StateMachine + WFMachinesAdapter,
    <SM as StateMachine>::Event: Display,
    <SM as StateMachine>::Command: Debug + Display,
    <SM as StateMachine>::State: Display,
{
    if !commands.is_empty() {
        trace!(commands=%commands.display(), state=%machine.state(),
               machine_name=%TemporalStateMachine::name(machine), "Machine produced commands");
    }
    let mut machine_responses = vec![];
    for cmd in commands {
        machine_responses.extend(machine.adapt_response(cmd, event_info)?);
    }
    Ok(machine_responses)
}

/// This trait exists to bridge [StateMachine]s and the [WorkflowMachines] instance. It has access
/// to the machine's concrete types while hiding those details from [WorkflowMachines]
trait WFMachinesAdapter: StateMachine {
    /// A command that this [StateMachine] instance just produced, and maybe the event being
    /// processed, perform any handling that needs inform the [WorkflowMachines] instance of some
    /// action to be taken in response to that command.
    fn adapt_response(
        &self,
        my_command: Self::Command,
        event_info: Option<EventInfo>,
    ) -> Result<Vec<MachineResponse>, WFMachinesError>;
}

/// Wraps a history event with extra relevant data that a machine might care about while the event
/// is being applied to it.
#[derive(Debug, derive_more::Display)]
#[display("{event}")]
struct HistEventData {
    event: HistoryEvent,
    /// Is the current workflow task under replay or not during application of this event?
    replaying: bool,
    /// Is the current workflow task the last task in history?
    current_task_is_last_in_history: bool,
}

#[derive(Debug, Copy, Clone)]
struct EventInfo {
    event_id: i64,
    event_type: EventType,
}

/// We need to wrap calls to [StateMachine::on_event] to track coverage, or anything else
/// we'd like to do on every call.
pub(crate) trait OnEventWrapper: StateMachine
where
    <Self as StateMachine>::State: Display,
    <Self as StateMachine>::Event: Display,
    Self: Clone,
{
    fn on_event_mut(
        &mut self,
        event: Self::Event,
    ) -> Result<Vec<Self::Command>, MachineError<Self::Error>> {
        #[cfg(test)]
        let from_state = self.state().to_string();
        #[cfg(test)]
        let converted_event_str = event.to_string();

        let res = StateMachine::on_event(self, event);
        if res.is_ok() {
            #[cfg(test)]
            add_coverage(
                self.name().to_owned(),
                from_state,
                self.state().to_string(),
                converted_event_str,
            );
        }
        res
    }
}

impl<SM> OnEventWrapper for SM
where
    SM: StateMachine,
    <Self as StateMachine>::State: Display,
    <Self as StateMachine>::Event: Display,
    Self: Clone,
{
}

struct NewMachineWithCommand {
    command: temporal_sdk_core_protos::temporal::api::command::v1::command::Attributes,
    machine: Machines,
}

struct NewMachineWithResponse {
    machine: Machines,
    response: MachineResponse,
}
