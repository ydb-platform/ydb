use super::{
    EventInfo, NewMachineWithCommand, OnEventWrapper, StateMachine, TransitionResult,
    WFMachinesAdapter, WFMachinesError, fsm, workflow_machines::MachineResponse,
};
use crate::worker::workflow::{fatal, machines::HistEventData, nondeterminism};
use std::convert::TryFrom;
use temporalio_common::protos::{
    coresdk::{
        common::NamespacedWorkflowExecution,
        workflow_activation::ResolveRequestCancelExternalWorkflow,
    },
    temporal::api::{
        command::v1::{RequestCancelExternalWorkflowExecutionCommandAttributes, command},
        enums::v1::{CancelExternalWorkflowExecutionFailedCause, CommandType, EventType},
        failure::v1::{ApplicationFailureInfo, Failure, failure::FailureInfo},
        history::v1::history_event,
    },
};

fsm! {
    pub(super)
    name CancelExternalMachine;
    command CancelExternalCommand;
    error WFMachinesError;
    shared_state SharedState;

    Created --(Schedule, on_schedule) --> RequestCancelExternalCommandCreated;

    RequestCancelExternalCommandCreated --(CommandRequestCancelExternalWorkflowExecution)
      --> RequestCancelExternalCommandCreated;
    RequestCancelExternalCommandCreated
      --(RequestCancelExternalWorkflowExecutionInitiated,
         on_request_cancel_external_workflow_execution_initiated)
      --> RequestCancelExternalCommandRecorded;

    RequestCancelExternalCommandRecorded
      --(ExternalWorkflowExecutionCancelRequested, on_external_workflow_execution_cancel_requested)
      --> CancelRequested;
    RequestCancelExternalCommandRecorded
      --(RequestCancelExternalWorkflowExecutionFailed(CancelExternalWorkflowExecutionFailedCause),
         on_request_cancel_external_workflow_execution_failed) --> RequestCancelFailed;
}

#[derive(Default, Clone)]
pub(super) struct SharedState {
    seq: u32,
}

#[derive(Debug, derive_more::Display)]
pub(super) enum CancelExternalCommand {
    /// The target workflow has been notified of the cancel
    Requested,
    #[display("Failed")]
    Failed(CancelExternalWorkflowExecutionFailedCause),
}

pub(super) fn new_external_cancel(
    seq: u32,
    workflow_execution: NamespacedWorkflowExecution,
    only_child: bool,
    reason: String,
) -> NewMachineWithCommand {
    let mut s = CancelExternalMachine::from_parts(Created {}.into(), SharedState { seq });
    OnEventWrapper::on_event_mut(&mut s, CancelExternalMachineEvents::Schedule)
        .expect("Scheduling cancel external wf command doesn't fail");
    #[allow(deprecated)]
    let cmd_attrs = command::Attributes::RequestCancelExternalWorkflowExecutionCommandAttributes(
        RequestCancelExternalWorkflowExecutionCommandAttributes {
            namespace: workflow_execution.namespace,
            workflow_id: workflow_execution.workflow_id,
            run_id: workflow_execution.run_id,
            child_workflow_only: only_child,
            reason,
            ..Default::default()
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
pub(super) struct Created {}

impl Created {
    pub(super) fn on_schedule(
        self,
    ) -> CancelExternalMachineTransition<RequestCancelExternalCommandCreated> {
        TransitionResult::default()
    }
}

#[derive(Default, Clone)]
pub(super) struct RequestCancelExternalCommandCreated {}

impl RequestCancelExternalCommandCreated {
    pub(super) fn on_request_cancel_external_workflow_execution_initiated(
        self,
    ) -> CancelExternalMachineTransition<RequestCancelExternalCommandRecorded> {
        TransitionResult::default()
    }
}

#[derive(Default, Clone)]
pub(super) struct RequestCancelExternalCommandRecorded {}

impl RequestCancelExternalCommandRecorded {
    pub(super) fn on_external_workflow_execution_cancel_requested(
        self,
    ) -> CancelExternalMachineTransition<CancelRequested> {
        TransitionResult::commands(vec![CancelExternalCommand::Requested])
    }
    pub(super) fn on_request_cancel_external_workflow_execution_failed(
        self,
        cause: CancelExternalWorkflowExecutionFailedCause,
    ) -> CancelExternalMachineTransition<RequestCancelFailed> {
        TransitionResult::commands(vec![CancelExternalCommand::Failed(cause)])
    }
}

#[derive(Default, Clone)]
pub(super) struct RequestCancelFailed {}

impl TryFrom<CommandType> for CancelExternalMachineEvents {
    type Error = ();

    fn try_from(c: CommandType) -> Result<Self, Self::Error> {
        match c {
            CommandType::RequestCancelExternalWorkflowExecution => {
                Ok(Self::CommandRequestCancelExternalWorkflowExecution)
            }
            _ => Err(()),
        }
    }
}

impl TryFrom<HistEventData> for CancelExternalMachineEvents {
    type Error = WFMachinesError;

    fn try_from(e: HistEventData) -> Result<Self, Self::Error> {
        let e = e.event;
        Ok(match e.event_type() {
            EventType::ExternalWorkflowExecutionCancelRequested => {
                Self::ExternalWorkflowExecutionCancelRequested
            }
            EventType::RequestCancelExternalWorkflowExecutionInitiated => {
                Self::RequestCancelExternalWorkflowExecutionInitiated
            }
            EventType::RequestCancelExternalWorkflowExecutionFailed => {
                if let Some(history_event::Attributes::RequestCancelExternalWorkflowExecutionFailedEventAttributes(attrs)) = e.attributes {
                    Self::RequestCancelExternalWorkflowExecutionFailed(attrs.cause())
                } else {
                    return Err(fatal!(
                        "Cancelworkflow failed attributes were unset: {e}"
                    ));
                }
            }
            _ => {
                return Err(nondeterminism!(
                    "Cancel external WF machine does not handle this event: {e}"
                ))
            }
        })
    }
}

impl WFMachinesAdapter for CancelExternalMachine {
    fn adapt_response(
        &self,
        my_command: Self::Command,
        _event_info: Option<EventInfo>,
    ) -> Result<Vec<MachineResponse>, WFMachinesError> {
        Ok(match my_command {
            CancelExternalCommand::Requested => {
                vec![
                    ResolveRequestCancelExternalWorkflow {
                        seq: self.shared_state.seq,
                        failure: None,
                    }
                    .into(),
                ]
            }
            CancelExternalCommand::Failed(f) => {
                let reason = match f {
                    CancelExternalWorkflowExecutionFailedCause::Unspecified => "unknown",
                    CancelExternalWorkflowExecutionFailedCause::ExternalWorkflowExecutionNotFound
                    | CancelExternalWorkflowExecutionFailedCause::NamespaceNotFound  => "not found"
                };
                vec![
                    ResolveRequestCancelExternalWorkflow {
                        seq: self.shared_state.seq,
                        failure: Some(Failure {
                            message: format!("Unable to cancel external workflow because {reason}"),
                            failure_info: Some(FailureInfo::ApplicationFailureInfo(
                                ApplicationFailureInfo {
                                    r#type: f.to_string(),
                                    ..Default::default()
                                },
                            )),
                            ..Default::default()
                        }),
                    }
                    .into(),
                ]
            }
        })
    }
}
