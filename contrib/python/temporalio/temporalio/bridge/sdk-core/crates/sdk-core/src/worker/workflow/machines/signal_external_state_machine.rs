use super::{
    EventInfo, MachineError, NewMachineWithCommand, OnEventWrapper, StateMachine, TransitionResult,
    WFMachinesAdapter, WFMachinesError, fsm, workflow_machines::MachineResponse,
};
use crate::worker::workflow::{fatal, machines::HistEventData, nondeterminism};
use std::convert::TryFrom;
use temporalio_common::protos::{
    coresdk::{
        IntoPayloadsExt,
        common::NamespacedWorkflowExecution,
        workflow_activation::ResolveSignalExternalWorkflow,
        workflow_commands::{
            SignalExternalWorkflowExecution, signal_external_workflow_execution as sig_we,
        },
    },
    temporal::api::{
        command::v1::{SignalExternalWorkflowExecutionCommandAttributes, command},
        common::v1::WorkflowExecution as UpstreamWE,
        enums::v1::{CommandType, EventType, SignalExternalWorkflowExecutionFailedCause},
        failure::v1::{ApplicationFailureInfo, CanceledFailureInfo, Failure, failure::FailureInfo},
        history::v1::history_event,
    },
};

const SIG_CANCEL_MSG: &str = "Signal was cancelled before being sent";

fsm! {
    pub(super) name SignalExternalMachine;
    command SignalExternalCommand;
    error WFMachinesError;
    shared_state SharedState;

    Created --(Schedule, on_schedule) --> SignalExternalCommandCreated;

    SignalExternalCommandCreated --(CommandSignalExternalWorkflowExecution)
        --> SignalExternalCommandCreated;
    SignalExternalCommandCreated --(Cancel, on_cancel) --> Cancelled;
    SignalExternalCommandCreated
      --(SignalExternalWorkflowExecutionInitiated, on_signal_external_workflow_execution_initiated)
        --> SignalExternalCommandRecorded;

    SignalExternalCommandRecorded --(Cancel) --> SignalExternalCommandRecorded;
    SignalExternalCommandRecorded
      --(ExternalWorkflowExecutionSignaled, on_external_workflow_execution_signaled) --> Signaled;
    SignalExternalCommandRecorded
      --(SignalExternalWorkflowExecutionFailed(SignalExternalWorkflowExecutionFailedCause),
         on_signal_external_workflow_execution_failed) --> Failed;

    // Ignore any spurious cancellations after resolution
    Cancelled --(Cancel) --> Cancelled;
    Signaled --(Cancel) --> Signaled;
}

#[derive(Default, Clone)]
pub(super) struct SharedState {
    seq: u32,
}

#[derive(Debug, derive_more::Display)]
pub(super) enum SignalExternalCommand {
    Signaled,
    #[display("Failed")]
    Failed(SignalExternalWorkflowExecutionFailedCause),
    Cancelled,
}

pub(super) fn new_external_signal(
    attrs: SignalExternalWorkflowExecution,
    this_namespace: &str,
) -> Result<NewMachineWithCommand, WFMachinesError> {
    let (workflow_execution, only_child) = match attrs.target {
        None => {
            return Err(fatal!(
                "Signal external workflow command had empty target field"
            ));
        }
        Some(sig_we::Target::ChildWorkflowId(wfid)) => (
            NamespacedWorkflowExecution {
                namespace: this_namespace.to_string(),
                workflow_id: wfid,
                run_id: "".to_string(),
            },
            true,
        ),
        Some(sig_we::Target::WorkflowExecution(we)) => (we, false),
    };

    let mut s =
        SignalExternalMachine::from_parts(Created {}.into(), SharedState { seq: attrs.seq });
    OnEventWrapper::on_event_mut(&mut s, SignalExternalMachineEvents::Schedule)
        .expect("Scheduling signal external wf command doesn't fail");
    #[allow(deprecated)]
    let cmd_attrs = command::Attributes::SignalExternalWorkflowExecutionCommandAttributes(
        SignalExternalWorkflowExecutionCommandAttributes {
            namespace: workflow_execution.namespace,
            execution: Some(UpstreamWE {
                workflow_id: workflow_execution.workflow_id,
                run_id: workflow_execution.run_id,
            }),
            header: if attrs.headers.is_empty() {
                None
            } else {
                Some(attrs.headers.into())
            },
            signal_name: attrs.signal_name,
            input: attrs.args.into_payloads(),
            child_workflow_only: only_child,
            ..Default::default()
        },
    );
    Ok(NewMachineWithCommand {
        command: cmd_attrs,
        machine: s.into(),
    })
}

#[derive(Default, Clone)]
pub(super) struct Cancelled {}

#[derive(Default, Clone)]
pub(super) struct Created {}

impl Created {
    pub(super) fn on_schedule(
        self,
    ) -> SignalExternalMachineTransition<SignalExternalCommandCreated> {
        TransitionResult::default()
    }
}

#[derive(Default, Clone)]
pub(super) struct Failed {}

#[derive(Default, Clone)]
pub(super) struct SignalExternalCommandCreated {}

impl SignalExternalCommandCreated {
    pub(super) fn on_cancel(self) -> SignalExternalMachineTransition<Cancelled> {
        TransitionResult::commands(vec![SignalExternalCommand::Cancelled])
    }
    pub(super) fn on_signal_external_workflow_execution_initiated(
        self,
    ) -> SignalExternalMachineTransition<SignalExternalCommandRecorded> {
        TransitionResult::default()
    }
}

#[derive(Default, Clone)]
pub(super) struct SignalExternalCommandRecorded {}

impl SignalExternalCommandRecorded {
    pub(super) fn on_external_workflow_execution_signaled(
        self,
    ) -> SignalExternalMachineTransition<Signaled> {
        TransitionResult::commands(vec![SignalExternalCommand::Signaled])
    }
    pub(super) fn on_signal_external_workflow_execution_failed(
        self,
        cause: SignalExternalWorkflowExecutionFailedCause,
    ) -> SignalExternalMachineTransition<Failed> {
        TransitionResult::commands(vec![SignalExternalCommand::Failed(cause)])
    }
}

#[derive(Default, Clone)]
pub(super) struct Signaled {}

impl TryFrom<CommandType> for SignalExternalMachineEvents {
    type Error = ();

    fn try_from(c: CommandType) -> Result<Self, Self::Error> {
        Ok(match c {
            CommandType::SignalExternalWorkflowExecution => {
                Self::CommandSignalExternalWorkflowExecution
            }
            _ => return Err(()),
        })
    }
}
impl TryFrom<HistEventData> for SignalExternalMachineEvents {
    type Error = WFMachinesError;

    fn try_from(e: HistEventData) -> Result<Self, Self::Error> {
        let e = e.event;
        Ok(match e.event_type() {
            EventType::ExternalWorkflowExecutionSignaled => Self::ExternalWorkflowExecutionSignaled,
            EventType::SignalExternalWorkflowExecutionInitiated => {
                Self::SignalExternalWorkflowExecutionInitiated
            }
            EventType::SignalExternalWorkflowExecutionFailed => {
                if let Some(
                    history_event::Attributes::SignalExternalWorkflowExecutionFailedEventAttributes(
                        attrs,
                    ),
                ) = e.attributes
                {
                    Self::SignalExternalWorkflowExecutionFailed(attrs.cause())
                } else {
                    return Err(fatal!("Signal workflow failed attributes were unset: {e}"));
                }
            }
            _ => {
                return Err(nondeterminism!(
                    "Signal external WF machine does not handle this event: {e}"
                ));
            }
        })
    }
}

impl WFMachinesAdapter for SignalExternalMachine {
    fn adapt_response(
        &self,
        my_command: Self::Command,
        _event_info: Option<EventInfo>,
    ) -> Result<Vec<MachineResponse>, WFMachinesError> {
        Ok(match my_command {
            SignalExternalCommand::Signaled => {
                vec![
                    ResolveSignalExternalWorkflow {
                        seq: self.shared_state.seq,
                        failure: None,
                    }
                    .into(),
                ]
            }
            SignalExternalCommand::Failed(f) => {
                let reason = match f {
                    SignalExternalWorkflowExecutionFailedCause::Unspecified => "unknown",
                    SignalExternalWorkflowExecutionFailedCause::ExternalWorkflowExecutionNotFound
                    | SignalExternalWorkflowExecutionFailedCause::NamespaceNotFound =>
                        "it was not found",
                    SignalExternalWorkflowExecutionFailedCause::SignalCountLimitExceeded => {
                        "The per-workflow signal limit was exceeded"
                    }
                };
                vec![
                    ResolveSignalExternalWorkflow {
                        seq: self.shared_state.seq,
                        // TODO: Create new failure type upstream for this
                        failure: Some(Failure {
                            message: format!("Unable to signal external workflow because {reason}"),
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
            SignalExternalCommand::Cancelled => {
                panic!("Cancelled command not expected as part of non-cancel transition")
            }
        })
    }
}

impl SignalExternalMachine {
    pub(super) fn cancel(&mut self) -> Result<Vec<MachineResponse>, MachineError<WFMachinesError>> {
        let res = OnEventWrapper::on_event_mut(self, SignalExternalMachineEvents::Cancel)?;
        let mut ret = vec![];
        match res.first() {
            Some(SignalExternalCommand::Cancelled) => {
                ret = vec![
                    ResolveSignalExternalWorkflow {
                        seq: self.shared_state.seq,
                        failure: Some(Failure {
                            message: SIG_CANCEL_MSG.to_string(),
                            failure_info: Some(FailureInfo::CanceledFailureInfo(
                                CanceledFailureInfo {
                                    details: None,
                                    identity: Default::default(),
                                },
                            )),
                            ..Default::default()
                        }),
                    }
                    .into(),
                ];
            }
            Some(_) => panic!("Signal external machine cancel produced unexpected result"),
            None => (),
        };
        Ok(ret)
    }

    pub(super) fn was_cancelled_before_sent_to_server(&self) -> bool {
        // We are only ever in the cancelled state if cancelled before sent to server, there is no
        // after sent cancellation here.
        matches!(self.state(), SignalExternalMachineState::Cancelled(_))
    }
}
