#![allow(clippy::enum_variant_names)]

use super::{EventInfo, WFMachinesAdapter, WFMachinesError, workflow_machines::MachineResponse};
use crate::worker::workflow::machines::HistEventData;
use rustfsm::{StateMachine, TransitionResult, fsm};
use std::{
    convert::{TryFrom, TryInto},
    time::SystemTime,
};
use temporal_sdk_core_protos::temporal::api::{
    enums::v1::{CommandType, EventType, WorkflowTaskFailedCause},
    history::v1::history_event::Attributes::WorkflowTaskFailedEventAttributes,
};

fsm! {
    pub(super) name WorkflowTaskMachine;
    command WFTaskMachineCommand;
    error WFMachinesError;
    shared_state SharedState;

    Created --(WorkflowTaskScheduled) --> Scheduled;

    Scheduled --(WorkflowTaskStarted(WFTStartedDat), shared on_workflow_task_started) --> Started;
    Scheduled --(WorkflowTaskTimedOut) --> TimedOut;

    Started --(WorkflowTaskCompleted, on_workflow_task_completed) --> Completed;
    Started --(WorkflowTaskFailed(WFTFailedDat), on_workflow_task_failed) --> Failed;
    Started --(WorkflowTaskTimedOut) --> TimedOut;
}

impl WorkflowTaskMachine {
    pub(super) fn new(wf_task_started_event_id: i64) -> Self {
        Self::from_parts(
            Created {}.into(),
            SharedState {
                wf_task_started_event_id,
            },
        )
    }
}

#[derive(Debug, derive_more::Display)]
pub(super) enum WFTaskMachineCommand {
    /// Issued to (possibly) trigger the event loop
    #[display("WFTaskStartedTrigger")]
    WFTaskStartedTrigger {
        task_started_event_id: i64,
        time: SystemTime,
    },
    #[display("RunIdOnWorkflowResetUpdate({run_id})")]
    RunIdOnWorkflowResetUpdate { run_id: String },
}

impl WFMachinesAdapter for WorkflowTaskMachine {
    fn adapt_response(
        &self,
        my_command: WFTaskMachineCommand,
        event_info: Option<EventInfo>,
    ) -> Result<Vec<MachineResponse>, WFMachinesError> {
        match my_command {
            WFTaskMachineCommand::WFTaskStartedTrigger {
                task_started_event_id,
                time,
            } => {
                let (event_id, event_type) = if let Some(ei) = event_info {
                    (ei.event_id, ei.event_type)
                } else {
                    return Err(WFMachinesError::Fatal(
                        "WF Task machine should never issue a task started trigger \
                        command in response to non-history events"
                            .to_string(),
                    ));
                };

                let cur_event_past_or_at_start = event_id >= task_started_event_id;
                if event_type == EventType::WorkflowTaskStarted && !cur_event_past_or_at_start {
                    return Ok(vec![]);
                }
                Ok(vec![MachineResponse::TriggerWFTaskStarted {
                    task_started_event_id,
                    time,
                }])
            }
            WFTaskMachineCommand::RunIdOnWorkflowResetUpdate { run_id } => {
                Ok(vec![MachineResponse::UpdateRunIdOnWorkflowReset { run_id }])
            }
        }
    }
}

impl TryFrom<HistEventData> for WorkflowTaskMachineEvents {
    type Error = WFMachinesError;

    fn try_from(e: HistEventData) -> Result<Self, Self::Error> {
        let e = e.event;
        Ok(match e.event_type() {
            EventType::WorkflowTaskScheduled => Self::WorkflowTaskScheduled,
            EventType::WorkflowTaskStarted => Self::WorkflowTaskStarted({
                let time = if let Some(time) = e.event_time {
                    match time.try_into() {
                        Ok(t) => t,
                        Err(_) => {
                            return Err(WFMachinesError::Fatal(
                                "Workflow task started event timestamp was inconvertible"
                                    .to_string(),
                            ));
                        }
                    }
                } else {
                    return Err(WFMachinesError::Fatal(format!(
                        "Workflow task started event must contain timestamp: {e}"
                    )));
                };
                WFTStartedDat {
                    started_event_id: e.event_id,
                    current_time_millis: time,
                }
            }),
            EventType::WorkflowTaskTimedOut => Self::WorkflowTaskTimedOut,
            EventType::WorkflowTaskCompleted => Self::WorkflowTaskCompleted,
            EventType::WorkflowTaskFailed => {
                if let Some(attributes) = e.attributes {
                    Self::WorkflowTaskFailed(WFTFailedDat {
                        new_run_id: match attributes {
                            WorkflowTaskFailedEventAttributes(a) => {
                                let cause = WorkflowTaskFailedCause::try_from(a.cause);
                                match cause {
                                    Ok(WorkflowTaskFailedCause::ResetWorkflow) => {
                                        Some(a.new_run_id)
                                    }
                                    _ => None,
                                }
                            }
                            _ => None,
                        },
                    })
                } else {
                    return Err(WFMachinesError::Fatal(format!(
                        "Workflow task failed is missing attributes: {e}"
                    )));
                }
            }
            _ => {
                return Err(WFMachinesError::Nondeterminism(format!(
                    "Event does not apply to a wf task machine: {e}"
                )));
            }
        })
    }
}

impl TryFrom<CommandType> for WorkflowTaskMachineEvents {
    type Error = ();

    fn try_from(_: CommandType) -> Result<Self, Self::Error> {
        Err(())
    }
}

#[derive(Debug, Clone)]
pub(super) struct SharedState {
    wf_task_started_event_id: i64,
}

#[derive(Default, Clone)]
pub(super) struct Completed {}

#[derive(Default, Clone)]
pub(super) struct Created {}

#[derive(Default, Clone)]
pub(super) struct Failed {}

#[derive(Default, Clone)]
pub(super) struct Scheduled {}

pub(super) struct WFTStartedDat {
    current_time_millis: SystemTime,
    started_event_id: i64,
}

pub(super) struct WFTFailedDat {
    new_run_id: Option<String>,
}

impl Scheduled {
    pub(super) fn on_workflow_task_started(
        self,
        shared: &mut SharedState,
        WFTStartedDat {
            current_time_millis,
            started_event_id,
        }: WFTStartedDat,
    ) -> WorkflowTaskMachineTransition<Started> {
        TransitionResult::ok(
            vec![WFTaskMachineCommand::WFTaskStartedTrigger {
                task_started_event_id: shared.wf_task_started_event_id,
                time: current_time_millis,
            }],
            Started {
                current_time_millis,
                started_event_id,
            },
        )
    }
}

impl From<Created> for Scheduled {
    fn from(_: Created) -> Self {
        Self::default()
    }
}

#[derive(Clone)]
pub(super) struct Started {
    /// Started event's timestamp
    current_time_millis: SystemTime,
    /// Started event's id
    started_event_id: i64,
}

impl Started {
    pub(super) fn on_workflow_task_completed(self) -> WorkflowTaskMachineTransition<Completed> {
        TransitionResult::commands(vec![WFTaskMachineCommand::WFTaskStartedTrigger {
            task_started_event_id: self.started_event_id,
            time: self.current_time_millis,
        }])
    }
    pub(super) fn on_workflow_task_failed(
        self,
        data: WFTFailedDat,
    ) -> WorkflowTaskMachineTransition<Failed> {
        let commands = match data.new_run_id {
            Some(run_id) => vec![WFTaskMachineCommand::RunIdOnWorkflowResetUpdate { run_id }],
            None => vec![],
        };
        TransitionResult::commands(commands)
    }
}

#[derive(Default, Clone)]
pub(super) struct TimedOut {}

impl From<Scheduled> for TimedOut {
    fn from(_: Scheduled) -> Self {
        Self::default()
    }
}

impl From<Started> for TimedOut {
    fn from(_: Started) -> Self {
        Self::default()
    }
}
