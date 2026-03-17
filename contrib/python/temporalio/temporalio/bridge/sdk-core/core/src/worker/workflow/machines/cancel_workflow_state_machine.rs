use super::{
    EventInfo, NewMachineWithCommand, OnEventWrapper, WFMachinesAdapter, WFMachinesError,
    workflow_machines::MachineResponse,
};
use crate::worker::workflow::machines::HistEventData;
use rustfsm::{StateMachine, TransitionResult, fsm};
use std::convert::TryFrom;
use temporal_sdk_core_protos::{
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
                return Err(WFMachinesError::Nondeterminism(format!(
                    "Cancel workflow machine does not handle this event: {e}"
                )));
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_help::{MockPollCfg, build_fake_sdk, canned_histories};
    use std::time::Duration;
    use temporal_sdk::{WfContext, WfExitValue, WorkflowResult};
    use temporal_sdk_core_protos::{
        DEFAULT_WORKFLOW_TYPE,
        coresdk::workflow_activation::{WorkflowActivationJob, workflow_activation_job},
    };
    use temporal_sdk_core_test_utils::interceptors::ActivationAssertionsInterceptor;

    async fn wf_with_timer(ctx: WfContext) -> WorkflowResult<()> {
        ctx.timer(Duration::from_millis(500)).await;
        Ok(WfExitValue::Cancelled)
    }

    #[tokio::test]
    async fn wf_completing_with_cancelled() {
        let t = canned_histories::timer_wf_cancel_req_cancelled("1");

        let mut aai = ActivationAssertionsInterceptor::default();
        aai.then(|a| {
            assert_matches!(
                a.jobs.as_slice(),
                [WorkflowActivationJob {
                    variant: Some(workflow_activation_job::Variant::InitializeWorkflow(_)),
                }]
            )
        });
        aai.then(|a| {
            assert_matches!(
                a.jobs.as_slice(),
                [
                    WorkflowActivationJob {
                        variant: Some(workflow_activation_job::Variant::FireTimer(_)),
                    },
                    WorkflowActivationJob {
                        variant: Some(workflow_activation_job::Variant::CancelWorkflow(_)),
                    }
                ]
            );
        });

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
                        CommandType::CancelWorkflowExecution
                    );
                });
        });

        let mut worker = build_fake_sdk(mock_cfg);
        worker.register_wf(DEFAULT_WORKFLOW_TYPE, wf_with_timer);
        worker.set_worker_interceptor(aai);
        worker.run().await.unwrap();
    }
}
