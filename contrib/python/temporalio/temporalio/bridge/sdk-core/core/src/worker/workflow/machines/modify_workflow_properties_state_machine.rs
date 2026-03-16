use super::{NewMachineWithCommand, workflow_machines::MachineResponse};
use crate::worker::workflow::{
    WFMachinesError,
    machines::{EventInfo, HistEventData, WFMachinesAdapter},
};
use rustfsm::{StateMachine, TransitionResult, fsm};
use temporal_sdk_core_protos::{
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        replay::TestHistoryBuilder,
        test_help::{MockPollCfg, build_fake_sdk},
    };
    use temporal_sdk::WfContext;
    use temporal_sdk_core_protos::{
        DEFAULT_WORKFLOW_TYPE,
        temporal::api::{
            command::v1::{Command, command},
            common::v1::Payload,
        },
    };

    #[tokio::test]
    async fn workflow_modify_props() {
        let mut t = TestHistoryBuilder::default();
        t.add_by_type(EventType::WorkflowExecutionStarted);
        t.add_full_wf_task();
        t.add_workflow_execution_completed();

        let (k1, k2) = ("foo", "bar");

        let mut mock_cfg = MockPollCfg::from_hist_builder(t);
        mock_cfg.completion_asserts_from_expectations(|mut asserts| {
            asserts.then(|wft| {
                assert_matches!(
                    wft.commands.as_slice(),
                    [Command {
                        attributes: Some(
                            command::Attributes::ModifyWorkflowPropertiesCommandAttributes(msg)
                        ),
                        ..
                    }, ..] => {
                        let fields = &msg.upserted_memo.as_ref().unwrap().fields;
                        let payload1 = fields.get(k1).unwrap();
                        let payload2 = fields.get(k2).unwrap();
                        assert_eq!(payload1.data[0], 0x01);
                        assert_eq!(payload2.data[0], 0x02);
                        assert_eq!(fields.len(), 2);
                    }
                );
            });
        });

        let mut worker = build_fake_sdk(mock_cfg);
        worker.register_wf(DEFAULT_WORKFLOW_TYPE, move |ctx: WfContext| async move {
            ctx.upsert_memo([
                (
                    String::from(k1),
                    Payload {
                        data: vec![0x01],
                        ..Default::default()
                    },
                ),
                (
                    String::from(k2),
                    Payload {
                        data: vec![0x02],
                        ..Default::default()
                    },
                ),
            ]);
            Ok(().into())
        });
        worker.run().await.unwrap();
    }
}
