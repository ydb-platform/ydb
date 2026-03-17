use super::{NewMachineWithCommand, workflow_machines::MachineResponse};
use crate::{
    internal_flags::CoreInternalFlags,
    worker::workflow::{
        InternalFlagsRef, WFMachinesError,
        machines::{
            EventInfo, HistEventData, WFMachinesAdapter,
            patch_state_machine::VERSION_SEARCH_ATTR_KEY,
        },
    },
};
use rustfsm::{StateMachine, TransitionResult, fsm};
use temporal_sdk_core_protos::{
    coresdk::workflow_commands::UpsertWorkflowSearchAttributes,
    temporal::api::{
        command::v1::{UpsertWorkflowSearchAttributesCommandAttributes, command},
        common::v1::SearchAttributes,
        enums::v1::CommandType,
        history::v1::history_event,
    },
};

/// By default the server permits SA values under 2k.
pub(crate) const MAX_SEARCH_ATTR_PAYLOAD_SIZE: usize = 2048;

fsm! {
    pub(super) name UpsertSearchAttributesMachine;
    command UpsertSearchAttributesMachineCommand;
    error WFMachinesError;
    shared_state SharedState;

    // Machine is instantiated into the Created state and then transitions into the CommandIssued
    // state when it receives the CommandScheduled event that is a result of looping back the
    // Command initially packaged with NewMachineWithCommand (see upsert_search_attrs)
    Created --(CommandScheduled) --> CommandIssued;

    // Having sent the command to the server, the machine transitions into a terminal state (Done)
    // upon observing a history event indicating that the command has been recorded. Note that this
    // does not imply that the command has been _executed_, only that it _will be_ executed at some
    // point in the future.
    CommandIssued --(CommandRecorded, on_command_recorded) --> Done;
}

/// Instantiates an UpsertSearchAttributesMachine and packs it together with an initial command
/// to apply the provided search attribute update.
pub(super) fn upsert_search_attrs(
    attribs: UpsertWorkflowSearchAttributes,
    internal_flags: InternalFlagsRef,
    replaying: bool,
) -> NewMachineWithCommand {
    let has_flag = internal_flags
        .borrow_mut()
        .try_use(CoreInternalFlags::UpsertSearchAttributeOnPatch, !replaying);
    if has_flag
        && attribs
            .search_attributes
            .contains_key(VERSION_SEARCH_ATTR_KEY)
    {
        warn!(
            "Upserting the {VERSION_SEARCH_ATTR_KEY} search attribute directly from workflow code \
             is not permitted and has no effect!"
        );
        // We must still create the command to preserve compatability with anyone previously doing
        // this.
        create_new(Default::default())
    } else {
        create_new(attribs.search_attributes.into())
    }
}

/// May be used by other state machines / internal needs which desire upserting search attributes.
pub(super) fn upsert_search_attrs_internal(
    attribs: UpsertWorkflowSearchAttributesCommandAttributes,
) -> NewMachineWithCommand {
    create_new(attribs.search_attributes.unwrap_or_default())
}

fn create_new(sa_map: SearchAttributes) -> NewMachineWithCommand {
    let sm = UpsertSearchAttributesMachine::from_parts(Created {}.into(), SharedState {});
    NewMachineWithCommand {
        command: command::Attributes::UpsertWorkflowSearchAttributesCommandAttributes(
            UpsertWorkflowSearchAttributesCommandAttributes {
                search_attributes: Some(sa_map),
            },
        ),

        machine: sm.into(),
    }
}

#[derive(Clone)]
pub(super) struct SharedState {}

/// The state-machine-specific set of commands that are the results of state transition in the
/// UpsertSearchAttributesMachine. There are none of these because this state machine emits the
/// UpsertSearchAttributes API command during construction and then does not emit any subsequent
/// state-machine specific commands.
#[derive(Debug, derive_more::Display)]
pub(super) enum UpsertSearchAttributesMachineCommand {}

/// The state of the UpsertSearchAttributesMachine at time zero (i.e. at instantiation)
#[derive(Debug, Default, Clone, derive_more::Display)]
pub(super) struct Created {}

/// Once the UpsertSearchAttributesMachine has been known to have issued the upsert command to
/// higher-level machinery, it transitions into this state.
#[derive(Debug, Default, Clone, derive_more::Display)]
pub(super) struct CommandIssued {}

impl CommandIssued {
    pub(super) fn on_command_recorded(self) -> UpsertSearchAttributesMachineTransition<Done> {
        TransitionResult::default()
    }
}

/// Once the server has recorded its receipt of the search attribute update, the
/// UpsertSearchAttributesMachine transitions into this terminal state.
#[derive(Debug, Default, Clone, derive_more::Display)]
pub(super) struct Done {}

impl WFMachinesAdapter for UpsertSearchAttributesMachine {
    /// Transforms an UpsertSearchAttributesMachine-specific command (i.e. an instance of the type
    /// UpsertSearchAttributesMachineCommand) to a more generic form supported by the abstract
    /// StateMachine type.
    fn adapt_response(
        &self,
        _my_command: Self::Command,
        _event_info: Option<EventInfo>,
    ) -> Result<Vec<MachineResponse>, Self::Error> {
        // No implementation needed until this state machine emits state machine commands
        Err(Self::Error::Nondeterminism(
            "UpsertWorkflowSearchAttributesMachine does not use commands".to_string(),
        ))
    }
}

// Converts the generic history event with type EventType::UpsertWorkflowSearchAttributes into the
// UpsertSearchAttributesMachine-specific event type
// UpsertSearchAttributesMachineEvents::CommandRecorded.
impl TryFrom<HistEventData> for UpsertSearchAttributesMachineEvents {
    type Error = WFMachinesError;

    fn try_from(e: HistEventData) -> Result<Self, Self::Error> {
        match e.event.attributes {
            Some(history_event::Attributes::UpsertWorkflowSearchAttributesEventAttributes(_)) => {
                Ok(UpsertSearchAttributesMachineEvents::CommandRecorded)
            }
            _ => Err(Self::Error::Nondeterminism(format!(
                "UpsertWorkflowSearchAttributesMachine does not handle {e}"
            ))),
        }
    }
}

// Converts generic state machine command type CommandType::UpsertWorkflowSearchAttributes into
// the UpsertSearchAttributesMachine-specific event
impl TryFrom<CommandType> for UpsertSearchAttributesMachineEvents {
    type Error = WFMachinesError;

    fn try_from(c: CommandType) -> Result<Self, Self::Error> {
        match c {
            CommandType::UpsertWorkflowSearchAttributes => {
                Ok(UpsertSearchAttributesMachineEvents::CommandScheduled)
            }
            _ => Err(Self::Error::Nondeterminism(format!(
                "UpsertWorkflowSearchAttributesMachine does not handle command type {c:?}"
            ))),
        }
    }
}

// There is no Command/Response associated with this transition
impl From<Created> for CommandIssued {
    fn from(_: Created) -> Self {
        Self {}
    }
}

#[cfg(test)]
mod tests {
    use super::{super::OnEventWrapper, *};
    use crate::{
        replay::TestHistoryBuilder,
        test_help::{MockPollCfg, ResponseType, build_fake_sdk, build_mock_pollers, mock_worker},
        worker::{
            client::mocks::mock_worker_client,
            workflow::machines::patch_state_machine::VERSION_SEARCH_ATTR_KEY,
        },
    };
    use rustfsm::StateMachine;
    use std::collections::HashMap;
    use temporal_sdk::WfContext;
    use temporal_sdk_core_api::Worker;
    use temporal_sdk_core_protos::{
        DEFAULT_WORKFLOW_TYPE,
        coresdk::{
            AsJsonPayloadExt,
            workflow_activation::{WorkflowActivationJob, workflow_activation_job},
            workflow_commands::SetPatchMarker,
            workflow_completion::WorkflowActivationCompletion,
        },
        temporal::api::{
            command::v1::{Command, command::Attributes},
            common::v1::Payload,
            enums::v1::EventType,
            history::v1::{HistoryEvent, UpsertWorkflowSearchAttributesEventAttributes},
        },
    };
    use temporal_sdk_core_test_utils::WorkerTestHelpers;

    #[tokio::test]
    async fn upsert_search_attrs_from_workflow() {
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
                    [Command { attributes: Some(
                          command::Attributes::UpsertWorkflowSearchAttributesCommandAttributes(msg)
                        ), .. }, ..] => {
                        let fields = &msg.search_attributes.as_ref().unwrap().indexed_fields;
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
            ctx.upsert_search_attributes([
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

    #[rstest::rstest]
    fn upsert_search_attrs_sm() {
        let mut sm = UpsertSearchAttributesMachine::from_parts(Created {}.into(), SharedState {});

        let sa_attribs = UpsertWorkflowSearchAttributesEventAttributes {
            workflow_task_completed_event_id: 0,
            search_attributes: Some(SearchAttributes {
                indexed_fields: HashMap::from([("Yo".to_string(), Payload::default())]),
            }),
        };
        let recorded_history_event = HistoryEvent {
            event_type: EventType::UpsertWorkflowSearchAttributes as i32,
            attributes: Some(
                history_event::Attributes::UpsertWorkflowSearchAttributesEventAttributes(
                    sa_attribs,
                ),
            ),
            ..Default::default()
        };
        let cmd_recorded_sm_event = HistEventData {
            event: recorded_history_event,
            replaying: false,
            current_task_is_last_in_history: true,
        }
        .try_into()
        .unwrap();

        OnEventWrapper::on_event_mut(
            &mut sm,
            CommandType::UpsertWorkflowSearchAttributes
                .try_into()
                .unwrap(),
        )
        .expect("CommandScheduled should transition Created -> CommandIssued");
        assert_eq!(CommandIssued {}.to_string(), sm.state().to_string());

        let recorded_res = OnEventWrapper::on_event_mut(&mut sm, cmd_recorded_sm_event);
        recorded_res.expect("CommandRecorded should transition CommandIssued -> Done");
        assert_eq!(Done {}.to_string(), sm.state().to_string());
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn upserting_change_version_directly(
        #[values(true, false)] flag_in_history: bool,
        #[values(true, false)] with_patched_cmd: bool,
    ) {
        let patch_id = "whatever".to_string();
        let mut t = TestHistoryBuilder::default();
        t.add_by_type(EventType::WorkflowExecutionStarted);
        t.add_full_wf_task();
        if flag_in_history {
            t.set_flags_first_wft(
                &[CoreInternalFlags::UpsertSearchAttributeOnPatch as u32],
                &[],
            );
        }
        if with_patched_cmd {
            t.add_has_change_marker(&patch_id, false);
        }
        t.add_upsert_search_attrs_for_patch(std::slice::from_ref(&patch_id));
        t.add_we_signaled("hi", vec![]);
        t.add_full_wf_task();
        t.add_workflow_execution_completed();

        let mut mp = MockPollCfg::from_resp_batches(
            "fakeid",
            t,
            [ResponseType::ToTaskNum(1), ResponseType::ToTaskNum(2)],
            mock_worker_client(),
        );
        // Ensure the upsert command has an empty map when not using the patched command
        if !with_patched_cmd {
            mp.completion_mock_fn = Some(Box::new(|wftc| {
                let cmd_attrs = wftc
                    .commands
                    .first()
                    .and_then(|c| c.attributes.as_ref())
                    .unwrap();
                if matches!(
                    cmd_attrs,
                    Attributes::CompleteWorkflowExecutionCommandAttributes(_)
                ) {
                    return Ok(Default::default());
                }
                assert_matches!(cmd_attrs,
                    Attributes::UpsertWorkflowSearchAttributesCommandAttributes(attrs)
                    if attrs.search_attributes.clone().unwrap_or_default().indexed_fields.is_empty());
                Ok(Default::default())
            }));
        }
        let mut mock = build_mock_pollers(mp);
        mock.worker_cfg(|w| w.max_cached_workflows = 1);
        let core = mock_worker(mock);

        let mut ver_upsert = HashMap::new();
        ver_upsert.insert(
            VERSION_SEARCH_ATTR_KEY.to_string(),
            "hi".as_json_payload().unwrap(),
        );
        let act = core.poll_workflow_activation().await.unwrap();
        let mut cmds = if with_patched_cmd {
            vec![
                SetPatchMarker {
                    patch_id,
                    deprecated: false,
                }
                .into(),
            ]
        } else {
            vec![]
        };
        cmds.push(
            UpsertWorkflowSearchAttributes {
                search_attributes: ver_upsert,
            }
            .into(),
        );
        core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            act.run_id, cmds,
        ))
        .await
        .unwrap();
        // Now ensure that encountering the upsert in history works fine
        let act = core.poll_workflow_activation().await.unwrap();
        assert_matches!(
            act.jobs.as_slice(),
            [WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::SignalWorkflow(_)),
            }]
        );
        core.complete_execution(&act.run_id).await;
    }
}
