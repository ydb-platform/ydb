//! The patch machine can be difficult to follow. Refer to below table for behavior. The
//! deprecated calls simply say "allowed" because if they returned a value, it's always true. Old
//! code cannot exist in workflows which use the deprecated call.
//!
//! | History Has                  | Workflow Has    | Outcome                                                                            |
//! |------------------------------|-----------------|------------------------------------------------------------------------------------|
//! | not replaying                | no patched      | Nothing interesting. Versioning not involved.                                      |
//! | marker for change            | no patched      | No matching command / workflow does not support this version                       |
//! | deprecated marker for change | no patched      | Marker ignored, workflow continues as if it didn't exist                           |
//! | replaying, no marker         | no patched      | Nothing interesting. Versioning not involved.                                      |
//! | not replaying                | patched         | Marker command sent to server and recorded. Call returns true                      |
//! | marker for change            | patched         | Call returns true upon replay                                                      |
//! | deprecated marker for change | patched         | Call returns true upon replay                                                      |
//! | replaying, no marker         | patched         | Call returns false upon replay                                                     |
//! | not replaying                | deprecate_patch | Marker command sent to server and recorded with deprecated flag. Call allowed      |
//! | marker for change            | deprecate_patch | Call allowed                                                                       |
//! | deprecated marker for change | deprecate_patch | Call allowed                                                                       |
//! | replaying, no marker         | deprecate_patch | Call allowed                                                                       |

use super::{
    EventInfo, NewMachineWithCommand, OnEventWrapper, WFMachinesAdapter, WFMachinesError,
    workflow_machines::MachineResponse,
};
use crate::{
    internal_flags::CoreInternalFlags,
    protosext::HistoryEventExt,
    worker::workflow::{
        InternalFlagsRef,
        machines::{
            HistEventData, upsert_search_attributes_state_machine::MAX_SEARCH_ATTR_PAYLOAD_SIZE,
        },
    },
};
use anyhow::Context;
use rustfsm::{StateMachine, TransitionResult, fsm};
use std::{
    collections::{BTreeSet, HashMap},
    convert::TryFrom,
};
use temporal_sdk_core_protos::{
    constants::PATCH_MARKER_NAME,
    coresdk::{AsJsonPayloadExt, common::build_has_change_marker_details},
    temporal::api::{
        command::v1::{
            RecordMarkerCommandAttributes, UpsertWorkflowSearchAttributesCommandAttributes,
        },
        common::v1::SearchAttributes,
        enums::v1::CommandType,
    },
};

pub(crate) const VERSION_SEARCH_ATTR_KEY: &str = "TemporalChangeVersion";

fsm! {
    pub(super) name PatchMachine;
    command PatchCommand;
    error WFMachinesError;
    shared_state SharedState;

    // Machine is created in either executing or replaying, and then immediately scheduled and
    // transitions to the command created state (creating the command in the process)
    Executing --(Schedule, on_schedule) --> MarkerCommandCreated;
    Replaying --(Schedule, on_schedule) --> MarkerCommandCreatedReplaying;

    // Pretty much nothing happens here - once we issue the command it is the responsibility of
    // machinery above us to notify lang SDK about the change. This is in order to allow the
    // change call to be sync and not have to wait for the command to resolve.
    MarkerCommandCreated --(CommandRecordMarker, on_command_record_marker) --> Notified;
    MarkerCommandCreatedReplaying --(CommandRecordMarker) --> Notified;

    // Once we've played back the marker recorded event, all we need to do is double-check that
    // it matched what we expected
    Notified --(MarkerRecorded(String), shared on_marker_recorded) --> MarkerCommandRecorded;
}

#[derive(Clone)]
pub(super) struct SharedState {
    patch_id: String,
}

#[derive(Debug, derive_more::Display)]
pub(super) enum PatchCommand {}

/// Patch machines are created when the user invokes `has_change` (or whatever it may be named
/// in that lang).
///
/// `patch_id`: identifier of a particular change. All calls to get_version that share a change id
/// are guaranteed to return the same value.
/// `replaying_when_invoked`: If the workflow is replaying when this invocation occurs, this needs
/// to be set to true.
pub(super) fn has_change<'a>(
    patch_id: String,
    replaying_when_invoked: bool,
    deprecated: bool,
    seen_in_peekahead: bool,
    existing_patch_ids: impl Iterator<Item = &'a str>,
    internal_flags: InternalFlagsRef,
) -> Result<(NewMachineWithCommand, Vec<MachineResponse>), WFMachinesError> {
    let shared_state = SharedState { patch_id };
    let initial_state = if replaying_when_invoked {
        Replaying {}.into()
    } else {
        Executing {}.into()
    };
    let command = RecordMarkerCommandAttributes {
        marker_name: PATCH_MARKER_NAME.to_string(),
        details: build_has_change_marker_details(&shared_state.patch_id, deprecated)
            .context("While encoding patch marker details")?,
        header: None,
        failure: None,
    }
    .into();
    let mut machine = PatchMachine::from_parts(initial_state, shared_state);

    OnEventWrapper::on_event_mut(&mut machine, PatchMachineEvents::Schedule)
        .expect("Patch machine scheduling doesn't fail");

    // If we're replaying but this patch isn't in the peekahead, then we wouldn't have
    // upserted either, and thus should not create the machine
    let replaying_and_not_in_history = replaying_when_invoked && !seen_in_peekahead;
    let cannot_use_flag = !internal_flags.borrow_mut().try_use(
        CoreInternalFlags::UpsertSearchAttributeOnPatch,
        !replaying_when_invoked,
    );
    let maybe_upsert_cmd = if replaying_and_not_in_history || cannot_use_flag {
        vec![]
    } else {
        // Produce an upsert SA command for this patch.
        let mut all_ids = BTreeSet::from_iter(existing_patch_ids);
        all_ids.insert(machine.shared_state.patch_id.as_str());
        let serialized = all_ids
            .as_json_payload()
            .context("Could not serialize search attribute value for patch machine")
            .map_err(|e| WFMachinesError::Fatal(e.to_string()))?;

        if serialized.data.len() >= MAX_SEARCH_ATTR_PAYLOAD_SIZE {
            warn!(
                "Serialized size of {VERSION_SEARCH_ATTR_KEY} search attribute update would \
                 exceed the maximum value size. Skipping this upsert. Be aware that your \
                 visibility records will not include the following patch: {}",
                machine.shared_state.patch_id
            );
            vec![]
        } else {
            let indexed_fields = {
                let mut m = HashMap::new();
                m.insert(VERSION_SEARCH_ATTR_KEY.to_string(), serialized);
                m
            };
            vec![MachineResponse::NewCoreOriginatedCommand(
                UpsertWorkflowSearchAttributesCommandAttributes {
                    search_attributes: Some(SearchAttributes { indexed_fields }),
                }
                .into(),
            )]
        }
    };

    Ok((
        NewMachineWithCommand {
            command,
            machine: machine.into(),
        },
        maybe_upsert_cmd,
    ))
}

impl PatchMachine {}

#[derive(Default, Clone)]
pub(super) struct Executing {}

impl Executing {
    pub(super) fn on_schedule(self) -> PatchMachineTransition<MarkerCommandCreated> {
        TransitionResult::default()
    }
}

#[derive(Default, Clone)]
pub(super) struct MarkerCommandCreated {}

impl MarkerCommandCreated {
    pub(super) fn on_command_record_marker(self) -> PatchMachineTransition<Notified> {
        TransitionResult::default()
    }
}

#[derive(Default, Clone)]
pub(super) struct MarkerCommandCreatedReplaying {}

#[derive(Default, Clone)]
pub(super) struct MarkerCommandRecorded {}

#[derive(Default, Clone)]
pub(super) struct Replaying {}

impl Replaying {
    pub(super) fn on_schedule(self) -> PatchMachineTransition<MarkerCommandCreatedReplaying> {
        TransitionResult::default()
    }
}

#[derive(Default, Clone)]
pub(super) struct Notified {}
impl From<MarkerCommandCreatedReplaying> for Notified {
    fn from(_: MarkerCommandCreatedReplaying) -> Self {
        Self::default()
    }
}
impl Notified {
    pub(super) fn on_marker_recorded(
        self,
        dat: &mut SharedState,
        id: String,
    ) -> PatchMachineTransition<MarkerCommandRecorded> {
        if id != dat.patch_id {
            return TransitionResult::Err(WFMachinesError::Nondeterminism(format!(
                "Change id {} does not match expected id {}",
                id, dat.patch_id
            )));
        }
        TransitionResult::default()
    }
}

impl WFMachinesAdapter for PatchMachine {
    fn adapt_response(
        &self,
        _my_command: Self::Command,
        _event_info: Option<EventInfo>,
    ) -> Result<Vec<MachineResponse>, WFMachinesError> {
        panic!("Patch machine does not produce commands")
    }
}

impl TryFrom<CommandType> for PatchMachineEvents {
    type Error = ();

    fn try_from(c: CommandType) -> Result<Self, Self::Error> {
        Ok(match c {
            CommandType::RecordMarker => Self::CommandRecordMarker,
            _ => return Err(()),
        })
    }
}

impl TryFrom<HistEventData> for PatchMachineEvents {
    type Error = WFMachinesError;

    fn try_from(e: HistEventData) -> Result<Self, Self::Error> {
        let e = e.event;
        match e.get_patch_marker_details() {
            Some((id, _)) => Ok(Self::MarkerRecorded(id)),
            _ => Err(WFMachinesError::Nondeterminism(format!(
                "Change machine cannot handle this event: {e}"
            ))),
        }
    }
}

impl PatchMachine {
    /// Returns true if this patch machine has the same id as the one provided
    pub(crate) fn matches_patch(&self, id: &str) -> bool {
        self.shared_state.patch_id == id
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        internal_flags::CoreInternalFlags,
        replay::TestHistoryBuilder,
        test_help::{MockPollCfg, ResponseType, build_fake_sdk},
        worker::workflow::machines::patch_state_machine::VERSION_SEARCH_ATTR_KEY,
    };
    use rstest::rstest;
    use std::{
        collections::{HashSet, VecDeque, hash_map::RandomState},
        time::Duration,
    };
    use temporal_sdk::{ActivityOptions, WfContext};
    use temporal_sdk_core_protos::{
        DEFAULT_WORKFLOW_TYPE,
        constants::PATCH_MARKER_NAME,
        coresdk::{
            AsJsonPayloadExt, FromJsonPayloadExt,
            common::decode_change_marker_details,
            workflow_activation::{NotifyHasPatch, WorkflowActivationJob, workflow_activation_job},
        },
        temporal::api::{
            command::v1::{
                RecordMarkerCommandAttributes, ScheduleActivityTaskCommandAttributes,
                UpsertWorkflowSearchAttributesCommandAttributes, command::Attributes,
            },
            common::v1::ActivityType,
            enums::v1::{CommandType, EventType},
            history::v1::{
                ActivityTaskCompletedEventAttributes, ActivityTaskScheduledEventAttributes,
                ActivityTaskStartedEventAttributes, TimerFiredEventAttributes,
            },
        },
    };
    use temporal_sdk_core_test_utils::interceptors::ActivationAssertionsInterceptor;

    const MY_PATCH_ID: &str = "test_patch_id";
    #[derive(Eq, PartialEq, Copy, Clone, Debug)]
    enum MarkerType {
        Deprecated,
        NotDeprecated,
        NoMarker,
    }

    const ONE_SECOND: Duration = Duration::from_secs(1);

    /// EVENT_TYPE_WORKFLOW_EXECUTION_STARTED
    /// EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
    /// EVENT_TYPE_WORKFLOW_TASK_STARTED
    /// EVENT_TYPE_WORKFLOW_TASK_COMPLETED
    /// EVENT_TYPE_MARKER_RECORDED (depending on marker_type)
    /// EVENT_TYPE_ACTIVITY_TASK_SCHEDULED
    /// EVENT_TYPE_ACTIVITY_TASK_STARTED
    /// EVENT_TYPE_ACTIVITY_TASK_COMPLETED
    /// EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
    /// EVENT_TYPE_WORKFLOW_TASK_STARTED
    /// EVENT_TYPE_WORKFLOW_TASK_COMPLETED
    /// EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED
    fn patch_marker_single_activity(
        marker_type: MarkerType,
        version: usize,
        replay: bool,
    ) -> TestHistoryBuilder {
        let mut t = TestHistoryBuilder::default();
        t.add_by_type(EventType::WorkflowExecutionStarted);
        t.add_full_wf_task();
        t.set_flags_first_wft(
            &[CoreInternalFlags::UpsertSearchAttributeOnPatch as u32],
            &[],
        );
        match marker_type {
            MarkerType::Deprecated => {
                t.add_has_change_marker(MY_PATCH_ID, true);
                t.add_upsert_search_attrs_for_patch(&[MY_PATCH_ID.to_string()]);
            }
            MarkerType::NotDeprecated => {
                t.add_has_change_marker(MY_PATCH_ID, false);
                t.add_upsert_search_attrs_for_patch(&[MY_PATCH_ID.to_string()]);
            }
            MarkerType::NoMarker => {}
        };

        let activity_id = if replay {
            match (marker_type, version) {
                (_, 1) => "no_change",
                (MarkerType::NotDeprecated, 2) => "had_change",
                (MarkerType::Deprecated, 2) => "had_change",
                (MarkerType::NoMarker, 2) => "no_change",
                (_, 3) => "had_change",
                (_, 4) => "had_change",
                v => panic!("Nonsense marker / version combo {v:?}"),
            }
        } else {
            // If the workflow isn't replaying (we're creating history here for a workflow which
            // wasn't replaying at the time of scheduling the activity, and has done that, and now
            // we're feeding back the history it would have produced) then it always has the change,
            // except in v1.
            if version > 1 {
                "had_change"
            } else {
                "no_change"
            }
        };

        let scheduled_event_id = t.add(ActivityTaskScheduledEventAttributes {
            activity_id: activity_id.to_string(),
            ..Default::default()
        });
        let started_event_id = t.add(ActivityTaskStartedEventAttributes {
            scheduled_event_id,
            ..Default::default()
        });
        t.add(ActivityTaskCompletedEventAttributes {
            scheduled_event_id,
            started_event_id,
            ..Default::default()
        });
        t.add_full_wf_task();
        t.add_workflow_execution_completed();
        t
    }

    async fn v1(ctx: &mut WfContext) {
        ctx.activity(ActivityOptions {
            activity_id: Some("no_change".to_owned()),
            ..Default::default()
        })
        .await;
    }

    async fn v2(ctx: &mut WfContext) -> bool {
        if ctx.patched(MY_PATCH_ID) {
            ctx.activity(ActivityOptions {
                activity_id: Some("had_change".to_owned()),
                ..Default::default()
            })
            .await;
            true
        } else {
            ctx.activity(ActivityOptions {
                activity_id: Some("no_change".to_owned()),
                ..Default::default()
            })
            .await;
            false
        }
    }

    async fn v3(ctx: &mut WfContext) {
        ctx.deprecate_patch(MY_PATCH_ID);
        ctx.activity(ActivityOptions {
            activity_id: Some("had_change".to_owned()),
            ..Default::default()
        })
        .await;
    }

    async fn v4(ctx: &mut WfContext) {
        ctx.activity(ActivityOptions {
            activity_id: Some("had_change".to_owned()),
            ..Default::default()
        })
        .await;
    }

    fn patch_setup(
        replaying: bool,
        marker_type: MarkerType,
        workflow_version: usize,
    ) -> MockPollCfg {
        let t = patch_marker_single_activity(marker_type, workflow_version, replaying);
        if replaying {
            MockPollCfg::from_resps(t, [ResponseType::AllHistory])
        } else {
            MockPollCfg::from_hist_builder(t)
        }
    }

    macro_rules! patch_wf {
        ($workflow_version:ident) => {
            move |mut ctx: WfContext| async move {
                match $workflow_version {
                    1 => {
                        v1(&mut ctx).await;
                    }
                    2 => {
                        v2(&mut ctx).await;
                    }
                    3 => {
                        v3(&mut ctx).await;
                    }
                    4 => {
                        v4(&mut ctx).await;
                    }
                    _ => panic!("Invalid workflow version for test setup"),
                }
                Ok(().into())
            }
        };
    }

    #[rstest]
    #[case::v1_breaks_on_normal_marker(false, MarkerType::NotDeprecated, 1)]
    #[case::v1_accepts_dep_marker(false, MarkerType::Deprecated, 1)]
    #[case::v1_replay_breaks_on_normal_marker(true, MarkerType::NotDeprecated, 1)]
    #[case::v1_replay_accepts_dep_marker(true, MarkerType::Deprecated, 1)]
    #[case::v4_breaks_on_normal_marker(false, MarkerType::NotDeprecated, 4)]
    #[case::v4_accepts_dep_marker(false, MarkerType::Deprecated, 4)]
    #[case::v4_replay_breaks_on_normal_marker(true, MarkerType::NotDeprecated, 4)]
    #[case::v4_replay_accepts_dep_marker(true, MarkerType::Deprecated, 4)]
    #[tokio::test]
    async fn v1_and_v4_changes(
        #[case] replaying: bool,
        #[case] marker_type: MarkerType,
        #[case] wf_version: usize,
    ) {
        let mut mock_cfg = patch_setup(replaying, marker_type, wf_version);

        if marker_type != MarkerType::Deprecated {
            // should explode b/c non-dep marker is present
            mock_cfg.num_expected_fails = 1;
        }

        let mut aai = ActivationAssertionsInterceptor::default();
        aai.skip_one().then(move |a| {
            if marker_type == MarkerType::Deprecated {
                // Activity is resolved
                assert_matches!(
                    a.jobs.as_slice(),
                    [WorkflowActivationJob {
                        variant: Some(workflow_activation_job::Variant::ResolveActivity(_))
                    }]
                );
            }
        });

        if !replaying {
            mock_cfg.completion_asserts_from_expectations(|mut asserts| {
                asserts.then(|wft| {
                    assert_eq!(wft.commands.len(), 1);
                    assert_eq!(
                        wft.commands[0].command_type,
                        CommandType::ScheduleActivityTask as i32
                    );
                });
            });
        }

        let mut worker = build_fake_sdk(mock_cfg);
        worker.set_worker_interceptor(aai);
        worker.register_wf(DEFAULT_WORKFLOW_TYPE, patch_wf!(wf_version));
        worker.run().await.unwrap();
    }

    // Note that the not-replaying and no-marker cases don't make sense and hence are absent
    #[rstest]
    #[case::v2_marker_new_path(false, MarkerType::NotDeprecated, 2)]
    #[case::v2_dep_marker_new_path(false, MarkerType::Deprecated, 2)]
    #[case::v2_replay_no_marker_old_path(true, MarkerType::NoMarker, 2)]
    #[case::v2_replay_marker_new_path(true, MarkerType::NotDeprecated, 2)]
    #[case::v2_replay_dep_marker_new_path(true, MarkerType::Deprecated, 2)]
    #[case::v3_marker_new_path(false, MarkerType::NotDeprecated, 3)]
    #[case::v3_dep_marker_new_path(false, MarkerType::Deprecated, 3)]
    #[case::v3_replay_no_marker_old_path(true, MarkerType::NoMarker, 3)]
    #[case::v3_replay_marker_new_path(true, MarkerType::NotDeprecated, 3)]
    #[case::v3_replay_dep_marker_new_path(true, MarkerType::Deprecated, 3)]
    #[tokio::test]
    async fn v2_and_v3_changes(
        #[case] replaying: bool,
        #[case] marker_type: MarkerType,
        #[case] wf_version: usize,
    ) {
        let mut mock_cfg = patch_setup(replaying, marker_type, wf_version);

        let mut aai = ActivationAssertionsInterceptor::default();
        aai.then(move |act| {
            // replaying cases should immediately get a resolve change activation when marker is
            // present
            if replaying && marker_type != MarkerType::NoMarker {
                assert_matches!(
                    &act.jobs[1],
                     WorkflowActivationJob {
                        variant: Some(workflow_activation_job::Variant::NotifyHasPatch(
                            NotifyHasPatch {
                                patch_id,
                            }
                        ))
                    } => patch_id == MY_PATCH_ID
                );
            } else {
                assert_eq!(act.jobs.len(), 1);
            }
        })
        .then(move |act| {
            assert_matches!(
                act.jobs.as_slice(),
                [WorkflowActivationJob {
                    variant: Some(workflow_activation_job::Variant::ResolveActivity(_))
                }]
            );
        });

        if !replaying {
            mock_cfg.completion_asserts_from_expectations(|mut asserts| {
                asserts.then(move |wft| {
                    let mut commands = VecDeque::from(wft.commands.clone());
                    let expected_num_cmds = if marker_type == MarkerType::NoMarker {
                        2
                    } else {
                        3
                    };
                    assert_eq!(commands.len(), expected_num_cmds);
                    let dep_flag_expected = wf_version != 2;
                    assert_matches!(
                        commands.pop_front().unwrap().attributes.as_ref().unwrap(),
                        Attributes::RecordMarkerCommandAttributes(
                            RecordMarkerCommandAttributes { marker_name, details,.. })
                        if marker_name == PATCH_MARKER_NAME
                          && decode_change_marker_details(details).unwrap().1 == dep_flag_expected
                    );
                    if expected_num_cmds == 3 {
                        assert_matches!(
                            commands.pop_front().unwrap().attributes.as_ref().unwrap(),
                            Attributes::UpsertWorkflowSearchAttributesCommandAttributes(
                                UpsertWorkflowSearchAttributesCommandAttributes
                                { search_attributes: Some(attrs) }
                            )
                            if attrs.indexed_fields.get(VERSION_SEARCH_ATTR_KEY).unwrap()
                              == &[MY_PATCH_ID].as_json_payload().unwrap()
                        );
                    }
                    // The only time the "old" timer should fire is in v2, replaying, without a marker.
                    let expected_activity_id =
                        if replaying && marker_type == MarkerType::NoMarker && wf_version == 2 {
                            "no_change"
                        } else {
                            "had_change"
                        };
                    assert_matches!(
                        commands.pop_front().unwrap().attributes.as_ref().unwrap(),
                        Attributes::ScheduleActivityTaskCommandAttributes(
                            ScheduleActivityTaskCommandAttributes { activity_id, .. }
                        )
                        if activity_id == expected_activity_id
                    );
                });
            });
        }

        let mut worker = build_fake_sdk(mock_cfg);
        worker.set_worker_interceptor(aai);
        worker.register_wf(DEFAULT_WORKFLOW_TYPE, patch_wf!(wf_version));
        worker.run().await.unwrap();
    }

    #[rstest]
    #[case::has_change_replay(true, true)]
    #[case::no_change_replay(false, true)]
    #[case::has_change_inc(true, false)]
    // The false-false case doesn't make sense, as the incremental cases act as if working against
    // a sticky queue, and it'd be impossible for a worker with the call to get an incremental
    // history that then suddenly doesn't have the marker.
    #[tokio::test]
    async fn same_change_multiple_spots(#[case] have_marker_in_hist: bool, #[case] replay: bool) {
        let mut t = TestHistoryBuilder::default();
        t.add_by_type(EventType::WorkflowExecutionStarted);
        t.add_full_wf_task();
        t.set_flags_first_wft(
            &[CoreInternalFlags::UpsertSearchAttributeOnPatch as u32],
            &[],
        );
        if have_marker_in_hist {
            t.add_has_change_marker(MY_PATCH_ID, false);
            t.add_upsert_search_attrs_for_patch(&[MY_PATCH_ID.to_string()]);
            let scheduled_event_id = t.add(ActivityTaskScheduledEventAttributes {
                activity_id: "1".to_owned(),
                activity_type: Some(ActivityType {
                    name: "".to_string(),
                }),
                ..Default::default()
            });
            let started_event_id = t.add(ActivityTaskStartedEventAttributes {
                scheduled_event_id,
                ..Default::default()
            });
            t.add(ActivityTaskCompletedEventAttributes {
                scheduled_event_id,
                started_event_id,
                ..Default::default()
            });
            t.add_full_wf_task();
            let timer_started_event_id = t.add_by_type(EventType::TimerStarted);
            t.add(TimerFiredEventAttributes {
                started_event_id: timer_started_event_id,
                timer_id: "1".to_owned(),
            });
        } else {
            let started_event_id = t.add_by_type(EventType::TimerStarted);
            t.add(TimerFiredEventAttributes {
                started_event_id,
                timer_id: "1".to_owned(),
            });
            t.add_full_wf_task();
            let timer_started_event_id = t.add_by_type(EventType::TimerStarted);
            t.add(TimerFiredEventAttributes {
                started_event_id: timer_started_event_id,
                timer_id: "2".to_owned(),
            });
        }
        t.add_full_wf_task();

        if have_marker_in_hist {
            let scheduled_event_id = t.add(ActivityTaskScheduledEventAttributes {
                activity_id: "2".to_string(),
                activity_type: Some(ActivityType {
                    name: "".to_string(),
                }),
                ..Default::default()
            });
            let started_event_id = t.add(ActivityTaskStartedEventAttributes {
                scheduled_event_id,
                ..Default::default()
            });
            t.add(ActivityTaskCompletedEventAttributes {
                scheduled_event_id,
                started_event_id,
                ..Default::default()
            });
        } else {
            let started_event_id = t.add_by_type(EventType::TimerStarted);
            t.add(TimerFiredEventAttributes {
                started_event_id,
                timer_id: "3".to_owned(),
            });
        }
        t.add_full_wf_task();
        t.add_workflow_execution_completed();

        let mock_cfg = if replay {
            MockPollCfg::from_resps(t, [ResponseType::AllHistory])
        } else {
            MockPollCfg::from_hist_builder(t)
        };

        // Errors would appear as nondeterminism problems, so just run it.
        let mut worker = build_fake_sdk(mock_cfg);
        worker.register_wf(DEFAULT_WORKFLOW_TYPE, move |ctx: WfContext| async move {
            if ctx.patched(MY_PATCH_ID) {
                ctx.activity(ActivityOptions::default()).await;
            } else {
                ctx.timer(ONE_SECOND).await;
            }
            ctx.timer(ONE_SECOND).await;
            if ctx.patched(MY_PATCH_ID) {
                ctx.activity(ActivityOptions::default()).await;
            } else {
                ctx.timer(ONE_SECOND).await;
            }
            Ok(().into())
        });
        worker.run().await.unwrap();
    }

    const SIZE_OVERFLOW_PATCH_AMOUNT: usize = 180;
    #[rstest]
    #[case::happy_path(50)]
    // We start exceeding the 2k size limit at 180 patches with this format
    #[case::size_overflow(SIZE_OVERFLOW_PATCH_AMOUNT)]
    #[tokio::test]
    async fn many_patches_combine_in_search_attrib_update(#[case] num_patches: usize) {
        let mut t = TestHistoryBuilder::default();
        t.add_by_type(EventType::WorkflowExecutionStarted);
        t.add_full_wf_task();
        t.set_flags_first_wft(
            &[CoreInternalFlags::UpsertSearchAttributeOnPatch as u32],
            &[],
        );
        for i in 1..=num_patches {
            let id = format!("patch-{i}");
            t.add_has_change_marker(&id, false);
            if i < SIZE_OVERFLOW_PATCH_AMOUNT {
                t.add_upsert_search_attrs_for_patch(&[id]);
            }
            let timer_started_event_id = t.add_by_type(EventType::TimerStarted);
            t.add(TimerFiredEventAttributes {
                started_event_id: timer_started_event_id,
                timer_id: i.to_string(),
            });
            t.add_full_wf_task();
        }
        t.add_workflow_execution_completed();

        let mut mock_cfg = MockPollCfg::from_hist_builder(t);
        mock_cfg.completion_asserts_from_expectations(|mut asserts| {
            // Iterate through all activations/responses except the final one with complete workflow
            for i in 2..=num_patches + 1 {
                asserts.then(move |wft| {
                    let cmds = &wft.commands;
                    if i > SIZE_OVERFLOW_PATCH_AMOUNT {
                        assert_eq!(2, cmds.len());
                        assert_matches!(cmds[1].command_type(), CommandType::StartTimer);
                    } else {
                        assert_eq!(3, cmds.len());
                        let attrs = assert_matches!(
                            cmds[1].attributes.as_ref().unwrap(),
                            Attributes::UpsertWorkflowSearchAttributesCommandAttributes(
                                UpsertWorkflowSearchAttributesCommandAttributes
                                { search_attributes: Some(attrs) }
                            ) => attrs
                        );
                        let expected_patches: HashSet<String, _> =
                            (1..i).map(|i| format!("patch-{i}")).collect();
                        let deserialized = HashSet::<String, RandomState>::from_json_payload(
                            attrs.indexed_fields.get(VERSION_SEARCH_ATTR_KEY).unwrap(),
                        )
                        .unwrap();
                        assert_eq!(deserialized, expected_patches);
                    }
                });
            }
        });

        let mut worker = build_fake_sdk(mock_cfg);
        worker.register_wf(DEFAULT_WORKFLOW_TYPE, move |ctx: WfContext| async move {
            for i in 1..=num_patches {
                let _dontcare = ctx.patched(&format!("patch-{i}"));
                ctx.timer(ONE_SECOND).await;
            }
            Ok(().into())
        });
        worker.run().await.unwrap();
    }
}
