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
    EventInfo, NewMachineWithCommand, OnEventWrapper, StateMachine, TransitionResult,
    WFMachinesAdapter, WFMachinesError, fsm, workflow_machines::MachineResponse,
};
use crate::{
    internal_flags::CoreInternalFlags,
    protosext::HistoryEventExt,
    worker::workflow::{
        InternalFlagsRef, fatal,
        machines::{
            HistEventData, upsert_search_attributes_state_machine::MAX_SEARCH_ATTR_PAYLOAD_SIZE,
        },
        nondeterminism,
    },
};
use anyhow::Context;
use std::{
    collections::{BTreeSet, HashMap},
    convert::TryFrom,
};
use temporalio_common::protos::{
    VERSION_SEARCH_ATTR_KEY,
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
        let mut serialized = all_ids
            .as_json_payload()
            .context("Could not serialize search attribute value for patch machine")
            .map_err(|e| fatal!("{}", e))?;

        if serialized.data.len() >= MAX_SEARCH_ATTR_PAYLOAD_SIZE {
            warn!(
                "Serialized size of {VERSION_SEARCH_ATTR_KEY} search attribute update would \
                 exceed the maximum value size. Skipping this upsert. Be aware that your \
                 visibility records will not include the following patch: {}",
                machine.shared_state.patch_id
            );
            vec![]
        } else {
            // "KeywordList" is the expected type for this search attribute
            serialized
                .metadata
                .insert("type".to_string(), "KeywordList".as_bytes().to_vec());
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
            return TransitionResult::Err(nondeterminism!(
                "Change id {} does not match expected id {}",
                id,
                dat.patch_id
            ));
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
            _ => Err(nondeterminism!(
                "Change machine cannot handle this event: {e}"
            )),
        }
    }
}

impl PatchMachine {
    /// Returns true if this patch machine has the same id as the one provided
    pub(crate) fn matches_patch(&self, id: &str) -> bool {
        self.shared_state.patch_id == id
    }
}
