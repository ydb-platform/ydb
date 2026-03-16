use super::{EventInfo, WFMachinesAdapter, WFMachinesError, workflow_machines::MachineResponse};
use crate::{
    protosext::protocol_messages::UpdateRequest,
    worker::workflow::machines::{HistEventData, NewMachineWithResponse},
};
use itertools::Itertools;
use prost::EncodeError;
use rustfsm::{MachineError, StateMachine, TransitionResult, fsm};
use std::convert::TryFrom;
use temporal_sdk_core_protos::{
    coresdk::{
        workflow_activation::DoUpdate,
        workflow_commands::{UpdateResponse, update_response},
    },
    temporal::api::{
        command::v1::{ProtocolMessageCommandAttributes, command},
        common::v1::Payload,
        enums::v1::{CommandType, EventType},
        failure::v1::Failure,
        protocol::v1::Message as ProtocolMessage,
        update,
        update::v1::{Acceptance, Outcome, Rejection, Response, outcome},
    },
    utilities::pack_any,
};

fsm! {
    pub(super) name UpdateMachine;
    command UpdateMachineCommand;
    error WFMachinesError;
    shared_state SharedState;

    RequestInitiated --(Accept, on_accept)--> Accepted;
    RequestInitiated --(Reject(Failure), on_reject)--> Rejected;

    Accepted --(Complete(Payload), on_complete)--> CompletedImmediately;
    Accepted --(Reject(Failure), on_fail)--> CompletedImmediately;
    Accepted --(CommandProtocolMessage)--> AcceptCommandCreated;

    AcceptCommandCreated --(WorkflowExecutionUpdateAccepted)--> AcceptCommandRecorded;
    // These transitions may be taken after we've sent the command, but not seen the event,
    // such as during local activity execution inside of an update handler.
    AcceptCommandCreated --(Complete(Payload), on_complete)--> CompletedImmediatelyAcceptCreated;
    AcceptCommandCreated --(Reject(Failure), on_fail)--> CompletedImmediatelyAcceptCreated;

    AcceptCommandRecorded --(Complete(Payload), on_complete)--> Completed;
    AcceptCommandRecorded --(Reject(Failure), on_fail)--> Completed;

    Completed --(CommandProtocolMessage)--> CompletedCommandCreated;
    CompletedCommandCreated --(WorkflowExecutionUpdateCompleted)--> CompletedCommandRecorded;

    // When the update is immediately completed, we create two messages in a row, and both will
    // produce another transition for us
    CompletedImmediately --(CommandProtocolMessage)--> CompletedImmediatelyAcceptCreated;
    CompletedImmediatelyAcceptCreated --(CommandProtocolMessage)--> CompletedImmediatelyCompleteCreated;
    // Then once we've seen the accepted event in history, we go back to the normal complete path
    CompletedImmediatelyCompleteCreated --(WorkflowExecutionUpdateAccepted)--> CompletedCommandCreated;
}

#[derive(Debug, derive_more::Display)]
pub(super) enum UpdateMachineCommand {
    #[display("Accept")]
    Accept(update::v1::Request),
    #[display("Reject")]
    Reject(update::v1::Request, Failure),
    #[display("Complete")]
    Complete(Payload),
    #[display("Fail")]
    Fail(Failure),
}

#[derive(Clone)]
pub(super) struct SharedState {
    message_id: String,
    instance_id: String,
    event_seq_id: i64,
    meta: update::v1::Meta,
}

impl UpdateMachine {
    pub(crate) fn init(
        message_id: String,
        instance_id: String,
        event_seq_id: i64,
        request: UpdateRequest,
        replaying: bool,
    ) -> NewMachineWithResponse {
        let meta = request.meta().clone();
        let do_update = DoUpdate {
            id: meta.update_id.clone(),
            protocol_instance_id: instance_id.clone(),
            name: request.name().to_string(),
            input: request.input(),
            headers: request.headers(),
            meta: Some(meta.clone()),
            run_validator: !replaying,
        };
        let me = Self::from_parts(
            RequestInitiated {
                original_request: request.original,
            }
            .into(),
            SharedState {
                message_id,
                instance_id,
                event_seq_id,
                meta,
            },
        );
        NewMachineWithResponse {
            machine: me.into(),
            response: MachineResponse::PushWFJob(do_update.into()),
        }
    }

    pub(crate) fn handle_response(
        &mut self,
        resp: UpdateResponse,
    ) -> Result<Vec<MachineResponse>, WFMachinesError> {
        let cmds = match resp.response {
            None => {
                return Err(WFMachinesError::Fatal(format!(
                    "Update response for update {} had an empty result, this is a lang layer bug.",
                    &self.shared_state.meta.update_id
                )));
            }
            Some(update_response::Response::Accepted(_)) => {
                self.on_event(UpdateMachineEvents::Accept)
            }
            Some(update_response::Response::Rejected(f)) => {
                self.on_event(UpdateMachineEvents::Reject(f))
            }
            Some(update_response::Response::Completed(p)) => {
                self.on_event(UpdateMachineEvents::Complete(p))
            }
        }
        .map_err(|e| match e {
            MachineError::InvalidTransition => WFMachinesError::Nondeterminism(format!(
                "Invalid transition while handling update response (id {}) in state {}",
                &self.shared_state.meta.update_id,
                self.state(),
            )),
            MachineError::Underlying(e) => e,
        })?;
        cmds.into_iter()
            .map(|c| self.adapt_response(c, None))
            .flatten_ok()
            .try_collect()
    }

    fn build_command_msg(
        &self,
        outgoing_id: String,
        msg: UpdateMsg,
    ) -> Result<Vec<MachineResponse>, WFMachinesError> {
        Ok(vec![
            self.build_msg(outgoing_id.clone(), msg)?,
            MachineResponse::IssueNewCommand(
                command::Attributes::ProtocolMessageCommandAttributes(
                    ProtocolMessageCommandAttributes {
                        message_id: outgoing_id,
                    },
                )
                .into(),
            ),
        ])
    }

    /// Build an outgoing protocol message.
    fn build_msg(
        &self,
        outgoing_id: String,
        msg: UpdateMsg,
    ) -> Result<MachineResponse, WFMachinesError> {
        let accept_body = msg.pack().map_err(|e| {
            WFMachinesError::Fatal(format!("Failed to serialize update response: {e:?}"))
        })?;
        Ok(MachineResponse::IssueNewMessage(ProtocolMessage {
            id: outgoing_id.clone(),
            protocol_instance_id: self.shared_state.instance_id.clone(),
            body: Some(accept_body),
            ..Default::default()
        }))
    }
}

enum UpdateMsg {
    Accept(Acceptance),
    Reject(Rejection),
    Response(Response),
}
impl UpdateMsg {
    fn pack(self) -> Result<prost_types::Any, EncodeError> {
        match self {
            UpdateMsg::Accept(m) => pack_any(
                "type.googleapis.com/temporal.api.update.v1.Acceptance".to_string(),
                &m,
            ),
            UpdateMsg::Reject(m) => pack_any(
                "type.googleapis.com/temporal.api.update.v1.Rejection".to_string(),
                &m,
            ),
            UpdateMsg::Response(m) => pack_any(
                "type.googleapis.com/temporal.api.update.v1.Response".to_string(),
                &m,
            ),
        }
    }
}

impl TryFrom<HistEventData> for UpdateMachineEvents {
    type Error = WFMachinesError;

    fn try_from(e: HistEventData) -> Result<Self, Self::Error> {
        let e = e.event;
        Ok(match e.event_type() {
            EventType::WorkflowExecutionUpdateAccepted => {
                UpdateMachineEvents::WorkflowExecutionUpdateAccepted
            }
            EventType::WorkflowExecutionUpdateCompleted => {
                UpdateMachineEvents::WorkflowExecutionUpdateCompleted
            }
            _ => {
                return Err(WFMachinesError::Nondeterminism(format!(
                    "Update machine does not handle this event: {e}"
                )));
            }
        })
    }
}

impl WFMachinesAdapter for UpdateMachine {
    fn adapt_response(
        &self,
        my_command: UpdateMachineCommand,
        _event_info: Option<EventInfo>,
    ) -> Result<Vec<MachineResponse>, WFMachinesError> {
        Ok(match my_command {
            UpdateMachineCommand::Accept(orig) => self.build_command_msg(
                format!("{}/accept", self.shared_state.message_id),
                UpdateMsg::Accept(Acceptance {
                    accepted_request_message_id: self.shared_state.message_id.clone(),
                    accepted_request_sequencing_event_id: self.shared_state.event_seq_id,
                    accepted_request: Some(orig),
                }),
            )?,
            UpdateMachineCommand::Reject(orig, fail) => {
                vec![self.build_msg(
                    format!("{}/reject", self.shared_state.message_id),
                    UpdateMsg::Reject(Rejection {
                        rejected_request_message_id: self.shared_state.message_id.clone(),
                        rejected_request_sequencing_event_id: self.shared_state.event_seq_id,
                        rejected_request: Some(orig),
                        failure: Some(fail),
                    }),
                )?]
            }
            UpdateMachineCommand::Complete(p) => self.build_command_msg(
                format!("{}/complete", self.shared_state.message_id),
                UpdateMsg::Response(Response {
                    meta: Some(self.shared_state.meta.clone()),
                    outcome: Some(Outcome {
                        value: Some(outcome::Value::Success(p.into())),
                    }),
                }),
            )?,
            UpdateMachineCommand::Fail(f) => self.build_command_msg(
                format!("{}/complete", self.shared_state.message_id),
                UpdateMsg::Response(Response {
                    meta: Some(self.shared_state.meta.clone()),
                    outcome: Some(Outcome {
                        value: Some(outcome::Value::Failure(f)),
                    }),
                }),
            )?,
        })
    }
}

impl TryFrom<CommandType> for UpdateMachineEvents {
    type Error = ();

    fn try_from(c: CommandType) -> Result<Self, Self::Error> {
        Ok(match c {
            CommandType::ProtocolMessage => UpdateMachineEvents::CommandProtocolMessage,
            _ => return Err(()),
        })
    }
}

#[derive(Default, Clone)]
pub(super) struct RequestInitiated {
    original_request: update::v1::Request,
}
impl RequestInitiated {
    fn on_accept(self) -> UpdateMachineTransition<Accepted> {
        UpdateMachineTransition::commands([UpdateMachineCommand::Accept(self.original_request)])
    }
    fn on_reject(self, fail: Failure) -> UpdateMachineTransition<Rejected> {
        UpdateMachineTransition::commands([UpdateMachineCommand::Reject(
            self.original_request,
            fail,
        )])
    }
}

#[derive(Default, Clone)]
pub(super) struct Accepted {}
impl From<RequestInitiated> for Accepted {
    fn from(_: RequestInitiated) -> Self {
        Accepted {}
    }
}
impl Accepted {
    fn on_complete(self, p: Payload) -> UpdateMachineTransition<CompletedImmediately> {
        UpdateMachineTransition::commands([UpdateMachineCommand::Complete(p)])
    }
    fn on_fail(self, f: Failure) -> UpdateMachineTransition<CompletedImmediately> {
        UpdateMachineTransition::commands([UpdateMachineCommand::Fail(f)])
    }
}

#[derive(Default, Clone)]
pub(super) struct AcceptCommandCreated {}
impl From<Accepted> for AcceptCommandCreated {
    fn from(_: Accepted) -> Self {
        AcceptCommandCreated {}
    }
}
impl AcceptCommandCreated {
    fn on_complete(self, p: Payload) -> UpdateMachineTransition<CompletedImmediatelyAcceptCreated> {
        UpdateMachineTransition::commands([UpdateMachineCommand::Complete(p)])
    }
    fn on_fail(self, f: Failure) -> UpdateMachineTransition<CompletedImmediatelyAcceptCreated> {
        UpdateMachineTransition::commands([UpdateMachineCommand::Fail(f)])
    }
}

#[derive(Default, Clone)]
pub(super) struct AcceptCommandRecorded {}
impl AcceptCommandRecorded {
    fn on_complete(self, p: Payload) -> UpdateMachineTransition<Completed> {
        UpdateMachineTransition::commands([UpdateMachineCommand::Complete(p)])
    }
    fn on_fail(self, f: Failure) -> UpdateMachineTransition<Completed> {
        UpdateMachineTransition::commands([UpdateMachineCommand::Fail(f)])
    }
}
impl From<AcceptCommandCreated> for AcceptCommandRecorded {
    fn from(_: AcceptCommandCreated) -> Self {
        AcceptCommandRecorded {}
    }
}

#[derive(Default, Clone)]
pub(super) struct Completed {}
impl From<AcceptCommandRecorded> for Completed {
    fn from(_: AcceptCommandRecorded) -> Self {
        Completed {}
    }
}

#[derive(Default, Clone)]
pub(super) struct CompletedCommandCreated {}
impl From<Completed> for CompletedCommandCreated {
    fn from(_: Completed) -> Self {
        CompletedCommandCreated {}
    }
}
impl From<CompletedImmediatelyCompleteCreated> for CompletedCommandCreated {
    fn from(_: CompletedImmediatelyCompleteCreated) -> Self {
        CompletedCommandCreated {}
    }
}

#[derive(Default, Clone)]
pub(super) struct CompletedCommandRecorded {}
impl From<CompletedCommandCreated> for CompletedCommandRecorded {
    fn from(_: CompletedCommandCreated) -> Self {
        CompletedCommandRecorded {}
    }
}

#[derive(Default, Clone)]
pub(super) struct Rejected {}
impl From<RequestInitiated> for Rejected {
    fn from(_: RequestInitiated) -> Self {
        Rejected {}
    }
}

#[derive(Default, Clone)]
pub(super) struct CompletedImmediately {}

#[derive(Default, Clone)]
pub(super) struct CompletedImmediatelyAcceptCreated {}
impl From<CompletedImmediately> for CompletedImmediatelyAcceptCreated {
    fn from(_: CompletedImmediately) -> Self {
        CompletedImmediatelyAcceptCreated {}
    }
}

#[derive(Default, Clone)]
pub(super) struct CompletedImmediatelyCompleteCreated {}
impl From<CompletedImmediatelyAcceptCreated> for CompletedImmediatelyCompleteCreated {
    fn from(_: CompletedImmediatelyAcceptCreated) -> Self {
        CompletedImmediatelyCompleteCreated {}
    }
}
