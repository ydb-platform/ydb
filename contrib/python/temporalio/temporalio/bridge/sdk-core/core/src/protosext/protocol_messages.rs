use anyhow::{anyhow, bail};
use std::collections::HashMap;
use temporal_sdk_core_protos::temporal::api::{
    common::v1::Payload,
    history::v1::{HistoryEvent, history_event},
    protocol::v1::{Message, message::SequencingId},
    update,
};

/// A decoded & verified of a [Message] that came with a WFT.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct IncomingProtocolMessage {
    pub(crate) id: String,
    pub(crate) protocol_instance_id: String,
    pub(crate) sequencing_id: Option<SequencingId>,
    pub(crate) body: IncomingProtocolMessageBody,
}

impl IncomingProtocolMessage {
    pub(crate) fn processable_after_event_id(&self) -> Option<i64> {
        match self.sequencing_id {
            None => Some(0),
            Some(SequencingId::EventId(id)) => Some(id),
            Some(SequencingId::CommandIndex(_)) => None,
        }
    }
}

impl TryFrom<Message> for IncomingProtocolMessage {
    type Error = anyhow::Error;

    fn try_from(m: Message) -> Result<Self, Self::Error> {
        let body = m.body.try_into()?;
        Ok(Self {
            id: m.id,
            protocol_instance_id: m.protocol_instance_id,
            sequencing_id: m.sequencing_id,
            body,
        })
    }
}

impl TryFrom<&HistoryEvent> for IncomingProtocolMessage {
    type Error = anyhow::Error;

    fn try_from(event: &HistoryEvent) -> Result<Self, Self::Error> {
        match event.attributes {
            Some(history_event::Attributes::WorkflowExecutionUpdateAdmittedEventAttributes(
                ref atts,
            )) => {
                let request = atts.request.as_ref().ok_or_else(|| {
                    anyhow!("Update admitted event must contain request".to_string())
                })?;
                let protocol_instance_id = request
                    .meta
                    .as_ref()
                    .ok_or_else(|| {
                        anyhow!("Update request's `meta` field must be populated".to_string())
                    })?
                    .update_id
                    .clone();
                Ok(IncomingProtocolMessage {
                    id: format!("{protocol_instance_id}/request"),
                    protocol_instance_id,
                    // For an UpdateAdmitted history event (i.e. a "durable update request"), the sequencing event ID is
                    // the event ID itself.
                    sequencing_id: Some(SequencingId::EventId(event.event_id)),
                    body: IncomingProtocolMessageBody::UpdateRequest(request.clone().try_into()?),
                })
            }
            Some(history_event::Attributes::WorkflowExecutionUpdateAcceptedEventAttributes(
                ref atts,
            )) => Ok(IncomingProtocolMessage {
                id: atts.accepted_request_message_id.clone(),
                protocol_instance_id: atts.protocol_instance_id.clone(),
                // For an UpdateAccepted history event, the sequencing event ID is the sequencing event ID of the
                // accepted request. This is available as a field on the UpdateAccepted event (it is sent by the worker
                // in the accepted message).
                sequencing_id: Some(SequencingId::EventId(
                    atts.accepted_request_sequencing_event_id,
                )),
                body: IncomingProtocolMessageBody::UpdateRequest(
                    atts.accepted_request
                        .clone()
                        .ok_or_else(|| {
                            anyhow!("Update accepted event must contain accepted request")
                        })?
                        .try_into()?,
                ),
            }),
            _ => Err(anyhow!(
                "Cannot convert event of type {} into protocol message. This is an sdk-core bug.",
                event.event_type().as_str_name()
            )),
        }
    }
}

/// All the protocol [Message] bodies Core understands that might come to us when receiving a new
/// WFT.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum IncomingProtocolMessageBody {
    UpdateRequest(UpdateRequest),
}

impl TryFrom<Option<prost_types::Any>> for IncomingProtocolMessageBody {
    type Error = anyhow::Error;

    fn try_from(v: Option<prost_types::Any>) -> Result<Self, Self::Error> {
        let v = v.ok_or_else(|| anyhow!("Protocol message body must be populated"))?;
        // Undo explicit type url checks when https://github.com/fdeantoni/prost-wkt/issues/48 is
        // fixed
        Ok(match v.type_url.as_str() {
            "type.googleapis.com/temporal.api.update.v1.Request" => {
                IncomingProtocolMessageBody::UpdateRequest(
                    v.unpack_as(update::v1::Request::default())?.try_into()?,
                )
            }
            o => bail!("Could not understand protocol message type {}", o),
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct UpdateRequest {
    pub(crate) original: update::v1::Request,
}

impl UpdateRequest {
    pub(crate) fn name(&self) -> &str {
        &self
            .original
            .input
            .as_ref()
            .expect("Update request's `input` field must be populated")
            .name
    }

    pub(crate) fn headers(&self) -> HashMap<String, Payload> {
        self.original
            .input
            .as_ref()
            .expect("Update request's `input` field must be populated")
            .header
            .clone()
            .map(Into::into)
            .unwrap_or_default()
    }

    pub(crate) fn input(&self) -> Vec<Payload> {
        self.original
            .input
            .as_ref()
            .expect("Update request's `input` field must be populated")
            .args
            .clone()
            .map(|ps| ps.payloads)
            .unwrap_or_default()
    }

    pub(crate) fn meta(&self) -> &update::v1::Meta {
        self.original
            .meta
            .as_ref()
            .expect("Update request's `meta` field must be populated")
    }
}

impl TryFrom<update::v1::Request> for UpdateRequest {
    type Error = anyhow::Error;

    fn try_from(r: update::v1::Request) -> Result<Self, Self::Error> {
        if r.input.is_none() {
            return Err(anyhow!("Update request's `input` field must be populated"));
        }
        if r.meta.is_none() {
            return Err(anyhow!("Update request's `meta` field must be populated"));
        }
        Ok(UpdateRequest { original: r })
    }
}
