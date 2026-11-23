use crate::{
    data_converters::{
        GenericPayloadConverter, PayloadConversionError, PayloadConverter, SerializationContext,
        SerializationContextData, TemporalDeserializable,
    },
    protos::temporal::api::common::v1::{Memo as ProtoMemo, Payload},
};

/// A collection of memo payloads that can be deserialized into typed values.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct Memo {
    raw: ProtoMemo,
    payload_converter: PayloadConverter,
    context: SerializationContextData,
}

impl Memo {
    /// Construct a memo with the payload converter and serialization context associated with its
    /// source.
    #[doc(hidden)]
    pub fn from_raw(
        raw: Option<ProtoMemo>,
        payload_converter: PayloadConverter,
        context: SerializationContextData,
    ) -> Self {
        Self {
            raw: raw.unwrap_or_default(),
            payload_converter,
            context,
        }
    }

    /// Decode a memo value as `T`, returning `None` when the key is absent.
    pub fn get<T: TemporalDeserializable + 'static>(
        &self,
        key: &str,
    ) -> Result<Option<T>, PayloadConversionError> {
        let Some(payload) = self.raw.fields.get(key) else {
            return Ok(None);
        };
        self.payload_converter
            .from_payload(
                &SerializationContext {
                    data: &self.context,
                    converter: &self.payload_converter,
                },
                payload.clone(),
            )
            .map(Some)
    }

    /// Returns whether the memo contains `key`.
    pub fn contains_key(&self, key: &str) -> bool {
        self.raw.fields.contains_key(key)
    }

    /// Returns the number of memo entries.
    pub fn len(&self) -> usize {
        self.raw.fields.len()
    }

    /// Returns whether the memo has no entries.
    pub fn is_empty(&self) -> bool {
        self.raw.fields.is_empty()
    }

    /// Iterates over memo keys.
    pub fn keys(&self) -> impl Iterator<Item = &str> {
        self.raw.fields.keys().map(String::as_str)
    }

    /// Returns the underlying payload without applying payload conversion.
    pub fn raw_value(&self, key: &str) -> Option<&Payload> {
        self.raw.fields.get(key)
    }

    /// Access the underlying memo protobuf.
    pub fn raw(&self) -> &ProtoMemo {
        &self.raw
    }

    /// Consume this wrapper and return the underlying memo protobuf.
    pub fn into_raw(self) -> ProtoMemo {
        self.raw
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn memo_decodes_serialized_values() {
        let payload_converter = PayloadConverter::default();
        let context = SerializationContext {
            data: &SerializationContextData::Workflow,
            converter: &payload_converter,
        };
        let payload = payload_converter.to_payload(&context, &7_u32).unwrap();
        let raw = ProtoMemo {
            fields: HashMap::from([("count".to_owned(), payload.clone())]),
        };
        let memo = Memo::from_raw(
            Some(raw.clone()),
            payload_converter,
            SerializationContextData::Workflow,
        );

        assert_eq!(memo.get::<u32>("count").unwrap(), Some(7));
        assert_eq!(memo.get::<u32>("missing").unwrap(), None);
        assert_eq!(memo.raw_value("count"), Some(&payload));
        assert_eq!(memo.into_raw(), raw);
    }

    #[test]
    fn memo_reports_deserialization_errors() {
        let payload_converter = PayloadConverter::default();
        let context = SerializationContext {
            data: &SerializationContextData::Workflow,
            converter: &payload_converter,
        };
        let payload = payload_converter.to_payload(&context, &7_u32).unwrap();
        let memo = Memo::from_raw(
            Some(ProtoMemo {
                fields: HashMap::from([("count".to_owned(), payload)]),
            }),
            payload_converter,
            SerializationContextData::Workflow,
        );

        assert!(memo.get::<String>("count").is_err());
    }
}
