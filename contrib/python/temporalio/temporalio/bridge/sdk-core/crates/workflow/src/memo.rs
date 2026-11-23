use std::{collections::BTreeMap, rc::Rc};

use temporalio_common_wasm::{
    data_converters::{
        GenericPayloadConverter, PayloadConversionError, PayloadConverter, SerializationContext,
        SerializationContextData, TemporalSerializable,
    },
    protos::temporal::api::common::v1::Payload,
};

trait SerializableMemoValue {
    fn to_payload(
        &self,
        payload_converter: &PayloadConverter,
    ) -> Result<Payload, PayloadConversionError>;
}

impl<T> SerializableMemoValue for T
where
    T: TemporalSerializable + 'static,
{
    fn to_payload(
        &self,
        payload_converter: &PayloadConverter,
    ) -> Result<Payload, PayloadConversionError> {
        payload_converter.to_payload(
            &SerializationContext {
                data: &SerializationContextData::Workflow,
                converter: payload_converter,
            },
            self,
        )
    }
}

/// A typed value used in a workflow memo update.
#[derive(Clone, derive_more::Debug)]
#[non_exhaustive]
pub struct MemoValue {
    #[debug(skip)]
    value: Rc<dyn SerializableMemoValue>,
}

impl MemoValue {
    /// Create a memo value that will be serialized with the workflow's data converter.
    pub fn new<T: TemporalSerializable + 'static>(value: T) -> Self {
        Self {
            value: Rc::new(value),
        }
    }

    pub(crate) fn to_payload(
        &self,
        payload_converter: &PayloadConverter,
    ) -> Result<Payload, PayloadConversionError> {
        self.value.to_payload(payload_converter)
    }
}

/// A complete set of memo values for a new workflow execution.
#[derive(Clone, Debug, Default)]
#[non_exhaustive]
pub struct MemoValues {
    values: BTreeMap<String, MemoValue>,
}

impl MemoValues {
    /// Create an empty set of memo values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add or replace a memo value.
    pub fn insert<T>(&mut self, key: impl Into<String>, value: T) -> &mut Self
    where
        T: TemporalSerializable + 'static,
    {
        self.values.insert(key.into(), MemoValue::new(value));
        self
    }

    pub(crate) fn encode(
        &self,
        payload_converter: &PayloadConverter,
    ) -> Result<std::collections::HashMap<String, Payload>, PayloadConversionError> {
        self.values
            .iter()
            .map(|(key, value)| {
                value
                    .to_payload(payload_converter)
                    .map(|payload| (key.clone(), payload))
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use temporalio_common_wasm::{Memo, protos::temporal::api::common::v1::Memo as ProtoMemo};

    #[test]
    fn memo_values_serialize_heterogeneous_values() {
        let payload_converter = PayloadConverter::default();
        let mut values = MemoValues::new();
        values
            .insert("count", 7_u32)
            .insert("label", "hello".to_string());

        let memo = Memo::from_raw(
            Some(ProtoMemo {
                fields: values.encode(&payload_converter).unwrap(),
            }),
            payload_converter,
            SerializationContextData::Workflow,
        );

        assert_eq!(memo.get::<u32>("count").unwrap(), Some(7));
        assert_eq!(
            memo.get::<String>("label").unwrap(),
            Some("hello".to_string())
        );
    }
}
