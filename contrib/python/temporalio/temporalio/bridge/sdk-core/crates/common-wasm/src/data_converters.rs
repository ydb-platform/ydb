//! Contains traits for and default implementations of data converters, codecs, and other
//! serialization related functionality.

mod failure_converter;

pub use failure_converter::{
    ActivityExecutionDecodeHint, ChildWorkflowExecutionDecodeHint, ChildWorkflowStartDecodeHint,
    DefaultFailureConverter, FailureConverter, FailureDecodeHint, NoopDecodeHint,
    WorkflowSignalDecodeHint,
};

use crate::protos::temporal::api::common::v1::Payload;
use futures::{FutureExt, future::BoxFuture};
use std::{collections::HashMap, sync::Arc};

/// Combines a [`PayloadConverter`], [`FailureConverter`], and [`PayloadCodec`] to handle all
/// serialization needs for communicating with the Temporal server.
#[derive(Clone)]
pub struct DataConverter {
    payload_converter: PayloadConverter,
    #[allow(dead_code)] // Will be used for failure conversion
    failure_converter: Arc<dyn FailureConverter + Send + Sync>,
    codec: Arc<dyn PayloadCodec + Send + Sync>,
}

impl std::fmt::Debug for DataConverter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DataConverter")
            .field("payload_converter", &self.payload_converter)
            .finish_non_exhaustive()
    }
}

impl DataConverter {
    /// Create a new DataConverter with the given payload converter, failure converter, and codec.
    pub fn new(
        payload_converter: PayloadConverter,
        failure_converter: impl FailureConverter + Send + Sync + 'static,
        codec: impl PayloadCodec + Send + Sync + 'static,
    ) -> Self {
        Self {
            payload_converter,
            failure_converter: Arc::new(failure_converter),
            codec: Arc::new(codec),
        }
    }

    /// Serialize a value into a single payload, applying the codec.
    pub async fn to_payload<T: TemporalSerializable + 'static>(
        &self,
        data: &SerializationContextData,
        val: &T,
    ) -> Result<Payload, PayloadConversionError> {
        let context = SerializationContext {
            data,
            converter: &self.payload_converter,
        };
        let payload = self.payload_converter.to_payload(&context, val)?;
        let encoded = self.codec.encode(data, vec![payload]).await?;
        encoded
            .into_iter()
            .next()
            .ok_or(PayloadConversionError::WrongEncoding)
    }

    /// Deserialize a value from a single payload, applying the codec.
    pub async fn from_payload<T: TemporalDeserializable + 'static>(
        &self,
        data: &SerializationContextData,
        payload: Payload,
    ) -> Result<T, PayloadConversionError> {
        let context = SerializationContext {
            data,
            converter: &self.payload_converter,
        };
        let decoded = self.codec.decode(data, vec![payload]).await?;
        let payload = decoded
            .into_iter()
            .next()
            .ok_or(PayloadConversionError::WrongEncoding)?;
        self.payload_converter.from_payload(&context, payload)
    }

    /// Serialize a value into multiple payloads (e.g. for multi-arg support), applying the codec.
    pub async fn to_payloads<T: TemporalSerializable + 'static>(
        &self,
        data: &SerializationContextData,
        val: &T,
    ) -> Result<Vec<Payload>, PayloadConversionError> {
        let context = SerializationContext {
            data,
            converter: &self.payload_converter,
        };
        let payloads = self.payload_converter.to_payloads(&context, val)?;
        self.codec.encode(data, payloads).await
    }

    /// Deserialize a value from multiple payloads (e.g. for multi-arg support), applying the codec.
    pub async fn from_payloads<T: TemporalDeserializable + 'static>(
        &self,
        data: &SerializationContextData,
        payloads: Vec<Payload>,
    ) -> Result<T, PayloadConversionError> {
        let context = SerializationContext {
            data,
            converter: &self.payload_converter,
        };
        let decoded = self.codec.decode(data, payloads).await?;
        self.payload_converter.from_payloads(&context, decoded)
    }

    /// Returns the payload converter component of this data converter.
    pub fn payload_converter(&self) -> &PayloadConverter {
        &self.payload_converter
    }

    /// Returns the failure converter component of this data converter.
    pub fn failure_converter(&self) -> &(dyn FailureConverter + Send + Sync) {
        self.failure_converter.as_ref()
    }

    /// Decode a Temporal failure into a caller-facing Rust error surface.
    pub fn to_error<H: FailureDecodeHint>(
        &self,
        context: &SerializationContextData,
        failure: crate::protos::temporal::api::failure::v1::Failure,
        hint: H,
    ) -> Result<H::Output, PayloadConversionError> {
        let normalized =
            self.failure_converter
                .to_error(failure, &self.payload_converter, context)?;
        Ok(hint.adapt(normalized))
    }

    /// Encode a typed Rust error surface into a Temporal failure.
    pub fn to_failure(
        &self,
        context: &SerializationContextData,
        error: crate::error::OutgoingError,
    ) -> crate::protos::temporal::api::failure::v1::Failure {
        self.failure_converter
            .to_failure(error, &self.payload_converter, context)
    }

    /// Returns the codec component of this data converter.
    pub fn codec(&self) -> &(dyn PayloadCodec + Send + Sync) {
        self.codec.as_ref()
    }
}

/// Data about the serialization context, indicating where the serialization is occurring.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SerializationContextData {
    /// Serialization is occurring in a workflow context.
    Workflow,
    /// Serialization is occurring in an activity context.
    Activity,
    /// Serialization is occurring in a nexus context.
    Nexus,
    /// No specific serialization context.
    None,
}

/// Context for serialization operations, including the kind of context and the
/// payload converter for nested serialization.
#[derive(Clone, Copy)]
pub struct SerializationContext<'a> {
    /// The kind of serialization context (workflow, activity, etc.).
    pub data: &'a SerializationContextData,
    /// Allows nested types to serialize their contents using the same converter.
    pub converter: &'a PayloadConverter,
}
/// Converts values to and from [`Payload`]s using different encoding strategies.
#[derive(Clone)]
pub enum PayloadConverter {
    /// Uses a serde-based converter for encoding/decoding.
    Serde(Arc<dyn ErasedSerdePayloadConverter>),
    /// This variant signals the user wants to delegate to wrapper types
    UseWrappers,
    /// Tries multiple converters in order until one succeeds.
    Composite(Arc<CompositePayloadConverter>),
}

impl std::fmt::Debug for PayloadConverter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PayloadConverter::Serde(_) => write!(f, "PayloadConverter::Serde(...)"),
            PayloadConverter::UseWrappers => write!(f, "PayloadConverter::UseWrappers"),
            PayloadConverter::Composite(_) => write!(f, "PayloadConverter::Composite(...)"),
        }
    }
}
impl PayloadConverter {
    /// Create a payload converter that uses JSON serialization via serde.
    pub fn serde_json() -> Self {
        Self::Serde(Arc::new(SerdeJsonPayloadConverter))
    }
    // TODO [rust-sdk-branch]: Proto binary, other standard built-ins
}

impl Default for PayloadConverter {
    fn default() -> Self {
        Self::Composite(Arc::new(CompositePayloadConverter {
            converters: vec![Self::UseWrappers, Self::serde_json()],
        }))
    }
}

/// Errors that can occur during payload conversion.
#[derive(Debug)]
pub enum PayloadConversionError {
    /// The payload's encoding does not match what the converter expects.
    WrongEncoding,
    /// An error occurred during encoding or decoding.
    EncodingError(Box<dyn std::error::Error + Send + Sync>),
}

impl std::fmt::Display for PayloadConversionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PayloadConversionError::WrongEncoding => write!(f, "Wrong encoding"),
            PayloadConversionError::EncodingError(err) => write!(f, "Encoding error: {}", err),
        }
    }
}

impl std::error::Error for PayloadConversionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            PayloadConversionError::WrongEncoding => None,
            PayloadConversionError::EncodingError(err) => Some(err.as_ref()),
        }
    }
}

/// Encodes and decodes payloads, enabling encryption or compression.
///
/// Operational codec failures should be returned as
/// [`PayloadConversionError::EncodingError`].
pub trait PayloadCodec {
    /// Encode payloads before they are sent to the server.
    fn encode(
        &self,
        context: &SerializationContextData,
        payloads: Vec<Payload>,
    ) -> BoxFuture<'static, Result<Vec<Payload>, PayloadConversionError>>;
    /// Decode payloads after they are received from the server.
    fn decode(
        &self,
        context: &SerializationContextData,
        payloads: Vec<Payload>,
    ) -> BoxFuture<'static, Result<Vec<Payload>, PayloadConversionError>>;
}

impl<T: PayloadCodec> PayloadCodec for Arc<T> {
    fn encode(
        &self,
        context: &SerializationContextData,
        payloads: Vec<Payload>,
    ) -> BoxFuture<'static, Result<Vec<Payload>, PayloadConversionError>> {
        (**self).encode(context, payloads)
    }
    fn decode(
        &self,
        context: &SerializationContextData,
        payloads: Vec<Payload>,
    ) -> BoxFuture<'static, Result<Vec<Payload>, PayloadConversionError>> {
        (**self).decode(context, payloads)
    }
}

/// A no-op codec that passes payloads through unchanged.
pub struct DefaultPayloadCodec;

/// Indicates some type can be serialized for use with Temporal.
///
/// You don't need to implement this unless you are using a non-serde-compatible custom converter,
/// in which case you should implement the to/from_payload functions on some wrapper type.
pub trait TemporalSerializable {
    /// Return a reference to this value as a serde-serializable trait object.
    fn as_serde(&self) -> Result<&dyn erased_serde::Serialize, PayloadConversionError> {
        Err(PayloadConversionError::WrongEncoding)
    }
    /// Convert this value into a single [`Payload`].
    fn to_payload(&self, _: &SerializationContext<'_>) -> Result<Payload, PayloadConversionError> {
        Err(PayloadConversionError::WrongEncoding)
    }
    /// Convert to multiple payloads. Override this for types representing multiple arguments.
    fn to_payloads(
        &self,
        ctx: &SerializationContext<'_>,
    ) -> Result<Vec<Payload>, PayloadConversionError> {
        Ok(vec![self.to_payload(ctx)?])
    }
}

/// Indicates some type can be deserialized for use with Temporal.
///
/// You don't need to implement this unless you are using a non-serde-compatible custom converter,
/// in which case you should implement the to/from_payload functions on some wrapper type.
pub trait TemporalDeserializable: Sized {
    /// Deserialize from a serde-based payload converter.
    fn from_serde(
        _: &dyn ErasedSerdePayloadConverter,
        _ctx: &SerializationContext<'_>,
        _: Payload,
    ) -> Result<Self, PayloadConversionError> {
        Err(PayloadConversionError::WrongEncoding)
    }
    /// Deserialize from a single [`Payload`].
    fn from_payload(
        ctx: &SerializationContext<'_>,
        payload: Payload,
    ) -> Result<Self, PayloadConversionError> {
        let _ = (ctx, payload);
        Err(PayloadConversionError::WrongEncoding)
    }
    /// Convert from multiple payloads. Override this for types representing multiple arguments.
    fn from_payloads(
        ctx: &SerializationContext<'_>,
        payloads: Vec<Payload>,
    ) -> Result<Self, PayloadConversionError> {
        if payloads.len() != 1 {
            return Err(PayloadConversionError::WrongEncoding);
        }
        Self::from_payload(ctx, payloads.into_iter().next().unwrap())
    }
}

/// A codec-decoded set of payloads that can be deserialized later with to a user provided type.
#[derive(Clone, Debug)]
pub struct DecodablePayloads {
    payloads: Vec<Payload>,
    payload_converter: PayloadConverter,
    context: SerializationContextData,
}

impl DecodablePayloads {
    /// Create a new decodable payload set from raw payloads and the converter context needed to
    /// deserialize them later.
    pub fn new(
        payloads: Vec<Payload>,
        payload_converter: PayloadConverter,
        context: SerializationContextData,
    ) -> Self {
        Self {
            payloads,
            payload_converter,
            context,
        }
    }

    /// Deserialize these payloads into a typed value using the stored payload converter.
    pub fn deserialize<T: TemporalDeserializable + 'static>(
        &self,
    ) -> Result<T, PayloadConversionError> {
        self.payload_converter.from_payloads(
            &SerializationContext {
                data: &self.context,
                converter: &self.payload_converter,
            },
            self.payloads.clone(),
        )
    }

    /// Returns the underlying payloads.
    pub fn raw(&self) -> &[Payload] {
        &self.payloads
    }

    /// Consume this value and return the underlying payloads as a [`RawValue`].
    pub fn into_raw(self) -> RawValue {
        RawValue::new(self.payloads)
    }
}

/// An unconverted set of payloads, used when the caller wants to defer deserialization.
#[derive(Clone, Debug, Default)]
pub struct RawValue {
    /// The underlying payloads.
    pub payloads: Vec<Payload>,
}
impl RawValue {
    /// A RawValue representing no meaningful data, containing a single default payload.
    /// This ensures the value can still be serialized as a single payload.
    pub fn empty() -> Self {
        Self {
            payloads: vec![Payload::default()],
        }
    }

    /// Create a new RawValue from a vector of payloads.
    pub fn new(payloads: Vec<Payload>) -> Self {
        Self { payloads }
    }

    /// Create a [`RawValue`] by serializing a value with the given converter.
    pub fn from_value<T: TemporalSerializable + 'static>(
        value: &T,
        converter: &PayloadConverter,
    ) -> RawValue {
        RawValue::new(vec![
            converter
                .to_payload(
                    &SerializationContext {
                        data: &SerializationContextData::None,
                        converter,
                    },
                    value,
                )
                .unwrap(),
        ])
    }

    /// Deserialize this [`RawValue`] into a typed value using the given converter.
    pub fn to_value<T: TemporalDeserializable + 'static>(self, converter: &PayloadConverter) -> T {
        converter
            .from_payload(
                &SerializationContext {
                    data: &SerializationContextData::None,
                    converter,
                },
                self.payloads.into_iter().next().unwrap(),
            )
            .unwrap()
    }
}

impl TemporalSerializable for RawValue {
    fn to_payload(&self, _: &SerializationContext<'_>) -> Result<Payload, PayloadConversionError> {
        Ok(self.payloads.first().cloned().unwrap_or_default())
    }
    fn to_payloads(
        &self,
        _: &SerializationContext<'_>,
    ) -> Result<Vec<Payload>, PayloadConversionError> {
        Ok(self.payloads.clone())
    }
}

impl TemporalDeserializable for RawValue {
    fn from_payload(
        _: &SerializationContext<'_>,
        p: Payload,
    ) -> Result<Self, PayloadConversionError> {
        Ok(RawValue { payloads: vec![p] })
    }
    fn from_payloads(
        _: &SerializationContext<'_>,
        payloads: Vec<Payload>,
    ) -> Result<Self, PayloadConversionError> {
        Ok(RawValue { payloads })
    }
}

/// Generic interface for converting between typed values and [`Payload`]s.
pub trait GenericPayloadConverter {
    /// Serialize a value into a single [`Payload`].
    fn to_payload<T: TemporalSerializable + 'static>(
        &self,
        context: &SerializationContext<'_>,
        val: &T,
    ) -> Result<Payload, PayloadConversionError>;
    /// Deserialize a value from a single [`Payload`].
    #[allow(clippy::wrong_self_convention)]
    fn from_payload<T: TemporalDeserializable + 'static>(
        &self,
        context: &SerializationContext<'_>,
        payload: Payload,
    ) -> Result<T, PayloadConversionError>;
    /// Serialize a value into multiple [`Payload`]s.
    fn to_payloads<T: TemporalSerializable + 'static>(
        &self,
        context: &SerializationContext<'_>,
        val: &T,
    ) -> Result<Vec<Payload>, PayloadConversionError> {
        Ok(vec![self.to_payload(context, val)?])
    }
    /// Deserialize a value from multiple [`Payload`]s.
    #[allow(clippy::wrong_self_convention)]
    fn from_payloads<T: TemporalDeserializable + 'static>(
        &self,
        context: &SerializationContext<'_>,
        payloads: Vec<Payload>,
    ) -> Result<T, PayloadConversionError> {
        if payloads.len() != 1 {
            return Err(PayloadConversionError::WrongEncoding);
        }
        self.from_payload(context, payloads.into_iter().next().unwrap())
    }
}

impl GenericPayloadConverter for PayloadConverter {
    fn to_payload<T: TemporalSerializable + 'static>(
        &self,
        context: &SerializationContext<'_>,
        val: &T,
    ) -> Result<Payload, PayloadConversionError> {
        // If a single payload is explicitly needed for `()`, then produce a null payload
        if std::any::TypeId::of::<T>() == std::any::TypeId::of::<()>() {
            return Ok(Payload {
                metadata: {
                    let mut hm = HashMap::new();
                    hm.insert("encoding".to_string(), b"binary/null".to_vec());
                    hm
                },
                data: vec![],
                external_payloads: vec![],
            });
        }
        let mut payloads = self.to_payloads(context, val)?;
        if payloads.len() != 1 {
            return Err(PayloadConversionError::WrongEncoding);
        }
        Ok(payloads.pop().unwrap())
    }

    fn from_payload<T: TemporalDeserializable + 'static>(
        &self,
        context: &SerializationContext<'_>,
        payload: Payload,
    ) -> Result<T, PayloadConversionError> {
        self.from_payloads(context, vec![payload])
    }

    fn to_payloads<T: TemporalSerializable + 'static>(
        &self,
        context: &SerializationContext<'_>,
        val: &T,
    ) -> Result<Vec<Payload>, PayloadConversionError> {
        match self {
            PayloadConverter::Serde(pc) => {
                // Since Rust SDK uses () to denote no input, we must match other SDKs by producing
                // no payloads for it.
                if std::any::TypeId::of::<T>() == std::any::TypeId::of::<()>() {
                    Ok(Vec::new())
                } else {
                    Ok(vec![pc.to_payload(context.data, val.as_serde()?)?])
                }
            }
            PayloadConverter::UseWrappers => T::to_payloads(val, context),
            PayloadConverter::Composite(composite) => {
                for converter in &composite.converters {
                    match converter.to_payloads(context, val) {
                        Ok(payloads) => return Ok(payloads),
                        Err(PayloadConversionError::WrongEncoding) => continue,
                        Err(e) => return Err(e),
                    }
                }
                Err(PayloadConversionError::WrongEncoding)
            }
        }
    }

    fn from_payloads<T: TemporalDeserializable + 'static>(
        &self,
        context: &SerializationContext<'_>,
        payloads: Vec<Payload>,
    ) -> Result<T, PayloadConversionError> {
        // Accept empty payloads (no args) and a single binary/null payload (result from a
        // workflow/update with () return type as ().
        if std::any::TypeId::of::<T>() == std::any::TypeId::of::<()>()
            && is_unit_payloads(&payloads)
        {
            let boxed: Box<dyn std::any::Any> = Box::new(());
            return Ok(*boxed.downcast::<T>().unwrap());
        }

        match self {
            PayloadConverter::Serde(pc) => {
                if payloads.len() != 1 {
                    return Err(PayloadConversionError::WrongEncoding);
                }
                T::from_serde(pc.as_ref(), context, payloads.into_iter().next().unwrap())
            }
            PayloadConverter::UseWrappers => T::from_payloads(context, payloads),
            PayloadConverter::Composite(composite) => {
                for converter in &composite.converters {
                    match converter.from_payloads(context, payloads.clone()) {
                        Ok(val) => return Ok(val),
                        Err(PayloadConversionError::WrongEncoding) => continue,
                        Err(e) => return Err(e),
                    }
                }
                Err(PayloadConversionError::WrongEncoding)
            }
        }
    }
}

fn is_unit_payloads(payloads: &[Payload]) -> bool {
    match payloads {
        [] => true,
        [payload] => {
            payload.data.is_empty()
                && payload
                    .metadata
                    .get("encoding")
                    .map(|encoding| encoding == b"binary/null")
                    .unwrap_or(false)
        }
        _ => false,
    }
}

// TODO [rust-sdk-branch]: Potentially allow opt-out / no-serde compile flags
impl<T> TemporalSerializable for T
where
    T: serde::Serialize,
{
    fn as_serde(&self) -> Result<&dyn erased_serde::Serialize, PayloadConversionError> {
        Ok(self)
    }
}
impl<T> TemporalDeserializable for T
where
    T: serde::de::DeserializeOwned,
{
    fn from_serde(
        pc: &dyn ErasedSerdePayloadConverter,
        context: &SerializationContext<'_>,
        payload: Payload,
    ) -> Result<Self, PayloadConversionError>
    where
        Self: Sized,
    {
        let mut de = pc.from_payload(context.data, payload)?;
        erased_serde::deserialize(&mut de)
            .map_err(|e| PayloadConversionError::EncodingError(Box::new(e)))
    }
}

struct SerdeJsonPayloadConverter;
impl ErasedSerdePayloadConverter for SerdeJsonPayloadConverter {
    fn to_payload(
        &self,
        _: &SerializationContextData,
        value: &dyn erased_serde::Serialize,
    ) -> Result<Payload, PayloadConversionError> {
        let as_json = serde_json::to_vec(value)
            .map_err(|e| PayloadConversionError::EncodingError(e.into()))?;
        Ok(Payload {
            metadata: {
                let mut hm = HashMap::new();
                hm.insert("encoding".to_string(), b"json/plain".to_vec());
                hm
            },
            data: as_json,
            external_payloads: vec![],
        })
    }

    fn from_payload(
        &self,
        _: &SerializationContextData,
        payload: Payload,
    ) -> Result<Box<dyn erased_serde::Deserializer<'static>>, PayloadConversionError> {
        let encoding = payload.metadata.get("encoding").map(|v| v.as_slice());
        if encoding != Some(b"json/plain".as_slice()) {
            return Err(PayloadConversionError::WrongEncoding);
        }
        let json_v: serde_json::Value = serde_json::from_slice(&payload.data)
            .map_err(|e| PayloadConversionError::EncodingError(Box::new(e)))?;
        Ok(Box::new(<dyn erased_serde::Deserializer>::erase(json_v)))
    }
}
/// Type-erased serde-based payload converter for use behind `dyn` trait objects.
pub trait ErasedSerdePayloadConverter: Send + Sync {
    /// Serialize a type-erased serde value into a [`Payload`].
    fn to_payload(
        &self,
        context: &SerializationContextData,
        value: &dyn erased_serde::Serialize,
    ) -> Result<Payload, PayloadConversionError>;
    /// Deserialize a [`Payload`] into a type-erased serde deserializer.
    #[allow(clippy::wrong_self_convention)]
    fn from_payload(
        &self,
        context: &SerializationContextData,
        payload: Payload,
    ) -> Result<Box<dyn erased_serde::Deserializer<'static>>, PayloadConversionError>;
}

// TODO [rust-sdk-branch]: All prost things should be behind a compile flag

/// Wrapper for protobuf messages that implements [`TemporalSerializable`]/[`TemporalDeserializable`]
/// using `binary/protobuf` encoding.
pub struct ProstSerializable<T: prost::Message>(pub T);
impl<T> TemporalSerializable for ProstSerializable<T>
where
    T: prost::Message + Default + 'static,
{
    fn to_payload(&self, _: &SerializationContext<'_>) -> Result<Payload, PayloadConversionError> {
        let as_proto = prost::Message::encode_to_vec(&self.0);
        Ok(Payload {
            metadata: {
                let mut hm = HashMap::new();
                hm.insert("encoding".to_string(), b"binary/protobuf".to_vec());
                hm
            },
            data: as_proto,
            external_payloads: vec![],
        })
    }
}
impl<T> TemporalDeserializable for ProstSerializable<T>
where
    T: prost::Message + Default + 'static,
{
    fn from_payload(
        _: &SerializationContext<'_>,
        p: Payload,
    ) -> Result<Self, PayloadConversionError>
    where
        Self: Sized,
    {
        let encoding = p.metadata.get("encoding").map(|v| v.as_slice());
        if encoding != Some(b"binary/protobuf".as_slice()) {
            return Err(PayloadConversionError::WrongEncoding);
        }
        T::decode(p.data.as_slice())
            .map(ProstSerializable)
            .map_err(|e| PayloadConversionError::EncodingError(Box::new(e)))
    }
}

/// A payload converter that delegates to an ordered list of inner converters.
#[derive(Clone)]
pub struct CompositePayloadConverter {
    converters: Vec<PayloadConverter>,
}

impl Default for DataConverter {
    fn default() -> Self {
        Self::new(
            PayloadConverter::default(),
            DefaultFailureConverter,
            DefaultPayloadCodec,
        )
    }
}
impl PayloadCodec for DefaultPayloadCodec {
    fn encode(
        &self,
        _: &SerializationContextData,
        payloads: Vec<Payload>,
    ) -> BoxFuture<'static, Result<Vec<Payload>, PayloadConversionError>> {
        async move { Ok(payloads) }.boxed()
    }
    fn decode(
        &self,
        _: &SerializationContextData,
        payloads: Vec<Payload>,
    ) -> BoxFuture<'static, Result<Vec<Payload>, PayloadConversionError>> {
        async move { Ok(payloads) }.boxed()
    }
}

/// Represents multiple arguments for workflows/activities that accept more than one argument.
/// Use this when interoperating with other language SDKs that allow multiple arguments.
macro_rules! impl_multi_args {
    ($name:ident; $count:expr; $($idx:tt: $ty:ident),+) => {
        #[doc = concat!("Wrapper for ", stringify!($count), " typed arguments, enabling multi-arg serialization.")]
        #[derive(Clone, Debug, PartialEq, Eq)]
        pub struct $name<$($ty),+>($(pub $ty),+);

        impl<$($ty),+> TemporalSerializable for $name<$($ty),+>
        where
            $($ty: TemporalSerializable + 'static),+
        {
            fn to_payload(&self, _: &SerializationContext<'_>) -> Result<Payload, PayloadConversionError> {
                Err(PayloadConversionError::WrongEncoding)
            }
            fn to_payloads(
                &self,
                ctx: &SerializationContext<'_>,
            ) -> Result<Vec<Payload>, PayloadConversionError> {
                Ok(vec![$(ctx.converter.to_payload(ctx, &self.$idx)?),+])
            }
        }

        #[allow(non_snake_case)]
        impl<$($ty),+> From<($($ty),+,)> for $name<$($ty),+> {
            fn from(t: ($($ty),+,)) -> Self {
                $name($(t.$idx),+)
            }
        }

        impl<$($ty),+> TemporalDeserializable for $name<$($ty),+>
        where
            $($ty: TemporalDeserializable + 'static),+
        {
            fn from_payload(_: &SerializationContext<'_>, _: Payload) -> Result<Self, PayloadConversionError> {
                Err(PayloadConversionError::WrongEncoding)
            }
            fn from_payloads(
                ctx: &SerializationContext<'_>,
                payloads: Vec<Payload>,
            ) -> Result<Self, PayloadConversionError> {
                if payloads.len() != $count {
                    return Err(PayloadConversionError::WrongEncoding);
                }
                let mut iter = payloads.into_iter();
                Ok($name(
                    $(ctx.converter.from_payload::<$ty>(ctx, iter.next().unwrap())?),+
                ))
            }
        }
    };
}

impl_multi_args!(MultiArgs2; 2; 0: A, 1: B);
impl_multi_args!(MultiArgs3; 3; 0: A, 1: B, 2: C);
impl_multi_args!(MultiArgs4; 4; 0: A, 1: B, 2: C, 3: D);
impl_multi_args!(MultiArgs5; 5; 0: A, 1: B, 2: C, 3: D, 4: E);
impl_multi_args!(MultiArgs6; 6; 0: A, 1: B, 2: C, 3: D, 4: E, 5: F);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_payloads_as_unit_type() {
        let converter = PayloadConverter::default();
        let ctx = SerializationContext {
            data: &SerializationContextData::Workflow,
            converter: &converter,
        };

        let empty_payloads: Vec<Payload> = vec![];
        let result: Result<(), _> = converter.from_payloads(&ctx, empty_payloads);

        assert!(result.is_ok(), "Empty payloads should deserialize as ()");
    }

    #[test]
    fn test_unit_type_roundtrip_serde() {
        let converter = PayloadConverter::serde_json();
        let ctx = SerializationContext {
            data: &SerializationContextData::Workflow,
            converter: &converter,
        };

        let payloads = converter.to_payloads(&ctx, &()).unwrap();
        assert!(payloads.is_empty());

        let result: () = converter.from_payloads(&ctx, payloads).unwrap();
        assert_eq!(result, ());
    }

    #[test]
    fn test_unit_composite_roundtrip() {
        let converter = PayloadConverter::default();
        let ctx = SerializationContext {
            data: &SerializationContextData::Workflow,
            converter: &converter,
        };

        let payloads = converter.to_payloads(&ctx, &()).unwrap();
        assert!(payloads.is_empty());

        let result: () = converter.from_payloads(&ctx, payloads).unwrap();
        assert_eq!(result, ());
    }

    #[test]
    fn test_unit_to_payload_roundtrip() {
        let converter = PayloadConverter::default();
        let ctx = SerializationContext {
            data: &SerializationContextData::Workflow,
            converter: &converter,
        };

        let mut payloads = vec![converter.to_payload(&ctx, &()).unwrap()];
        assert!(is_unit_payloads(&payloads));
        let result: () = converter
            .from_payload(&ctx, payloads.pop().unwrap())
            .unwrap();
        assert_eq!(result, ());
    }

    #[test]
    fn test_unit_use_wrappers_returns_wrong_encoding() {
        let converter = PayloadConverter::UseWrappers;
        let ctx = SerializationContext {
            data: &SerializationContextData::Workflow,
            converter: &converter,
        };

        let result = converter.to_payloads(&ctx, &());
        assert!(
            matches!(result, Err(PayloadConversionError::WrongEncoding)),
            "{result:?}"
        );
    }

    #[test]
    fn multi_args_round_trip() {
        let converter = PayloadConverter::default();
        let ctx = SerializationContext {
            data: &SerializationContextData::Workflow,
            converter: &converter,
        };

        let args = MultiArgs2("hello".to_string(), 42i32);
        let payloads = converter.to_payloads(&ctx, &args).unwrap();
        assert_eq!(payloads.len(), 2);

        let result: MultiArgs2<String, i32> = converter.from_payloads(&ctx, payloads).unwrap();
        assert_eq!(result, args);
    }

    #[test]
    fn multi_args_from_tuple() {
        let args: MultiArgs2<String, i32> = ("hello".to_string(), 42i32).into();
        assert_eq!(args, MultiArgs2("hello".to_string(), 42));
    }

    fn decodable_from_value<T: TemporalSerializable + 'static>(value: &T) -> DecodablePayloads {
        let converter = PayloadConverter::default();
        let payloads = converter
            .to_payloads(
                &SerializationContext {
                    data: &SerializationContextData::Workflow,
                    converter: &converter,
                },
                value,
            )
            .unwrap();
        DecodablePayloads::new(payloads, converter, SerializationContextData::Workflow)
    }
    #[test]
    fn decodable_payloads_roundtrip_string() {
        let payloads = decodable_from_value(&"hello".to_string());

        let result: String = payloads.deserialize().unwrap();

        assert_eq!(result, "hello");
    }

    #[test]
    fn decodable_payloads_roundtrip_option_string() {
        let payloads = decodable_from_value(&Some("hello".to_string()));

        let result: Option<String> = payloads.deserialize().unwrap();

        assert_eq!(result, Some("hello".to_string()));
    }

    #[test]
    fn decodable_payloads_roundtrip_unit() {
        let payloads = decodable_from_value(&());

        let result: () = payloads.deserialize().unwrap();

        assert_eq!(result, ());
    }

    #[test]
    fn decodable_payloads_roundtrip_vec_string() {
        let payloads = decodable_from_value(&vec!["hello".to_string(), "world".to_string()]);

        let result: Vec<String> = payloads.deserialize().unwrap();

        assert_eq!(result, vec!["hello".to_string(), "world".to_string()]);
    }
}
