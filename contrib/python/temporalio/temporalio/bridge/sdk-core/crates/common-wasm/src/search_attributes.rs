//! Type-safe search attribute APIs for the Temporal Rust SDK.
//!
//! Search attributes are key-value pairs attached to workflows that enable
//! server-side filtering via visibility queries. This module provides a typed
//! layer over the raw proto payloads so that attribute values are checked at
//! compile time.
//!
//! # Example
//!
//! ```
//! use temporalio_common_wasm::search_attributes::SearchAttributeKey;
//!
//! const MY_BOOL: SearchAttributeKey<bool> = SearchAttributeKey::bool("my_bool");
//! const MY_KW: SearchAttributeKey<String> = SearchAttributeKey::keyword("my_keyword");
//!
//! let update = MY_BOOL.value_set(true);
//! let unset = MY_KW.value_unset();
//! ```

use std::{collections::HashMap, marker::PhantomData};

use tracing::warn;

use crate::{
    data_converters::{
        GenericPayloadConverter, PayloadConversionError, PayloadConverter, SerializationContext,
        SerializationContextData,
    },
    protos::temporal::api::{
        common::v1::{Payload, SearchAttributes as ProtoSearchAttributes},
        enums::v1::IndexedValueType,
    },
};

/// Metadata key for the search attribute value type, kept consistent across all SDKs.
const TYPE_METADATA_KEY: &str = "type";

/// Errors arising from search attribute serialization or deserialization.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SearchAttributeError {
    /// Payload conversion failed.
    #[error("failed to convert search attribute payload: {0}")]
    PayloadConversion(#[from] PayloadConversionError),

    /// The payload is missing required metadata or has an unexpected encoding.
    #[error("invalid search attribute payload: {reason}")]
    InvalidPayload {
        /// Description of what was wrong with the payload.
        reason: String,
    },

    /// A timestamp value could not be formatted or parsed as RFC3339.
    #[error("invalid timestamp: {0}")]
    InvalidTimestamp(String),
}

// ---------------------------------------------------------------------------
// SDK-owned Timestamp type
// ---------------------------------------------------------------------------

/// An SDK-owned timestamp for Datetime search attributes.
///
/// This type decouples the public API from `prost_types::Timestamp`. Conversion
/// traits are provided for [`prost_types::Timestamp`] and
/// [`std::time::SystemTime`].
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Timestamp {
    seconds: i64,
    nanos: i32,
}

impl Timestamp {
    /// The maximum valid value for nanoseconds.
    const MAX_NANOS: i32 = 999_999_999;

    /// Creates a new `Timestamp`.
    ///
    /// # Arguments
    /// * `seconds` — seconds since the Unix epoch (negative for pre-epoch).
    /// * `nanos` — non-negative nanosecond offset within the second,
    ///   in the range `[0, 999_999_999]`. Values outside this range are
    ///   clamped.
    pub fn new(seconds: i64, nanos: i32) -> Self {
        Self {
            seconds,
            nanos: nanos.clamp(0, Self::MAX_NANOS),
        }
    }

    /// Returns seconds since the Unix epoch.
    pub fn seconds(&self) -> i64 {
        self.seconds
    }

    /// Returns the nanosecond component (always in `[0, 999_999_999]`).
    pub fn nanos(&self) -> i32 {
        self.nanos
    }

    /// Returns this timestamp as a `prost_types::Timestamp`.
    pub fn to_prost(&self) -> prost_types::Timestamp {
        prost_types::Timestamp {
            seconds: self.seconds,
            nanos: self.nanos,
        }
    }
}

impl std::fmt::Display for Timestamp {
    /// Formats the timestamp as an RFC3339 string (e.g., `2023-11-14T22:13:20.000000000Z`).
    /// Falls back to `Debug` formatting if the timestamp is out of chrono's range.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match timestamp_to_rfc3339(self) {
            Ok(s) => f.write_str(&s),
            Err(_) => write!(f, "Timestamp({}, {})", self.seconds, self.nanos),
        }
    }
}

impl From<prost_types::Timestamp> for Timestamp {
    fn from(ts: prost_types::Timestamp) -> Self {
        Timestamp::new(ts.seconds, ts.nanos)
    }
}

impl From<Timestamp> for prost_types::Timestamp {
    fn from(ts: Timestamp) -> Self {
        prost_types::Timestamp {
            seconds: ts.seconds(),
            nanos: ts.nanos(),
        }
    }
}

impl From<std::time::SystemTime> for Timestamp {
    fn from(st: std::time::SystemTime) -> Self {
        match st.duration_since(std::time::UNIX_EPOCH) {
            Ok(dur) => Timestamp::new(dur.as_secs() as i64, dur.subsec_nanos() as i32),
            Err(e) => {
                // Normalize to protobuf convention: nanos always non-negative.
                // Example: 1.25s before epoch → { seconds: -2, nanos: 750_000_000 }
                let dur = e.duration();
                let secs = dur.as_secs() as i64;
                let nanos = dur.subsec_nanos();
                if nanos == 0 {
                    Timestamp::new(-secs, 0)
                } else {
                    Timestamp::new(-(secs + 1), (1_000_000_000 - nanos) as i32)
                }
            }
        }
    }
}

impl TryFrom<Timestamp> for std::time::SystemTime {
    type Error = SearchAttributeError;

    fn try_from(ts: Timestamp) -> Result<Self, Self::Error> {
        let epoch = std::time::UNIX_EPOCH;
        if ts.seconds >= 0 {
            epoch
                .checked_add(std::time::Duration::new(
                    ts.seconds as u64,
                    ts.nanos.max(0) as u32,
                ))
                .ok_or_else(|| {
                    SearchAttributeError::InvalidTimestamp(
                        "timestamp out of SystemTime range".into(),
                    )
                })
        } else {
            // Reverse the normalization: { seconds: -2, nanos: 750_000_000 }
            // means 1.25s before epoch → Duration::new(1, 250_000_000)
            let abs_secs = ts.seconds.unsigned_abs();
            let nanos = ts.nanos.max(0) as u32;
            let dur = if nanos == 0 {
                std::time::Duration::new(abs_secs, 0)
            } else {
                std::time::Duration::new(abs_secs - 1, 1_000_000_000 - nanos)
            };
            epoch.checked_sub(dur).ok_or_else(|| {
                SearchAttributeError::InvalidTimestamp("timestamp out of SystemTime range".into())
            })
        }
    }
}

// ---------------------------------------------------------------------------
// SearchAttributeValue trait
// ---------------------------------------------------------------------------

mod private {
    pub trait Sealed {}
    impl Sealed for bool {}
    impl Sealed for i64 {}
    impl Sealed for f64 {}
    impl Sealed for String {}
    impl Sealed for super::Timestamp {}
    impl Sealed for Vec<String> {}
}

/// A value type that can be stored as a Temporal search attribute.
///
/// This trait is sealed and implemented for: `bool`, `i64`, `f64`, `String`,
/// [`Timestamp`], and `Vec<String>`.
pub trait SearchAttributeValue: private::Sealed + Clone + Sized {
    /// Encode this value into a search attribute [`Payload`].
    fn to_search_attribute_payload(
        &self,
        indexed_value_type: IndexedValueType,
    ) -> Result<Payload, SearchAttributeError>;

    /// Decode a value from a search attribute [`Payload`].
    fn from_search_attribute_payload(payload: &Payload) -> Result<Self, SearchAttributeError>;

    /// The default [`IndexedValueType`] for this Rust type.
    ///
    /// This is used internally when a key does not explicitly specify the
    /// indexed value type. Most callers should use [`SearchAttributeKey`]
    /// constructors rather than calling this directly.
    fn default_indexed_value_type() -> IndexedValueType;
}

// ---------------------------------------------------------------------------
// Shared JSON payload helpers (reuses the SDK's JSON payload encoding conventions)
// ---------------------------------------------------------------------------

fn type_metadata_str(ivt: IndexedValueType) -> &'static str {
    match ivt {
        IndexedValueType::Bool => "Bool",
        IndexedValueType::Int => "Int",
        IndexedValueType::Double => "Double",
        IndexedValueType::Keyword => "Keyword",
        IndexedValueType::Text => "Text",
        IndexedValueType::Datetime => "Datetime",
        IndexedValueType::KeywordList => "KeywordList",
        IndexedValueType::Unspecified => "Unspecified",
    }
}

/// Encode a serde-serializable value into a search attribute [`Payload`].
///
/// This uses the SDK's JSON payload converter, then adds the search-attribute
/// `type` metadata key.
fn encode_json_search_attr<T: serde::Serialize + 'static>(
    value: &T,
    indexed_value_type: IndexedValueType,
) -> Result<Payload, SearchAttributeError> {
    let converter = PayloadConverter::serde_json();
    let context = SerializationContext {
        data: &SerializationContextData::None,
        converter: &converter,
    };
    let mut payload = converter.to_payload(&context, value)?;
    payload.metadata.insert(
        TYPE_METADATA_KEY.to_string(),
        type_metadata_str(indexed_value_type).as_bytes().to_vec(),
    );
    Ok(payload)
}

/// Decode a search attribute [`Payload`] back into a concrete type.
///
/// This delegates payload interpretation to the SDK's JSON payload converter.
fn decode_json_search_attr<T: serde::de::DeserializeOwned + 'static>(
    payload: &Payload,
) -> Result<T, SearchAttributeError> {
    let converter = PayloadConverter::serde_json();
    let context = SerializationContext {
        data: &SerializationContextData::None,
        converter: &converter,
    };
    Ok(converter.from_payload(&context, payload.clone())?)
}

// ---------------------------------------------------------------------------
// Macro for simple (serde-native) SearchAttributeValue impls
// ---------------------------------------------------------------------------

/// Implements [`SearchAttributeValue`] for types that are directly
/// serde-serializable as their JSON wire representation (no special conversion).
macro_rules! impl_simple_search_attribute_value {
    ($ty:ty, $ivt:expr) => {
        impl SearchAttributeValue for $ty {
            fn to_search_attribute_payload(
                &self,
                indexed_value_type: IndexedValueType,
            ) -> Result<Payload, SearchAttributeError> {
                encode_json_search_attr(self, indexed_value_type)
            }

            fn from_search_attribute_payload(
                payload: &Payload,
            ) -> Result<Self, SearchAttributeError> {
                decode_json_search_attr(payload)
            }

            fn default_indexed_value_type() -> IndexedValueType {
                $ivt
            }
        }
    };
}

impl_simple_search_attribute_value!(bool, IndexedValueType::Bool);
impl_simple_search_attribute_value!(i64, IndexedValueType::Int);
impl_simple_search_attribute_value!(String, IndexedValueType::Keyword);
impl_simple_search_attribute_value!(Vec<String>, IndexedValueType::KeywordList);

// f64 requires a manual impl to reject NaN and Infinity, which serde_json
// silently serializes as `null` rather than returning an error.
impl SearchAttributeValue for f64 {
    fn to_search_attribute_payload(
        &self,
        indexed_value_type: IndexedValueType,
    ) -> Result<Payload, SearchAttributeError> {
        if !self.is_finite() {
            return Err(SearchAttributeError::InvalidPayload {
                reason: format!("f64 search attribute value must be finite, got {}", self),
            });
        }
        encode_json_search_attr(self, indexed_value_type)
    }

    fn from_search_attribute_payload(payload: &Payload) -> Result<Self, SearchAttributeError> {
        decode_json_search_attr(payload)
    }

    fn default_indexed_value_type() -> IndexedValueType {
        IndexedValueType::Double
    }
}

// ---------------------------------------------------------------------------
// Timestamp SearchAttributeValue impl (RFC3339 string on the wire)
// ---------------------------------------------------------------------------

/// Format a [`Timestamp`] as an RFC3339 string using `chrono`.
fn timestamp_to_rfc3339(ts: &Timestamp) -> Result<String, SearchAttributeError> {
    use chrono::{DateTime, Utc};

    let nanos = u32::try_from(ts.nanos()).unwrap_or(0);
    let dt = DateTime::<Utc>::from_timestamp(ts.seconds(), nanos).ok_or_else(|| {
        SearchAttributeError::InvalidTimestamp(format!(
            "cannot represent seconds={} nanos={} as DateTime",
            ts.seconds(),
            ts.nanos()
        ))
    })?;
    Ok(dt.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true))
}

/// Parse an RFC3339 string into a [`Timestamp`] using `chrono`.
fn rfc3339_to_timestamp(s: &str) -> Result<Timestamp, SearchAttributeError> {
    use chrono::DateTime;

    // Strip surrounding quotes if present — some SDKs or raw payloads may
    // pass the RFC3339 string with JSON-style quotes still attached.
    let s = s.trim_matches('"');
    let dt = DateTime::parse_from_rfc3339(s).map_err(|e| {
        SearchAttributeError::InvalidTimestamp(format!("failed to parse RFC3339 '{}': {}", s, e))
    })?;
    Ok(Timestamp::new(
        dt.timestamp(),
        dt.timestamp_subsec_nanos() as i32,
    ))
}

impl SearchAttributeValue for Timestamp {
    fn to_search_attribute_payload(
        &self,
        indexed_value_type: IndexedValueType,
    ) -> Result<Payload, SearchAttributeError> {
        let rfc3339 = timestamp_to_rfc3339(self)?;
        encode_json_search_attr(&rfc3339, indexed_value_type)
    }

    fn from_search_attribute_payload(payload: &Payload) -> Result<Self, SearchAttributeError> {
        let s: String = decode_json_search_attr(payload)?;
        rfc3339_to_timestamp(&s)
    }

    fn default_indexed_value_type() -> IndexedValueType {
        IndexedValueType::Datetime
    }
}

// ---------------------------------------------------------------------------
// SearchAttributeKey
// ---------------------------------------------------------------------------

/// A typed handle for a named search attribute, carrying its value type at the
/// type level. Construct via the const factory methods such as
/// [`SearchAttributeKey::bool`], [`SearchAttributeKey::keyword`], etc.
///
/// # Key names
///
/// Key names must be `&'static str`, which enables compile-time construction
/// via `const` but means runtime-determined key names are not supported.
/// For dynamic key names (e.g., from config), use
/// [`SearchAttributes::raw_payload`] as an escape hatch for untyped access.
///
/// ```
/// use temporalio_common_wasm::search_attributes::SearchAttributeKey;
///
/// const MY_KEY: SearchAttributeKey<String> = SearchAttributeKey::keyword("my_attr");
/// ```
#[derive(Debug, Clone, Copy)]
pub struct SearchAttributeKey<T: SearchAttributeValue> {
    name: &'static str,
    indexed_value_type: IndexedValueType,
    _marker: PhantomData<T>,
}

impl<T: SearchAttributeValue> SearchAttributeKey<T> {
    /// Returns the attribute name used as the key in the proto map.
    pub fn name(&self) -> &'static str {
        self.name
    }

    /// Returns the [`IndexedValueType`] configured for this key.
    pub fn indexed_value_type(&self) -> IndexedValueType {
        self.indexed_value_type
    }

    /// Create a [`SearchAttributeUpdate`] that sets the attribute to the given value.
    ///
    /// # Panics
    ///
    /// Panics if the value cannot be serialized to JSON. This can happen for
    /// `f64` values that are `NaN` or `Infinity` (which are not valid JSON),
    /// or for `Timestamp` values with out-of-range seconds. Use
    /// [`try_value_set`](Self::try_value_set) for a fallible alternative.
    pub fn value_set(&self, val: T) -> SearchAttributeUpdate {
        self.try_value_set(val)
            .expect("search attribute serialization failed (use try_value_set for non-finite f64 or out-of-range timestamps)")
    }

    /// Fallible version of [`value_set`](Self::value_set). Returns an error
    /// instead of panicking if the value cannot be serialized.
    pub fn try_value_set(&self, val: T) -> Result<SearchAttributeUpdate, SearchAttributeError> {
        let payload = val.to_search_attribute_payload(self.indexed_value_type)?;
        Ok(SearchAttributeUpdate {
            name: self.name.to_string(),
            payload: Some(payload),
        })
    }

    /// Create a [`SearchAttributeUpdate`] that removes this attribute.
    pub fn value_unset(&self) -> SearchAttributeUpdate {
        SearchAttributeUpdate {
            name: self.name.to_string(),
            payload: None,
        }
    }
}

impl SearchAttributeKey<bool> {
    /// Create a key for a `Bool`-typed search attribute.
    pub const fn bool(name: &'static str) -> Self {
        Self {
            name,
            indexed_value_type: IndexedValueType::Bool,
            _marker: PhantomData,
        }
    }
}

impl SearchAttributeKey<i64> {
    /// Create a key for an `Int`-typed search attribute.
    pub const fn int(name: &'static str) -> Self {
        Self {
            name,
            indexed_value_type: IndexedValueType::Int,
            _marker: PhantomData,
        }
    }
}

impl SearchAttributeKey<f64> {
    /// Create a key for a `Double`-typed search attribute.
    pub const fn float(name: &'static str) -> Self {
        Self {
            name,
            indexed_value_type: IndexedValueType::Double,
            _marker: PhantomData,
        }
    }
}

impl SearchAttributeKey<String> {
    /// Create a key for a `Keyword`-typed search attribute.
    pub const fn keyword(name: &'static str) -> Self {
        Self {
            name,
            indexed_value_type: IndexedValueType::Keyword,
            _marker: PhantomData,
        }
    }

    /// Create a key for a `Text`-typed search attribute.
    pub const fn text(name: &'static str) -> Self {
        Self {
            name,
            indexed_value_type: IndexedValueType::Text,
            _marker: PhantomData,
        }
    }
}

impl SearchAttributeKey<Timestamp> {
    /// Create a key for a `Datetime`-typed search attribute.
    pub const fn datetime(name: &'static str) -> Self {
        Self {
            name,
            indexed_value_type: IndexedValueType::Datetime,
            _marker: PhantomData,
        }
    }
}

impl SearchAttributeKey<Vec<String>> {
    /// Create a key for a `KeywordList`-typed search attribute.
    pub const fn keyword_list(name: &'static str) -> Self {
        Self {
            name,
            indexed_value_type: IndexedValueType::KeywordList,
            _marker: PhantomData,
        }
    }
}

// ---------------------------------------------------------------------------
// SearchAttributeUpdate
// ---------------------------------------------------------------------------

/// A pending mutation to a single search attribute.
///
/// When `payload` is `None`, the attribute should be removed. The semantics
/// differ slightly depending on how the update is consumed:
///
/// - [`SearchAttributes::new`] / [`SearchAttributes::apply`]: a `None` payload
///   removes the key from the in-memory collection (the key is simply absent).
/// - [`SearchAttributes::updates_to_proto`]: a `None` payload produces an
///   empty [`Payload`] in the proto map, signaling the server to clear that
///   attribute.
#[derive(Debug, Clone)]
pub struct SearchAttributeUpdate {
    pub(crate) name: String,
    pub(crate) payload: Option<Payload>,
}

impl SearchAttributeUpdate {
    /// Returns the attribute name being updated.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns `true` if this update removes the attribute.
    pub fn is_unset(&self) -> bool {
        self.payload.is_none()
    }
}

// ---------------------------------------------------------------------------
// SearchAttributes
// ---------------------------------------------------------------------------

/// A collection of search attribute payloads, providing type-safe access via
/// [`SearchAttributeKey`].
#[derive(Debug, Clone, Default, PartialEq)]
pub struct SearchAttributes {
    fields: HashMap<String, Payload>,
}

impl SearchAttributes {
    /// Construct from an iterator of [`SearchAttributeUpdate`]s.
    ///
    /// Updates with `None` payloads remove any existing entry for that key.
    pub fn new(updates: impl IntoIterator<Item = SearchAttributeUpdate>) -> Self {
        let mut fields = HashMap::new();
        for update in updates {
            match update.payload {
                Some(payload) => {
                    fields.insert(update.name, payload);
                }
                None => {
                    fields.remove(&update.name);
                }
            }
        }
        Self { fields }
    }

    /// Apply a single update to this collection. If the update sets a value,
    /// it is inserted (replacing any existing entry); if the update unsets a
    /// value, the entry is removed.
    pub fn apply(&mut self, update: SearchAttributeUpdate) {
        match update.payload {
            Some(payload) => {
                self.fields.insert(update.name, payload);
            }
            None => {
                self.fields.remove(&update.name);
            }
        }
    }

    /// Retrieve a typed value. Returns `None` if the key is absent or
    /// deserialization fails (graceful degradation — no panic on type mismatch).
    pub fn get<T: SearchAttributeValue>(&self, key: &SearchAttributeKey<T>) -> Option<T> {
        let payload = self.fields.get(key.name())?;
        match T::from_search_attribute_payload(payload) {
            Ok(val) => Some(val),
            Err(e) => {
                warn!(
                    key = key.name(),
                    error = %e,
                    "Failed to deserialize search attribute; returning None. \
                     Use try_get() for explicit error handling."
                );
                None
            }
        }
    }

    /// Retrieve a typed value, distinguishing "key absent" from "deserialization
    /// failed". Returns `Ok(None)` if the key is absent, `Ok(Some(val))` on
    /// success, or `Err` if the payload is present but cannot be deserialized.
    pub fn try_get<T: SearchAttributeValue>(
        &self,
        key: &SearchAttributeKey<T>,
    ) -> Result<Option<T>, SearchAttributeError> {
        match self.fields.get(key.name()) {
            None => Ok(None),
            Some(payload) => T::from_search_attribute_payload(payload).map(Some),
        }
    }

    /// Returns `true` if a payload exists for the given key.
    pub fn contains_key<T: SearchAttributeValue>(&self, key: &SearchAttributeKey<T>) -> bool {
        self.fields.contains_key(key.name())
    }

    /// Returns true if there are no search attributes.
    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }

    /// Returns the number of search attributes.
    pub fn len(&self) -> usize {
        self.fields.len()
    }

    /// Returns an iterator over the attribute names in this collection.
    pub fn keys(&self) -> impl Iterator<Item = &str> {
        self.fields.keys().map(|s| s.as_str())
    }

    /// Returns a reference to the raw payload for the given attribute name,
    /// if present. This is useful for advanced use cases such as forwarding
    /// payloads without deserializing them.
    pub fn raw_payload(&self, name: &str) -> Option<&Payload> {
        self.fields.get(name)
    }

    /// Convert to the proto wire representation.
    pub fn to_proto(&self) -> ProtoSearchAttributes {
        ProtoSearchAttributes {
            indexed_fields: self.fields.clone(),
        }
    }

    /// Convert to the proto wire representation, consuming `self` to avoid
    /// cloning.
    pub fn into_proto(self) -> ProtoSearchAttributes {
        ProtoSearchAttributes {
            indexed_fields: self.fields,
        }
    }

    /// Construct from the proto wire representation by cloning the inner map.
    pub fn from_proto(attrs: &ProtoSearchAttributes) -> Self {
        Self {
            fields: attrs.indexed_fields.clone(),
        }
    }
}

impl From<ProtoSearchAttributes> for SearchAttributes {
    /// Construct from an owned proto, moving the inner map without cloning.
    fn from(attrs: ProtoSearchAttributes) -> Self {
        Self {
            fields: attrs.indexed_fields,
        }
    }
}

impl SearchAttributes {
    /// Convert to the proto representation, producing empty-data payloads for
    /// entries that were unset. This is used when building an upsert command
    /// that needs to explicitly clear attributes on the server.
    pub fn updates_to_proto(
        updates: impl IntoIterator<Item = SearchAttributeUpdate>,
    ) -> ProtoSearchAttributes {
        let mut indexed_fields = HashMap::new();
        for update in updates {
            let payload = update.payload.unwrap_or_default();
            indexed_fields.insert(update.name, payload);
        }
        ProtoSearchAttributes { indexed_fields }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const BOOL_KEY: SearchAttributeKey<bool> = SearchAttributeKey::bool("my_bool");
    const INT_KEY: SearchAttributeKey<i64> = SearchAttributeKey::int("my_int");
    const FLOAT_KEY: SearchAttributeKey<f64> = SearchAttributeKey::float("my_float");
    const KW_KEY: SearchAttributeKey<String> = SearchAttributeKey::keyword("my_keyword");
    const TEXT_KEY: SearchAttributeKey<String> = SearchAttributeKey::text("my_text");
    const DT_KEY: SearchAttributeKey<Timestamp> = SearchAttributeKey::datetime("my_datetime");
    const KWL_KEY: SearchAttributeKey<Vec<String>> =
        SearchAttributeKey::keyword_list("my_keyword_list");

    fn assert_payload_metadata(payload: &Payload, expected_type: &str) {
        assert_eq!(
            payload.metadata.get("encoding").unwrap(),
            b"json/plain".as_slice()
        );
        assert_eq!(
            payload.metadata.get(TYPE_METADATA_KEY).unwrap(),
            expected_type.as_bytes()
        );
    }

    #[test]
    fn round_trip_bool() {
        let val = true;
        let payload = val
            .to_search_attribute_payload(IndexedValueType::Bool)
            .unwrap();
        assert_payload_metadata(&payload, "Bool");
        assert!(bool::from_search_attribute_payload(&payload).unwrap());
    }

    #[test]
    fn round_trip_int() {
        let val: i64 = -42;
        let payload = val
            .to_search_attribute_payload(IndexedValueType::Int)
            .unwrap();
        assert_payload_metadata(&payload, "Int");
        assert_eq!(i64::from_search_attribute_payload(&payload).unwrap(), -42);
    }

    #[test]
    fn round_trip_double() {
        let val: f64 = 1.23;
        let payload = val
            .to_search_attribute_payload(IndexedValueType::Double)
            .unwrap();
        assert_payload_metadata(&payload, "Double");
        let decoded = f64::from_search_attribute_payload(&payload).unwrap();
        assert!((decoded - 1.23).abs() < f64::EPSILON);
    }

    #[test]
    fn round_trip_keyword() {
        let val = "hello".to_string();
        let payload = val
            .to_search_attribute_payload(IndexedValueType::Keyword)
            .unwrap();
        assert_payload_metadata(&payload, "Keyword");
        assert_eq!(
            String::from_search_attribute_payload(&payload).unwrap(),
            "hello"
        );
    }

    #[test]
    fn round_trip_text() {
        let val = "some long text".to_string();
        let payload = val
            .to_search_attribute_payload(IndexedValueType::Text)
            .unwrap();
        assert_payload_metadata(&payload, "Text");
        assert_eq!(
            String::from_search_attribute_payload(&payload).unwrap(),
            "some long text"
        );
    }

    #[test]
    fn round_trip_datetime() {
        let ts = Timestamp::new(1_700_000_000, 123_456_789);
        let payload = ts
            .to_search_attribute_payload(IndexedValueType::Datetime)
            .unwrap();
        assert_payload_metadata(&payload, "Datetime");

        let json_str: String = serde_json::from_slice(&payload.data).unwrap();
        assert!(json_str.ends_with('Z'));
        assert!(json_str.contains('T'));

        let decoded = Timestamp::from_search_attribute_payload(&payload).unwrap();
        assert_eq!(decoded.seconds(), ts.seconds());
        assert_eq!(decoded.nanos(), ts.nanos());

        let attrs = SearchAttributes::new([DT_KEY.value_set(ts.clone())]);
        let got = attrs.get(&DT_KEY).unwrap();
        assert_eq!(got.seconds(), ts.seconds());
        assert_eq!(got.nanos(), ts.nanos());
    }

    #[test]
    fn round_trip_datetime_no_nanos() {
        let ts = Timestamp::new(0, 0);
        let payload = ts
            .to_search_attribute_payload(IndexedValueType::Datetime)
            .unwrap();
        let decoded = Timestamp::from_search_attribute_payload(&payload).unwrap();
        assert_eq!(decoded.seconds(), 0);
        assert_eq!(decoded.nanos(), 0);
    }

    #[test]
    fn round_trip_keyword_list() {
        let val = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let payload = val
            .to_search_attribute_payload(IndexedValueType::KeywordList)
            .unwrap();
        assert_payload_metadata(&payload, "KeywordList");
        assert_eq!(
            Vec::<String>::from_search_attribute_payload(&payload).unwrap(),
            vec!["a", "b", "c"]
        );
    }

    #[test]
    fn typed_search_attributes_new_and_get() {
        let attrs = SearchAttributes::new([
            BOOL_KEY.value_set(true),
            INT_KEY.value_set(99),
            FLOAT_KEY.value_set(2.72),
            KW_KEY.value_set("kw_val".into()),
            TEXT_KEY.value_set("text_val".into()),
            KWL_KEY.value_set(vec!["x".into(), "y".into()]),
        ]);

        assert_eq!(attrs.len(), 6);
        assert!(!attrs.is_empty());
        assert_eq!(attrs.get(&BOOL_KEY), Some(true));
        assert_eq!(attrs.get(&INT_KEY), Some(99));
        assert!((attrs.get(&FLOAT_KEY).unwrap() - 2.72).abs() < f64::EPSILON);
        assert_eq!(attrs.get(&KW_KEY), Some("kw_val".into()));
        assert_eq!(attrs.get(&TEXT_KEY), Some("text_val".into()));
        assert_eq!(
            attrs.get(&KWL_KEY),
            Some(vec!["x".to_string(), "y".to_string()])
        );
    }

    #[test]
    fn to_proto_from_proto_round_trip() {
        let attrs = SearchAttributes::new([BOOL_KEY.value_set(false), INT_KEY.value_set(7)]);

        let proto = attrs.to_proto();
        assert_eq!(proto.indexed_fields.len(), 2);

        let restored = SearchAttributes::from_proto(&proto);
        assert_eq!(restored.get(&BOOL_KEY), Some(false));
        assert_eq!(restored.get(&INT_KEY), Some(7));
    }

    #[test]
    fn value_unset_removes_entry() {
        let attrs = SearchAttributes::new([BOOL_KEY.value_set(true), BOOL_KEY.value_unset()]);
        assert!(attrs.is_empty());
        assert_eq!(attrs.get(&BOOL_KEY), None);
    }

    #[test]
    fn keyword_vs_text_disambiguation() {
        let kw_update = KW_KEY.value_set("same_value".into());
        let text_update = TEXT_KEY.value_set("same_value".into());

        let kw_payload = kw_update.payload.as_ref().unwrap();
        let text_payload = text_update.payload.as_ref().unwrap();

        assert_eq!(
            kw_payload.metadata.get(TYPE_METADATA_KEY).unwrap(),
            b"Keyword"
        );
        assert_eq!(
            text_payload.metadata.get(TYPE_METADATA_KEY).unwrap(),
            b"Text"
        );

        assert_eq!(KW_KEY.indexed_value_type(), IndexedValueType::Keyword);
        assert_eq!(TEXT_KEY.indexed_value_type(), IndexedValueType::Text);
    }

    #[test]
    fn get_returns_none_for_missing_key() {
        let attrs = SearchAttributes::default();
        assert_eq!(attrs.get(&BOOL_KEY), None);
        assert!(!attrs.contains_key(&INT_KEY));
    }

    #[test]
    fn get_returns_none_for_type_mismatch() {
        let attrs = SearchAttributes::new([BOOL_KEY.value_set(true)]);
        // Try to read the bool payload as an i64 — should gracefully return None
        let mismatched_key = SearchAttributeKey::<i64>::int("my_bool");
        assert_eq!(attrs.get(&mismatched_key), None);
    }

    #[test]
    fn updates_to_proto_includes_empty_payload_for_unset() {
        let proto =
            SearchAttributes::updates_to_proto([BOOL_KEY.value_set(true), INT_KEY.value_unset()]);

        let bool_payload = proto.indexed_fields.get("my_bool").unwrap();
        assert!(!bool_payload.data.is_empty());

        let int_payload = proto.indexed_fields.get("my_int").unwrap();
        assert!(int_payload.data.is_empty());
        assert!(int_payload.metadata.is_empty());
    }

    #[test]
    fn contains_key_returns_true_when_present() {
        let attrs = SearchAttributes::new([INT_KEY.value_set(42)]);
        assert!(attrs.contains_key(&INT_KEY));
    }

    #[test]
    fn timestamp_rfc3339_format() {
        let ts = Timestamp::new(1_700_000_000, 0);
        let rfc = timestamp_to_rfc3339(&ts).unwrap();
        // SecondsFormat::Nanos emits full precision even for zero nanos
        assert_eq!(rfc, "2023-11-14T22:13:20.000000000Z");
    }

    #[test]
    fn timestamp_rfc3339_with_nanos() {
        let ts = Timestamp::new(1_700_000_000, 500_000_000);
        let rfc = timestamp_to_rfc3339(&ts).unwrap();
        assert_eq!(rfc, "2023-11-14T22:13:20.500000000Z");

        let parsed = rfc3339_to_timestamp(&rfc).unwrap();
        assert_eq!(parsed.seconds(), ts.seconds());
        assert_eq!(parsed.nanos(), ts.nanos());
    }

    #[test]
    fn search_attribute_update_accessors() {
        let set = BOOL_KEY.value_set(true);
        assert_eq!(set.name(), "my_bool");
        assert!(!set.is_unset());

        let unset = BOOL_KEY.value_unset();
        assert_eq!(unset.name(), "my_bool");
        assert!(unset.is_unset());
    }

    #[test]
    fn timestamp_from_prost_types() {
        let prost_ts = prost_types::Timestamp {
            seconds: 1_000_000,
            nanos: 42,
        };
        let ts: Timestamp = prost_ts.into();
        assert_eq!(ts.seconds(), 1_000_000);
        assert_eq!(ts.nanos(), 42);

        let back: prost_types::Timestamp = ts.into();
        assert_eq!(back.seconds, 1_000_000);
        assert_eq!(back.nanos, 42);
    }

    #[test]
    fn timestamp_from_system_time() {
        // Use nanos aligned to 100ns boundary for Windows compatibility
        // (Windows SystemTime uses FILETIME with 100ns tick resolution)
        let st = std::time::UNIX_EPOCH + std::time::Duration::new(1_700_000_000, 123_456_700);
        let ts: Timestamp = st.into();
        assert_eq!(ts.seconds(), 1_700_000_000);
        assert_eq!(ts.nanos(), 123_456_700);

        let back: std::time::SystemTime = ts.try_into().unwrap();
        assert_eq!(back, st);
    }

    // --- Edge-case tests (from review feedback) ---

    #[test]
    fn timestamp_pre_epoch_normalized() {
        // 1.25 seconds before epoch → { seconds: -2, nanos: 750_000_000 }
        let st = std::time::UNIX_EPOCH - std::time::Duration::new(1, 250_000_000);
        let ts: Timestamp = st.into();
        assert_eq!(ts.seconds(), -2);
        assert_eq!(ts.nanos(), 750_000_000);

        let back: std::time::SystemTime = ts.try_into().unwrap();
        assert_eq!(back, st);
    }

    #[test]
    fn timestamp_pre_epoch_exact_second() {
        // Exactly 5 seconds before epoch
        let st = std::time::UNIX_EPOCH - std::time::Duration::new(5, 0);
        let ts: Timestamp = st.into();
        assert_eq!(ts.seconds(), -5);
        assert_eq!(ts.nanos(), 0);

        let back: std::time::SystemTime = ts.try_into().unwrap();
        assert_eq!(back, st);
    }

    #[test]
    fn timestamp_pre_epoch_rfc3339_round_trip() {
        let ts = Timestamp::new(-2, 750_000_000);
        let payload = ts
            .to_search_attribute_payload(IndexedValueType::Datetime)
            .unwrap();
        let decoded = Timestamp::from_search_attribute_payload(&payload).unwrap();
        assert_eq!(decoded.seconds(), ts.seconds());
        assert_eq!(decoded.nanos(), ts.nanos());
    }

    #[test]
    #[should_panic(expected = "search attribute serialization failed")]
    fn value_set_panics_on_nan() {
        FLOAT_KEY.value_set(f64::NAN);
    }

    #[test]
    #[should_panic(expected = "search attribute serialization failed")]
    fn value_set_panics_on_infinity() {
        FLOAT_KEY.value_set(f64::INFINITY);
    }

    #[test]
    fn try_value_set_returns_error_on_nan() {
        let result = FLOAT_KEY.try_value_set(f64::NAN);
        assert!(result.is_err());
    }

    #[test]
    fn try_value_set_returns_error_on_infinity() {
        let result = FLOAT_KEY.try_value_set(f64::INFINITY);
        assert!(result.is_err());
    }

    #[test]
    fn round_trip_empty_string() {
        let val = String::new();
        let payload = val
            .to_search_attribute_payload(IndexedValueType::Keyword)
            .unwrap();
        assert_eq!(String::from_search_attribute_payload(&payload).unwrap(), "");
    }

    #[test]
    fn round_trip_empty_keyword_list() {
        let val: Vec<String> = vec![];
        let payload = val
            .to_search_attribute_payload(IndexedValueType::KeywordList)
            .unwrap();
        assert_eq!(
            Vec::<String>::from_search_attribute_payload(&payload).unwrap(),
            Vec::<String>::new()
        );
    }

    #[test]
    fn round_trip_large_int_boundaries() {
        for val in [i64::MAX, i64::MIN, 0i64] {
            let payload = val
                .to_search_attribute_payload(IndexedValueType::Int)
                .unwrap();
            assert_eq!(i64::from_search_attribute_payload(&payload).unwrap(), val);
        }
    }

    #[test]
    fn decode_missing_encoding_metadata() {
        let payload = Payload {
            metadata: HashMap::new(),
            data: b"true".to_vec(),
            ..Default::default()
        };
        let result = bool::from_search_attribute_payload(&payload);
        assert!(result.is_err());
    }

    #[test]
    fn decode_wrong_encoding_metadata() {
        let mut metadata = HashMap::new();
        metadata.insert("encoding".to_string(), b"binary/plain".to_vec());
        let payload = Payload {
            metadata,
            data: b"true".to_vec(),
            ..Default::default()
        };
        let result = bool::from_search_attribute_payload(&payload);
        assert!(result.is_err());
    }

    #[test]
    fn decode_garbage_json_data() {
        let mut metadata = HashMap::new();
        metadata.insert("encoding".to_string(), b"json/plain".to_vec());
        let payload = Payload {
            metadata,
            data: b"not-valid-json!!!".to_vec(),
            ..Default::default()
        };
        let result = bool::from_search_attribute_payload(&payload);
        assert!(result.is_err());
    }

    #[test]
    fn keys_returns_attribute_names() {
        let attrs = SearchAttributes::new([BOOL_KEY.value_set(true), INT_KEY.value_set(42)]);
        let mut keys: Vec<&str> = attrs.keys().collect();
        keys.sort();
        assert_eq!(keys, vec!["my_bool", "my_int"]);
    }

    #[test]
    fn raw_payload_returns_payload() {
        let attrs = SearchAttributes::new([BOOL_KEY.value_set(true)]);
        let payload = attrs.raw_payload("my_bool").unwrap();
        assert!(!payload.data.is_empty());
        assert!(attrs.raw_payload("nonexistent").is_none());
    }

    #[test]
    fn into_proto_moves_without_clone() {
        let attrs = SearchAttributes::new([INT_KEY.value_set(7)]);
        let proto = attrs.into_proto();
        assert_eq!(proto.indexed_fields.len(), 1);
    }

    #[test]
    fn search_attribute_key_is_copy() {
        let key = BOOL_KEY;
        let key2 = key; // Copy, not move
        assert_eq!(key.name(), key2.name());
    }

    #[test]
    fn timestamp_new_clamps_negative_nanos() {
        let ts = Timestamp::new(100, -42);
        assert_eq!(ts.seconds(), 100);
        assert_eq!(ts.nanos(), 0); // clamped to 0
    }

    #[test]
    fn timestamp_new_clamps_excessive_nanos() {
        let ts = Timestamp::new(100, 2_000_000_000);
        assert_eq!(ts.seconds(), 100);
        assert_eq!(ts.nanos(), 999_999_999); // clamped to MAX_NANOS
    }

    #[test]
    fn timestamp_to_prost_round_trips() {
        let ts = Timestamp::new(1_700_000_000, 123_456_789);
        let prost_ts = ts.to_prost();
        assert_eq!(prost_ts.seconds, 1_700_000_000);
        assert_eq!(prost_ts.nanos, 123_456_789);
        let back: Timestamp = prost_ts.into();
        assert_eq!(back, ts);
    }

    #[test]
    fn apply_inserts_and_removes() {
        let mut attrs = SearchAttributes::new([INT_KEY.value_set(42)]);
        assert_eq!(attrs.get(&INT_KEY), Some(42));

        // Apply an update that changes the value
        attrs.apply(INT_KEY.value_set(99));
        assert_eq!(attrs.get(&INT_KEY), Some(99));

        // Apply an unset
        attrs.apply(INT_KEY.value_unset());
        assert_eq!(attrs.get(&INT_KEY), None);
        assert!(attrs.is_empty());
    }

    #[test]
    fn from_owned_proto_moves_without_clone() {
        let proto = ProtoSearchAttributes {
            indexed_fields: {
                let mut m = HashMap::new();
                m.insert("k".to_string(), INT_KEY.value_set(7).payload.unwrap());
                m
            },
        };
        let attrs: SearchAttributes = proto.into();
        assert_eq!(attrs.get(&SearchAttributeKey::int("k")), Some(7));
    }

    #[test]
    fn search_attributes_equality() {
        let a = SearchAttributes::new([BOOL_KEY.value_set(true), INT_KEY.value_set(42)]);
        let b = SearchAttributes::new([BOOL_KEY.value_set(true), INT_KEY.value_set(42)]);
        let c = SearchAttributes::new([BOOL_KEY.value_set(false), INT_KEY.value_set(42)]);
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn from_proto_trait_matches_from_proto_method() {
        let updates = [INT_KEY.value_set(99), BOOL_KEY.value_set(true)];
        let proto = SearchAttributes::new(updates).to_proto();
        let via_method = SearchAttributes::from_proto(&proto);
        let via_trait: SearchAttributes = proto.into();
        assert_eq!(via_method, via_trait);
    }
}
