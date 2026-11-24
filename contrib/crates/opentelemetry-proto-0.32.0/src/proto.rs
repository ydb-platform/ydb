/// provide serde support for proto traceIds and spanIds.
/// Those are hex encoded strings in the jsons but they are byte arrays in the proto.
/// See https://opentelemetry.io/docs/specs/otlp/#json-protobuf-encoding for more details
#[cfg(all(feature = "with-serde", feature = "gen-tonic-messages"))]
pub(crate) mod serializers {
    use crate::tonic::common::v1::any_value::{self, Value};
    use crate::tonic::common::v1::AnyValue;
    use serde::de::{self, MapAccess, Visitor};
    use serde::ser::{SerializeMap, SerializeSeq, SerializeStruct};
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::fmt;

    pub fn is_default<T>(value: &T) -> bool
    where
        T: Default + PartialEq,
    {
        value == &T::default()
    }

    // hex string <-> bytes conversion

    pub fn serialize_to_hex_string<S>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let hex_string = const_hex::encode(bytes);
        serializer.serialize_str(&hex_string)
    }

    pub fn deserialize_from_hex_string<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct BytesVisitor;

        impl<'de> Visitor<'de> for BytesVisitor {
            type Value = Vec<u8>;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a string representing hex-encoded bytes")
            }

            fn visit_str<E>(self, value: &str) -> Result<Vec<u8>, E>
            where
                E: de::Error,
            {
                const_hex::decode(value).map_err(E::custom)
            }
        }

        deserializer.deserialize_str(BytesVisitor)
    }

    // AnyValue <-> KeyValue conversion
    pub fn serialize_to_value<S>(value: &Option<Value>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match &value {
            Some(Value::IntValue(i)) => {
                // Attempt to serialize the intValue field
                let mut map = serializer.serialize_map(Some(1))?;
                map.serialize_entry("intValue", &i.to_string());
                map.end()
            }
            Some(Value::BytesValue(b)) => {
                let mut map = serializer.serialize_map(Some(1))?;
                map.serialize_entry("bytesValue", &base64::encode(b));
                map.end()
            }
            Some(value) => value.serialize(serializer),
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize_from_value<'de, D>(deserializer: D) -> Result<Option<Value>, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct ValueVisitor;

        #[derive(Deserialize)]
        #[serde(untagged)]
        enum StringOrInt {
            Int(i64),
            String(String),
        }

        impl StringOrInt {
            fn get_int<'de, V>(&self) -> Result<i64, V::Error>
            where
                V: de::MapAccess<'de>,
            {
                match self {
                    Self::Int(val) => Ok(*val),
                    Self::String(val) => Ok(val.parse::<i64>().map_err(de::Error::custom)?),
                }
            }
        }

        impl<'de> de::Visitor<'de> for ValueVisitor {
            type Value = Option<Value>;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a JSON object for AnyValue")
            }

            fn visit_map<V>(self, mut map: V) -> Result<Option<Value>, V::Error>
            where
                V: de::MapAccess<'de>,
            {
                let mut value: Option<any_value::Value> = None;

                while let Some(key) = map.next_key::<String>()? {
                    let key_str = key.as_str();
                    match key_str {
                        "stringValue" => {
                            let s = map.next_value()?;
                            value = Some(any_value::Value::StringValue(s));
                        }
                        "boolValue" => {
                            let b = map.next_value()?;
                            value = Some(any_value::Value::BoolValue(b));
                        }
                        "intValue" => {
                            let int_value = map.next_value::<StringOrInt>()?.get_int::<V>()?;
                            value = Some(any_value::Value::IntValue(int_value));
                        }
                        "doubleValue" => {
                            let d = map.next_value()?;
                            value = Some(any_value::Value::DoubleValue(d));
                        }
                        "arrayValue" => {
                            let a = map.next_value()?;
                            value = Some(any_value::Value::ArrayValue(a));
                        }
                        "kvlistValue" => {
                            let kv = map.next_value()?;
                            value = Some(any_value::Value::KvlistValue(kv));
                        }
                        "bytesValue" => {
                            let base64: String = map.next_value()?;
                            let decoded = base64::decode(base64.as_bytes())
                                .map_err(|e| de::Error::custom(e))?;
                            value = Some(any_value::Value::BytesValue(decoded));
                        }
                        _ => {
                            //skip unknown keys, and handle error later.
                            continue;
                        }
                    }
                }

                if let Some(v) = value {
                    Ok(Some(v))
                } else {
                    Err(de::Error::custom(
                        "Invalid data for Value, no known keys found",
                    ))
                }
            }
        }

        let value = deserializer.deserialize_map(ValueVisitor)?;
        Ok(value)
    }

    pub fn serialize_u64_to_string<S>(value: &u64, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let s = value.to_string();
        serializer.serialize_str(&s)
    }

    pub fn deserialize_string_to_u64<'de, D>(deserializer: D) -> Result<u64, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct U64Visitor;

        impl<'de> de::Visitor<'de> for U64Visitor {
            type Value = u64;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a u64 integer or a string containing a u64 integer")
            }

            fn visit_u64<E>(self, value: u64) -> Result<u64, E>
            where
                E: de::Error,
            {
                Ok(value)
            }

            fn visit_i64<E>(self, value: i64) -> Result<u64, E>
            where
                E: de::Error,
            {
                u64::try_from(value)
                    .map_err(|_| E::custom(format!("i64 value {} is out of range for u64", value)))
            }

            fn visit_str<E>(self, value: &str) -> Result<u64, E>
            where
                E: de::Error,
            {
                value.parse::<u64>().map_err(de::Error::custom)
            }
        }

        deserializer.deserialize_any(U64Visitor)
    }

    pub fn serialize_vec_u64_to_string<S>(value: &[u64], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let s = value.iter().map(|v| v.to_string()).collect::<Vec<_>>();
        let mut sq = serializer.serialize_seq(Some(s.len()))?;
        for v in value {
            sq.serialize_element(&v.to_string())?;
        }
        sq.end()
    }

    pub fn deserialize_vec_string_to_vec_u64<'de, D>(deserializer: D) -> Result<Vec<u64>, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct U64ElemVisitor;

        impl<'de> de::Visitor<'de> for U64ElemVisitor {
            type Value = u64;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a u64 integer or a string containing a u64 integer")
            }

            fn visit_u64<E>(self, value: u64) -> Result<u64, E>
            where
                E: de::Error,
            {
                Ok(value)
            }

            fn visit_i64<E>(self, value: i64) -> Result<u64, E>
            where
                E: de::Error,
            {
                u64::try_from(value)
                    .map_err(|_| E::custom(format!("i64 value {} is out of range for u64", value)))
            }

            fn visit_str<E>(self, value: &str) -> Result<u64, E>
            where
                E: de::Error,
            {
                value.parse::<u64>().map_err(de::Error::custom)
            }
        }

        struct VecU64Visitor;

        impl<'de> de::Visitor<'de> for VecU64Visitor {
            type Value = Vec<u64>;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a sequence of u64 integers or strings containing u64 integers")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Vec<u64>, A::Error>
            where
                A: de::SeqAccess<'de>,
            {
                let mut values = Vec::with_capacity(seq.size_hint().unwrap_or(0));
                while let Some(value) = seq.next_element_seed(U64ElemSeed)? {
                    values.push(value);
                }
                Ok(values)
            }
        }

        struct U64ElemSeed;

        impl<'de> de::DeserializeSeed<'de> for U64ElemSeed {
            type Value = u64;

            fn deserialize<D2>(self, deserializer: D2) -> Result<u64, D2::Error>
            where
                D2: Deserializer<'de>,
            {
                deserializer.deserialize_any(U64ElemVisitor)
            }
        }

        deserializer.deserialize_seq(VecU64Visitor)
    }

    pub fn serialize_i64_to_string<S>(value: &i64, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let s = value.to_string();
        serializer.serialize_str(&s)
    }

    pub fn deserialize_string_to_i64<'de, D>(deserializer: D) -> Result<i64, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct I64Visitor;

        impl<'de> de::Visitor<'de> for I64Visitor {
            type Value = i64;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("an i64 integer or a string containing an i64 integer")
            }

            fn visit_i64<E>(self, value: i64) -> Result<i64, E>
            where
                E: de::Error,
            {
                Ok(value)
            }

            fn visit_u64<E>(self, value: u64) -> Result<i64, E>
            where
                E: de::Error,
            {
                i64::try_from(value)
                    .map_err(|_| E::custom(format!("u64 value {} is out of range for i64", value)))
            }

            fn visit_str<E>(self, value: &str) -> Result<i64, E>
            where
                E: de::Error,
            {
                value.parse::<i64>().map_err(de::Error::custom)
            }
        }

        deserializer.deserialize_any(I64Visitor)
    }

    // Special serializer and deserializer for NaN, Infinity, and -Infinity
    pub fn serialize_f64_special<S>(value: &f64, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if value.is_nan() {
            serializer.serialize_str("NaN")
        } else if value.is_infinite() {
            if value.is_sign_positive() {
                serializer.serialize_str("Infinity")
            } else {
                serializer.serialize_str("-Infinity")
            }
        } else {
            serializer.serialize_f64(*value)
        }
    }

    pub fn deserialize_f64_special<'de, D>(deserializer: D) -> Result<f64, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct F64Visitor;

        impl<'de> de::Visitor<'de> for F64Visitor {
            type Value = f64;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a float or a string representing NaN, Infinity, or -Infinity")
            }

            fn visit_f64<E>(self, value: f64) -> Result<f64, E>
            where
                E: de::Error,
            {
                Ok(value)
            }

            fn visit_u64<E>(self, value: u64) -> Result<f64, E>
            where
                E: de::Error,
            {
                Ok(value as f64)
            }

            fn visit_i64<E>(self, value: i64) -> Result<f64, E>
            where
                E: de::Error,
            {
                Ok(value as f64)
            }

            fn visit_str<E>(self, value: &str) -> Result<f64, E>
            where
                E: de::Error,
            {
                match value {
                    "NaN" => Ok(f64::NAN),
                    "Infinity" => Ok(f64::INFINITY),
                    "-Infinity" => Ok(f64::NEG_INFINITY),
                    _ => value.parse::<f64>().map_err(|_| {
                        E::custom(format!(
                            "invalid string for f64: expected a number, NaN, Infinity, or -Infinity but got '{}'",
                            value
                        ))
                    }),
                }
            }
        }

        deserializer.deserialize_any(F64Visitor)
    }
}

#[cfg(feature = "gen-tonic-messages")]
#[path = "proto/tonic"]
/// Generated files using [`tonic`](https://docs.rs/crate/tonic) and [`prost`](https://docs.rs/crate/prost)
pub mod tonic {
    /// Service stub and clients
    #[path = ""]
    pub mod collector {
        #[cfg(feature = "logs")]
        #[path = ""]
        pub mod logs {
            #[path = "opentelemetry.proto.collector.logs.v1.rs"]
            pub mod v1;
        }

        #[cfg(feature = "metrics")]
        #[path = ""]
        pub mod metrics {
            #[path = "opentelemetry.proto.collector.metrics.v1.rs"]
            pub mod v1;
        }

        #[cfg(feature = "trace")]
        #[path = ""]
        pub mod trace {
            #[path = "opentelemetry.proto.collector.trace.v1.rs"]
            pub mod v1;
        }

        #[cfg(feature = "profiles")]
        #[path = ""]
        pub mod profiles {
            #[path = "opentelemetry.proto.collector.profiles.v1development.rs"]
            pub mod v1development;
        }
    }

    /// Common types used across all signals
    #[path = ""]
    pub mod common {
        #[path = "opentelemetry.proto.common.v1.rs"]
        pub mod v1;
    }

    /// Generated types used in logging.
    #[cfg(feature = "logs")]
    #[path = ""]
    pub mod logs {
        #[path = "opentelemetry.proto.logs.v1.rs"]
        pub mod v1;
    }

    /// Generated types used in metrics.
    #[cfg(feature = "metrics")]
    #[path = ""]
    pub mod metrics {
        #[path = "opentelemetry.proto.metrics.v1.rs"]
        pub mod v1;
    }

    /// Generated types used in resources.
    #[path = ""]
    pub mod resource {
        #[path = "opentelemetry.proto.resource.v1.rs"]
        pub mod v1;
    }

    /// Generated types used in traces.
    #[cfg(feature = "trace")]
    #[path = ""]
    pub mod trace {
        #[path = "opentelemetry.proto.trace.v1.rs"]
        pub mod v1;
    }

    /// Generated types used in zpages.
    #[cfg(feature = "zpages")]
    #[path = ""]
    pub mod tracez {
        #[path = "opentelemetry.proto.tracez.v1.rs"]
        pub mod v1;
    }

    /// Generated types used in zpages.
    #[cfg(feature = "profiles")]
    #[path = ""]
    pub mod profiles {
        #[path = "opentelemetry.proto.profiles.v1development.rs"]
        pub mod v1development;
    }

    pub use crate::transform::common::tonic::Attributes;
}
