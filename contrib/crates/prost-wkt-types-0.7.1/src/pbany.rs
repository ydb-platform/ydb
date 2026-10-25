use prost_wkt::MessageSerde;
use serde::de::{Deserialize, Deserializer};
use serde::ser::{Serialize, SerializeStruct, Serializer};

include!(concat!(env!("OUT_DIR"), "/pbany/google.protobuf.rs"));

use prost::{DecodeError, EncodeError, Message, Name};

use std::borrow::Cow;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AnyError {
    description: Cow<'static, str>,
}

impl AnyError {
    pub fn new<S>(description: S) -> Self
    where
        S: Into<Cow<'static, str>>,
    {
        AnyError {
            description: description.into(),
        }
    }
}

impl std::error::Error for AnyError {
    fn description(&self) -> &str {
        &self.description
    }
}

impl std::fmt::Display for AnyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("failed to convert Value: ")?;
        f.write_str(&self.description)
    }
}

impl From<prost::DecodeError> for AnyError {
    fn from(error: DecodeError) -> Self {
        AnyError::new(format!("Error decoding message: {error:?}"))
    }
}

impl From<prost::EncodeError> for AnyError {
    fn from(error: prost::EncodeError) -> Self {
        AnyError::new(format!("Error encoding message: {error:?}"))
    }
}

impl Any {
    //#[deprecated(since = "0.5.0", note = "please use `from_msg` instead")]
    /// Packs a message into an `Any` containing a `type_url` which will take the format
    /// of `type.googleapis.com/package_name.struct_name`, and a value containing the
    /// encoded message.
    pub fn try_pack<T>(message: T) -> Result<Self, AnyError>
    where
        T: Message + MessageSerde + Default,
    {
        let type_url = MessageSerde::type_url(&message).to_string();
        // Serialize the message into a value
        let mut buf = Vec::with_capacity(message.encoded_len());
        message.encode(&mut buf)?;
        let encoded = Any {
            type_url,
            value: buf,
        };
        Ok(encoded)
    }

    //#[deprecated(since = "0.5.0", note = "please use `to_msg` instead")]
    /// Unpacks the contents of the `Any` into the provided message type. Example usage:
    ///
    /// ```ignore
    /// let back: Foo = any.unpack_as(Foo::default())?;
    /// ```
    pub fn unpack_as<T: Message>(self, mut target: T) -> Result<T, AnyError> {
        let instance = target.merge(self.value.as_slice()).map(|_| target)?;
        Ok(instance)
    }

    /// Unpacks the contents of the `Any` into the `MessageSerde` trait object. Example
    /// usage:
    ///
    /// ```ignore
    /// let back: Box<dyn MessageSerde> = any.try_unpack()?;
    /// ```
    pub fn try_unpack(self) -> Result<Box<dyn prost_wkt::MessageSerde>, AnyError> {
        ::prost_wkt::inventory::iter::<::prost_wkt::MessageSerdeDecoderEntry>
            .into_iter()
            .find(|entry| self.type_url == entry.type_url)
            .ok_or_else(|| format!("Failed to deserialize {}. Make sure prost-wkt-build is executed.", self.type_url))
            .and_then(|entry| {
                (entry.decoder)(&self.value).map_err(|error| {
                    format!(
                        "Failed to deserialize {}. Make sure it implements prost::Message. Error reported: {}",
                        self.type_url,
                        error
                    )
                })
            })
            .map_err(AnyError::new)
    }

    /// From Prost's [`Any`] implementation.
    /// Serialize the given message type `M` as [`Any`].
    pub fn from_msg<M>(msg: &M) -> Result<Self, EncodeError>
    where
        M: Name,
    {
        let type_url = M::type_url();
        let mut value = Vec::new();
        Message::encode(msg, &mut value)?;
        Ok(Any { type_url, value })
    }

    /// From Prost's [`Any`] implementation.
    /// Decode the given message type `M` from [`Any`], validating that it has
    /// the expected type URL.
    #[allow(clippy::all)]
    pub fn to_msg<M>(&self) -> Result<M, DecodeError>
    where
        M: Default + Name + Sized,
    {
        let expected_type_url = M::type_url();

        match (
            TypeUrl::new(&expected_type_url),
            TypeUrl::new(&self.type_url),
        ) {
            (Some(expected), Some(actual)) => {
                if expected == actual {
                    return Ok(M::decode(&*self.value)?);
                }
            }
            _ => (),
        }

        let mut err = DecodeError::new(format!(
            "expected type URL: \"{}\" (got: \"{}\")",
            expected_type_url, &self.type_url
        ));
        err.push("unexpected type URL", "type_url");
        Err(err)
    }
}

impl Serialize for Any {
    fn serialize<S>(&self, serializer: S) -> Result<<S as Serializer>::Ok, <S as Serializer>::Error>
    where
        S: Serializer,
    {
        match self.clone().try_unpack() {
            Ok(result) => serde::ser::Serialize::serialize(result.as_ref(), serializer),
            Err(_) => {
                let mut state = serializer.serialize_struct("Any", 3)?;
                state.serialize_field("@type", &self.type_url)?;
                state.serialize_field("value", &self.value)?;
                state.end()
            }
        }
    }
}

impl<'de> Deserialize<'de> for Any {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let erased: Box<dyn prost_wkt::MessageSerde> =
            serde::de::Deserialize::deserialize(deserializer)?;
        let type_url = erased.type_url().to_string();
        let value = erased.try_encoded().map_err(|err| {
            serde::de::Error::custom(format!("Failed to encode message: {err:?}"))
        })?;
        Ok(Any { type_url, value })
    }
}

#[cfg(feature = "schemars")]
mod schemars_impl {
    use super::Any;
    use schemars::generate::SchemaGenerator;
    use schemars::{json_schema, JsonSchema, Schema};
    use std::borrow::Cow;

    impl JsonSchema for Any {
        fn schema_name() -> Cow<'static, str> {
            Cow::Borrowed("Any")
        }

        fn schema_id() -> Cow<'static, str> {
            Cow::Borrowed("prost_wkt_types::Any")
        }

        fn json_schema(_gen: &mut SchemaGenerator) -> Schema {
            json_schema!({
                "type": "object",
                "description": "Represents a dynamically typed protocol buffer message",
                "examples": [
                    {
                        "@type": "type.googleapis.com/google.protobuf.Duration",
                        "value": "1.5s",
                    }
                ],
                "properties": {
                    "@type": { "type": "string" },
                    "value": { "type": "string" },
                },
                "required": ["@type", "value"],
            })
        }
    }
}

/// URL/resource name that uniquely identifies the type of the serialized protocol buffer message,
/// e.g. `type.googleapis.com/google.protobuf.Duration`.
///
/// This string must contain at least one "/" character.
///
/// The last segment of the URL's path must represent the fully qualified name of the type (as in
/// `path/google.protobuf.Duration`). The name should be in a canonical form (e.g., leading "." is
/// not accepted).
///
/// If no scheme is provided, `https` is assumed.
///
/// Schemes other than `http`, `https` (or the empty scheme) might be used with implementation
/// specific semantics.
#[derive(Debug, Eq, PartialEq)]
struct TypeUrl<'a> {
    /// Fully qualified name of the type, e.g. `google.protobuf.Duration`
    full_name: &'a str,
}

impl<'a> TypeUrl<'a> {
    fn new(s: &'a str) -> core::option::Option<Self> {
        // Must contain at least one "/" character.
        let slash_pos = s.rfind('/')?;

        // The last segment of the URL's path must represent the fully qualified name
        // of the type (as in `path/google.protobuf.Duration`)
        let full_name = s.get((slash_pos + 1)..)?;

        // The name should be in a canonical form (e.g., leading "." is not accepted).
        if full_name.starts_with('.') {
            return None;
        }

        Some(Self { full_name })
    }
}

#[cfg(test)]
mod tests {
    use crate::pbany::*;
    use prost::{DecodeError, EncodeError, Message};
    use prost_wkt::*;
    use serde::*;
    use serde_json::json;

    #[derive(Clone, Eq, PartialEq, ::prost::Message, Serialize, Deserialize)]
    #[serde(default, rename_all = "camelCase")]
    pub struct Foo {
        #[prost(string, tag = "1")]
        pub string: std::string::String,
    }

    impl Name for Foo {
        const NAME: &'static str = "Foo";
        const PACKAGE: &'static str = "any.test";
    }

    #[typetag::serde(name = "type.googleapis.com/any.test.Foo")]
    impl prost_wkt::MessageSerde for Foo {
        fn message_name(&self) -> &'static str {
            "Foo"
        }

        fn package_name(&self) -> &'static str {
            "any.test"
        }

        fn type_url(&self) -> &'static str {
            "type.googleapis.com/any.test.Foo"
        }
        fn new_instance(&self, data: Vec<u8>) -> Result<Box<dyn MessageSerde>, DecodeError> {
            let mut target = Self::default();
            Message::merge(&mut target, data.as_slice())?;
            let erased: Box<dyn MessageSerde> = Box::new(target);
            Ok(erased)
        }

        fn try_encoded(&self) -> Result<Vec<u8>, EncodeError> {
            let mut buf = Vec::with_capacity(Message::encoded_len(self));
            Message::encode(self, &mut buf)?;
            Ok(buf)
        }
    }

    #[test]
    fn pack_unpack_test() {
        let msg = Foo {
            string: "Hello World!".to_string(),
        };
        let any = Any::try_pack(msg.clone()).unwrap();
        println!("{any:?}");
        let unpacked = any.unpack_as(Foo::default()).unwrap();
        println!("{unpacked:?}");
        assert_eq!(unpacked, msg)
    }

    #[test]
    fn pack_unpack_with_downcast_test() {
        let msg = Foo {
            string: "Hello World!".to_string(),
        };
        let any = Any::try_pack(msg.clone()).unwrap();
        println!("{any:?}");
        let unpacked: &dyn MessageSerde = &any.unpack_as(Foo::default()).unwrap();
        let downcast = unpacked.downcast_ref::<Foo>().unwrap();
        assert_eq!(downcast, &msg);
    }

    #[test]
    fn deserialize_default_test() {
        let type_url = "type.googleapis.com/any.test.Foo";
        let data = json!({
            "@type": type_url,
            "value": {}
        });
        let erased: Box<dyn MessageSerde> = serde_json::from_value(data).unwrap();
        let foo: &Foo = erased.downcast_ref::<Foo>().unwrap();
        println!("Deserialize default: {foo:?}");
        assert_eq!(foo, &Foo::default())
    }

    #[test]
    fn check_prost_any_serialization() {
        let message = crate::Timestamp::date(2000, 1, 1).unwrap();
        let any = Any::from_msg(&message).unwrap();
        assert_eq!(
            &any.type_url,
            "type.googleapis.com/google.protobuf.Timestamp"
        );

        let message2 = any.to_msg::<crate::Timestamp>().unwrap();
        assert_eq!(message, message2);

        // Wrong type URL
        assert!(any.to_msg::<crate::Duration>().is_err());
    }
}
