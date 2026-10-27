include!(concat!(env!("OUT_DIR"), "/pbempty/google.protobuf.rs"));

const EMPTY: Empty = Empty {};

impl From<()> for Empty {
    fn from(_value: ()) -> Self {
        EMPTY
    }
}

/// Schema description shared by the schemars and utoipa impls.
#[cfg(any(feature = "schemars", feature = "utoipa"))]
pub(crate) const EMPTY_DESCRIPTION: &str = "Represents an empty message";

#[cfg(feature = "schemars")]
mod schemars_impl {
    use super::{Empty, EMPTY_DESCRIPTION};
    use schemars::generate::SchemaGenerator;
    use schemars::{json_schema, JsonSchema, Schema};
    use std::borrow::Cow;

    impl JsonSchema for Empty {
        fn schema_name() -> Cow<'static, str> {
            Cow::Borrowed("Empty")
        }

        fn schema_id() -> Cow<'static, str> {
            Cow::Borrowed("prost_wkt_types::Empty")
        }

        fn json_schema(_gen: &mut SchemaGenerator) -> Schema {
            json_schema!({
                "type": "object",
                "description": EMPTY_DESCRIPTION,
            })
        }
    }
}

#[cfg(feature = "utoipa")]
mod utoipa_impl {
    use super::{Empty, EMPTY_DESCRIPTION};
    use std::borrow::Cow;
    use utoipa::openapi::schema::{ObjectBuilder, SchemaType, Type};
    use utoipa::openapi::{RefOr, Schema};
    use utoipa::{PartialSchema, ToSchema};

    impl PartialSchema for Empty {
        fn schema() -> RefOr<Schema> {
            ObjectBuilder::new()
                .schema_type(SchemaType::Type(Type::Object))
                .description(Some(EMPTY_DESCRIPTION))
                .into()
        }
    }

    impl ToSchema for Empty {
        fn name() -> Cow<'static, str> {
            Cow::Borrowed("google.protobuf.Empty")
        }
    }
}

#[cfg(test)]
mod tests {

    use crate::pbempty::*;

    #[test]
    fn serialize_empty() {
        let msg = EMPTY;
        println!(
            "Serialized to string: {}",
            serde_json::to_string_pretty(&msg).unwrap()
        );
    }

    #[test]
    fn deserialize_empty() {
        let msg: Empty =
            serde_json::from_str("{}").expect("Could not deserialize `{}` to an Empty struct!");
        assert_eq!(msg, EMPTY);
    }

    #[test]
    fn convert_unit() {
        let msg: Empty = ().into();
        assert_eq!(msg, Empty {});
    }
}

#[cfg(all(test, feature = "utoipa"))]
mod utoipa_tests {
    use super::Empty;
    use utoipa::{PartialSchema, ToSchema};

    #[test]
    fn utoipa_empty_schema() {
        let schema = serde_json::to_value(Empty::schema()).expect("json");
        assert_eq!(schema["type"], "object");
        assert_eq!(schema["description"], super::EMPTY_DESCRIPTION);
        assert_eq!(Empty::name(), "google.protobuf.Empty");
    }
}
