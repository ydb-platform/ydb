include!(concat!(env!("OUT_DIR"), "/pbempty/google.protobuf.rs"));

const EMPTY: Empty = Empty {};

impl From<()> for Empty {
    fn from(_value: ()) -> Self {
        EMPTY
    }
}

#[cfg(feature = "schemars")]
mod schemars_impl {
    use super::Empty;
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
                "description": "Represents an empty message",
            })
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
