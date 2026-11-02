#[cfg(all(
    feature = "gen-tonic-messages",
    any(feature = "trace", feature = "metrics", feature = "logs")
))]
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[cfg(all(
    feature = "gen-tonic-messages",
    any(feature = "trace", feature = "metrics", feature = "logs")
))]
pub(crate) fn to_nanos(time: SystemTime) -> u64 {
    time.duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0))
        .as_nanos() as u64
}

#[cfg(feature = "gen-tonic-messages")]
pub mod tonic {
    use crate::proto::tonic::common::v1::{
        any_value, AnyValue, ArrayValue, InstrumentationScope, KeyValue,
    };
    use opentelemetry::{Array, Value};
    use std::borrow::Cow;

    #[cfg(any(feature = "trace", feature = "logs"))]
    #[derive(Debug, Default)]
    pub struct ResourceAttributesWithSchema {
        pub attributes: Attributes,
        pub schema_url: Option<String>,
    }

    #[cfg(any(feature = "trace", feature = "logs"))]
    impl From<&opentelemetry_sdk::Resource> for ResourceAttributesWithSchema {
        fn from(resource: &opentelemetry_sdk::Resource) -> Self {
            ResourceAttributesWithSchema {
                attributes: resource_attributes(resource),
                schema_url: resource.schema_url().map(ToString::to_string),
            }
        }
    }

    #[cfg(any(feature = "trace", feature = "logs"))]
    use opentelemetry_sdk::Resource;

    impl
        From<(
            opentelemetry::InstrumentationScope,
            Option<Cow<'static, str>>,
        )> for InstrumentationScope
    {
        fn from(
            data: (
                opentelemetry::InstrumentationScope,
                Option<Cow<'static, str>>,
            ),
        ) -> Self {
            let (library, target) = data;
            InstrumentationScope {
                name: target.map_or_else(|| library.name().to_owned(), Cow::into_owned),
                version: library.version().unwrap_or_default().to_owned(),
                attributes: Attributes::from(library.attributes().cloned()).0,
                ..Default::default()
            }
        }
    }

    impl
        From<(
            &opentelemetry::InstrumentationScope,
            Option<Cow<'static, str>>,
        )> for InstrumentationScope
    {
        fn from(
            data: (
                &opentelemetry::InstrumentationScope,
                Option<Cow<'static, str>>,
            ),
        ) -> Self {
            let (library, target) = data;
            InstrumentationScope {
                name: target.map_or_else(|| library.name().to_owned(), Cow::into_owned),
                version: library.version().unwrap_or_default().to_owned(),
                attributes: Attributes::from(library.attributes().cloned()).0,
                ..Default::default()
            }
        }
    }

    /// Wrapper type for Vec<`KeyValue`>
    #[derive(Default, Debug)]
    pub struct Attributes(pub ::std::vec::Vec<crate::proto::tonic::common::v1::KeyValue>);

    impl<I: IntoIterator<Item = opentelemetry::KeyValue>> From<I> for Attributes {
        fn from(kvs: I) -> Self {
            Attributes(
                kvs.into_iter()
                    .map(|api_kv| KeyValue {
                        key: api_kv.key.as_str().to_string(),
                        value: Some(api_kv.value.into()),
                        key_strindex: 0,
                    })
                    .collect(),
            )
        }
    }

    #[cfg(feature = "logs")]
    impl<K: Into<String>, V: Into<AnyValue>> FromIterator<(K, V)> for Attributes {
        fn from_iter<T: IntoIterator<Item = (K, V)>>(iter: T) -> Self {
            Attributes(
                iter.into_iter()
                    .map(|(k, v)| KeyValue {
                        key: k.into(),
                        value: Some(v.into()),
                        key_strindex: 0,
                    })
                    .collect(),
            )
        }
    }

    impl From<Value> for AnyValue {
        fn from(value: Value) -> Self {
            AnyValue {
                value: match value {
                    Value::Bool(val) => Some(any_value::Value::BoolValue(val)),
                    Value::I64(val) => Some(any_value::Value::IntValue(val)),
                    Value::F64(val) => Some(any_value::Value::DoubleValue(val)),
                    Value::String(val) => Some(any_value::Value::StringValue(val.to_string())),
                    Value::Array(array) => Some(any_value::Value::ArrayValue(match array {
                        Array::Bool(vals) => array_into_proto(vals),
                        Array::I64(vals) => array_into_proto(vals),
                        Array::F64(vals) => array_into_proto(vals),
                        Array::String(vals) => array_into_proto(vals),
                        _ => unreachable!("Nonexistent array type"), // Needs to be updated when new array types are added
                    })),
                    _ => unreachable!("Nonexistent value type"), // Needs to be updated when new value types are added
                },
            }
        }
    }

    fn array_into_proto<T>(vals: Vec<T>) -> ArrayValue
    where
        Value: From<T>,
    {
        let values = vals
            .into_iter()
            .map(|val| AnyValue::from(Value::from(val)))
            .collect();

        ArrayValue { values }
    }

    #[cfg(any(feature = "trace", feature = "logs"))]
    pub(crate) fn resource_attributes(resource: &Resource) -> Attributes {
        resource
            .iter()
            .map(|(k, v)| opentelemetry::KeyValue::new(k.clone(), v.clone()))
            .collect::<Vec<_>>()
            .into()
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use opentelemetry::KeyValue;
        use std::borrow::Cow;

        fn assert_scope_fields(
            proto_scope: &InstrumentationScope,
            expected_name: &str,
            expected_version: &str,
            expected_attr_key: &str,
        ) {
            assert_eq!(proto_scope.name, expected_name);
            assert_eq!(proto_scope.version, expected_version);
            assert_eq!(proto_scope.attributes.len(), 1);
            assert_eq!(proto_scope.attributes[0].key, expected_attr_key);
        }

        #[test]
        fn instrumentation_scope_with_target_overrides_name_but_preserves_version_and_attributes() {
            let scope = opentelemetry::InstrumentationScope::builder("my-lib")
                .with_version("1.0.0")
                .with_attributes([KeyValue::new("feature", "metrics")])
                .build();
            let target: Option<Cow<'static, str>> = Some(Cow::Borrowed("my_app::handlers"));

            let from_owned = InstrumentationScope::from((scope.clone(), target.clone()));
            let from_ref = InstrumentationScope::from((&scope, target));

            assert_scope_fields(&from_owned, "my_app::handlers", "1.0.0", "feature");
            assert_scope_fields(&from_ref, "my_app::handlers", "1.0.0", "feature");
        }

        #[test]
        fn instrumentation_scope_without_target_preserves_all_fields() {
            let scope = opentelemetry::InstrumentationScope::builder("my-lib")
                .with_version("1.0.0")
                .with_attributes([KeyValue::new("feature", "metrics")])
                .build();
            let target: Option<Cow<'static, str>> = None;

            let from_owned = InstrumentationScope::from((scope.clone(), target.clone()));
            let from_ref = InstrumentationScope::from((&scope, target));

            assert_scope_fields(&from_owned, "my-lib", "1.0.0", "feature");
            assert_scope_fields(&from_ref, "my-lib", "1.0.0", "feature");
        }
    }
}
