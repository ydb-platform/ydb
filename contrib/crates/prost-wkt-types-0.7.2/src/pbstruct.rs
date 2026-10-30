use serde::de::{self, Deserialize, Deserializer, MapAccess, SeqAccess, Visitor};
use serde::ser::{Serialize, SerializeMap, SerializeSeq, Serializer};

use std::borrow::Cow;
use std::convert::TryFrom;
use std::fmt;

include!(concat!(env!("OUT_DIR"), "/pbstruct/google.protobuf.rs"));

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ValueError {
    description: Cow<'static, str>,
}

impl ValueError {
    pub fn new<S>(description: S) -> Self
    where
        S: Into<Cow<'static, str>>,
    {
        ValueError {
            description: description.into(),
        }
    }
}

impl std::error::Error for ValueError {
    fn description(&self) -> &str {
        &self.description
    }
}

impl std::fmt::Display for ValueError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("failed to convert Value: ")?;
        f.write_str(&self.description)
    }
}

impl Value {
    pub fn null() -> Self {
        let kind = Some(value::Kind::NullValue(0));
        Value { kind }
    }
    pub fn number(num: f64) -> Self {
        Value::from(num)
    }
    pub fn string(s: String) -> Self {
        Value::from(s)
    }
    pub fn bool(b: bool) -> Self {
        Value::from(b)
    }
    pub fn pb_struct(m: std::collections::HashMap<std::string::String, Value>) -> Self {
        Value::from(m)
    }
    pub fn pb_list(l: std::vec::Vec<Value>) -> Self {
        Value::from(l)
    }
}

impl From<NullValue> for Value {
    fn from(_: NullValue) -> Self {
        Value::null()
    }
}

impl From<f64> for Value {
    fn from(num: f64) -> Self {
        let kind = Some(value::Kind::NumberValue(num));
        Value { kind }
    }
}

impl TryFrom<Value> for f64 {
    type Error = ValueError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value.kind {
            Some(value::Kind::NumberValue(num)) => Ok(num),
            Some(_other) => Err(ValueError::new(
                "Cannot convert to f64 because this is not a ValueNumber.",
            )),
            _ => Err(ValueError::new(
                "Conversion to f64 failed because value is empty!",
            )),
        }
    }
}

impl From<String> for Value {
    fn from(s: String) -> Self {
        let kind = Some(value::Kind::StringValue(s));
        Value { kind }
    }
}

impl TryFrom<Value> for String {
    type Error = ValueError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value.kind {
            Some(value::Kind::StringValue(string)) => Ok(string),
            Some(_other) => Err(ValueError::new(
                "Cannot convert to String because this is not a StringValue.",
            )),
            _ => Err(ValueError::new(
                "Conversion to String failed because value is empty!",
            )),
        }
    }
}

impl From<bool> for Value {
    fn from(b: bool) -> Self {
        let kind = Some(value::Kind::BoolValue(b));
        Value { kind }
    }
}

impl TryFrom<Value> for bool {
    type Error = ValueError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value.kind {
            Some(value::Kind::BoolValue(b)) => Ok(b),
            Some(_other) => Err(ValueError::new(
                "Cannot convert to bool because this is not a BoolValue.",
            )),
            _ => Err(ValueError::new(
                "Conversion to bool failed because value is empty!",
            )),
        }
    }
}

impl From<std::collections::HashMap<std::string::String, Value>> for Value {
    fn from(fields: std::collections::HashMap<String, Value>) -> Self {
        let s = Struct { fields };
        let kind = Some(value::Kind::StructValue(s));
        Value { kind }
    }
}

impl TryFrom<Value> for std::collections::HashMap<std::string::String, Value> {
    type Error = ValueError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value.kind {
            Some(value::Kind::StructValue(s)) => Ok(s.fields),
            Some(_other) => Err(ValueError::new(
                "Cannot convert to HashMap<String, Value> because this is not a StructValue.",
            )),
            _ => Err(ValueError::new(
                "Conversion to HashMap<String, Value> failed because value is empty!",
            )),
        }
    }
}

impl From<std::vec::Vec<Value>> for Value {
    fn from(values: Vec<Value>) -> Self {
        let v = ListValue { values };
        let kind = Some(value::Kind::ListValue(v));
        Value { kind }
    }
}

impl TryFrom<Value> for std::vec::Vec<Value> {
    type Error = ValueError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value.kind {
            Some(value::Kind::ListValue(list)) => Ok(list.values),
            Some(_other) => Err(ValueError::new(
                "Cannot convert to Vec<Value> because this is not a ListValue.",
            )),
            _ => Err(ValueError::new(
                "Conversion to Vec<Value> failed because value is empty!",
            )),
        }
    }
}

impl Serialize for ListValue {
    fn serialize<S>(&self, serializer: S) -> Result<<S as Serializer>::Ok, <S as Serializer>::Error>
    where
        S: Serializer,
    {
        let mut seq = serializer.serialize_seq(Some(self.values.len()))?;
        for e in &self.values {
            seq.serialize_element(e)?;
        }
        seq.end()
    }
}

impl Serialize for Struct {
    fn serialize<S>(&self, serializer: S) -> Result<<S as Serializer>::Ok, <S as Serializer>::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(self.fields.len()))?;
        for (k, v) in &self.fields {
            map.serialize_entry(k, v)?;
        }
        map.end()
    }
}

impl Serialize for Value {
    fn serialize<S>(&self, serializer: S) -> Result<<S as Serializer>::Ok, <S as Serializer>::Error>
    where
        S: Serializer,
    {
        match &self.kind {
            Some(value::Kind::NumberValue(num)) => serializer.serialize_f64(*num),
            Some(value::Kind::StringValue(string)) => serializer.serialize_str(string),
            Some(value::Kind::BoolValue(boolean)) => serializer.serialize_bool(*boolean),
            Some(value::Kind::NullValue(_)) => serializer.serialize_none(),
            Some(value::Kind::ListValue(list)) => list.serialize(serializer),
            Some(value::Kind::StructValue(object)) => object.serialize(serializer),
            _ => serializer.serialize_none(),
        }
    }
}

struct ListValueVisitor;
impl<'de> Visitor<'de> for ListValueVisitor {
    type Value = crate::ListValue;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a prost_wkt_types::ListValue struct")
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut values: Vec<Value> = Vec::new();
        while let Some(el) = seq.next_element()? {
            values.push(el)
        }
        Ok(ListValue { values })
    }
}

impl<'de> Deserialize<'de> for ListValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, <D as Deserializer<'de>>::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(ListValueVisitor)
    }
}

struct StructVisitor;
impl<'de> Visitor<'de> for StructVisitor {
    type Value = crate::Struct;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a prost_wkt_types::Struct struct")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut fields: std::collections::HashMap<String, Value> = std::collections::HashMap::new();
        while let Some((key, value)) = map.next_entry::<String, Value>()? {
            fields.insert(key, value);
        }
        Ok(Struct { fields })
    }
}

impl<'de> Deserialize<'de> for Struct {
    fn deserialize<D>(deserializer: D) -> Result<Self, <D as Deserializer<'de>>::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(StructVisitor)
    }
}

impl<'de> Deserialize<'de> for Value {
    fn deserialize<D>(deserializer: D) -> Result<Self, <D as Deserializer<'de>>::Error>
    where
        D: Deserializer<'de>,
    {
        struct ValueVisitor;

        impl<'de> Visitor<'de> for ValueVisitor {
            type Value = crate::Value;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a prost_wkt_types::Value struct")
            }

            fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(Value::from(value))
            }

            fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(Value::from(value as f64))
            }

            fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(Value::from(value as f64))
            }

            fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(Value::from(value))
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(Value::from(String::from(value)))
            }

            fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(Value::from(value))
            }

            fn visit_none<E>(self) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(Value::null())
            }

            fn visit_unit<E>(self) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(Value::null())
            }

            fn visit_seq<A>(self, seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                ListValueVisitor.visit_seq(seq).map(|lv| {
                    let kind = Some(value::Kind::ListValue(lv));
                    Value { kind }
                })
            }

            fn visit_map<A>(self, map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                StructVisitor.visit_map(map).map(|s| {
                    let kind = Some(value::Kind::StructValue(s));
                    Value { kind }
                })
            }
        }
        deserializer.deserialize_any(ValueVisitor)
    }
}

#[cfg(test)]
mod tests {
    use crate::pbstruct::*;
    use std::collections::HashMap;

    #[test]
    fn conversion_test() {
        let number: Value = Value::from(10.0);
        println!("Number: {number:?}");
        let null: Value = Value::null();
        println!("Null: {null:?}");
        let string: Value = Value::from(String::from("Hello"));
        println!("String: {string:?}");
        let list = vec![Value::null(), Value::from(100.0)];
        let pb_list: Value = Value::from(list);
        println!("List: {pb_list:?}");
        let mut map: HashMap<String, Value> = HashMap::new();
        map.insert(String::from("some_number"), number);
        map.insert(String::from("a_null_value"), null);
        map.insert(String::from("string"), string);
        map.insert(String::from("list"), pb_list);
        let pb_struct: Value = Value::from(map);
        println!("Struct: {pb_struct:?}");
    }

    #[test]
    fn convert_serde_json_test() {
        let data = r#"{
            "string":"hello",
            "timestamp":"1970-01-01T00:01:39.000000042Z",
            "boolean":true,
            "data": {
              "test_number": 1.0,
              "test_bool": true,
              "testString": "hi there",
              "testList": [1.0, 2.0, 3.0, 4.0],
              "testInnerStruct": {
                "one": 1.0,
                "two": 2.0
              }
            },
            "list": []
          }"#;
        let sj: serde_json::Value = serde_json::from_str(data).unwrap();
        println!("serde_json::Value: {sj:#?}");
        let pj: Value = serde_json::from_value(sj.clone()).unwrap();
        let string: String = serde_json::to_string_pretty(&pj).unwrap();
        println!("prost_wkt_types String: {string}");
        let back: serde_json::Value = serde_json::from_str(&string).unwrap();
        assert_eq!(sj, back);
    }
}
