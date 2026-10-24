use crate::LanguageTag;
use serde::de::{Error, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;

impl Serialize for LanguageTag {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

struct LanguageTagVisitor;

impl<'de> Visitor<'de> for LanguageTagVisitor {
    type Value = LanguageTag;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "a language tag string")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: Error,
    {
        value.parse::<Self::Value>().map_err(Error::custom)
    }
}

impl<'de> Deserialize<'de> for LanguageTag {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(LanguageTagVisitor {})
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn basic() {
        let input = "\"en-Latn-gb-boont-r-extended-sequence-x-private\"";
        let deser: LanguageTag = serde_json::from_str(input).unwrap();
        deser.validate().unwrap();
        let ser = serde_json::to_string(&deser).unwrap();
        assert!(ser.eq_ignore_ascii_case(input));
    }

    #[test]
    fn reader_works() {
        let input = "\"en-Latn-gb-boont-r-extended-sequence-x-private\"";
        let rdr = Cursor::new(input);
        let _: LanguageTag = serde_json::from_reader(rdr).unwrap();
    }
}
