use crate::de::{FnApply, MapLookupVisitor};
use crate::private::Registry;
use crate::ser::Wrap;
use alloc::boxed::Box;
use core::fmt;
use serde::de::{self, Deserializer, MapAccess, Visitor};
use serde::ser::{SerializeMap, Serializer};

pub fn serialize<S, T>(
    serializer: S,
    variant: &'static str,
    concrete: &T,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
    T: ?Sized + erased_serde::Serialize,
{
    let mut ser = serializer.serialize_map(Some(1))?;
    ser.serialize_entry(variant, &Wrap(concrete))?;
    ser.end()
}

pub fn deserialize<'de, D, T>(
    deserializer: D,
    trait_object: &'static str,
    registry: &'static Registry<T>,
) -> Result<Box<T>, D::Error>
where
    D: Deserializer<'de>,
    T: ?Sized,
{
    let visitor = TaggedVisitor {
        trait_object,
        registry,
    };
    deserializer.deserialize_map(visitor)
}

struct TaggedVisitor<T: ?Sized + 'static> {
    trait_object: &'static str,
    registry: &'static Registry<T>,
}

impl<'de, T: ?Sized> Visitor<'de> for TaggedVisitor<T> {
    type Value = Box<T>;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        write!(formatter, "dyn {}", self.trait_object)
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let map_lookup = MapLookupVisitor {
            expected: &self,
            registry: self.registry,
        };
        let Some(deserialize_fn) = map.next_key_seed(map_lookup)? else {
            return Err(de::Error::custom(format_args!(
                "expected externally tagged dyn {}",
                self.trait_object
            )));
        };
        map.next_value_seed(FnApply { deserialize_fn })
    }
}
