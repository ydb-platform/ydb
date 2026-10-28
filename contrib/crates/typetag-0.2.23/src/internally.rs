use crate::content::Content;
use crate::de::{FnApply, MapLookupVisitor};
use crate::private::Registry;
use crate::ser::{InternallyTaggedSerializer, Wrap};
use alloc::borrow::ToOwned;
use alloc::boxed::Box;
use alloc::string::String;
use alloc::vec::Vec;
use core::fmt;
use serde::de::{
    self, DeserializeSeed, Deserializer, EnumAccess, IgnoredAny, IntoDeserializer, MapAccess,
    VariantAccess, Visitor,
};
use serde::forward_to_deserialize_any;
use serde::ser::{Serialize, Serializer};

pub fn serialize<S, T>(
    serializer: S,
    tag: &'static str,
    variant: &'static str,
    concrete: &T,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
    T: ?Sized + erased_serde::Serialize,
{
    let adapter = InternallyTaggedSerializer {
        tag,
        variant,
        delegate: serializer,
    };
    Wrap(concrete).serialize(adapter)
}

pub fn deserialize<'de, D, T>(
    deserializer: D,
    trait_object: &'static str,
    tag: &'static str,
    default_variant: Option<&'static str>,
    registry: &'static Registry<T>,
) -> Result<Box<T>, D::Error>
where
    D: Deserializer<'de>,
    T: ?Sized,
{
    let visitor = TaggedVisitor {
        trait_object,
        tag,
        default_variant,
        registry,
    };
    deserializer.deserialize_map(visitor)
}

pub(crate) const DEFAULT_KEY: &str = "value";

struct TaggedVisitor<T: ?Sized + 'static> {
    trait_object: &'static str,
    tag: &'static str,
    default_variant: Option<&'static str>,
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
        let key_visitor = KeyVisitor {
            trait_object: self.trait_object,
            tag: self.tag,
        };

        let map_lookup = MapLookupVisitor {
            expected: &self,
            registry: self.registry,
        };

        let mut deserialize_fn = None;
        let mut entries = Vec::new();

        while let Some(key) = map.next_key_seed(key_visitor)? {
            match key {
                Key::Tag => {
                    let value = map.next_value_seed(map_lookup)?;
                    if entries.is_empty() {
                        let fn_apply = FnApply {
                            deserialize_fn: value,
                        };
                        let rest = MapWithStringKeys { map };
                        return fn_apply.deserialize(rest);
                    }
                    deserialize_fn = Some(value);
                    while let Some(key) = map.next_key::<String>()? {
                        let key = Content::String(key);
                        let value = map.next_value::<Content>()?;
                        entries.push((key, value));
                    }
                    break;
                }
                Key::Other(key) => {
                    let key = Content::String(key);
                    let value = map.next_value::<Content>()?;
                    entries.push((key, value));
                }
            }
        }

        let deserialize_fn = match deserialize_fn {
            Some(deserialize_fn) => deserialize_fn,
            None => match self.default_variant {
                Some(variant) => map_lookup.visit_str(variant)?,
                None => return Err(de::Error::missing_field(self.tag)),
            },
        };

        let fn_apply = FnApply { deserialize_fn };
        let content = Content::Map(entries).into_deserializer();
        fn_apply.deserialize(content)
    }
}

enum Key {
    Tag,
    Other(String),
}

#[derive(Copy, Clone)]
struct KeyVisitor {
    trait_object: &'static str,
    tag: &'static str,
}

impl<'de> Visitor<'de> for KeyVisitor {
    type Value = Key;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        write!(formatter, "a key in dyn {}", self.trait_object)
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        if value == self.tag {
            Ok(Key::Tag)
        } else {
            Ok(Key::Other(value.to_owned()))
        }
    }
}

impl<'de> DeserializeSeed<'de> for KeyVisitor {
    type Value = Key;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(self)
    }
}

struct MapWithStringKeys<A> {
    map: A,
}

impl<'de, A> MapWithStringKeys<A>
where
    A: MapAccess<'de>,
{
    fn try_default_key(&mut self) -> Result<(), A::Error> {
        self.map
            .next_key_seed(DefaultKey)?
            .ok_or_else(|| de::Error::missing_field(DEFAULT_KEY))
    }
}

macro_rules! deserialize_default_key {
    ($self:ident, $method:ident, $visitor:ident $(, $k:ident : $ty:ty)*) => {{
        struct Wrap<V> {
            $(
                $k : $ty,
            )*
            visitor: V,
        }

        impl<'de, V> DeserializeSeed<'de> for Wrap<V>
        where
            V: Visitor<'de>,
        {
            type Value = V::Value;

            fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
            where
                D: Deserializer<'de>,
            {
                deserializer.$method($(self.$k,)* self.visitor)
            }
        }

        let mut this = $self;
        this.try_default_key()?;
        this.map.next_value_seed(Wrap { $($k,)* $visitor })
    }};
}

impl<'de, A> Deserializer<'de> for MapWithStringKeys<A>
where
    A: MapAccess<'de>,
{
    type Error = A::Error;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_map(self)
    }

    fn deserialize_bool<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        deserialize_default_key!(self, deserialize_bool, visitor)
    }

    fn deserialize_i8<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        deserialize_default_key!(self, deserialize_i8, visitor)
    }

    fn deserialize_i16<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        deserialize_default_key!(self, deserialize_i16, visitor)
    }

    fn deserialize_i32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        deserialize_default_key!(self, deserialize_i32, visitor)
    }

    fn deserialize_i64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        deserialize_default_key!(self, deserialize_i64, visitor)
    }

    fn deserialize_i128<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        deserialize_default_key!(self, deserialize_i128, visitor)
    }

    fn deserialize_u8<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        deserialize_default_key!(self, deserialize_u8, visitor)
    }

    fn deserialize_u16<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        deserialize_default_key!(self, deserialize_u16, visitor)
    }

    fn deserialize_u32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        deserialize_default_key!(self, deserialize_u32, visitor)
    }

    fn deserialize_u64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        deserialize_default_key!(self, deserialize_u64, visitor)
    }

    fn deserialize_u128<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        deserialize_default_key!(self, deserialize_u128, visitor)
    }

    fn deserialize_f32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        deserialize_default_key!(self, deserialize_f32, visitor)
    }

    fn deserialize_f64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        deserialize_default_key!(self, deserialize_f64, visitor)
    }

    fn deserialize_char<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        deserialize_default_key!(self, deserialize_char, visitor)
    }

    fn deserialize_str<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        deserialize_default_key!(self, deserialize_str, visitor)
    }

    fn deserialize_string<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        deserialize_default_key!(self, deserialize_string, visitor)
    }

    fn deserialize_bytes<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        deserialize_default_key!(self, deserialize_bytes, visitor)
    }

    fn deserialize_byte_buf<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        deserialize_default_key!(self, deserialize_byte_buf, visitor)
    }

    fn deserialize_option<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.map.next_key_seed(DefaultKey)? {
            None => visitor.visit_none(),
            Some(()) => visitor.visit_some(MapValueAsDeserializer { map: self.map }),
        }
    }

    fn deserialize_unit<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_unit()
    }

    fn deserialize_unit_struct<V>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_unit()
    }

    fn deserialize_newtype_struct<V>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_newtype_struct(self)
    }

    fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        deserialize_default_key!(self, deserialize_seq, visitor)
    }

    fn deserialize_tuple<V>(self, len: usize, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        deserialize_default_key!(self, deserialize_tuple, visitor, len: usize)
    }

    fn deserialize_tuple_struct<V>(
        self,
        name: &'static str,
        len: usize,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        deserialize_default_key!(
            self,
            deserialize_tuple_struct,
            visitor,
            name: &'static str,
            len: usize
        )
    }

    fn deserialize_map<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_map(self)
    }

    fn deserialize_struct<V>(
        self,
        _name: &'static str,
        _fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_map(self)
    }

    fn deserialize_enum<V>(
        self,
        name: &'static str,
        _variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_enum(MapEntryAsEnum {
            map: self.map,
            name,
        })
    }

    fn deserialize_identifier<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        deserialize_default_key!(self, deserialize_identifier, visitor)
    }

    fn deserialize_ignored_any<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        if self.map.next_key_seed(DefaultKey)?.is_some() {
            self.map.next_value::<IgnoredAny>()?;
        }
        visitor.visit_unit()
    }
}

impl<'de, A> MapAccess<'de> for MapWithStringKeys<A>
where
    A: MapAccess<'de>,
{
    type Error = A::Error;

    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>, Self::Error>
    where
        K: DeserializeSeed<'de>,
    {
        self.map.next_key_seed(StringKeySeed { seed })
    }

    fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value, Self::Error>
    where
        V: DeserializeSeed<'de>,
    {
        self.map.next_value_seed(seed)
    }
}

struct MapEntryAsEnum<A> {
    map: A,
    name: &'static str,
}

impl<'de, A> EnumAccess<'de> for MapEntryAsEnum<A>
where
    A: MapAccess<'de>,
{
    type Error = A::Error;
    type Variant = Self;

    fn variant_seed<V>(mut self, seed: V) -> Result<(V::Value, Self::Variant), Self::Error>
    where
        V: DeserializeSeed<'de>,
    {
        match self.map.next_key_seed(seed)? {
            Some(variant) => Ok((variant, self)),
            None => Err(de::Error::custom(format_args!(
                "expected enum {}",
                self.name
            ))),
        }
    }
}

impl<'de, A> VariantAccess<'de> for MapEntryAsEnum<A>
where
    A: MapAccess<'de>,
{
    type Error = A::Error;

    fn unit_variant(mut self) -> Result<(), Self::Error> {
        self.map.next_value()
    }

    fn newtype_variant_seed<T>(mut self, seed: T) -> Result<T::Value, Self::Error>
    where
        T: DeserializeSeed<'de>,
    {
        self.map.next_value_seed(seed)
    }

    fn tuple_variant<V>(mut self, len: usize, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        struct Wrap<V> {
            len: usize,
            visitor: V,
        }

        impl<'de, V> DeserializeSeed<'de> for Wrap<V>
        where
            V: Visitor<'de>,
        {
            type Value = V::Value;

            fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
            where
                D: Deserializer<'de>,
            {
                deserializer.deserialize_tuple(self.len, self.visitor)
            }
        }

        self.map.next_value_seed(Wrap { len, visitor })
    }

    fn struct_variant<V>(
        mut self,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        struct Wrap<V> {
            name: &'static str,
            fields: &'static [&'static str],
            visitor: V,
        }

        impl<'de, V> DeserializeSeed<'de> for Wrap<V>
        where
            V: Visitor<'de>,
        {
            type Value = V::Value;

            fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
            where
                D: Deserializer<'de>,
            {
                deserializer.deserialize_struct(self.name, self.fields, self.visitor)
            }
        }

        self.map.next_value_seed(Wrap {
            name: self.name,
            fields,
            visitor,
        })
    }
}

struct StringKeySeed<K> {
    seed: K,
}

impl<'de, K> DeserializeSeed<'de> for StringKeySeed<K>
where
    K: DeserializeSeed<'de>,
{
    type Value = K::Value;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        self.seed.deserialize(StringKeyDeserializer {
            delegate: deserializer,
        })
    }
}

struct StringKeyDeserializer<D> {
    delegate: D,
}

impl<'de, D> Deserializer<'de> for StringKeyDeserializer<D>
where
    D: Deserializer<'de>,
{
    type Error = D::Error;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.delegate.deserialize_str(visitor)
    }

    forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string
        bytes byte_buf option unit unit_struct newtype_struct seq tuple
        tuple_struct map struct enum identifier ignored_any
    }
}

struct DefaultKey;

impl<'de> Visitor<'de> for DefaultKey {
    type Value = ();

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        write!(formatter, "the string \"{}\"", DEFAULT_KEY)
    }

    fn visit_str<E>(self, string: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        if string == DEFAULT_KEY {
            Ok(())
        } else {
            Err(de::Error::unknown_field(string, &[DEFAULT_KEY]))
        }
    }
}

impl<'de> DeserializeSeed<'de> for DefaultKey {
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(self)
    }
}

struct MapValueAsDeserializer<A> {
    map: A,
}

macro_rules! deserialize_map_value {
    ($self:ident, $method:ident, $visitor:ident $(, $k:ident : $ty:ty)*) => {{
        struct Wrap<V> {
            $(
                $k : $ty,
            )*
            visitor: V,
        }

        impl<'de, V> DeserializeSeed<'de> for Wrap<V>
        where
            V: Visitor<'de>,
        {
            type Value = V::Value;

            fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
            where
                D: Deserializer<'de>,
            {
                deserializer.$method($(self.$k,)* self.visitor)
            }
        }

        let mut this = $self;
        this.map.next_value_seed(Wrap { $($k,)* $visitor })
    }};
}

impl<'de, A> Deserializer<'de> for MapValueAsDeserializer<A>
where
    A: MapAccess<'de>,
{
    type Error = A::Error;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        deserialize_map_value!(self, deserialize_any, visitor)
    }

    fn deserialize_bool<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        deserialize_map_value!(self, deserialize_bool, visitor)
    }

    fn deserialize_i8<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        deserialize_map_value!(self, deserialize_i8, visitor)
    }

    fn deserialize_i16<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        deserialize_map_value!(self, deserialize_i16, visitor)
    }

    fn deserialize_i32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        deserialize_map_value!(self, deserialize_i32, visitor)
    }

    fn deserialize_i64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        deserialize_map_value!(self, deserialize_i64, visitor)
    }

    fn deserialize_i128<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        deserialize_map_value!(self, deserialize_i128, visitor)
    }

    fn deserialize_u8<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        deserialize_map_value!(self, deserialize_u8, visitor)
    }

    fn deserialize_u16<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        deserialize_map_value!(self, deserialize_u16, visitor)
    }

    fn deserialize_u32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        deserialize_map_value!(self, deserialize_u32, visitor)
    }

    fn deserialize_u64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        deserialize_map_value!(self, deserialize_u64, visitor)
    }

    fn deserialize_u128<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        deserialize_map_value!(self, deserialize_u128, visitor)
    }

    fn deserialize_f32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        deserialize_map_value!(self, deserialize_f32, visitor)
    }

    fn deserialize_f64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        deserialize_map_value!(self, deserialize_f64, visitor)
    }

    fn deserialize_char<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        deserialize_map_value!(self, deserialize_char, visitor)
    }

    fn deserialize_str<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        deserialize_map_value!(self, deserialize_str, visitor)
    }

    fn deserialize_string<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        deserialize_map_value!(self, deserialize_string, visitor)
    }

    fn deserialize_bytes<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        deserialize_map_value!(self, deserialize_bytes, visitor)
    }

    fn deserialize_byte_buf<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        deserialize_map_value!(self, deserialize_byte_buf, visitor)
    }

    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        deserialize_map_value!(self, deserialize_option, visitor)
    }

    fn deserialize_unit<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        deserialize_map_value!(self, deserialize_unit, visitor)
    }

    fn deserialize_unit_struct<V>(
        self,
        name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        deserialize_map_value!(self, deserialize_unit_struct, visitor, name: &'static str)
    }

    fn deserialize_newtype_struct<V>(
        self,
        name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        deserialize_map_value!(
            self,
            deserialize_newtype_struct,
            visitor,
            name: &'static str
        )
    }

    fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        deserialize_map_value!(self, deserialize_seq, visitor)
    }

    fn deserialize_tuple<V>(self, len: usize, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        deserialize_map_value!(self, deserialize_tuple, visitor, len: usize)
    }

    fn deserialize_tuple_struct<V>(
        self,
        name: &'static str,
        len: usize,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        deserialize_map_value!(
            self,
            deserialize_tuple_struct,
            visitor,
            name: &'static str,
            len: usize
        )
    }

    fn deserialize_map<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        deserialize_map_value!(self, deserialize_map, visitor)
    }

    fn deserialize_struct<V>(
        self,
        name: &'static str,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        deserialize_map_value!(
            self,
            deserialize_struct,
            visitor,
            name: &'static str,
            fields: &'static [&'static str]
        )
    }

    fn deserialize_enum<V>(
        self,
        name: &'static str,
        variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        deserialize_map_value!(
            self,
            deserialize_enum,
            visitor,
            name: &'static str,
            variants: &'static [&'static str]
        )
    }

    fn deserialize_identifier<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        deserialize_map_value!(self, deserialize_identifier, visitor)
    }

    fn deserialize_ignored_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        deserialize_map_value!(self, deserialize_ignored_any, visitor)
    }
}
