use crate::internally::DEFAULT_KEY;
use crate::is_serialize_str::expect_str;
use alloc::borrow::ToOwned;
use alloc::boxed::Box;
use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;
use core::marker::PhantomData;
use serde::ser::{
    self, Serialize, SerializeMap, SerializeSeq, SerializeStruct, SerializeStructVariant,
    SerializeTuple, SerializeTupleStruct, SerializeTupleVariant, Serializer,
};

pub struct Wrap<'a, T: ?Sized>(pub &'a T);

impl<'a, T> Serialize for Wrap<'a, T>
where
    T: ?Sized + erased_serde::Serialize + 'a,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        erased_serde::serialize(self.0, serializer)
    }
}

pub struct InternallyTaggedSerializer<S> {
    pub tag: &'static str,
    pub variant: &'static str,
    pub delegate: S,
}

impl<S> InternallyTaggedSerializer<S>
where
    S: Serializer,
{
    fn serialize_default<T>(self, value: &T) -> Result<S::Ok, S::Error>
    where
        T: ?Sized + Serialize,
    {
        let mut map = self.delegate.serialize_map(Some(2))?;
        map.serialize_entry(self.tag, self.variant)?;
        map.serialize_entry(DEFAULT_KEY, value)?;
        map.end()
    }
}

impl<S> Serializer for InternallyTaggedSerializer<S>
where
    S: Serializer,
{
    type Ok = S::Ok;
    type Error = S::Error;

    type SerializeSeq = SerializeSeqAsMapValue<S::SerializeMap>;
    type SerializeTuple = SerializeTupleAsMapValue<S::SerializeMap>;
    type SerializeTupleStruct = SerializeTupleStructAsMapValue<S::SerializeMap>;
    type SerializeTupleVariant = SerializeTupleStructAsMapValue<S::SerializeMap>;
    type SerializeMap = S::SerializeMap;
    type SerializeStruct = SerializeStructAsMap<S::SerializeMap>;
    type SerializeStructVariant = SerializeStructVariantAsMapValue<S::SerializeMap>;

    fn serialize_bool(self, value: bool) -> Result<Self::Ok, Self::Error> {
        self.serialize_default(&value)
    }

    fn serialize_i8(self, value: i8) -> Result<Self::Ok, Self::Error> {
        self.serialize_default(&value)
    }

    fn serialize_i16(self, value: i16) -> Result<Self::Ok, Self::Error> {
        self.serialize_default(&value)
    }

    fn serialize_i32(self, value: i32) -> Result<Self::Ok, Self::Error> {
        self.serialize_default(&value)
    }

    fn serialize_i64(self, value: i64) -> Result<Self::Ok, Self::Error> {
        self.serialize_default(&value)
    }

    fn serialize_i128(self, value: i128) -> Result<Self::Ok, Self::Error> {
        self.serialize_default(&value)
    }

    fn serialize_u8(self, value: u8) -> Result<Self::Ok, Self::Error> {
        self.serialize_default(&value)
    }

    fn serialize_u16(self, value: u16) -> Result<Self::Ok, Self::Error> {
        self.serialize_default(&value)
    }

    fn serialize_u32(self, value: u32) -> Result<Self::Ok, Self::Error> {
        self.serialize_default(&value)
    }

    fn serialize_u64(self, value: u64) -> Result<Self::Ok, Self::Error> {
        self.serialize_default(&value)
    }

    fn serialize_u128(self, value: u128) -> Result<Self::Ok, Self::Error> {
        self.serialize_default(&value)
    }

    fn serialize_f32(self, value: f32) -> Result<Self::Ok, Self::Error> {
        self.serialize_default(&value)
    }

    fn serialize_f64(self, value: f64) -> Result<Self::Ok, Self::Error> {
        self.serialize_default(&value)
    }

    fn serialize_char(self, value: char) -> Result<Self::Ok, Self::Error> {
        self.serialize_default(&value)
    }

    fn serialize_str(self, value: &str) -> Result<Self::Ok, Self::Error> {
        self.serialize_default(value)
    }

    fn serialize_bytes(self, value: &[u8]) -> Result<Self::Ok, Self::Error> {
        self.serialize_default(value)
    }

    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        self.serialize_unit()
    }

    fn serialize_some<T>(self, value: &T) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + Serialize,
    {
        self.serialize_default(value)
    }

    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        let mut map = self.delegate.serialize_map(Some(1))?;
        map.serialize_entry(self.tag, self.variant)?;
        map.end()
    }

    fn serialize_unit_struct(self, _: &'static str) -> Result<Self::Ok, Self::Error> {
        self.serialize_unit()
    }

    fn serialize_unit_variant(
        self,
        _: &'static str,
        _: u32,
        inner_variant: &'static str,
    ) -> Result<Self::Ok, Self::Error> {
        let mut map = self.delegate.serialize_map(Some(2))?;
        map.serialize_entry(self.tag, self.variant)?;
        map.serialize_entry(inner_variant, &())?;
        map.end()
    }

    fn serialize_newtype_struct<T>(
        self,
        _: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(self)
    }

    fn serialize_newtype_variant<T>(
        self,
        _: &'static str,
        _: u32,
        inner_variant: &'static str,
        inner_value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + Serialize,
    {
        let mut map = self.delegate.serialize_map(Some(2))?;
        map.serialize_entry(self.tag, self.variant)?;
        map.serialize_entry(inner_variant, inner_value)?;
        map.end()
    }

    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        let mut map = self.delegate.serialize_map(Some(2))?;
        map.serialize_entry(self.tag, self.variant)?;
        map.serialize_key(DEFAULT_KEY)?;
        Ok(SerializeSeqAsMapValue::new(map, len))
    }

    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        let mut map = self.delegate.serialize_map(Some(2))?;
        map.serialize_entry(self.tag, self.variant)?;
        map.serialize_key(DEFAULT_KEY)?;
        Ok(SerializeTupleAsMapValue::new(map, len))
    }

    fn serialize_tuple_struct(
        self,
        name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        let mut map = self.delegate.serialize_map(Some(2))?;
        map.serialize_entry(self.tag, self.variant)?;
        map.serialize_key(DEFAULT_KEY)?;
        Ok(SerializeTupleStructAsMapValue::new(map, name, len))
    }

    fn serialize_tuple_variant(
        self,
        _: &'static str,
        _: u32,
        name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        let mut map = self.delegate.serialize_map(Some(2))?;
        map.serialize_entry(self.tag, self.variant)?;
        map.serialize_key(name)?;
        Ok(SerializeTupleStructAsMapValue::new(map, name, len))
    }

    fn serialize_map(self, len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        let mut map = self.delegate.serialize_map(len.map(|len| len + 1))?;
        map.serialize_entry(self.tag, self.variant)?;
        Ok(map)
    }

    fn serialize_struct(
        self,
        _: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        let mut state = self.delegate.serialize_map(Some(len + 1))?;
        state.serialize_entry(self.tag, self.variant)?;
        Ok(SerializeStructAsMap::new(state, self.tag, self.variant))
    }

    fn serialize_struct_variant(
        self,
        _: &'static str,
        _: u32,
        name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        let mut map = self.delegate.serialize_map(Some(2))?;
        map.serialize_entry(self.tag, self.variant)?;
        map.serialize_key(name)?;
        Ok(SerializeStructVariantAsMapValue::new(map, name, len))
    }
}

pub struct SerializeSeqAsMapValue<M> {
    map: M,
    fields: Vec<Content>,
}

impl<M> SerializeSeqAsMapValue<M> {
    pub fn new(map: M, len: Option<usize>) -> Self {
        SerializeSeqAsMapValue {
            map,
            fields: Vec::with_capacity(len.unwrap_or(0)),
        }
    }
}

impl<M> SerializeSeq for SerializeSeqAsMapValue<M>
where
    M: SerializeMap,
{
    type Ok = M::Ok;
    type Error = M::Error;

    fn serialize_element<T>(&mut self, value: &T) -> Result<(), M::Error>
    where
        T: ?Sized + Serialize,
    {
        let value = value.serialize(ContentSerializer::<M::Error>::new())?;
        self.fields.push(value);
        Ok(())
    }

    fn end(mut self) -> Result<M::Ok, M::Error> {
        self.map.serialize_value(&Content::Seq(self.fields))?;
        self.map.end()
    }
}

pub struct SerializeTupleAsMapValue<M> {
    map: M,
    fields: Vec<Content>,
}

impl<M> SerializeTupleAsMapValue<M> {
    pub fn new(map: M, len: usize) -> Self {
        SerializeTupleAsMapValue {
            map,
            fields: Vec::with_capacity(len),
        }
    }
}

impl<M> SerializeTuple for SerializeTupleAsMapValue<M>
where
    M: SerializeMap,
{
    type Ok = M::Ok;
    type Error = M::Error;

    fn serialize_element<T>(&mut self, value: &T) -> Result<(), M::Error>
    where
        T: ?Sized + Serialize,
    {
        let value = value.serialize(ContentSerializer::<M::Error>::new())?;
        self.fields.push(value);
        Ok(())
    }

    fn end(mut self) -> Result<M::Ok, M::Error> {
        self.map.serialize_value(&Content::Tuple(self.fields))?;
        self.map.end()
    }
}

pub struct SerializeTupleStructAsMapValue<M> {
    map: M,
    name: &'static str,
    fields: Vec<Content>,
}

impl<M> SerializeTupleStructAsMapValue<M> {
    pub fn new(map: M, name: &'static str, len: usize) -> Self {
        SerializeTupleStructAsMapValue {
            map,
            name,
            fields: Vec::with_capacity(len),
        }
    }
}

impl<M> SerializeTupleStruct for SerializeTupleStructAsMapValue<M>
where
    M: SerializeMap,
{
    type Ok = M::Ok;
    type Error = M::Error;

    fn serialize_field<T>(&mut self, value: &T) -> Result<(), M::Error>
    where
        T: ?Sized + Serialize,
    {
        let value = value.serialize(ContentSerializer::<M::Error>::new())?;
        self.fields.push(value);
        Ok(())
    }

    fn end(mut self) -> Result<M::Ok, M::Error> {
        self.map
            .serialize_value(&Content::TupleStruct(self.name, self.fields))?;
        self.map.end()
    }
}

impl<M> SerializeTupleVariant for SerializeTupleStructAsMapValue<M>
where
    M: SerializeMap,
{
    type Ok = M::Ok;
    type Error = M::Error;

    fn serialize_field<T>(&mut self, value: &T) -> Result<(), M::Error>
    where
        T: ?Sized + Serialize,
    {
        let value = value.serialize(ContentSerializer::<M::Error>::new())?;
        self.fields.push(value);
        Ok(())
    }

    fn end(mut self) -> Result<M::Ok, M::Error> {
        self.map
            .serialize_value(&Content::TupleStruct(self.name, self.fields))?;
        self.map.end()
    }
}

pub struct SerializeStructAsMap<M> {
    map: M,
    tag: &'static str,
    variant: &'static str,
}

impl<M> SerializeStructAsMap<M> {
    fn new(map: M, tag: &'static str, variant: &'static str) -> Self {
        SerializeStructAsMap { map, tag, variant }
    }
}

impl<M> SerializeStruct for SerializeStructAsMap<M>
where
    M: SerializeMap,
{
    type Ok = M::Ok;
    type Error = M::Error;

    fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        if key == self.tag {
            match expect_str(value, self.variant) {
                Ok(()) => Ok(()),
                Err(unexpected) => Err(ser::Error::custom(format!(
                    "mismatched value for tag {:?}: {:?} vs {:?}",
                    self.tag, self.variant, unexpected,
                ))),
            }
        } else {
            self.map.serialize_entry(key, value)
        }
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.map.end()
    }
}

pub struct SerializeStructVariantAsMapValue<M> {
    map: M,
    name: &'static str,
    fields: Vec<(&'static str, Content)>,
}

impl<M> SerializeStructVariantAsMapValue<M> {
    pub fn new(map: M, name: &'static str, len: usize) -> Self {
        SerializeStructVariantAsMapValue {
            map,
            name,
            fields: Vec::with_capacity(len),
        }
    }
}

impl<M> SerializeStructVariant for SerializeStructVariantAsMapValue<M>
where
    M: SerializeMap,
{
    type Ok = M::Ok;
    type Error = M::Error;

    fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), M::Error>
    where
        T: ?Sized + Serialize,
    {
        let value = value.serialize(ContentSerializer::<M::Error>::new())?;
        self.fields.push((key, value));
        Ok(())
    }

    fn end(mut self) -> Result<M::Ok, M::Error> {
        self.map
            .serialize_value(&Content::Struct(self.name, self.fields))?;
        self.map.end()
    }
}

#[derive(Debug)]
pub enum Content {
    Bool(bool),

    U8(u8),
    U16(u16),
    U32(u32),
    U64(u64),
    U128(u128),

    I8(i8),
    I16(i16),
    I32(i32),
    I64(i64),
    I128(i128),

    F32(f32),
    F64(f64),

    Char(char),
    String(String),
    Bytes(Vec<u8>),

    None,
    Some(Box<Content>),

    Unit,
    UnitStruct(&'static str),
    UnitVariant(&'static str, u32, &'static str),
    NewtypeStruct(&'static str, Box<Content>),
    NewtypeVariant(&'static str, u32, &'static str, Box<Content>),

    Seq(Vec<Content>),
    Tuple(Vec<Content>),
    TupleStruct(&'static str, Vec<Content>),
    TupleVariant(&'static str, u32, &'static str, Vec<Content>),
    Map(Vec<(Content, Content)>),
    Struct(&'static str, Vec<(&'static str, Content)>),
    StructVariant(
        &'static str,
        u32,
        &'static str,
        Vec<(&'static str, Content)>,
    ),
}

impl Serialize for Content {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match *self {
            Content::Bool(b) => serializer.serialize_bool(b),
            Content::U8(u) => serializer.serialize_u8(u),
            Content::U16(u) => serializer.serialize_u16(u),
            Content::U32(u) => serializer.serialize_u32(u),
            Content::U64(u) => serializer.serialize_u64(u),
            Content::U128(u) => serializer.serialize_u128(u),
            Content::I8(i) => serializer.serialize_i8(i),
            Content::I16(i) => serializer.serialize_i16(i),
            Content::I32(i) => serializer.serialize_i32(i),
            Content::I64(i) => serializer.serialize_i64(i),
            Content::I128(i) => serializer.serialize_i128(i),
            Content::F32(f) => serializer.serialize_f32(f),
            Content::F64(f) => serializer.serialize_f64(f),
            Content::Char(c) => serializer.serialize_char(c),
            Content::String(ref s) => serializer.serialize_str(s),
            Content::Bytes(ref b) => serializer.serialize_bytes(b),
            Content::None => serializer.serialize_none(),
            Content::Some(ref c) => serializer.serialize_some(&**c),
            Content::Unit => serializer.serialize_unit(),
            Content::UnitStruct(n) => serializer.serialize_unit_struct(n),
            Content::UnitVariant(n, i, v) => serializer.serialize_unit_variant(n, i, v),
            Content::NewtypeStruct(n, ref c) => serializer.serialize_newtype_struct(n, &**c),
            Content::NewtypeVariant(n, i, v, ref c) => {
                serializer.serialize_newtype_variant(n, i, v, &**c)
            }
            Content::Seq(ref elements) => elements.serialize(serializer),
            Content::Tuple(ref elements) => {
                let mut tuple = serializer.serialize_tuple(elements.len())?;
                for e in elements {
                    tuple.serialize_element(e)?;
                }
                tuple.end()
            }
            Content::TupleStruct(n, ref fields) => {
                let mut ts = serializer.serialize_tuple_struct(n, fields.len())?;
                for f in fields {
                    ts.serialize_field(f)?;
                }
                ts.end()
            }
            Content::TupleVariant(n, i, v, ref fields) => {
                let mut tv = serializer.serialize_tuple_variant(n, i, v, fields.len())?;
                for f in fields {
                    tv.serialize_field(f)?;
                }
                tv.end()
            }
            Content::Map(ref entries) => {
                let mut map = serializer.serialize_map(Some(entries.len()))?;
                for (k, v) in entries {
                    map.serialize_entry(k, v)?;
                }
                map.end()
            }
            Content::Struct(n, ref fields) => {
                let mut s = serializer.serialize_struct(n, fields.len())?;
                for &(k, ref v) in fields {
                    s.serialize_field(k, v)?;
                }
                s.end()
            }
            Content::StructVariant(n, i, v, ref fields) => {
                let mut sv = serializer.serialize_struct_variant(n, i, v, fields.len())?;
                for &(k, ref v) in fields {
                    sv.serialize_field(k, v)?;
                }
                sv.end()
            }
        }
    }
}

pub struct ContentSerializer<E> {
    error: PhantomData<E>,
}

impl<E> ContentSerializer<E> {
    pub fn new() -> Self {
        ContentSerializer { error: PhantomData }
    }
}

impl<E> Serializer for ContentSerializer<E>
where
    E: ser::Error,
{
    type Ok = Content;
    type Error = E;

    type SerializeSeq = ContentSerializeSeq<E>;
    type SerializeTuple = ContentSerializeTuple<E>;
    type SerializeTupleStruct = ContentSerializeTupleStruct<E>;
    type SerializeTupleVariant = ContentSerializeTupleVariant<E>;
    type SerializeMap = ContentSerializeMap<E>;
    type SerializeStruct = ContentSerializeStruct<E>;
    type SerializeStructVariant = ContentSerializeStructVariant<E>;

    fn serialize_bool(self, v: bool) -> Result<Content, E> {
        Ok(Content::Bool(v))
    }

    fn serialize_i8(self, v: i8) -> Result<Content, E> {
        Ok(Content::I8(v))
    }

    fn serialize_i16(self, v: i16) -> Result<Content, E> {
        Ok(Content::I16(v))
    }

    fn serialize_i32(self, v: i32) -> Result<Content, E> {
        Ok(Content::I32(v))
    }

    fn serialize_i64(self, v: i64) -> Result<Content, E> {
        Ok(Content::I64(v))
    }

    fn serialize_i128(self, v: i128) -> Result<Content, E> {
        Ok(Content::I128(v))
    }

    fn serialize_u8(self, v: u8) -> Result<Content, E> {
        Ok(Content::U8(v))
    }

    fn serialize_u16(self, v: u16) -> Result<Content, E> {
        Ok(Content::U16(v))
    }

    fn serialize_u32(self, v: u32) -> Result<Content, E> {
        Ok(Content::U32(v))
    }

    fn serialize_u64(self, v: u64) -> Result<Content, E> {
        Ok(Content::U64(v))
    }

    fn serialize_u128(self, v: u128) -> Result<Content, E> {
        Ok(Content::U128(v))
    }

    fn serialize_f32(self, v: f32) -> Result<Content, E> {
        Ok(Content::F32(v))
    }

    fn serialize_f64(self, v: f64) -> Result<Content, E> {
        Ok(Content::F64(v))
    }

    fn serialize_char(self, v: char) -> Result<Content, E> {
        Ok(Content::Char(v))
    }

    fn serialize_str(self, value: &str) -> Result<Content, E> {
        Ok(Content::String(value.to_owned()))
    }

    fn serialize_bytes(self, value: &[u8]) -> Result<Content, E> {
        Ok(Content::Bytes(value.to_owned()))
    }

    fn serialize_none(self) -> Result<Content, E> {
        Ok(Content::None)
    }

    fn serialize_some<T>(self, value: &T) -> Result<Content, E>
    where
        T: ?Sized + Serialize,
    {
        Ok(Content::Some(Box::new(value.serialize(self)?)))
    }

    fn serialize_unit(self) -> Result<Content, E> {
        Ok(Content::Unit)
    }

    fn serialize_unit_struct(self, name: &'static str) -> Result<Content, E> {
        Ok(Content::UnitStruct(name))
    }

    fn serialize_unit_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
    ) -> Result<Content, E> {
        Ok(Content::UnitVariant(name, variant_index, variant))
    }

    fn serialize_newtype_struct<T>(self, name: &'static str, value: &T) -> Result<Content, E>
    where
        T: ?Sized + Serialize,
    {
        Ok(Content::NewtypeStruct(
            name,
            Box::new(value.serialize(self)?),
        ))
    }

    fn serialize_newtype_variant<T>(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<Content, E>
    where
        T: ?Sized + Serialize,
    {
        Ok(Content::NewtypeVariant(
            name,
            variant_index,
            variant,
            Box::new(value.serialize(self)?),
        ))
    }

    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, E> {
        Ok(ContentSerializeSeq {
            elements: Vec::with_capacity(len.unwrap_or(0)),
            error: PhantomData,
        })
    }

    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple, E> {
        Ok(ContentSerializeTuple {
            elements: Vec::with_capacity(len),
            error: PhantomData,
        })
    }

    fn serialize_tuple_struct(
        self,
        name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleStruct, E> {
        Ok(ContentSerializeTupleStruct {
            name,
            fields: Vec::with_capacity(len),
            error: PhantomData,
        })
    }

    fn serialize_tuple_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleVariant, E> {
        Ok(ContentSerializeTupleVariant {
            name,
            variant_index,
            variant,
            fields: Vec::with_capacity(len),
            error: PhantomData,
        })
    }

    fn serialize_map(self, len: Option<usize>) -> Result<Self::SerializeMap, E> {
        Ok(ContentSerializeMap {
            entries: Vec::with_capacity(len.unwrap_or(0)),
            key: None,
            error: PhantomData,
        })
    }

    fn serialize_struct(self, name: &'static str, len: usize) -> Result<Self::SerializeStruct, E> {
        Ok(ContentSerializeStruct {
            name,
            fields: Vec::with_capacity(len),
            error: PhantomData,
        })
    }

    fn serialize_struct_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStructVariant, E> {
        Ok(ContentSerializeStructVariant {
            name,
            variant_index,
            variant,
            fields: Vec::with_capacity(len),
            error: PhantomData,
        })
    }
}

pub struct ContentSerializeSeq<E> {
    elements: Vec<Content>,
    error: PhantomData<E>,
}

impl<E> SerializeSeq for ContentSerializeSeq<E>
where
    E: ser::Error,
{
    type Ok = Content;
    type Error = E;

    fn serialize_element<T>(&mut self, value: &T) -> Result<(), E>
    where
        T: ?Sized + Serialize,
    {
        let value = value.serialize(ContentSerializer::<E>::new())?;
        self.elements.push(value);
        Ok(())
    }

    fn end(self) -> Result<Content, E> {
        Ok(Content::Seq(self.elements))
    }
}

pub struct ContentSerializeTuple<E> {
    elements: Vec<Content>,
    error: PhantomData<E>,
}

impl<E> SerializeTuple for ContentSerializeTuple<E>
where
    E: ser::Error,
{
    type Ok = Content;
    type Error = E;

    fn serialize_element<T>(&mut self, value: &T) -> Result<(), E>
    where
        T: ?Sized + Serialize,
    {
        let value = value.serialize(ContentSerializer::<E>::new())?;
        self.elements.push(value);
        Ok(())
    }

    fn end(self) -> Result<Content, E> {
        Ok(Content::Tuple(self.elements))
    }
}

pub struct ContentSerializeTupleStruct<E> {
    name: &'static str,
    fields: Vec<Content>,
    error: PhantomData<E>,
}

impl<E> SerializeTupleStruct for ContentSerializeTupleStruct<E>
where
    E: ser::Error,
{
    type Ok = Content;
    type Error = E;

    fn serialize_field<T>(&mut self, value: &T) -> Result<(), E>
    where
        T: ?Sized + Serialize,
    {
        let value = value.serialize(ContentSerializer::<E>::new())?;
        self.fields.push(value);
        Ok(())
    }

    fn end(self) -> Result<Content, E> {
        Ok(Content::TupleStruct(self.name, self.fields))
    }
}

pub struct ContentSerializeTupleVariant<E> {
    name: &'static str,
    variant_index: u32,
    variant: &'static str,
    fields: Vec<Content>,
    error: PhantomData<E>,
}

impl<E> SerializeTupleVariant for ContentSerializeTupleVariant<E>
where
    E: ser::Error,
{
    type Ok = Content;
    type Error = E;

    fn serialize_field<T>(&mut self, value: &T) -> Result<(), E>
    where
        T: ?Sized + Serialize,
    {
        let value = value.serialize(ContentSerializer::<E>::new())?;
        self.fields.push(value);
        Ok(())
    }

    fn end(self) -> Result<Content, E> {
        Ok(Content::TupleVariant(
            self.name,
            self.variant_index,
            self.variant,
            self.fields,
        ))
    }
}

pub struct ContentSerializeMap<E> {
    entries: Vec<(Content, Content)>,
    key: Option<Content>,
    error: PhantomData<E>,
}

impl<E> SerializeMap for ContentSerializeMap<E>
where
    E: ser::Error,
{
    type Ok = Content;
    type Error = E;

    fn serialize_key<T>(&mut self, key: &T) -> Result<(), E>
    where
        T: ?Sized + Serialize,
    {
        let key = key.serialize(ContentSerializer::<E>::new())?;
        self.key = Some(key);
        Ok(())
    }

    fn serialize_value<T>(&mut self, value: &T) -> Result<(), E>
    where
        T: ?Sized + Serialize,
    {
        let key = self
            .key
            .take()
            .expect("serialize_value called before serialize_key");
        let value = value.serialize(ContentSerializer::<E>::new())?;
        self.entries.push((key, value));
        Ok(())
    }

    fn end(self) -> Result<Content, E> {
        Ok(Content::Map(self.entries))
    }

    fn serialize_entry<K, V>(&mut self, key: &K, value: &V) -> Result<(), E>
    where
        K: ?Sized + Serialize,
        V: ?Sized + Serialize,
    {
        let key = key.serialize(ContentSerializer::<E>::new())?;
        let value = value.serialize(ContentSerializer::<E>::new())?;
        self.entries.push((key, value));
        Ok(())
    }
}

pub struct ContentSerializeStruct<E> {
    name: &'static str,
    fields: Vec<(&'static str, Content)>,
    error: PhantomData<E>,
}

impl<E> SerializeStruct for ContentSerializeStruct<E>
where
    E: ser::Error,
{
    type Ok = Content;
    type Error = E;

    fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), E>
    where
        T: ?Sized + Serialize,
    {
        let value = value.serialize(ContentSerializer::<E>::new())?;
        self.fields.push((key, value));
        Ok(())
    }

    fn end(self) -> Result<Content, E> {
        Ok(Content::Struct(self.name, self.fields))
    }
}

pub struct ContentSerializeStructVariant<E> {
    name: &'static str,
    variant_index: u32,
    variant: &'static str,
    fields: Vec<(&'static str, Content)>,
    error: PhantomData<E>,
}

impl<E> SerializeStructVariant for ContentSerializeStructVariant<E>
where
    E: ser::Error,
{
    type Ok = Content;
    type Error = E;

    fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), E>
    where
        T: ?Sized + Serialize,
    {
        let value = value.serialize(ContentSerializer::<E>::new())?;
        self.fields.push((key, value));
        Ok(())
    }

    fn end(self) -> Result<Content, E> {
        Ok(Content::StructVariant(
            self.name,
            self.variant_index,
            self.variant,
            self.fields,
        ))
    }
}
