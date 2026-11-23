// Take a look at the license at the top of the repository in the LICENSE file.

use glib::{
    object::ObjectType,
    translate::{IntoGlib, ToGlibPtr},
};

use crate::FileAttributeType;

use std::ffi::CStr;

#[derive(Debug)]
pub struct FileAttributeValue<'a>(FileAttributeValueInner<'a>);

impl From<&str> for FileAttributeValue<'_> {
    fn from(value: &str) -> Self {
        Self(FileAttributeValueInner::String(
            ToGlibPtr::<*mut libc::c_char>::to_glib_none(value).1,
        ))
    }
}

impl<'a> From<&'a CStr> for FileAttributeValue<'a> {
    fn from(value: &'a CStr) -> Self {
        Self(FileAttributeValueInner::ByteString(value))
    }
}

impl From<bool> for FileAttributeValue<'_> {
    fn from(value: bool) -> Self {
        Self(FileAttributeValueInner::Boolean(value.into_glib()))
    }
}

impl From<u32> for FileAttributeValue<'_> {
    fn from(value: u32) -> Self {
        Self(FileAttributeValueInner::Uint32(value))
    }
}

impl From<i32> for FileAttributeValue<'_> {
    fn from(value: i32) -> Self {
        Self(FileAttributeValueInner::Int32(value))
    }
}

impl From<u64> for FileAttributeValue<'_> {
    fn from(value: u64) -> Self {
        Self(FileAttributeValueInner::Uint64(value))
    }
}

impl From<i64> for FileAttributeValue<'_> {
    fn from(value: i64) -> Self {
        Self(FileAttributeValueInner::Int64(value))
    }
}

impl<'a, T: AsRef<glib::Object>> From<&'a T> for FileAttributeValue<'a> {
    fn from(value: &'a T) -> Self {
        Self(FileAttributeValueInner::Object(value.as_ref()))
    }
}

impl<'a> From<&'a [&str]> for FileAttributeValue<'a> {
    fn from(value: &'a [&str]) -> Self {
        Self(FileAttributeValueInner::Stringv(value.into()))
    }
}

impl FileAttributeValue<'_> {
    pub(crate) fn type_(&self) -> FileAttributeType {
        self.0.type_()
    }

    pub(crate) fn as_ptr(&self) -> glib::ffi::gpointer {
        self.0.as_ptr()
    }

    pub(crate) fn for_pointer(type_: FileAttributeType, value_p: *mut std::ffi::c_void) -> Self {
        Self(FileAttributeValueInner::Pointer(type_, value_p))
    }
}

#[derive(Debug)]
pub(crate) enum FileAttributeValueInner<'a> {
    Pointer(FileAttributeType, glib::ffi::gpointer),
    String(<&'a str as ToGlibPtr<'a, *mut libc::c_char>>::Storage),
    ByteString(&'a CStr),
    Boolean(glib::ffi::gboolean),
    Uint32(u32),
    Int32(i32),
    Uint64(u64),
    Int64(i64),
    Object(&'a glib::Object),
    Stringv(glib::StrV),
}

impl FileAttributeValueInner<'_> {
    pub(crate) fn type_(&self) -> FileAttributeType {
        match self {
            Self::Pointer(type_, _) => *type_,
            Self::String(_) => FileAttributeType::String,
            Self::ByteString(_) => FileAttributeType::ByteString,
            Self::Boolean(_) => FileAttributeType::Boolean,
            Self::Uint32(_) => FileAttributeType::Uint32,
            Self::Int32(_) => FileAttributeType::Int32,
            Self::Uint64(_) => FileAttributeType::Uint64,
            Self::Int64(_) => FileAttributeType::Int64,
            Self::Object(_) => FileAttributeType::Object,
            Self::Stringv(_) => FileAttributeType::Stringv,
        }
    }

    pub(crate) fn as_ptr(&self) -> glib::ffi::gpointer {
        match self {
            Self::Pointer(_, s) => *s,
            Self::String(s) => s.as_ptr() as _,
            Self::ByteString(s) => s.as_ptr() as _,
            Self::Boolean(s) => s as *const i32 as _,
            Self::Uint32(s) => s as *const u32 as _,
            Self::Int32(s) => s as *const i32 as _,
            Self::Uint64(s) => s as *const u64 as _,
            Self::Int64(s) => s as *const i64 as _,
            Self::Object(s) => s.as_ptr() as _,
            Self::Stringv(s) => s.as_ptr() as _,
        }
    }
}
