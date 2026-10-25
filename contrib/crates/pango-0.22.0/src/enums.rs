// Take a look at the license at the top of the repository in the LICENSE file.

#[cfg(feature = "v1_50")]
use crate::ffi;
#[cfg(feature = "v1_50")]
use glib::{Quark, prelude::*, translate::*};

#[cfg(feature = "v1_50")]
#[derive(Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Clone, Copy)]
#[non_exhaustive]
#[doc(alias = "PangoLayoutDeserializeError")]
pub enum LayoutDeserializeError {
    #[doc(alias = "PANGO_LAYOUT_DESERIALIZE_INVALID")]
    Invalid,
    #[doc(alias = "PANGO_LAYOUT_DESERIALIZE_INVALID_VALUE")]
    InvalidValue,
    #[doc(alias = "PANGO_LAYOUT_DESERIALIZE_MISSING_VALUE")]
    MissingValue,
    #[doc(hidden)]
    __Unknown(i32),
}

#[cfg(feature = "v1_50")]
#[cfg_attr(docsrs, doc(cfg(feature = "v1_50")))]
#[doc(hidden)]
impl IntoGlib for LayoutDeserializeError {
    type GlibType = ffi::PangoLayoutDeserializeError;

    #[inline]
    fn into_glib(self) -> ffi::PangoLayoutDeserializeError {
        match self {
            Self::Invalid => ffi::PANGO_LAYOUT_DESERIALIZE_INVALID,
            Self::InvalidValue => ffi::PANGO_LAYOUT_DESERIALIZE_INVALID_VALUE,
            Self::MissingValue => ffi::PANGO_LAYOUT_DESERIALIZE_MISSING_VALUE,
            Self::__Unknown(value) => value,
        }
    }
}

#[cfg(feature = "v1_50")]
#[cfg_attr(docsrs, doc(cfg(feature = "v1_50")))]
#[doc(hidden)]
impl FromGlib<ffi::PangoLayoutDeserializeError> for LayoutDeserializeError {
    #[inline]
    unsafe fn from_glib(value: ffi::PangoLayoutDeserializeError) -> Self {
        match value {
            ffi::PANGO_LAYOUT_DESERIALIZE_INVALID => Self::Invalid,
            ffi::PANGO_LAYOUT_DESERIALIZE_INVALID_VALUE => Self::InvalidValue,
            ffi::PANGO_LAYOUT_DESERIALIZE_MISSING_VALUE => Self::MissingValue,
            value => Self::__Unknown(value),
        }
    }
}

#[cfg(feature = "v1_50")]
#[cfg_attr(docsrs, doc(cfg(feature = "v1_50")))]
impl ErrorDomain for LayoutDeserializeError {
    // TODO: figure out why gir picks a non-existent quark function
    #[doc(alias = "pango_layout_deserialize_error_quark")]
    fn domain() -> Quark {
        unsafe { from_glib(ffi::pango_layout_deserialize_error_quark()) }
    }

    fn code(self) -> i32 {
        self.into_glib()
    }

    fn from(code: i32) -> Option<Self> {
        match code {
            ffi::PANGO_LAYOUT_DESERIALIZE_INVALID => Some(Self::Invalid),
            ffi::PANGO_LAYOUT_DESERIALIZE_INVALID_VALUE => Some(Self::InvalidValue),
            ffi::PANGO_LAYOUT_DESERIALIZE_MISSING_VALUE => Some(Self::MissingValue),
            value => Some(Self::__Unknown(value)),
        }
    }
}

#[cfg(feature = "v1_50")]
#[cfg_attr(docsrs, doc(cfg(feature = "v1_50")))]
impl StaticType for LayoutDeserializeError {
    #[inline]
    fn static_type() -> glib::Type {
        unsafe { from_glib(ffi::pango_layout_deserialize_error_get_type()) }
    }
}

#[cfg(feature = "v1_50")]
#[cfg_attr(docsrs, doc(cfg(feature = "v1_50")))]
impl glib::value::ValueType for LayoutDeserializeError {
    type Type = Self;
}

#[cfg(feature = "v1_50")]
#[cfg_attr(docsrs, doc(cfg(feature = "v1_50")))]
unsafe impl<'a> glib::value::FromValue<'a> for LayoutDeserializeError {
    type Checker = glib::value::GenericValueTypeChecker<Self>;

    unsafe fn from_value(value: &'a glib::Value) -> Self {
        unsafe { from_glib(glib::gobject_ffi::g_value_get_enum(value.to_glib_none().0)) }
    }
}

#[cfg(feature = "v1_50")]
#[cfg_attr(docsrs, doc(cfg(feature = "v1_50")))]
impl ToValue for LayoutDeserializeError {
    fn to_value(&self) -> glib::Value {
        let mut value = glib::Value::for_value_type::<Self>();
        unsafe {
            glib::gobject_ffi::g_value_set_enum(value.to_glib_none_mut().0, self.into_glib());
        }
        value
    }

    fn value_type(&self) -> glib::Type {
        Self::static_type()
    }
}

#[cfg(feature = "v1_50")]
#[cfg_attr(docsrs, doc(cfg(feature = "v1_50")))]
impl From<LayoutDeserializeError> for glib::Value {
    #[inline]
    fn from(v: LayoutDeserializeError) -> Self {
        v.to_value()
    }
}
