// Take a look at the license at the top of the repository in the LICENSE file.

#![cfg_attr(docsrs, feature(doc_cfg))]
#![allow(clippy::missing_safety_doc)]
#![doc = include_str!("../README.md")]

pub use cairo_sys as ffi;
#[cfg(feature = "freetype")]
#[cfg_attr(docsrs, doc(cfg(feature = "freetype")))]
pub use freetype;
#[cfg(feature = "use_glib")]
#[cfg_attr(docsrs, doc(cfg(feature = "use_glib")))]
pub use glib;

// Helper macros for our GValue related trait impls
#[cfg(feature = "use_glib")]
#[cfg_attr(docsrs, doc(cfg(feature = "use_glib")))]
macro_rules! gvalue_impl_inner {
    ($name:ty, $ffi_name:ty, $get_type:expr) => {
        #[allow(unused_imports)]
        use glib::translate::*;

        impl glib::prelude::StaticType for $name {
            #[inline]
            fn static_type() -> glib::types::Type {
                unsafe { from_glib($get_type()) }
            }
        }

        impl glib::value::ValueType for $name {
            type Type = Self;
        }

        impl glib::value::ValueTypeOptional for $name {}
    };
}

#[cfg(feature = "use_glib")]
#[cfg_attr(docsrs, doc(cfg(feature = "use_glib")))]
macro_rules! gvalue_impl {
    ($name:ty, $ffi_name:ty, $get_type:expr) => {
        gvalue_impl_inner!($name, $ffi_name, $get_type);

        unsafe impl<'a> glib::value::FromValue<'a> for $name {
            type Checker = glib::value::GenericValueTypeOrNoneChecker<Self>;

            unsafe fn from_value(value: &'a glib::Value) -> Self {
                unsafe {
                    let ptr = glib::gobject_ffi::g_value_dup_boxed(
                        glib::translate::ToGlibPtr::to_glib_none(value).0,
                    );
                    debug_assert!(!ptr.is_null());
                    <$name as glib::translate::FromGlibPtrFull<*mut $ffi_name>>::from_glib_full(
                        ptr as *mut $ffi_name,
                    )
                }
            }
        }

        unsafe impl<'a> glib::value::FromValue<'a> for &'a $name {
            type Checker = glib::value::GenericValueTypeOrNoneChecker<Self>;

            unsafe fn from_value(value: &'a glib::Value) -> Self {
                unsafe {
                    debug_assert_eq!(
                        std::mem::size_of::<Self>(),
                        std::mem::size_of::<glib::ffi::gpointer>()
                    );
                    let value = &*(value as *const glib::Value as *const glib::gobject_ffi::GValue);
                    let ptr = &value.data[0].v_pointer as *const glib::ffi::gpointer
                        as *const *const $ffi_name;
                    debug_assert!(!(*ptr).is_null());
                    &*(ptr as *const $name)
                }
            }
        }

        impl glib::value::ToValue for $name {
            fn to_value(&self) -> glib::Value {
                unsafe {
                    let mut value = glib::Value::from_type_unchecked(
                        <$name as glib::prelude::StaticType>::static_type(),
                    );
                    glib::gobject_ffi::g_value_take_boxed(
                        value.to_glib_none_mut().0,
                        self.to_glib_full() as *mut _,
                    );
                    value
                }
            }

            fn value_type(&self) -> glib::Type {
                <$name as glib::prelude::StaticType>::static_type()
            }
        }

        impl From<$name> for glib::Value {
            fn from(v: $name) -> Self {
                unsafe {
                    let mut value = glib::Value::from_type_unchecked(
                        <$name as glib::prelude::StaticType>::static_type(),
                    );
                    glib::gobject_ffi::g_value_take_boxed(
                        value.to_glib_none_mut().0,
                        glib::translate::IntoGlibPtr::into_glib_ptr(v) as *mut _,
                    );
                    value
                }
            }
        }

        impl glib::value::ToValueOptional for $name {
            fn to_value_optional(s: Option<&Self>) -> glib::Value {
                let mut value = glib::Value::for_value_type::<Self>();
                unsafe {
                    glib::gobject_ffi::g_value_take_boxed(
                        value.to_glib_none_mut().0,
                        glib::translate::ToGlibPtr::to_glib_full(&s) as *mut _,
                    );
                }

                value
            }
        }
    };
}

#[cfg(feature = "use_glib")]
#[cfg_attr(docsrs, doc(cfg(feature = "use_glib")))]
macro_rules! gvalue_impl_inline {
    ($name:ty, $ffi_name:ty, $get_type:expr) => {
        gvalue_impl_inner!($name, $ffi_name, $get_type);

        unsafe impl<'a> glib::value::FromValue<'a> for $name {
            type Checker = glib::value::GenericValueTypeOrNoneChecker<Self>;

            unsafe fn from_value(value: &'a glib::Value) -> Self {
                unsafe {
                    let ptr = glib::gobject_ffi::g_value_get_boxed(
                        glib::translate::ToGlibPtr::to_glib_none(value).0,
                    );
                    debug_assert!(!ptr.is_null());
                    <$name as glib::translate::FromGlibPtrNone<*mut $ffi_name>>::from_glib_none(
                        ptr as *mut $ffi_name,
                    )
                }
            }
        }

        unsafe impl<'a> glib::value::FromValue<'a> for &'a $name {
            type Checker = glib::value::GenericValueTypeOrNoneChecker<Self>;

            unsafe fn from_value(value: &'a glib::Value) -> Self {
                unsafe {
                    let ptr = glib::gobject_ffi::g_value_get_boxed(
                        glib::translate::ToGlibPtr::to_glib_none(value).0,
                    );
                    debug_assert!(!ptr.is_null());
                    &*(ptr as *mut $name)
                }
            }
        }

        impl glib::value::ToValue for $name {
            fn to_value(&self) -> glib::Value {
                unsafe {
                    let ptr =
                        glib::ffi::g_malloc0(std::mem::size_of::<$ffi_name>()) as *mut $ffi_name;
                    ptr.write(self.0);
                    let mut value = glib::Value::from_type_unchecked(
                        <$name as glib::prelude::StaticType>::static_type(),
                    );
                    glib::gobject_ffi::g_value_take_boxed(
                        value.to_glib_none_mut().0,
                        ptr as *mut _,
                    );
                    value
                }
            }

            fn value_type(&self) -> glib::Type {
                <$name as glib::prelude::StaticType>::static_type()
            }
        }

        impl glib::value::ToValueOptional for $name {
            fn to_value_optional(s: Option<&Self>) -> glib::Value {
                let ptr: *mut $ffi_name = match s {
                    Some(s) => unsafe {
                        let ptr = glib::ffi::g_malloc0(std::mem::size_of::<$ffi_name>())
                            as *mut $ffi_name;
                        ptr.write(s.0);
                        ptr
                    },
                    None => std::ptr::null_mut(),
                };
                let mut value = glib::Value::for_value_type::<Self>();
                unsafe {
                    glib::gobject_ffi::g_value_take_boxed(
                        value.to_glib_none_mut().0,
                        ptr as *mut _,
                    );
                }

                value
            }
        }
    };
}

#[cfg(feature = "pdf")]
#[cfg_attr(docsrs, doc(cfg(feature = "pdf")))]
pub use pdf::PdfSurface;
#[cfg(feature = "ps")]
#[cfg_attr(docsrs, doc(cfg(feature = "ps")))]
pub use ps::PsSurface;
#[cfg(any(feature = "pdf", feature = "svg", feature = "ps"))]
#[cfg_attr(
    docsrs,
    doc(cfg(any(feature = "pdf", feature = "svg", feature = "ps")))
)]
pub use stream::StreamWithError;
#[cfg(feature = "svg")]
#[cfg_attr(docsrs, doc(cfg(feature = "svg")))]
pub use svg::SvgSurface;
#[cfg(feature = "xcb")]
#[cfg_attr(docsrs, doc(cfg(feature = "xcb")))]
pub use xcb::{
    XCBConnection, XCBDrawable, XCBPixmap, XCBRenderPictFormInfo, XCBScreen, XCBSurface,
    XCBVisualType,
};

pub use crate::{
    context::{Context, RectangleList},
    device::Device,
    enums::*,
    error::{BorrowError, Error, IoError, Result},
    font::{
        Antialias, FontExtents, FontFace, FontOptions, FontSlant, FontType, FontWeight, Glyph,
        HintMetrics, HintStyle, ScaledFont, SubpixelOrder, TextCluster, TextExtents, UserFontFace,
    },
    image_surface::{ImageSurface, ImageSurfaceData, ImageSurfaceDataOwned},
    matrices::Matrix,
    paths::{Path, PathSegment, PathSegments},
    patterns::{
        Gradient, LinearGradient, Mesh, Pattern, RadialGradient, SolidPattern, SurfacePattern,
    },
    recording_surface::RecordingSurface,
    rectangle::Rectangle,
    rectangle_int::RectangleInt,
    region::Region,
    surface::{MappedImageSurface, Surface},
    user_data::UserDataKey,
};

#[macro_use]
mod surface_macros;
#[macro_use]
mod user_data;
mod constants;
pub use crate::constants::*;
mod utils;
pub use crate::utils::{Version, debug_reset_static_data, version_string};
mod context;
mod device;
mod enums;
mod error;
mod font;
mod image_surface;
mod matrices;
mod paths;
mod patterns;
mod recording_surface;
mod rectangle;
mod rectangle_int;
mod region;
mod surface;
#[cfg(feature = "png")]
mod surface_png;
#[cfg(feature = "xcb")]
mod xcb;

#[cfg(any(feature = "pdf", feature = "svg", feature = "ps"))]
#[macro_use]
mod stream;
#[cfg(feature = "pdf")]
mod pdf;
#[cfg(feature = "ps")]
mod ps;
#[cfg(feature = "svg")]
mod svg;

#[cfg(all(target_os = "macos", feature = "quartz-surface"))]
mod quartz_surface;
#[cfg(all(target_os = "macos", feature = "quartz-surface"))]
pub use quartz_surface::QuartzSurface;

#[cfg(all(windows, feature = "win32-surface"))]
mod win32_surface;

#[cfg(all(windows, feature = "win32-surface"))]
#[cfg_attr(docsrs, doc(cfg(all(windows, feature = "win32-surface"))))]
pub use win32_surface::Win32Surface;

#[cfg(not(feature = "use_glib"))]
#[cfg_attr(docsrs, doc(cfg(not(feature = "use_glib"))))]
mod borrowed {
    use std::mem;

    /// Wrapper around values representing borrowed C memory.
    ///
    /// This is returned by `from_glib_borrow()` and ensures that the wrapped value
    /// is never dropped when going out of scope.
    ///
    /// Borrowed values must never be passed by value or mutable reference to safe Rust code and must
    /// not leave the C scope in which they are valid.
    #[derive(Debug)]
    pub struct Borrowed<T>(mem::ManuallyDrop<T>);

    impl<T> Borrowed<T> {
        /// Creates a new borrowed value.
        #[inline]
        pub fn new(val: T) -> Self {
            Self(mem::ManuallyDrop::new(val))
        }

        /// Extracts the contained value.
        ///
        /// The returned value must never be dropped and instead has to be passed to `mem::forget()` or
        /// be directly wrapped in `mem::ManuallyDrop` or another `Borrowed` wrapper.
        #[inline]
        pub unsafe fn into_inner(self) -> T {
            mem::ManuallyDrop::into_inner(self.0)
        }
    }

    impl<T> AsRef<T> for Borrowed<T> {
        #[inline]
        fn as_ref(&self) -> &T {
            &*self.0
        }
    }

    impl<T> std::ops::Deref for Borrowed<T> {
        type Target = T;

        #[inline]
        fn deref(&self) -> &T {
            &*self.0
        }
    }
}

#[cfg(not(feature = "use_glib"))]
#[cfg_attr(docsrs, doc(cfg(not(feature = "use_glib"))))]
pub use borrowed::Borrowed;
#[cfg(feature = "use_glib")]
pub(crate) use glib::translate::Borrowed;
