// Take a look at the license at the top of the repository in the LICENSE file.

use crate::ffi;
use std::fmt;

#[cfg(feature = "use_glib")]
use std::{marker::PhantomData, mem};

#[cfg(feature = "use_glib")]
use glib::translate::*;

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
#[doc(alias = "cairo_rectangle_int_t")]
pub struct RectangleInt(ffi::cairo_rectangle_int_t);

impl RectangleInt {
    #[inline]
    pub fn new(x: i32, y: i32, width: i32, height: i32) -> Self {
        Self(ffi::cairo_rectangle_int_t {
            x,
            y,
            width,
            height,
        })
    }
    #[inline]
    pub fn x(&self) -> i32 {
        self.0.x
    }
    #[inline]
    pub fn set_x(&mut self, x: i32) {
        self.0.x = x;
    }
    #[inline]
    pub fn y(&self) -> i32 {
        self.0.y
    }
    #[inline]
    pub fn set_y(&mut self, y: i32) {
        self.0.y = y;
    }
    #[inline]
    pub fn width(&self) -> i32 {
        self.0.width
    }
    #[inline]
    pub fn set_width(&mut self, width: i32) {
        self.0.width = width;
    }
    #[inline]
    pub fn height(&self) -> i32 {
        self.0.height
    }
    #[inline]
    pub fn set_height(&mut self, height: i32) {
        self.0.height = height;
    }
}

impl fmt::Debug for RectangleInt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RectangleInt")
            .field("x", &self.x())
            .field("y", &self.y())
            .field("width", &self.width())
            .field("height", &self.height())
            .finish()
    }
}

#[cfg(feature = "use_glib")]
#[doc(hidden)]
impl Uninitialized for RectangleInt {
    #[inline]
    unsafe fn uninitialized() -> Self {
        unsafe { mem::zeroed() }
    }
}

#[cfg(feature = "use_glib")]
#[doc(hidden)]
impl<'a> ToGlibPtr<'a, *const ffi::cairo_rectangle_int_t> for RectangleInt {
    type Storage = PhantomData<&'a Self>;

    #[inline]
    fn to_glib_none(&'a self) -> Stash<'a, *const ffi::cairo_rectangle_int_t, Self> {
        Stash(
            self as *const RectangleInt as *const ffi::cairo_rectangle_int_t,
            PhantomData,
        )
    }
}

#[cfg(feature = "use_glib")]
#[doc(hidden)]
impl<'a> ToGlibPtrMut<'a, *mut ffi::cairo_rectangle_int_t> for RectangleInt {
    type Storage = PhantomData<&'a mut Self>;

    #[inline]
    fn to_glib_none_mut(&'a mut self) -> StashMut<'a, *mut ffi::cairo_rectangle_int_t, Self> {
        StashMut(
            self as *mut RectangleInt as *mut ffi::cairo_rectangle_int_t,
            PhantomData,
        )
    }
}

#[cfg(feature = "use_glib")]
#[doc(hidden)]
impl FromGlibPtrNone<*const ffi::cairo_rectangle_int_t> for RectangleInt {
    #[inline]
    unsafe fn from_glib_none(ptr: *const ffi::cairo_rectangle_int_t) -> Self {
        unsafe { *(ptr as *const RectangleInt) }
    }
}

#[cfg(feature = "use_glib")]
#[doc(hidden)]
impl FromGlibPtrBorrow<*mut ffi::cairo_rectangle_int_t> for RectangleInt {
    #[inline]
    unsafe fn from_glib_borrow(ptr: *mut ffi::cairo_rectangle_int_t) -> crate::Borrowed<Self> {
        unsafe { crate::Borrowed::new(*(ptr as *mut RectangleInt)) }
    }
}

#[cfg(feature = "use_glib")]
#[doc(hidden)]
impl FromGlibPtrNone<*mut ffi::cairo_rectangle_int_t> for RectangleInt {
    #[inline]
    unsafe fn from_glib_none(ptr: *mut ffi::cairo_rectangle_int_t) -> Self {
        unsafe { *(ptr as *mut RectangleInt) }
    }
}

#[cfg(feature = "use_glib")]
gvalue_impl_inline!(
    RectangleInt,
    ffi::cairo_rectangle_int_t,
    ffi::gobject::cairo_gobject_rectangle_int_get_type
);

impl RectangleInt {
    #[inline]
    pub fn to_raw_none(&self) -> *mut ffi::cairo_rectangle_int_t {
        &self.0 as *const ffi::cairo_rectangle_int_t as *mut ffi::cairo_rectangle_int_t
    }
}
