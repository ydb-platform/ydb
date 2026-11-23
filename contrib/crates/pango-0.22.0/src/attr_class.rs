// Take a look at the license at the top of the repository in the LICENSE file.

use std::marker::PhantomData;

use glib::translate::*;

use crate::{AttrType, ffi};

#[doc(hidden)]
impl<'a> ToGlibPtr<'a, *mut ffi::PangoAttrClass> for &'a AttrClass {
    type Storage = PhantomData<&'a AttrClass>;

    #[inline]
    fn to_glib_none(&self) -> Stash<'a, *mut ffi::PangoAttrClass, Self> {
        Stash(self.0, PhantomData)
    }
}

#[doc(hidden)]
impl FromGlibPtrNone<*mut ffi::PangoAttrClass> for AttrClass {
    #[inline]
    unsafe fn from_glib_none(ptr: *mut ffi::PangoAttrClass) -> Self {
        debug_assert!(!ptr.is_null());
        Self(ptr)
    }
}

#[doc(hidden)]
impl FromGlibPtrFull<*mut ffi::PangoAttrClass> for AttrClass {
    #[inline]
    unsafe fn from_glib_full(ptr: *mut ffi::PangoAttrClass) -> Self {
        debug_assert!(!ptr.is_null());
        Self(ptr)
    }
}

#[doc(hidden)]
impl FromGlibPtrNone<*const ffi::PangoAttrClass> for AttrClass {
    #[inline]
    unsafe fn from_glib_none(ptr: *const ffi::PangoAttrClass) -> Self {
        debug_assert!(!ptr.is_null());
        Self(ptr as *mut _)
    }
}

#[doc(hidden)]
impl FromGlibPtrFull<*const ffi::PangoAttrClass> for AttrClass {
    #[inline]
    unsafe fn from_glib_full(ptr: *const ffi::PangoAttrClass) -> Self {
        debug_assert!(!ptr.is_null());
        Self(ptr as *mut _)
    }
}

#[doc(alias = "PangoAttrClass")]
pub struct AttrClass(*mut ffi::PangoAttrClass);

impl AttrClass {
    #[inline]
    pub fn type_(&self) -> AttrType {
        unsafe { from_glib((*self.0).type_) }
    }
}

impl PartialEq for AttrClass {
    #[inline]
    fn eq(&self, other: &AttrClass) -> bool {
        self.0 == other.0
    }
}

impl Eq for AttrClass {}
