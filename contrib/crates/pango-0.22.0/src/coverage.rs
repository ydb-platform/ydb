// Take a look at the license at the top of the repository in the LICENSE file.

use std::{mem, ptr};

use glib::translate::*;

use crate::{CoverageLevel, ffi};

#[cfg(feature = "v1_44")]
glib::wrapper! {
    #[doc(alias = "PangoCoverage")]
    pub struct Coverage(Object<ffi::PangoCoverage>);

    match fn {
        type_ => || ffi::pango_coverage_get_type(),
    }
}

// There was no get_type() function before 1.44
#[cfg(not(feature = "v1_44"))]
glib::wrapper! {
    #[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
    #[doc(alias = "PangoCoverage")]
    pub struct Coverage(Shared<ffi::PangoCoverage>);

    match fn {
        ref => |ptr| ffi::pango_coverage_ref(ptr),
        unref => |ptr| ffi::pango_coverage_unref(ptr),
    }
}

impl Coverage {
    #[doc(alias = "pango_coverage_new")]
    pub fn new() -> Self {
        unsafe { from_glib_full(ffi::pango_coverage_new()) }
    }

    #[doc(alias = "pango_coverage_copy")]
    pub fn copy(&self) -> Option<Coverage> {
        unsafe { from_glib_full(ffi::pango_coverage_copy(self.to_glib_none().0)) }
    }

    #[doc(alias = "pango_coverage_get")]
    pub fn get(&self, index_: i32) -> CoverageLevel {
        unsafe { from_glib(ffi::pango_coverage_get(self.to_glib_none().0, index_)) }
    }

    #[cfg_attr(feature = "v1_44", deprecated)]
    #[doc(alias = "pango_coverage_max")]
    pub fn max(&self, other: &Coverage) {
        unsafe {
            ffi::pango_coverage_max(self.to_glib_none().0, other.to_glib_none().0);
        }
    }

    #[doc(alias = "pango_coverage_set")]
    pub fn set(&self, index_: i32, level: CoverageLevel) {
        unsafe {
            ffi::pango_coverage_set(self.to_glib_none().0, index_, level.into_glib());
        }
    }

    #[cfg_attr(feature = "v1_44", deprecated)]
    #[doc(alias = "pango_coverage_to_bytes")]
    pub fn to_bytes(&self) -> Vec<u8> {
        unsafe {
            let mut bytes = ptr::null_mut();
            let mut n_bytes = mem::MaybeUninit::uninit();
            ffi::pango_coverage_to_bytes(self.to_glib_none().0, &mut bytes, n_bytes.as_mut_ptr());
            FromGlibContainer::from_glib_full_num(bytes, n_bytes.assume_init() as usize)
        }
    }

    #[cfg_attr(feature = "v1_44", deprecated)]
    #[doc(alias = "pango_coverage_from_bytes")]
    pub fn from_bytes(bytes: &[u8]) -> Option<Coverage> {
        let n_bytes = bytes.len() as i32;
        unsafe {
            from_glib_full(ffi::pango_coverage_from_bytes(
                bytes.to_glib_none().0,
                n_bytes,
            ))
        }
    }
}

impl Default for Coverage {
    fn default() -> Self {
        Self::new()
    }
}
