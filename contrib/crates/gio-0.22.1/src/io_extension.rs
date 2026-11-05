// Take a look at the license at the top of the repository in the LICENSE file.

use std::{fmt, marker::PhantomData, ptr};

use crate::ffi;
use glib::{Type, translate::*};

// rustdoc-stripper-ignore-next
/// The implementation of an `IOExtensionPoint`.
#[doc(alias = "GIOExtension")]
#[derive(Copy, Clone, Eq, PartialEq)]
pub struct IOExtension(ptr::NonNull<ffi::GIOExtension>);

impl fmt::Debug for IOExtension {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("IOExtension")
            .field("name", &self.name())
            .field("priority", &self.priority())
            .field("type", &self.type_())
            .finish()
    }
}

impl FromGlibPtrNone<*mut ffi::GIOExtension> for IOExtension {
    #[inline]
    unsafe fn from_glib_none(ptr: *mut ffi::GIOExtension) -> Self {
        unsafe {
            debug_assert!(!ptr.is_null());
            IOExtension(ptr::NonNull::new_unchecked(ptr))
        }
    }
}

impl<'a> ToGlibPtr<'a, *mut ffi::GIOExtension> for &'a IOExtension {
    type Storage = PhantomData<&'a IOExtension>;

    #[inline]
    fn to_glib_none(&self) -> Stash<'a, *mut ffi::GIOExtension, &'a IOExtension> {
        Stash(self.0.as_ptr() as *mut ffi::GIOExtension, PhantomData)
    }
}

impl IOExtension {
    #[doc(alias = "g_io_extension_get_name")]
    pub fn name(&self) -> glib::GString {
        unsafe { from_glib_none(ffi::g_io_extension_get_name(self.0.as_ptr())) }
    }

    #[doc(alias = "g_io_extension_get_priority")]
    pub fn priority(&self) -> i32 {
        unsafe { ffi::g_io_extension_get_priority(self.0.as_ptr()) }
    }

    #[doc(alias = "g_io_extension_get_type")]
    pub fn type_(&self) -> Type {
        unsafe { from_glib(ffi::g_io_extension_get_type(self.0.as_ptr())) }
    }
}
