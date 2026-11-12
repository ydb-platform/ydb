// Take a look at the license at the top of the repository in the LICENSE file.

use std::{fmt, mem, ptr};

use crate::ffi;
use glib::translate::*;

glib::wrapper! {
    #[doc(alias = "GFileAttributeInfo")]
    pub struct FileAttributeInfo(BoxedInline<ffi::GFileAttributeInfo>);

    match fn {
        copy => |ptr| {
            let copy = glib::ffi::g_malloc0(mem::size_of::<ffi::GFileAttributeInfo>()) as *mut ffi::GFileAttributeInfo;
            (*copy).name = glib::ffi::g_strdup((*ptr).name);
            copy
        },
        free => |ptr| {
            glib::ffi::g_free((*ptr).name as *mut _);
            glib::ffi::g_free(ptr as *mut _);
        },
        init => |ptr| {
            *ptr = mem::zeroed();
        },
        copy_into => |dest, src| {
            ptr::copy_nonoverlapping(src, dest, 1);
            (*dest).name = glib::ffi::g_strdup((*dest).name);
        },
        clear => |ptr| {
            glib::ffi::g_free((*ptr).name as *mut _);
        },
    }
}

impl FileAttributeInfo {
    #[inline]
    pub fn name(&self) -> &str {
        unsafe {
            use std::ffi::CStr;

            CStr::from_ptr(self.inner.name)
                .to_str()
                .expect("non-UTF-8 string")
        }
    }

    #[inline]
    pub fn type_(&self) -> crate::FileAttributeType {
        unsafe { from_glib(self.inner.type_) }
    }

    #[inline]
    pub fn flags(&self) -> crate::FileAttributeInfoFlags {
        unsafe { from_glib(self.inner.flags) }
    }
}

impl fmt::Debug for FileAttributeInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FileAttributeInfo")
            .field("name", &self.name())
            .field("type", &self.type_())
            .field("flags", &self.flags())
            .finish()
    }
}
