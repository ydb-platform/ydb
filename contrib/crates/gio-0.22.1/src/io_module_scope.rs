// Take a look at the license at the top of the repository in the LICENSE file.

use std::{fmt, mem, ptr};

use crate::ffi;

glib::wrapper! {
    #[doc(alias = "GIOModuleScope")]
    pub struct IOModuleScope(BoxedInline<ffi::GIOModuleScope>);

    match fn {
        copy => |ptr| {
            let copy = glib::ffi::g_malloc0(mem::size_of::<ffi::GIOModuleScope>()) as *mut ffi::GIOModuleScope;
            ptr::copy_nonoverlapping(ptr, copy, 1);
            copy
        },
        free => |ptr| {
            glib::ffi::g_free(ptr as *mut _);
        },
        init => |ptr| {
            *ptr = mem::zeroed();
        },
        copy_into => |dest, src| {
            ptr::copy_nonoverlapping(src, dest, 1);
        },
        clear => |ptr| {
        },
    }
}

impl fmt::Debug for IOModuleScope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IOModuleScope").finish()
    }
}
