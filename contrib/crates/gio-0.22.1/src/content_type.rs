// Take a look at the license at the top of the repository in the LICENSE file.

use std::ptr;

use glib::translate::*;

use crate::ffi;

#[doc(alias = "g_content_type_guess")]
pub fn content_type_guess<'a>(
    filename: Option<impl AsRef<std::path::Path>>,
    data: impl Into<Option<&'a [u8]>>,
) -> (glib::GString, bool) {
    let data = data.into();
    let data_size = data.map_or(0, |d| d.len());
    unsafe {
        let mut result_uncertain = std::mem::MaybeUninit::uninit();
        let ret = from_glib_full(ffi::g_content_type_guess(
            filename.as_ref().map(|p| p.as_ref()).to_glib_none().0,
            data.map_or(ptr::null(), |d| d.to_glib_none().0),
            data_size,
            result_uncertain.as_mut_ptr(),
        ));
        (ret, from_glib(result_uncertain.assume_init()))
    }
}
