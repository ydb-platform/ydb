// Take a look at the license at the top of the repository in the LICENSE file.

use std::ptr;

use glib::translate::*;

use crate::{Resource, resources_register};

impl Resource {
    #[doc(alias = "g_resource_new_from_data")]
    pub fn from_data(data: &glib::Bytes) -> Result<Resource, glib::Error> {
        unsafe {
            let mut error = ptr::null_mut();

            // Create a copy of data if it is not pointer-aligned
            // https://bugzilla.gnome.org/show_bug.cgi?id=790030
            let mut data = data.clone();
            let data_ptr = glib::ffi::g_bytes_get_data(data.to_glib_none().0, ptr::null_mut());
            if !(data_ptr as *const *const ()).is_aligned() {
                data = glib::Bytes::from(&*data);
            }

            let ret = crate::ffi::g_resource_new_from_data(data.to_glib_none().0, &mut error);
            if error.is_null() {
                Ok(from_glib_full(ret))
            } else {
                Err(from_glib_full(error))
            }
        }
    }
}

#[doc(hidden)]
pub fn resources_register_include_impl(bytes: &'static [u8]) -> Result<(), glib::Error> {
    let bytes = glib::Bytes::from_static(bytes);
    let resource = Resource::from_data(&bytes)?;
    resources_register(&resource);
    Ok(())
}

// rustdoc-stripper-ignore-next
/// Include gresources generated with `glib_build_tools::compile_resources` and register with glib. `path` is
/// relative to `OUTDIR`.
///
/// ```ignore
/// gio::resources_register_include!("compiled.gresource").unwrap();
/// ```
#[macro_export]
macro_rules! resources_register_include {
    ($path:expr) => {
        $crate::resources_register_include_impl(include_bytes!(concat!(
            env!("OUT_DIR"),
            "/",
            $path
        )))
    };
}
