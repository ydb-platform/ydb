// Take a look at the license at the top of the repository in the LICENSE file.

use crate::ffi;

glib::wrapper! {
    #[doc(alias = "GIOModule")]
    pub struct IOModule(Object<ffi::GIOModule, ffi::GIOModuleClass>) @extends glib::TypeModule, @implements glib::TypePlugin;

    match fn {
        type_ => || ffi::g_io_module_get_type(),
    }
}
