// Take a look at the license at the top of the repository in the LICENSE file.

use glib::translate::*;

use crate::{AttrType, ffi};

define_attribute_struct!(AttrString, ffi::PangoAttrString, &[AttrType::Family]);

impl AttrString {
    #[doc(alias = "pango_attr_family_new")]
    pub fn new_family(family: &str) -> Self {
        unsafe { from_glib_full(ffi::pango_attr_family_new(family.to_glib_none().0)) }
    }

    pub fn value(&self) -> glib::GString {
        unsafe { from_glib_none(self.inner.value) }
    }
}
