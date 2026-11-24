// Take a look at the license at the top of the repository in the LICENSE file.

use glib::translate::*;

use crate::{AttrType, ffi};

define_attribute_struct!(
    AttrSize,
    ffi::PangoAttrSize,
    &[AttrType::Size, AttrType::AbsoluteSize]
);

impl AttrSize {
    #[doc(alias = "pango_attr_size_new")]
    pub fn new(size: i32) -> Self {
        unsafe { from_glib_full(ffi::pango_attr_size_new(size)) }
    }

    #[doc(alias = "pango_attr_size_new_absolute")]
    pub fn new_size_absolute(size: i32) -> Self {
        unsafe { from_glib_full(ffi::pango_attr_size_new_absolute(size)) }
    }

    pub fn size(&self) -> i32 {
        self.inner.size
    }

    pub fn absolute(&self) -> bool {
        unsafe { from_glib(self.inner.absolute as i32) }
    }
}
