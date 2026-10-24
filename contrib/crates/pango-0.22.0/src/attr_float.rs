// Take a look at the license at the top of the repository in the LICENSE file.

use glib::translate::*;

use crate::{AttrType, ffi};

define_attribute_struct!(
    AttrFloat,
    ffi::PangoAttrFloat,
    &[
        AttrType::Scale,
        #[cfg(feature = "v1_50")]
        AttrType::LineHeight
    ]
);

impl AttrFloat {
    #[doc(alias = "pango_attr_scale_new")]
    pub fn new_scale(scale_factor: f64) -> Self {
        unsafe { from_glib_full(ffi::pango_attr_scale_new(scale_factor)) }
    }

    #[cfg(feature = "v1_50")]
    #[cfg_attr(docsrs, doc(cfg(feature = "v1_50")))]
    #[doc(alias = "pango_attr_line_height_new")]
    pub fn new_line_height(factor: f64) -> Self {
        unsafe { from_glib_full(ffi::pango_attr_line_height_new(factor)) }
    }

    pub fn value(&self) -> f64 {
        self.inner.value
    }
}
