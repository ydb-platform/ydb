// Take a look at the license at the top of the repository in the LICENSE file.

use glib::translate::*;

use crate::{AttrType, ffi};

define_attribute_struct!(
    AttrFontFeatures,
    ffi::PangoAttrFontFeatures,
    &[AttrType::FontFeatures]
);

impl AttrFontFeatures {
    #[doc(alias = "pango_attr_font_features_new")]
    pub fn new(features: &str) -> Self {
        unsafe { from_glib_full(ffi::pango_attr_font_features_new(features.to_glib_none().0)) }
    }

    pub fn features(&self) -> glib::GString {
        unsafe { from_glib_none(self.inner.features) }
    }
}
