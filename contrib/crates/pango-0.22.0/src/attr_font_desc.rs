// Take a look at the license at the top of the repository in the LICENSE file.

use glib::translate::*;

use crate::{AttrType, FontDescription, ffi};

define_attribute_struct!(AttrFontDesc, ffi::PangoAttrFontDesc, &[AttrType::FontDesc]);

impl AttrFontDesc {
    #[doc(alias = "pango_attr_font_desc_new")]
    pub fn new(desc: &FontDescription) -> Self {
        unsafe { from_glib_full(ffi::pango_attr_font_desc_new(desc.to_glib_none().0)) }
    }

    pub fn desc(&self) -> FontDescription {
        unsafe { from_glib_none(self.inner.desc) }
    }
}
