// Take a look at the license at the top of the repository in the LICENSE file.

use glib::translate::*;

use crate::{GlyphItem, GlyphString, Item, ffi};

impl GlyphItem {
    pub fn item(&self) -> Item {
        unsafe { from_glib_none((*self.as_ptr()).item) }
    }

    pub fn glyph_string(&self) -> GlyphString {
        unsafe { from_glib_none((*self.as_ptr()).glyphs) }
    }

    #[doc(alias = "pango_glyph_item_get_logical_widths")]
    #[doc(alias = "get_logical_widths")]
    pub fn logical_widths(&self, text: &str) -> Vec<i32> {
        let count = text.chars().count();
        unsafe {
            let mut logical_widths = Vec::with_capacity(count);
            ffi::pango_glyph_item_get_logical_widths(
                mut_override(self.to_glib_none().0),
                text.to_glib_none().0,
                logical_widths.as_mut_ptr(),
            );
            logical_widths.set_len(count);
            logical_widths
        }
    }
}
