// Take a look at the license at the top of the repository in the LICENSE file.

use glib::{prelude::*, translate::*};

use crate::{FontMap, ffi};

pub trait PangoCairoFontMapExtManual: IsA<FontMap> + 'static {
    #[doc(alias = "pango_cairo_font_map_get_font_type")]
    #[doc(alias = "get_font_type")]
    fn font_type(&self) -> cairo::FontType {
        unsafe { ffi::pango_cairo_font_map_get_font_type(self.as_ref().to_glib_none().0).into() }
    }
}

impl<O: IsA<FontMap>> PangoCairoFontMapExtManual for O {}

impl FontMap {
    #[doc(alias = "pango_cairo_font_map_new_for_font_type")]
    #[doc(alias = "new_for_font_type")]
    pub fn for_font_type(fonttype: cairo::FontType) -> Option<pango::FontMap> {
        unsafe { from_glib_full(ffi::pango_cairo_font_map_new_for_font_type(fonttype.into())) }
    }

    #[allow(clippy::new_ret_no_self)]
    #[doc(alias = "pango_cairo_font_map_new")]
    pub fn new() -> pango::FontMap {
        unsafe { from_glib_full(ffi::pango_cairo_font_map_new()) }
    }

    #[doc(alias = "pango_cairo_font_map_set_default")]
    pub fn set_default(font_map: Option<&Self>) {
        unsafe {
            ffi::pango_cairo_font_map_set_default(font_map.as_ref().to_glib_none().0);
        }
    }
}
