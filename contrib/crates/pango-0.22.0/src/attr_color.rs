// Take a look at the license at the top of the repository in the LICENSE file.

use glib::translate::*;

use crate::{AttrType, Color, ffi};

define_attribute_struct!(
    AttrColor,
    ffi::PangoAttrColor,
    &[
        AttrType::Foreground,
        AttrType::Background,
        AttrType::UnderlineColor,
        AttrType::StrikethroughColor,
        AttrType::OverlineColor
    ]
);

impl AttrColor {
    #[doc(alias = "pango_attr_background_new")]
    pub fn new_background(red: u16, green: u16, blue: u16) -> Self {
        unsafe { from_glib_full(ffi::pango_attr_background_new(red, green, blue)) }
    }

    #[doc(alias = "pango_attr_foreground_new")]
    pub fn new_foreground(red: u16, green: u16, blue: u16) -> Self {
        unsafe { from_glib_full(ffi::pango_attr_foreground_new(red, green, blue)) }
    }

    #[cfg(feature = "v1_46")]
    #[cfg_attr(docsrs, doc(cfg(feature = "v1_46")))]
    #[doc(alias = "pango_attr_overline_color_new")]
    pub fn new_overline_color(red: u16, green: u16, blue: u16) -> Self {
        unsafe { from_glib_full(ffi::pango_attr_overline_color_new(red, green, blue)) }
    }

    #[doc(alias = "pango_attr_strikethrough_color_new")]
    pub fn new_strikethrough_color(red: u16, green: u16, blue: u16) -> Self {
        unsafe { from_glib_full(ffi::pango_attr_strikethrough_color_new(red, green, blue)) }
    }

    #[doc(alias = "pango_attr_underline_color_new")]
    pub fn new_underline_color(red: u16, green: u16, blue: u16) -> Self {
        unsafe { from_glib_full(ffi::pango_attr_underline_color_new(red, green, blue)) }
    }

    pub fn color(&self) -> Color {
        unsafe { from_glib_none((&self.inner.color) as *const ffi::PangoColor) }
    }
}
