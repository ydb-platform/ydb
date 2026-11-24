// Take a look at the license at the top of the repository in the LICENSE file.

use glib::translate::*;

use crate::{AttrType, Rectangle, ffi};

define_attribute_struct!(AttrShape, ffi::PangoAttrShape, &[AttrType::Shape]);

impl AttrShape {
    #[doc(alias = "pango_attr_shape_new")]
    pub fn new(ink_rect: &Rectangle, logical_rect: &Rectangle) -> Self {
        unsafe {
            from_glib_full(ffi::pango_attr_shape_new(
                ink_rect.to_glib_none().0,
                logical_rect.to_glib_none().0,
            ))
        }
    }

    pub fn ink_rect(&self) -> Rectangle {
        unsafe { from_glib_none(&self.inner.ink_rect as *const _) }
    }

    pub fn logical_rect(&self) -> Rectangle {
        unsafe { from_glib_none(&self.inner.logical_rect as *const _) }
    }
}
