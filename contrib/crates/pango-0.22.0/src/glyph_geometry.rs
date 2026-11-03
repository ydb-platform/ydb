// Take a look at the license at the top of the repository in the LICENSE file.

use std::fmt;

glib::wrapper! {
    #[doc(alias = "PangoGlyphGeometry")]
    pub struct GlyphGeometry(BoxedInline<crate::ffi::PangoGlyphGeometry>);
}

impl GlyphGeometry {
    #[inline]
    pub fn width(&self) -> i32 {
        self.inner.width
    }

    #[inline]
    pub fn set_width(&mut self, width: i32) {
        self.inner.width = width;
    }

    #[inline]
    pub fn x_offset(&self) -> i32 {
        self.inner.x_offset
    }

    #[inline]
    pub fn set_x_offset(&mut self, x_offset: i32) {
        self.inner.x_offset = x_offset;
    }

    #[inline]
    pub fn y_offset(&self) -> i32 {
        self.inner.y_offset
    }

    #[inline]
    pub fn set_y_offset(&mut self, y_offset: i32) {
        self.inner.y_offset = y_offset;
    }
}

impl fmt::Debug for GlyphGeometry {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("GlyphGeometry")
            .field("x_offset", &self.x_offset())
            .field("y_offset", &self.y_offset())
            .field("width", &self.width())
            .finish()
    }
}
