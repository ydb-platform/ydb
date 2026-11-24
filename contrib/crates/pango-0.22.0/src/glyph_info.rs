// Take a look at the license at the top of the repository in the LICENSE file.

use std::fmt;

use crate::GlyphGeometry;

glib::wrapper! {
    #[doc(alias = "PangoGlyphInfo")]
    pub struct GlyphInfo(BoxedInline<crate::ffi::PangoGlyphInfo>);
}

impl GlyphInfo {
    #[inline]
    pub fn glyph(&self) -> u32 {
        self.inner.glyph
    }

    #[inline]
    pub fn set_glyph(&mut self, glyph: u32) {
        self.inner.glyph = glyph
    }

    #[inline]
    pub fn geometry(&self) -> &GlyphGeometry {
        unsafe { &*(&self.inner.geometry as *const _ as *const GlyphGeometry) }
    }

    #[inline]
    pub fn geometry_mut(&mut self) -> &mut GlyphGeometry {
        unsafe { &mut *(&mut self.inner.geometry as *mut _ as *mut GlyphGeometry) }
    }
}

impl fmt::Debug for GlyphInfo {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("GlyphInfo")
            .field("glyph", &self.glyph())
            .field("geometry", &self.geometry())
            .finish()
    }
}
