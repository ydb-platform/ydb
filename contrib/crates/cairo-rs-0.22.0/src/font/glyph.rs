// Take a look at the license at the top of the repository in the LICENSE file.

use crate::ffi;
use std::fmt;

#[derive(Clone, Copy)]
#[repr(transparent)]
#[doc(alias = "cairo_glyph_t")]
pub struct Glyph(ffi::cairo_glyph_t);

impl Glyph {
    pub fn new(index: libc::c_ulong, x: f64, y: f64) -> Self {
        Self(ffi::cairo_glyph_t { index, x, y })
    }

    pub fn index(&self) -> libc::c_ulong {
        self.0.index
    }

    pub fn x(&self) -> f64 {
        self.0.x
    }

    pub fn y(&self) -> f64 {
        self.0.y
    }

    pub fn set_index(&mut self, index: libc::c_ulong) {
        self.0.index = index;
    }

    pub fn set_x(&mut self, x: f64) {
        self.0.x = x;
    }

    pub fn set_y(&mut self, y: f64) {
        self.0.y = y;
    }
}

impl fmt::Debug for Glyph {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Glyph")
            .field("index", &self.index())
            .field("x", &self.x())
            .field("y", &self.y())
            .finish()
    }
}
