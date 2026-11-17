// Take a look at the license at the top of the repository in the LICENSE file.

use crate::ffi;
use std::fmt;

#[derive(Clone, Copy)]
#[repr(transparent)]
#[doc(alias = "cairo_font_extents_t")]
pub struct FontExtents(ffi::cairo_font_extents_t);

impl FontExtents {
    pub fn ascent(&self) -> f64 {
        self.0.ascent
    }

    pub fn descent(&self) -> f64 {
        self.0.descent
    }

    pub fn height(&self) -> f64 {
        self.0.height
    }

    pub fn max_x_advance(&self) -> f64 {
        self.0.max_x_advance
    }

    pub fn max_y_advance(&self) -> f64 {
        self.0.max_y_advance
    }

    pub fn set_ascent(&mut self, ascent: f64) {
        self.0.ascent = ascent;
    }

    pub fn set_descent(&mut self, descent: f64) {
        self.0.descent = descent;
    }

    pub fn set_height(&mut self, height: f64) {
        self.0.height = height;
    }

    pub fn set_max_x_advance(&mut self, max_x_advance: f64) {
        self.0.max_x_advance = max_x_advance;
    }

    pub fn set_max_y_advance(&mut self, max_y_advance: f64) {
        self.0.max_y_advance = max_y_advance;
    }
}

impl fmt::Debug for FontExtents {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FontExtents")
            .field("ascent", &self.ascent())
            .field("descent", &self.descent())
            .field("height", &self.height())
            .field("max_x_advance", &self.max_x_advance())
            .field("max_y_advance", &self.max_y_advance())
            .finish()
    }
}

#[doc(hidden)]
impl From<FontExtents> for ffi::cairo_font_extents_t {
    fn from(val: FontExtents) -> ffi::cairo_font_extents_t {
        val.0
    }
}

#[doc(hidden)]
impl From<ffi::cairo_font_extents_t> for FontExtents {
    fn from(value: ffi::cairo_font_extents_t) -> Self {
        Self(value)
    }
}
