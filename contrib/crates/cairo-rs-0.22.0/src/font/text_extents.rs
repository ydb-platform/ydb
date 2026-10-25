// Take a look at the license at the top of the repository in the LICENSE file.

use crate::ffi;
use std::fmt;

#[derive(Clone, Copy)]
#[repr(transparent)]
#[doc(alias = "cairo_text_extents_t")]
pub struct TextExtents(ffi::cairo_text_extents_t);

impl TextExtents {
    pub fn new(
        x_bearing: f64,
        y_bearing: f64,
        width: f64,
        height: f64,
        x_advance: f64,
        y_advance: f64,
    ) -> Self {
        Self(ffi::cairo_text_extents_t {
            x_bearing,
            y_bearing,
            width,
            height,
            x_advance,
            y_advance,
        })
    }

    pub fn x_bearing(&self) -> f64 {
        self.0.x_bearing
    }

    pub fn y_bearing(&self) -> f64 {
        self.0.y_bearing
    }

    pub fn width(&self) -> f64 {
        self.0.width
    }

    pub fn height(&self) -> f64 {
        self.0.height
    }

    pub fn x_advance(&self) -> f64 {
        self.0.x_advance
    }

    pub fn y_advance(&self) -> f64 {
        self.0.y_advance
    }

    pub fn set_x_bearing(&mut self, x_bearing: f64) {
        self.0.x_bearing = x_bearing;
    }

    pub fn set_y_bearing(&mut self, y_bearing: f64) {
        self.0.y_bearing = y_bearing;
    }

    pub fn set_width(&mut self, width: f64) {
        self.0.width = width;
    }

    pub fn set_height(&mut self, height: f64) {
        self.0.height = height;
    }

    pub fn set_x_advance(&mut self, x_advance: f64) {
        self.0.x_advance = x_advance;
    }

    pub fn set_y_advance(&mut self, y_advance: f64) {
        self.0.y_advance = y_advance;
    }
}

impl fmt::Debug for TextExtents {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TextExtents")
            .field("x_bearing", &self.x_bearing())
            .field("y_bearing", &self.y_bearing())
            .field("width", &self.width())
            .field("height", &self.height())
            .field("x_advance", &self.x_advance())
            .field("y_advance", &self.y_advance())
            .finish()
    }
}

#[doc(hidden)]
impl From<TextExtents> for ffi::cairo_text_extents_t {
    fn from(val: TextExtents) -> ffi::cairo_text_extents_t {
        val.0
    }
}

#[doc(hidden)]
impl From<ffi::cairo_text_extents_t> for TextExtents {
    fn from(value: ffi::cairo_text_extents_t) -> Self {
        Self(value)
    }
}
