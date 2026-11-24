// Take a look at the license at the top of the repository in the LICENSE file.

use std::fmt;

use glib::translate::*;

use crate::{Matrix, Rectangle, ffi};

impl Matrix {
    pub fn new(xx: f64, xy: f64, yx: f64, yy: f64, x0: f64, y0: f64) -> Self {
        unsafe {
            Self::unsafe_from(ffi::PangoMatrix {
                xx,
                xy,
                yx,
                yy,
                x0,
                y0,
            })
        }
    }

    #[doc(alias = "pango_matrix_transform_pixel_rectangle")]
    pub fn transform_pixel_rectangle(&self, rect: &mut Rectangle) {
        unsafe {
            ffi::pango_matrix_transform_pixel_rectangle(
                self.to_glib_none().0,
                rect.to_glib_none_mut().0,
            )
        }
    }

    #[doc(alias = "pango_matrix_transform_rectangle")]
    pub fn transform_rectangle(&self, rect: &mut Rectangle) {
        unsafe {
            ffi::pango_matrix_transform_rectangle(self.to_glib_none().0, rect.to_glib_none_mut().0)
        }
    }

    pub fn xx(&self) -> f64 {
        self.inner.xx
    }

    pub fn xy(&self) -> f64 {
        self.inner.xy
    }

    pub fn yx(&self) -> f64 {
        self.inner.yx
    }

    pub fn yy(&self) -> f64 {
        self.inner.yy
    }

    pub fn x0(&self) -> f64 {
        self.inner.x0
    }

    pub fn y0(&self) -> f64 {
        self.inner.y0
    }
}

impl fmt::Debug for Matrix {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Matrix")
            .field("xx", &self.xx())
            .field("xy", &self.xy())
            .field("yx", &self.yx())
            .field("yy", &self.yy())
            .field("x0", &self.x0())
            .field("y0", &self.y0())
            .finish()
    }
}
