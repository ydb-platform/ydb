// Take a look at the license at the top of the repository in the LICENSE file.

use std::fmt;

use crate::ffi;
use glib::translate::*;

glib::wrapper! {
    #[doc(alias = "PangoRectangle")]
    pub struct Rectangle(BoxedInline<ffi::PangoRectangle>);
}

impl Rectangle {
    #[inline]
    pub fn new(x: i32, y: i32, width: i32, height: i32) -> Self {
        unsafe {
            Self::unsafe_from(ffi::PangoRectangle {
                x,
                y,
                width,
                height,
            })
        }
    }

    #[inline]
    pub fn x(&self) -> i32 {
        self.inner.x
    }

    #[inline]
    pub fn set_x(&mut self, x: i32) {
        self.inner.x = x;
    }

    #[inline]
    pub fn y(&self) -> i32 {
        self.inner.y
    }

    #[inline]
    pub fn set_y(&mut self, y: i32) {
        self.inner.y = y;
    }

    #[inline]
    pub fn width(&self) -> i32 {
        self.inner.width
    }

    #[inline]
    pub fn set_width(&mut self, width: i32) {
        self.inner.width = width;
    }

    #[inline]
    pub fn height(&self) -> i32 {
        self.inner.height
    }

    #[inline]
    pub fn set_height(&mut self, height: i32) {
        self.inner.height = height;
    }
}

impl fmt::Debug for Rectangle {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Rectangle")
            .field("x", &self.x())
            .field("y", &self.y())
            .field("width", &self.width())
            .field("height", &self.height())
            .finish()
    }
}
