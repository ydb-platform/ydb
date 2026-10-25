// Take a look at the license at the top of the repository in the LICENSE file.

use std::ops::Deref;

#[cfg(feature = "use_glib")]
use glib::translate::*;

use crate::{Content, Error, Rectangle, Surface, SurfaceType, ffi};

declare_surface!(RecordingSurface, SurfaceType::Recording);
impl RecordingSurface {
    #[doc(alias = "cairo_recording_surface_create")]
    pub fn create(content: Content, extends: Option<Rectangle>) -> Result<RecordingSurface, Error> {
        unsafe {
            let extends_ptr = match extends {
                Some(ref c) => c.to_raw_none(),
                None => ::std::ptr::null(),
            };

            Self::from_raw_full(ffi::cairo_recording_surface_create(
                content.into(),
                extends_ptr,
            ))
        }
    }

    #[doc(alias = "cairo_recording_surface_get_extents")]
    #[doc(alias = "get_extents")]
    pub fn extents(&self) -> Option<Rectangle> {
        unsafe {
            let rectangle: Rectangle = ::std::mem::zeroed();
            if ffi::cairo_recording_surface_get_extents(self.to_raw_none(), rectangle.to_raw_none())
                .as_bool()
            {
                Some(rectangle)
            } else {
                None
            }
        }
    }

    #[doc(alias = "cairo_recording_surface_ink_extents")]
    pub fn ink_extents(&self) -> (f64, f64, f64, f64) {
        let mut x0 = 0.;
        let mut y0 = 0.;
        let mut width = 0.;
        let mut height = 0.;

        unsafe {
            ffi::cairo_recording_surface_ink_extents(
                self.to_raw_none(),
                &mut x0,
                &mut y0,
                &mut width,
                &mut height,
            );
        }
        (x0, y0, width, height)
    }
}
