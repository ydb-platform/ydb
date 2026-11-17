// Take a look at the license at the top of the repository in the LICENSE file.

use std::{
    ops::{Deref, DerefMut},
    rc::Rc,
    slice,
};

#[cfg(feature = "use_glib")]
use glib::translate::*;

use crate::{BorrowError, Error, Format, Surface, SurfaceType, ffi, utils::status_to_result};

declare_surface!(ImageSurface, SurfaceType::Image);

impl ImageSurface {
    #[doc(alias = "cairo_image_surface_create")]
    pub fn create(format: Format, width: i32, height: i32) -> Result<ImageSurface, Error> {
        unsafe {
            Self::from_raw_full(ffi::cairo_image_surface_create(
                format.into(),
                width,
                height,
            ))
        }
    }

    // rustdoc-stripper-ignore-next
    /// Creates an image surface for the provided pixel data.
    /// - The pointer `data` is the beginning of the underlying slice,
    ///   and at least `width * stride` succeeding bytes should be allocated.
    /// - `data` must live longer than any reference to the returned surface.
    /// - You have to free `data` by yourself.
    #[doc(alias = "cairo_image_surface_create_for_data")]
    pub unsafe fn create_for_data_unsafe(
        data: *mut u8,
        format: Format,
        width: i32,
        height: i32,
        stride: i32,
    ) -> Result<ImageSurface, Error> {
        unsafe {
            ImageSurface::from_raw_full(ffi::cairo_image_surface_create_for_data(
                data,
                format.into(),
                width,
                height,
                stride,
            ))
        }
    }

    #[doc(alias = "cairo_image_surface_create_for_data")]
    pub fn create_for_data<D: AsMut<[u8]> + 'static>(
        data: D,
        format: Format,
        width: i32,
        height: i32,
        stride: i32,
    ) -> Result<ImageSurface, Error> {
        let mut data: Box<dyn AsMut<[u8]>> = Box::new(data);

        let (ptr, len) = {
            let data: &mut [u8] = (*data).as_mut();

            (data.as_mut_ptr(), data.len())
        };

        assert!(width >= 0, "width must be non-negative");
        assert!(height >= 0, "height must be non-negative");
        assert!(stride >= 0, "stride must be non-negative");

        // check if there is integer overflow
        assert!(len >= height.checked_mul(stride).unwrap() as usize);
        let result = unsafe {
            ImageSurface::from_raw_full(ffi::cairo_image_surface_create_for_data(
                ptr,
                format.into(),
                width,
                height,
                stride,
            ))
        };
        if let Ok(surface) = &result {
            static IMAGE_SURFACE_DATA: crate::UserDataKey<Box<dyn AsMut<[u8]>>> =
                crate::UserDataKey::new();
            surface.set_user_data(&IMAGE_SURFACE_DATA, Rc::new(data))?;
        }
        result
    }

    #[doc(alias = "cairo_image_surface_get_data")]
    #[doc(alias = "get_data")]
    pub fn data(&mut self) -> Result<ImageSurfaceData<'_>, BorrowError> {
        unsafe {
            if ffi::cairo_surface_get_reference_count(self.to_raw_none()) > 1 {
                return Err(BorrowError::NonExclusive);
            }

            self.flush();
            let status = ffi::cairo_surface_status(self.to_raw_none());
            if let Some(err) = status_to_result(status).err() {
                return Err(BorrowError::from(err));
            }
            if ffi::cairo_image_surface_get_data(self.to_raw_none()).is_null() || is_finished(self)
            {
                return Err(BorrowError::from(Error::SurfaceFinished));
            }
            Ok(ImageSurfaceData::new(self))
        }
    }

    pub fn take_data(self) -> Result<ImageSurfaceDataOwned, BorrowError> {
        unsafe {
            if ffi::cairo_surface_get_reference_count(self.to_raw_none()) > 1 {
                return Err(BorrowError::NonExclusive);
            }

            self.flush();
            let status = ffi::cairo_surface_status(self.to_raw_none());
            if let Some(err) = status_to_result(status).err() {
                return Err(BorrowError::from(err));
            }
            if ffi::cairo_image_surface_get_data(self.to_raw_none()).is_null() || is_finished(&self)
            {
                return Err(BorrowError::from(Error::SurfaceFinished));
            }
            Ok(ImageSurfaceDataOwned { surface: self })
        }
    }

    pub fn with_data<F: FnOnce(&[u8])>(&self, f: F) -> Result<(), BorrowError> {
        self.flush();
        unsafe {
            let status = ffi::cairo_surface_status(self.to_raw_none());
            if let Some(err) = status_to_result(status).err() {
                return Err(BorrowError::from(err));
            }
            let ptr = ffi::cairo_image_surface_get_data(self.to_raw_none());
            if ptr.is_null() || is_finished(self) {
                return Err(BorrowError::from(Error::SurfaceFinished));
            }
            let len = self.height() as usize * self.stride() as usize;
            let data = if len == 0 {
                &[]
            } else {
                slice::from_raw_parts(ptr, len)
            };
            f(data);
        }
        Ok(())
    }

    #[doc(alias = "cairo_image_surface_get_format")]
    #[doc(alias = "get_format")]
    pub fn format(&self) -> Format {
        unsafe { Format::from(ffi::cairo_image_surface_get_format(self.to_raw_none())) }
    }

    #[doc(alias = "cairo_image_surface_get_height")]
    #[doc(alias = "get_height")]
    pub fn height(&self) -> i32 {
        unsafe { ffi::cairo_image_surface_get_height(self.to_raw_none()) }
    }

    #[doc(alias = "cairo_image_surface_get_stride")]
    #[doc(alias = "get_stride")]
    pub fn stride(&self) -> i32 {
        unsafe { ffi::cairo_image_surface_get_stride(self.to_raw_none()) }
    }

    #[doc(alias = "cairo_image_surface_get_width")]
    #[doc(alias = "get_width")]
    pub fn width(&self) -> i32 {
        unsafe { ffi::cairo_image_surface_get_width(self.to_raw_none()) }
    }
}

pub struct ImageSurfaceDataOwned {
    surface: ImageSurface,
}

unsafe impl Send for ImageSurfaceDataOwned {}
unsafe impl Sync for ImageSurfaceDataOwned {}

impl ImageSurfaceDataOwned {
    #[inline]
    pub fn into_inner(self) -> ImageSurface {
        self.surface
    }
}

impl AsRef<[u8]> for ImageSurfaceDataOwned {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        let len = (self.surface.stride() as usize) * (self.surface.height() as usize);
        unsafe {
            let ptr = ffi::cairo_image_surface_get_data(self.surface.to_raw_none());
            if ptr.is_null() || len == 0 {
                return &[];
            }
            slice::from_raw_parts(ptr, len)
        }
    }
}

impl AsMut<[u8]> for ImageSurfaceDataOwned {
    #[inline]
    fn as_mut(&mut self) -> &mut [u8] {
        let len = (self.surface.stride() as usize) * (self.surface.height() as usize);
        unsafe {
            let ptr = ffi::cairo_image_surface_get_data(self.surface.to_raw_none());
            if ptr.is_null() || len == 0 {
                return &mut [];
            }
            slice::from_raw_parts_mut(ptr, len)
        }
    }
}

impl Deref for ImageSurfaceDataOwned {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

impl DerefMut for ImageSurfaceDataOwned {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.as_mut()
    }
}

#[derive(Debug)]
pub struct ImageSurfaceData<'a> {
    surface: &'a mut ImageSurface,
    slice: &'a mut [u8],
    dirty: bool,
}

unsafe impl Send for ImageSurfaceData<'_> {}
unsafe impl Sync for ImageSurfaceData<'_> {}

impl<'a> ImageSurfaceData<'a> {
    fn new(surface: &'a mut ImageSurface) -> ImageSurfaceData<'a> {
        unsafe {
            let ptr = ffi::cairo_image_surface_get_data(surface.to_raw_none());
            let len = (surface.stride() as usize) * (surface.height() as usize);
            ImageSurfaceData {
                surface,
                slice: if ptr.is_null() || len == 0 {
                    &mut []
                } else {
                    slice::from_raw_parts_mut(ptr, len)
                },
                dirty: false,
            }
        }
    }
}

impl Drop for ImageSurfaceData<'_> {
    #[inline]
    fn drop(&mut self) {
        if self.dirty {
            self.surface.mark_dirty()
        }
    }
}

impl Deref for ImageSurfaceData<'_> {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &[u8] {
        self.slice
    }
}

impl DerefMut for ImageSurfaceData<'_> {
    #[inline]
    fn deref_mut(&mut self) -> &mut [u8] {
        self.dirty = true;
        self.slice
    }
}

// Workaround for cairo not having a direct way to check if the surface is finished.
// See: https://gitlab.freedesktop.org/cairo/cairo/-/issues/406
fn is_finished(surface: &ImageSurface) -> bool {
    use super::Context;
    Context::new(surface).is_err()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_with_invalid_size_yields_error() {
        let result = ImageSurface::create(Format::ARgb32, 50000, 50000);
        assert!(result.is_err());
    }

    #[test]
    fn create_for_data_with_invalid_stride_yields_error() {
        let result = ImageSurface::create_for_data(vec![0u8; 10], Format::ARgb32, 1, 2, 5); // unaligned stride
        assert!(result.is_err());
    }

    #[test]
    fn create_with_valid_size() {
        let result = ImageSurface::create(Format::ARgb32, 10, 10);
        assert!(result.is_ok());

        let result = ImageSurface::create_for_data(vec![0u8; 40 * 10], Format::ARgb32, 10, 10, 40);
        assert!(result.is_ok());
    }

    #[test]
    fn no_crash_after_finish() {
        let mut surf = ImageSurface::create(Format::ARgb32, 1024, 1024).unwrap();

        surf.finish();

        assert!(surf.data().is_err());
    }

    #[test]
    fn create_from_owned() {
        let result = ImageSurface::create(Format::ARgb32, 10, 10);
        assert!(result.is_ok());
        let image_surface = result.unwrap();
        let stride = image_surface.stride();
        let data = image_surface.take_data().unwrap();
        let second = ImageSurface::create_for_data(data, Format::ARgb32, 10, 10, stride);
        assert!(second.is_ok())
    }

    #[cfg(feature = "use_glib")]
    #[test]
    fn surface_gvalues() {
        use glib::prelude::*;

        let surface = ImageSurface::create(Format::ARgb32, 10, 10).unwrap();
        let value = surface.to_value();
        assert_eq!(value.get::<ImageSurface>().unwrap().width(), 10);
        let _ = surface.to_value();
        let surface = Some(surface);
        let value = surface.to_value();
        assert_eq!(
            value
                .get::<Option<ImageSurface>>()
                .unwrap()
                .map(|s| s.width()),
            Some(10)
        );
        let _ = surface.as_ref().to_value();
        assert_eq!(
            value
                .get::<Option<&ImageSurface>>()
                .unwrap()
                .map(|s| s.width()),
            Some(10)
        );
    }
}
