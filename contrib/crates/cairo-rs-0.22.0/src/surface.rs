// Take a look at the license at the top of the repository in the LICENSE file.

#[cfg(feature = "use_glib")]
use std::marker::PhantomData;
use std::{ffi::CString, ops::Deref, ptr, slice};

#[cfg(feature = "use_glib")]
use glib::translate::*;
use libc::{c_ulong, c_void};

use crate::{
    Content, Device, Error, Format, ImageSurface, Rectangle, RectangleInt, SurfaceType, ffi,
    utils::status_to_result,
};

#[derive(Debug)]
#[doc(alias = "cairo_surface_t")]
#[repr(transparent)]
pub struct Surface(ptr::NonNull<ffi::cairo_surface_t>);

impl Surface {
    #[inline]
    pub unsafe fn from_raw_none(ptr: *mut ffi::cairo_surface_t) -> Surface {
        unsafe {
            debug_assert!(!ptr.is_null());
            ffi::cairo_surface_reference(ptr);
            Surface(ptr::NonNull::new_unchecked(ptr))
        }
    }

    #[inline]
    pub unsafe fn from_raw_borrow(ptr: *mut ffi::cairo_surface_t) -> crate::Borrowed<Surface> {
        unsafe {
            debug_assert!(!ptr.is_null());
            crate::Borrowed::new(Surface(ptr::NonNull::new_unchecked(ptr)))
        }
    }

    #[inline]
    pub unsafe fn from_raw_full(ptr: *mut ffi::cairo_surface_t) -> Result<Surface, Error> {
        unsafe {
            debug_assert!(!ptr.is_null());
            let status = ffi::cairo_surface_status(ptr);
            status_to_result(status)?;
            Ok(Surface(ptr::NonNull::new_unchecked(ptr)))
        }
    }

    #[inline]
    pub fn to_raw_none(&self) -> *mut ffi::cairo_surface_t {
        self.0.as_ptr()
    }

    #[doc(alias = "cairo_surface_create_similar")]
    pub fn create_similar(
        &self,
        content: Content,
        width: i32,
        height: i32,
    ) -> Result<Surface, Error> {
        unsafe {
            Self::from_raw_full(ffi::cairo_surface_create_similar(
                self.0.as_ptr(),
                content.into(),
                width,
                height,
            ))
        }
    }

    #[doc(alias = "cairo_surface_create_for_rectangle")]
    pub fn create_for_rectangle(&self, bounds: Rectangle) -> Result<Surface, Error> {
        unsafe {
            Self::from_raw_full(ffi::cairo_surface_create_for_rectangle(
                self.0.as_ptr(),
                bounds.x(),
                bounds.y(),
                bounds.width(),
                bounds.height(),
            ))
        }
    }

    #[doc(alias = "cairo_surface_get_mime_data")]
    #[doc(alias = "get_mime_data")]
    pub fn mime_data(&self, mime_type: &str) -> Option<Vec<u8>> {
        let mut data_ptr: *mut u8 = ptr::null_mut();
        let mut length: c_ulong = 0;
        // The function actually needs a mutable pointer
        #[allow(clippy::unnecessary_mut_passed)]
        unsafe {
            let mime_type = CString::new(mime_type).unwrap();
            ffi::cairo_surface_get_mime_data(
                self.to_raw_none(),
                mime_type.as_ptr(),
                &mut data_ptr,
                &mut length,
            );
            if !data_ptr.is_null() && length != 0 {
                Some(slice::from_raw_parts(data_ptr as *const u8, length as usize).to_vec())
            } else {
                None
            }
        }
    }

    #[doc(alias = "cairo_surface_get_mime_data")]
    #[doc(alias = "get_mime_data_raw")]
    pub unsafe fn mime_data_raw(&self, mime_type: &str) -> Option<&[u8]> {
        unsafe {
            let mut data_ptr: *mut u8 = ptr::null_mut();
            let mut length: c_ulong = 0;
            let mime_type = CString::new(mime_type).unwrap();
            // The function actually needs a mutable pointer
            #[allow(clippy::unnecessary_mut_passed)]
            {
                ffi::cairo_surface_get_mime_data(
                    self.to_raw_none(),
                    mime_type.as_ptr(),
                    &mut data_ptr,
                    &mut length,
                );
            }
            if !data_ptr.is_null() && length != 0 {
                Some(slice::from_raw_parts(
                    data_ptr as *const u8,
                    length as usize,
                ))
            } else {
                None
            }
        }
    }

    #[doc(alias = "cairo_surface_set_mime_data")]
    pub fn set_mime_data<T: AsRef<[u8]> + 'static>(
        &self,
        mime_type: &str,
        slice: T,
    ) -> Result<(), Error> {
        let b = Box::new(slice);
        let (size, data) = {
            let slice = (*b).as_ref();
            (slice.len(), slice.as_ptr())
        };

        let user_data = Box::into_raw(b);

        unsafe extern "C" fn unbox<T>(data: *mut c_void) {
            unsafe {
                let data: Box<T> = Box::from_raw(data as *mut T);
                drop(data);
            }
        }

        let status = unsafe {
            let mime_type = CString::new(mime_type).unwrap();
            ffi::cairo_surface_set_mime_data(
                self.to_raw_none(),
                mime_type.as_ptr(),
                data,
                size as c_ulong,
                Some(unbox::<T>),
                user_data as *mut _,
            )
        };
        status_to_result(status)
    }

    #[doc(alias = "cairo_surface_supports_mime_type")]
    pub fn supports_mime_type(&self, mime_type: &str) -> bool {
        unsafe {
            let mime_type = CString::new(mime_type).unwrap();
            ffi::cairo_surface_supports_mime_type(self.0.as_ptr(), mime_type.as_ptr()).as_bool()
        }
    }

    #[doc(alias = "cairo_surface_get_device")]
    #[doc(alias = "get_device")]
    pub fn device(&self) -> Option<Device> {
        unsafe {
            let device = ffi::cairo_surface_get_device(self.to_raw_none());
            if device.is_null() {
                None
            } else {
                Some(Device::from_raw_none(device))
            }
        }
    }

    #[doc(alias = "cairo_surface_get_content")]
    pub fn content(&self) -> Content {
        unsafe { ffi::cairo_surface_get_content(self.to_raw_none()) }.into()
    }

    #[doc(alias = "cairo_surface_set_device_offset")]
    pub fn set_device_offset(&self, x_offset: f64, y_offset: f64) {
        unsafe { ffi::cairo_surface_set_device_offset(self.to_raw_none(), x_offset, y_offset) }
    }

    #[doc(alias = "cairo_surface_get_device_offset")]
    #[doc(alias = "get_device_offset")]
    pub fn device_offset(&self) -> (f64, f64) {
        let mut x_offset = 0.0f64;
        let mut y_offset = 0.0f64;
        unsafe {
            ffi::cairo_surface_get_device_offset(self.to_raw_none(), &mut x_offset, &mut y_offset);
        }
        (x_offset, y_offset)
    }

    #[doc(alias = "cairo_surface_set_device_scale")]
    pub fn set_device_scale(&self, x_scale: f64, y_scale: f64) {
        unsafe { ffi::cairo_surface_set_device_scale(self.to_raw_none(), x_scale, y_scale) }
    }

    #[doc(alias = "cairo_surface_get_device_scale")]
    #[doc(alias = "get_device_scale")]
    pub fn device_scale(&self) -> (f64, f64) {
        let mut x_scale = 0.0f64;
        let mut y_scale = 0.0f64;
        unsafe {
            ffi::cairo_surface_get_device_scale(self.to_raw_none(), &mut x_scale, &mut y_scale);
        }
        (x_scale, y_scale)
    }

    #[doc(alias = "cairo_surface_set_fallback_resolution")]
    pub fn set_fallback_resolution(&self, x_pixels_per_inch: f64, y_pixels_per_inch: f64) {
        unsafe {
            ffi::cairo_surface_set_fallback_resolution(
                self.to_raw_none(),
                x_pixels_per_inch,
                y_pixels_per_inch,
            )
        }
    }

    #[doc(alias = "cairo_surface_get_fallback_resolution")]
    #[doc(alias = "get_fallback_resolution")]
    pub fn fallback_resolution(&self) -> (f64, f64) {
        let mut x_pixels_per_inch = 0.0f64;
        let mut y_pixels_per_inch = 0.0f64;
        unsafe {
            ffi::cairo_surface_get_fallback_resolution(
                self.to_raw_none(),
                &mut x_pixels_per_inch,
                &mut y_pixels_per_inch,
            );
        }
        (x_pixels_per_inch, y_pixels_per_inch)
    }

    #[doc(alias = "cairo_surface_create_similar_image")]
    pub fn create_similar_image(
        &self,
        format: Format,
        width: i32,
        height: i32,
    ) -> Result<ImageSurface, Error> {
        unsafe {
            ImageSurface::from_raw_full(ffi::cairo_surface_create_similar_image(
                self.to_raw_none(),
                format.into(),
                width,
                height,
            ))
        }
    }

    #[doc(alias = "cairo_surface_map_to_image")]
    pub fn map_to_image(&self, extents: Option<RectangleInt>) -> Result<MappedImageSurface, Error> {
        unsafe {
            ImageSurface::from_raw_none(match extents {
                Some(ref e) => ffi::cairo_surface_map_to_image(self.to_raw_none(), e.to_raw_none()),
                None => ffi::cairo_surface_map_to_image(self.to_raw_none(), std::ptr::null()),
            })
            .map(|s| MappedImageSurface {
                original_surface: self.clone(),
                image_surface: s,
            })
        }
    }

    #[doc(alias = "cairo_surface_mark_dirty")]
    pub fn mark_dirty(&self) {
        unsafe { ffi::cairo_surface_mark_dirty(self.to_raw_none()) }
    }

    #[doc(alias = "cairo_surface_mark_dirty_rectangle")]
    pub fn mark_dirty_rectangle(&self, x: i32, y: i32, width: i32, height: i32) {
        unsafe { ffi::cairo_surface_mark_dirty_rectangle(self.to_raw_none(), x, y, width, height) }
    }

    #[doc(alias = "cairo_surface_status")]
    #[inline]
    pub fn status(&self) -> Result<(), Error> {
        let status = unsafe { ffi::cairo_surface_status(self.to_raw_none()) };
        status_to_result(status)
    }

    user_data_methods! {
        ffi::cairo_surface_get_user_data,
        ffi::cairo_surface_set_user_data,
    }
}

#[cfg(feature = "use_glib")]
impl IntoGlibPtr<*mut ffi::cairo_surface_t> for Surface {
    #[inline]
    fn into_glib_ptr(self) -> *mut ffi::cairo_surface_t {
        std::mem::ManuallyDrop::new(self).to_glib_none().0
    }
}

#[cfg(feature = "use_glib")]
impl<'a> ToGlibPtr<'a, *mut ffi::cairo_surface_t> for Surface {
    type Storage = PhantomData<&'a Surface>;

    #[inline]
    fn to_glib_none(&'a self) -> Stash<'a, *mut ffi::cairo_surface_t, Self> {
        Stash(self.to_raw_none(), PhantomData)
    }

    #[inline]
    fn to_glib_full(&self) -> *mut ffi::cairo_surface_t {
        unsafe { ffi::cairo_surface_reference(self.to_raw_none()) }
    }
}

#[cfg(feature = "use_glib")]
impl FromGlibPtrNone<*mut ffi::cairo_surface_t> for Surface {
    #[inline]
    unsafe fn from_glib_none(ptr: *mut ffi::cairo_surface_t) -> Surface {
        unsafe { Self::from_raw_none(ptr) }
    }
}

#[cfg(feature = "use_glib")]
impl FromGlibPtrBorrow<*mut ffi::cairo_surface_t> for Surface {
    #[inline]
    unsafe fn from_glib_borrow(ptr: *mut ffi::cairo_surface_t) -> crate::Borrowed<Surface> {
        unsafe { Self::from_raw_borrow(ptr) }
    }
}

#[cfg(feature = "use_glib")]
impl FromGlibPtrFull<*mut ffi::cairo_surface_t> for Surface {
    #[inline]
    unsafe fn from_glib_full(ptr: *mut ffi::cairo_surface_t) -> Surface {
        unsafe { Self::from_raw_full(ptr).unwrap() }
    }
}

#[cfg(feature = "use_glib")]
gvalue_impl!(
    Surface,
    ffi::cairo_surface_t,
    ffi::gobject::cairo_gobject_surface_get_type
);

impl Clone for Surface {
    #[inline]
    fn clone(&self) -> Surface {
        unsafe { Self::from_raw_none(self.0.as_ptr()) }
    }
}

impl Drop for Surface {
    #[inline]
    fn drop(&mut self) {
        unsafe {
            ffi::cairo_surface_destroy(self.0.as_ptr());
        }
    }
}

impl AsRef<Surface> for Surface {
    #[inline]
    fn as_ref(&self) -> &Surface {
        self
    }
}

impl Surface {
    #[doc(alias = "cairo_surface_flush")]
    pub fn flush(&self) {
        unsafe {
            ffi::cairo_surface_flush(self.0.as_ptr());
        }
    }

    #[doc(alias = "cairo_surface_finish")]
    pub fn finish(&self) {
        unsafe {
            ffi::cairo_surface_finish(self.0.as_ptr());
        }
    }

    #[doc(alias = "cairo_surface_get_type")]
    #[doc(alias = "get_type")]
    pub fn type_(&self) -> SurfaceType {
        unsafe { SurfaceType::from(ffi::cairo_surface_get_type(self.0.as_ptr())) }
    }
}

#[derive(Debug)]
pub struct MappedImageSurface {
    original_surface: Surface,
    image_surface: ImageSurface,
}

impl Deref for MappedImageSurface {
    type Target = ImageSurface;

    #[inline]
    fn deref(&self) -> &ImageSurface {
        &self.image_surface
    }
}

impl AsRef<ImageSurface> for MappedImageSurface {
    #[inline]
    fn as_ref(&self) -> &ImageSurface {
        &self.image_surface
    }
}

impl Drop for MappedImageSurface {
    #[inline]
    fn drop(&mut self) {
        unsafe {
            ffi::cairo_surface_unmap_image(
                self.original_surface.to_raw_none(),
                self.image_surface.to_raw_none(),
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{Format, ImageSurface, constants::MIME_TYPE_PNG};

    #[test]
    fn mime_data() {
        let surface = ImageSurface::create(Format::ARgb32, 500, 500).unwrap();
        let data = surface.mime_data(MIME_TYPE_PNG);
        /* Initially the data for any mime type has to be none */
        assert!(data.is_none());

        assert!(surface.set_mime_data(MIME_TYPE_PNG, [1u8, 10u8]).is_ok());
        let data = surface.mime_data(MIME_TYPE_PNG).unwrap();
        assert_eq!(data, &[1u8, 10u8]);
    }
}
