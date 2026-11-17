// Take a look at the license at the top of the repository in the LICENSE file.

#[cfg(feature = "script")]
use std::ffi::CString;
#[cfg(feature = "use_glib")]
use std::marker::PhantomData;
#[cfg(feature = "script")]
use std::path::Path;
use std::ptr;

#[cfg(feature = "use_glib")]
use glib::translate::*;

#[cfg(feature = "script")]
use crate::{Content, RecordingSurface, ScriptMode, Surface};
use crate::{DeviceType, Error, ffi, utils::status_to_result};

#[derive(Debug)]
#[must_use = "if unused the Device will immediately be released"]
pub struct DeviceAcquireGuard<'a>(&'a Device);

impl Drop for DeviceAcquireGuard<'_> {
    #[inline]
    fn drop(&mut self) {
        self.0.release();
    }
}

#[derive(Debug)]
#[doc(alias = "cairo_device_t")]
#[repr(transparent)]
pub struct Device(ptr::NonNull<ffi::cairo_device_t>);

impl Device {
    #[inline]
    pub unsafe fn from_raw_none(ptr: *mut ffi::cairo_device_t) -> Device {
        unsafe {
            debug_assert!(!ptr.is_null());
            ffi::cairo_device_reference(ptr);
            Device(ptr::NonNull::new_unchecked(ptr))
        }
    }

    #[inline]
    pub unsafe fn from_raw_borrow(ptr: *mut ffi::cairo_device_t) -> crate::Borrowed<Device> {
        unsafe {
            debug_assert!(!ptr.is_null());
            crate::Borrowed::new(Device(ptr::NonNull::new_unchecked(ptr)))
        }
    }

    #[inline]
    pub unsafe fn from_raw_full(ptr: *mut ffi::cairo_device_t) -> Device {
        unsafe {
            debug_assert!(!ptr.is_null());
            Device(ptr::NonNull::new_unchecked(ptr))
        }
    }

    #[inline]
    pub fn to_raw_none(&self) -> *mut ffi::cairo_device_t {
        self.0.as_ptr()
    }

    #[cfg(feature = "script")]
    #[cfg_attr(docsrs, doc(cfg(feature = "script")))]
    #[doc(alias = "cairo_script_create")]
    pub fn create<P: AsRef<Path>>(filename: P) -> Option<Device> {
        unsafe {
            let filename = filename.as_ref().to_string_lossy().into_owned();
            let filename = CString::new(filename).unwrap();
            let p = ffi::cairo_script_create(filename.as_ptr());
            if p.is_null() {
                None
            } else {
                Some(Self::from_raw_full(p))
            }
        }
    }

    #[cfg(feature = "script")]
    #[cfg_attr(docsrs, doc(cfg(feature = "script")))]
    #[doc(alias = "cairo_script_from_recording_surface")]
    pub fn from_recording_surface(&self, surface: &RecordingSurface) -> Result<(), Error> {
        unsafe {
            let status =
                ffi::cairo_script_from_recording_surface(self.to_raw_none(), surface.to_raw_none());
            status_to_result(status)
        }
    }

    #[cfg(feature = "script")]
    #[cfg_attr(docsrs, doc(cfg(feature = "script")))]
    #[doc(alias = "cairo_script_get_mode")]
    #[doc(alias = "get_mode")]
    pub fn mode(&self) -> ScriptMode {
        unsafe { ScriptMode::from(ffi::cairo_script_get_mode(self.to_raw_none())) }
    }

    #[cfg(feature = "script")]
    #[cfg_attr(docsrs, doc(cfg(feature = "script")))]
    #[doc(alias = "cairo_script_set_mode")]
    pub fn set_mode(&self, mode: ScriptMode) {
        unsafe { ffi::cairo_script_set_mode(self.to_raw_none(), mode.into()) }
    }

    #[cfg(feature = "script")]
    #[cfg_attr(docsrs, doc(cfg(feature = "script")))]
    #[doc(alias = "cairo_script_surface_create")]
    pub fn surface_create(
        &self,
        content: Content,
        width: f64,
        height: f64,
    ) -> Result<Surface, Error> {
        unsafe {
            Surface::from_raw_full(ffi::cairo_script_surface_create(
                self.to_raw_none(),
                content.into(),
                width,
                height,
            ))
        }
    }

    #[cfg(feature = "script")]
    #[cfg_attr(docsrs, doc(cfg(feature = "script")))]
    #[doc(alias = "cairo_script_surface_create_for_target")]
    pub fn surface_create_for_target(&self, target: impl AsRef<Surface>) -> Result<Surface, Error> {
        let target = target.as_ref();
        target.status()?;
        unsafe {
            Surface::from_raw_full(ffi::cairo_script_surface_create_for_target(
                self.to_raw_none(),
                target.to_raw_none(),
            ))
        }
    }

    #[cfg(feature = "script")]
    #[cfg_attr(docsrs, doc(cfg(feature = "script")))]
    #[doc(alias = "cairo_script_write_comment")]
    pub fn write_comment(&self, comment: &str) {
        unsafe {
            let len = comment.len();
            let comment = CString::new(comment).unwrap();
            ffi::cairo_script_write_comment(self.to_raw_none(), comment.as_ptr(), len as i32)
        }
    }

    #[doc(alias = "cairo_device_finish")]
    pub fn finish(&self) {
        unsafe { ffi::cairo_device_finish(self.to_raw_none()) }
    }

    #[doc(alias = "cairo_device_flush")]
    pub fn flush(&self) {
        unsafe { ffi::cairo_device_flush(self.to_raw_none()) }
    }

    #[doc(alias = "cairo_device_get_type")]
    #[doc(alias = "get_type")]
    pub fn type_(&self) -> DeviceType {
        unsafe { DeviceType::from(ffi::cairo_device_get_type(self.to_raw_none())) }
    }

    #[doc(alias = "cairo_device_acquire")]
    pub fn acquire(&self) -> Result<DeviceAcquireGuard<'_>, Error> {
        unsafe {
            let status = ffi::cairo_device_acquire(self.to_raw_none());
            status_to_result(status)?;
        }
        Ok(DeviceAcquireGuard(self))
    }

    #[doc(alias = "cairo_device_release")]
    fn release(&self) {
        unsafe { ffi::cairo_device_release(self.to_raw_none()) }
    }

    #[doc(alias = "cairo_device_observer_elapsed")]
    pub fn observer_elapsed(&self) -> f64 {
        unsafe { ffi::cairo_device_observer_elapsed(self.to_raw_none()) }
    }

    #[doc(alias = "cairo_device_observer_fill_elapsed")]
    pub fn observer_fill_elapsed(&self) -> f64 {
        unsafe { ffi::cairo_device_observer_fill_elapsed(self.to_raw_none()) }
    }

    #[doc(alias = "cairo_device_observer_glyphs_elapsed")]
    pub fn observer_glyphs_elapsed(&self) -> f64 {
        unsafe { ffi::cairo_device_observer_glyphs_elapsed(self.to_raw_none()) }
    }

    #[doc(alias = "cairo_device_observer_mask_elapsed")]
    pub fn observer_mask_elapsed(&self) -> f64 {
        unsafe { ffi::cairo_device_observer_mask_elapsed(self.to_raw_none()) }
    }

    #[doc(alias = "cairo_device_observer_paint_elapsed")]
    pub fn observer_paint_elapsed(&self) -> f64 {
        unsafe { ffi::cairo_device_observer_paint_elapsed(self.to_raw_none()) }
    }

    #[doc(alias = "cairo_device_observer_stroke_elapsed")]
    pub fn observer_stroke_elapsed(&self) -> f64 {
        unsafe { ffi::cairo_device_observer_stroke_elapsed(self.to_raw_none()) }
    }

    #[cfg(any(feature = "xlib", feature = "xcb"))]
    #[cfg_attr(docsrs, doc(cfg(any(feature = "xlib", feature = "xcb"))))]
    #[doc(alias = "cairo_xlib_device_debug_cap_xrender_version")]
    #[doc(alias = "cairo_xcb_device_debug_cap_xrender_version")]
    pub fn debug_cap_xrender_version(&self, _major_version: i32, _minor_version: i32) {
        match self.type_() {
            DeviceType::Xlib => {
                #[cfg(feature = "xlib")]
                unsafe {
                    ffi::cairo_xlib_device_debug_cap_xrender_version(
                        self.to_raw_none(),
                        _major_version,
                        _minor_version,
                    )
                }
                #[cfg(not(feature = "xlib"))]
                {
                    panic!("you need to enable \"xlib\" feature")
                }
            }
            DeviceType::Xcb => {
                #[cfg(feature = "xcb")]
                unsafe {
                    ffi::cairo_xcb_device_debug_cap_xrender_version(
                        self.to_raw_none(),
                        _major_version,
                        _minor_version,
                    )
                }
                #[cfg(not(feature = "xcb"))]
                {
                    panic!("you need to enable \"xcb\" feature")
                }
            }
            d => panic!("invalid device type: {:#?}", d),
        }
    }

    #[cfg(any(feature = "xlib", feature = "xcb"))]
    #[cfg_attr(docsrs, doc(cfg(any(feature = "xlib", feature = "xcb"))))]
    #[doc(alias = "cairo_xlib_device_debug_get_precision")]
    #[doc(alias = "cairo_xcb_device_debug_get_precision")]
    pub fn debug_get_precision(&self) -> i32 {
        match self.type_() {
            DeviceType::Xlib => {
                #[cfg(feature = "xlib")]
                unsafe {
                    ffi::cairo_xlib_device_debug_get_precision(self.to_raw_none())
                }
                #[cfg(not(feature = "xlib"))]
                {
                    panic!("you need to enable \"xlib\" feature")
                }
            }
            DeviceType::Xcb => {
                #[cfg(feature = "xcb")]
                unsafe {
                    ffi::cairo_xcb_device_debug_get_precision(self.to_raw_none())
                }
                #[cfg(not(feature = "xcb"))]
                {
                    panic!("you need to enable \"xcb\" feature")
                }
            }
            d => panic!("invalid device type: {:#?}", d),
        }
    }

    #[cfg(any(feature = "xlib", feature = "xcb"))]
    #[cfg_attr(docsrs, doc(cfg(any(feature = "xlib", feature = "xcb"))))]
    #[doc(alias = "cairo_xlib_device_debug_set_precision")]
    #[doc(alias = "cairo_xcb_device_debug_set_precision")]
    pub fn debug_set_precision(&self, _precision: i32) {
        match self.type_() {
            DeviceType::Xlib => {
                #[cfg(feature = "xlib")]
                unsafe {
                    ffi::cairo_xlib_device_debug_set_precision(self.to_raw_none(), _precision)
                }
                #[cfg(not(feature = "xlib"))]
                {
                    panic!("you need to enable \"xlib\" feature")
                }
            }
            DeviceType::Xcb => {
                #[cfg(feature = "xcb")]
                unsafe {
                    ffi::cairo_xcb_device_debug_set_precision(self.to_raw_none(), _precision)
                }
                #[cfg(not(feature = "xcb"))]
                {
                    panic!("you need to enable \"xcb\" feature")
                }
            }
            d => panic!("invalid device type: {:#?}", d),
        }
    }

    #[doc(alias = "cairo_device_status")]
    #[inline]
    pub fn status(&self) -> Result<(), Error> {
        let status = unsafe { ffi::cairo_device_status(self.to_raw_none()) };
        status_to_result(status)
    }

    user_data_methods! {
        ffi::cairo_device_get_user_data,
        ffi::cairo_device_set_user_data,
    }
}

#[cfg(feature = "use_glib")]
#[cfg_attr(docsrs, doc(cfg(feature = "use_glib")))]
impl IntoGlibPtr<*mut ffi::cairo_device_t> for Device {
    #[inline]
    fn into_glib_ptr(self) -> *mut ffi::cairo_device_t {
        std::mem::ManuallyDrop::new(self).to_glib_none().0
    }
}

#[cfg(feature = "use_glib")]
#[cfg_attr(docsrs, doc(cfg(feature = "use_glib")))]
impl<'a> ToGlibPtr<'a, *mut ffi::cairo_device_t> for Device {
    type Storage = PhantomData<&'a Device>;

    #[inline]
    fn to_glib_none(&'a self) -> Stash<'a, *mut ffi::cairo_device_t, Self> {
        Stash(self.to_raw_none(), PhantomData)
    }

    #[inline]
    fn to_glib_full(&self) -> *mut ffi::cairo_device_t {
        unsafe { ffi::cairo_device_reference(self.to_raw_none()) }
    }
}

#[cfg(feature = "use_glib")]
#[cfg_attr(docsrs, doc(cfg(feature = "use_glib")))]
impl FromGlibPtrNone<*mut ffi::cairo_device_t> for Device {
    #[inline]
    unsafe fn from_glib_none(ptr: *mut ffi::cairo_device_t) -> Device {
        unsafe { Self::from_raw_none(ptr) }
    }
}

#[cfg(feature = "use_glib")]
#[cfg_attr(docsrs, doc(cfg(feature = "use_glib")))]
impl FromGlibPtrBorrow<*mut ffi::cairo_device_t> for Device {
    #[inline]
    unsafe fn from_glib_borrow(ptr: *mut ffi::cairo_device_t) -> crate::Borrowed<Device> {
        unsafe { Self::from_raw_borrow(ptr) }
    }
}

#[cfg(feature = "use_glib")]
#[cfg_attr(docsrs, doc(cfg(feature = "use_glib")))]
impl FromGlibPtrFull<*mut ffi::cairo_device_t> for Device {
    #[inline]
    unsafe fn from_glib_full(ptr: *mut ffi::cairo_device_t) -> Device {
        unsafe { Self::from_raw_full(ptr) }
    }
}

#[cfg(feature = "use_glib")]
gvalue_impl!(
    Device,
    ffi::cairo_device_t,
    ffi::gobject::cairo_gobject_device_get_type
);

impl Clone for Device {
    #[inline]
    fn clone(&self) -> Device {
        unsafe { Self::from_raw_none(ffi::cairo_device_reference(self.0.as_ptr())) }
    }
}

impl Drop for Device {
    #[inline]
    fn drop(&mut self) {
        unsafe {
            ffi::cairo_device_destroy(self.0.as_ptr());
        }
    }
}
