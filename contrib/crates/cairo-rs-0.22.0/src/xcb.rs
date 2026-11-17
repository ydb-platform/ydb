// Take a look at the license at the top of the repository in the LICENSE file.

#[cfg(feature = "use_glib")]
use std::marker::PhantomData;
use std::{ops::Deref, ptr};

#[cfg(feature = "use_glib")]
use glib::translate::*;

#[cfg(not(feature = "use_glib"))]
use crate::Borrowed;

use crate::{Error, Surface, SurfaceType, ffi};

#[derive(Debug)]
pub struct XCBDrawable(pub u32);

impl XCBDrawable {
    #[inline]
    fn to_raw_none(&self) -> u32 {
        self.0
    }
}

#[derive(Debug)]
pub struct XCBPixmap(pub u32);

impl XCBPixmap {
    #[inline]
    fn to_raw_none(&self) -> u32 {
        self.0
    }
}

#[derive(Debug)]
#[doc(alias = "xcb_connection_t")]
pub struct XCBConnection(pub ptr::NonNull<ffi::xcb_connection_t>);

impl XCBConnection {
    #[inline]
    pub fn to_raw_none(&self) -> *mut ffi::xcb_connection_t {
        self.0.as_ptr()
    }

    #[inline]
    pub unsafe fn from_raw_none(ptr: *mut ffi::xcb_connection_t) -> XCBConnection {
        unsafe {
            debug_assert!(!ptr.is_null());
            XCBConnection(ptr::NonNull::new_unchecked(ptr))
        }
    }

    #[inline]
    pub unsafe fn from_raw_borrow(ptr: *mut ffi::xcb_connection_t) -> Borrowed<XCBConnection> {
        unsafe {
            debug_assert!(!ptr.is_null());
            Borrowed::new(XCBConnection(ptr::NonNull::new_unchecked(ptr)))
        }
    }

    #[inline]
    pub unsafe fn from_raw_full(ptr: *mut ffi::xcb_connection_t) -> XCBConnection {
        unsafe {
            debug_assert!(!ptr.is_null());
            XCBConnection(ptr::NonNull::new_unchecked(ptr))
        }
    }
}

#[cfg(feature = "use_glib")]
impl<'a> ToGlibPtr<'a, *mut ffi::xcb_connection_t> for &'a XCBConnection {
    type Storage = PhantomData<&'a XCBConnection>;

    #[inline]
    fn to_glib_none(&self) -> Stash<'a, *mut ffi::xcb_connection_t, &'a XCBConnection> {
        Stash(self.to_raw_none(), PhantomData)
    }
}

#[cfg(feature = "use_glib")]
impl FromGlibPtrNone<*mut ffi::xcb_connection_t> for XCBConnection {
    #[inline]
    unsafe fn from_glib_none(ptr: *mut ffi::xcb_connection_t) -> XCBConnection {
        unsafe { Self::from_raw_none(ptr) }
    }
}

#[cfg(feature = "use_glib")]
impl FromGlibPtrBorrow<*mut ffi::xcb_connection_t> for XCBConnection {
    #[inline]
    unsafe fn from_glib_borrow(ptr: *mut ffi::xcb_connection_t) -> Borrowed<XCBConnection> {
        unsafe { Self::from_raw_borrow(ptr) }
    }
}

#[cfg(feature = "use_glib")]
impl FromGlibPtrFull<*mut ffi::xcb_connection_t> for XCBConnection {
    #[inline]
    unsafe fn from_glib_full(ptr: *mut ffi::xcb_connection_t) -> XCBConnection {
        unsafe { Self::from_raw_full(ptr) }
    }
}

impl Clone for XCBConnection {
    #[inline]
    fn clone(&self) -> XCBConnection {
        unsafe { Self::from_raw_none(self.to_raw_none()) }
    }
}

#[derive(Debug)]
#[doc(alias = "xcb_render_pictforminfo_t")]
pub struct XCBRenderPictFormInfo(pub ptr::NonNull<ffi::xcb_render_pictforminfo_t>);

impl XCBRenderPictFormInfo {
    #[inline]
    pub fn to_raw_none(&self) -> *mut ffi::xcb_render_pictforminfo_t {
        self.0.as_ptr()
    }

    #[inline]
    pub unsafe fn from_raw_none(ptr: *mut ffi::xcb_render_pictforminfo_t) -> XCBRenderPictFormInfo {
        unsafe {
            debug_assert!(!ptr.is_null());
            XCBRenderPictFormInfo(ptr::NonNull::new_unchecked(ptr))
        }
    }

    #[inline]
    pub unsafe fn from_raw_borrow(
        ptr: *mut ffi::xcb_render_pictforminfo_t,
    ) -> Borrowed<XCBRenderPictFormInfo> {
        unsafe {
            debug_assert!(!ptr.is_null());
            Borrowed::new(XCBRenderPictFormInfo(ptr::NonNull::new_unchecked(ptr)))
        }
    }

    #[inline]
    pub unsafe fn from_raw_full(ptr: *mut ffi::xcb_render_pictforminfo_t) -> XCBRenderPictFormInfo {
        unsafe {
            debug_assert!(!ptr.is_null());
            XCBRenderPictFormInfo(ptr::NonNull::new_unchecked(ptr))
        }
    }
}

#[cfg(feature = "use_glib")]
impl<'a> ToGlibPtr<'a, *mut ffi::xcb_render_pictforminfo_t> for &'a XCBRenderPictFormInfo {
    type Storage = PhantomData<&'a XCBRenderPictFormInfo>;

    #[inline]
    fn to_glib_none(
        &self,
    ) -> Stash<'a, *mut ffi::xcb_render_pictforminfo_t, &'a XCBRenderPictFormInfo> {
        Stash(self.to_raw_none(), PhantomData)
    }
}

#[cfg(feature = "use_glib")]
impl FromGlibPtrNone<*mut ffi::xcb_render_pictforminfo_t> for XCBRenderPictFormInfo {
    #[inline]
    unsafe fn from_glib_none(ptr: *mut ffi::xcb_render_pictforminfo_t) -> XCBRenderPictFormInfo {
        unsafe { Self::from_raw_none(ptr) }
    }
}

#[cfg(feature = "use_glib")]
impl FromGlibPtrBorrow<*mut ffi::xcb_render_pictforminfo_t> for XCBRenderPictFormInfo {
    #[inline]
    unsafe fn from_glib_borrow(
        ptr: *mut ffi::xcb_render_pictforminfo_t,
    ) -> Borrowed<XCBRenderPictFormInfo> {
        unsafe { Self::from_raw_borrow(ptr) }
    }
}

#[cfg(feature = "use_glib")]
impl FromGlibPtrFull<*mut ffi::xcb_render_pictforminfo_t> for XCBRenderPictFormInfo {
    #[inline]
    unsafe fn from_glib_full(ptr: *mut ffi::xcb_render_pictforminfo_t) -> XCBRenderPictFormInfo {
        unsafe { Self::from_raw_full(ptr) }
    }
}

impl Clone for XCBRenderPictFormInfo {
    #[inline]
    fn clone(&self) -> XCBRenderPictFormInfo {
        unsafe { Self::from_raw_none(self.to_raw_none()) }
    }
}

#[derive(Debug)]
#[doc(alias = "xcb_screen_t")]
pub struct XCBScreen(pub ptr::NonNull<ffi::xcb_screen_t>);

impl XCBScreen {
    #[inline]
    pub fn to_raw_none(&self) -> *mut ffi::xcb_screen_t {
        self.0.as_ptr()
    }

    #[inline]
    pub unsafe fn from_raw_none(ptr: *mut ffi::xcb_screen_t) -> XCBScreen {
        unsafe {
            debug_assert!(!ptr.is_null());
            XCBScreen(ptr::NonNull::new_unchecked(ptr))
        }
    }

    #[inline]
    pub unsafe fn from_raw_borrow(ptr: *mut ffi::xcb_screen_t) -> Borrowed<XCBScreen> {
        unsafe {
            debug_assert!(!ptr.is_null());
            Borrowed::new(XCBScreen(ptr::NonNull::new_unchecked(ptr)))
        }
    }

    #[inline]
    pub unsafe fn from_raw_full(ptr: *mut ffi::xcb_screen_t) -> XCBScreen {
        unsafe {
            debug_assert!(!ptr.is_null());
            XCBScreen(ptr::NonNull::new_unchecked(ptr))
        }
    }
}

#[cfg(feature = "use_glib")]
impl<'a> ToGlibPtr<'a, *mut ffi::xcb_screen_t> for &'a XCBScreen {
    type Storage = PhantomData<&'a XCBScreen>;

    #[inline]
    fn to_glib_none(&self) -> Stash<'a, *mut ffi::xcb_screen_t, &'a XCBScreen> {
        Stash(self.to_raw_none(), PhantomData)
    }
}

#[cfg(feature = "use_glib")]
impl FromGlibPtrNone<*mut ffi::xcb_screen_t> for XCBScreen {
    #[inline]
    unsafe fn from_glib_none(ptr: *mut ffi::xcb_screen_t) -> XCBScreen {
        unsafe { Self::from_raw_none(ptr) }
    }
}

#[cfg(feature = "use_glib")]
impl FromGlibPtrBorrow<*mut ffi::xcb_screen_t> for XCBScreen {
    #[inline]
    unsafe fn from_glib_borrow(ptr: *mut ffi::xcb_screen_t) -> Borrowed<XCBScreen> {
        unsafe { Self::from_raw_borrow(ptr) }
    }
}

#[cfg(feature = "use_glib")]
impl FromGlibPtrFull<*mut ffi::xcb_screen_t> for XCBScreen {
    #[inline]
    unsafe fn from_glib_full(ptr: *mut ffi::xcb_screen_t) -> XCBScreen {
        unsafe { Self::from_raw_full(ptr) }
    }
}

impl Clone for XCBScreen {
    #[inline]
    fn clone(&self) -> XCBScreen {
        unsafe { Self::from_raw_none(self.to_raw_none()) }
    }
}

declare_surface!(XCBSurface, SurfaceType::Xcb);

impl XCBSurface {
    #[doc(alias = "cairo_xcb_surface_create")]
    pub fn create(
        connection: &XCBConnection,
        drawable: &XCBDrawable,
        visual: &XCBVisualType,
        width: i32,
        height: i32,
    ) -> Result<Self, Error> {
        unsafe {
            Self::from_raw_full(ffi::cairo_xcb_surface_create(
                connection.to_raw_none(),
                drawable.to_raw_none(),
                visual.to_raw_none(),
                width,
                height,
            ))
        }
    }

    #[doc(alias = "cairo_xcb_surface_create_for_bitmap")]
    pub fn create_for_bitmap(
        connection: &XCBConnection,
        screen: &XCBScreen,
        bitmap: &XCBPixmap,
        width: i32,
        height: i32,
    ) -> Result<Self, Error> {
        unsafe {
            Ok(Self(Surface::from_raw_full(
                ffi::cairo_xcb_surface_create_for_bitmap(
                    connection.to_raw_none(),
                    screen.to_raw_none(),
                    bitmap.to_raw_none(),
                    width,
                    height,
                ),
            )?))
        }
    }

    #[doc(alias = "cairo_xcb_surface_create_with_xrender_format")]
    pub fn create_with_xrender_format(
        connection: &XCBConnection,
        screen: &XCBScreen,
        bitmap: &XCBPixmap,
        format: &XCBRenderPictFormInfo,
        width: i32,
        height: i32,
    ) -> Result<Self, Error> {
        unsafe {
            Ok(Self(Surface::from_raw_full(
                ffi::cairo_xcb_surface_create_with_xrender_format(
                    connection.to_raw_none(),
                    screen.to_raw_none(),
                    bitmap.to_raw_none(),
                    format.to_raw_none(),
                    width,
                    height,
                ),
            )?))
        }
    }

    #[doc(alias = "cairo_xcb_surface_set_size")]
    pub fn set_size(&self, width: i32, height: i32) -> Result<(), Error> {
        unsafe { ffi::cairo_xcb_surface_set_size(self.to_raw_none(), width, height) }
        self.status()
    }

    #[doc(alias = "cairo_xcb_surface_set_drawable")]
    pub fn set_drawable(
        &self,
        drawable: &XCBDrawable,
        width: i32,
        height: i32,
    ) -> Result<(), Error> {
        unsafe {
            ffi::cairo_xcb_surface_set_drawable(
                self.to_raw_none(),
                drawable.to_raw_none(),
                width,
                height,
            )
        }
        self.status()
    }
}

#[derive(Debug)]
#[doc(alias = "xcb_visualtype_t")]
pub struct XCBVisualType(pub ptr::NonNull<ffi::xcb_visualtype_t>);

impl XCBVisualType {
    #[inline]
    pub fn to_raw_none(&self) -> *mut ffi::xcb_visualtype_t {
        self.0.as_ptr()
    }

    #[inline]
    pub unsafe fn from_raw_none(ptr: *mut ffi::xcb_visualtype_t) -> XCBVisualType {
        unsafe {
            debug_assert!(!ptr.is_null());
            XCBVisualType(ptr::NonNull::new_unchecked(ptr))
        }
    }

    #[inline]
    pub unsafe fn from_raw_borrow(ptr: *mut ffi::xcb_visualtype_t) -> Borrowed<XCBVisualType> {
        unsafe {
            debug_assert!(!ptr.is_null());
            Borrowed::new(XCBVisualType(ptr::NonNull::new_unchecked(ptr)))
        }
    }

    #[inline]
    pub unsafe fn from_raw_full(ptr: *mut ffi::xcb_visualtype_t) -> XCBVisualType {
        unsafe {
            debug_assert!(!ptr.is_null());
            XCBVisualType(ptr::NonNull::new_unchecked(ptr))
        }
    }
}

#[cfg(feature = "use_glib")]
impl<'a> ToGlibPtr<'a, *mut ffi::xcb_visualtype_t> for &'a XCBVisualType {
    type Storage = PhantomData<&'a XCBVisualType>;

    #[inline]
    fn to_glib_none(&self) -> Stash<'a, *mut ffi::xcb_visualtype_t, &'a XCBVisualType> {
        Stash(self.to_raw_none(), PhantomData)
    }
}

#[cfg(feature = "use_glib")]
impl FromGlibPtrNone<*mut ffi::xcb_visualtype_t> for XCBVisualType {
    #[inline]
    unsafe fn from_glib_none(ptr: *mut ffi::xcb_visualtype_t) -> XCBVisualType {
        unsafe { Self::from_raw_none(ptr) }
    }
}

#[cfg(feature = "use_glib")]
impl FromGlibPtrBorrow<*mut ffi::xcb_visualtype_t> for XCBVisualType {
    #[inline]
    unsafe fn from_glib_borrow(ptr: *mut ffi::xcb_visualtype_t) -> Borrowed<XCBVisualType> {
        unsafe { Self::from_raw_borrow(ptr) }
    }
}

#[cfg(feature = "use_glib")]
impl FromGlibPtrFull<*mut ffi::xcb_visualtype_t> for XCBVisualType {
    #[inline]
    unsafe fn from_glib_full(ptr: *mut ffi::xcb_visualtype_t) -> XCBVisualType {
        unsafe { Self::from_raw_full(ptr) }
    }
}

impl Clone for XCBVisualType {
    #[inline]
    fn clone(&self) -> XCBVisualType {
        unsafe { Self::from_raw_none(self.to_raw_none()) }
    }
}

impl crate::device::Device {
    #[doc(alias = "cairo_xcb_device_get_connection")]
    #[doc(alias = "get_connection")]
    pub fn connection(&self) -> XCBConnection {
        unsafe {
            XCBConnection::from_raw_full(ffi::cairo_xcb_device_get_connection(self.to_raw_none()))
        }
    }

    #[doc(alias = "cairo_xcb_device_debug_cap_xshm_version")]
    pub fn debug_cap_xshm_version(&self, major_version: i32, minor_version: i32) {
        unsafe {
            ffi::cairo_xcb_device_debug_cap_xshm_version(
                self.to_raw_none(),
                major_version,
                minor_version,
            )
        }
    }
}
