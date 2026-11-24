// Take a look at the license at the top of the repository in the LICENSE file.

use std::ffi::{CStr, CString};
#[cfg(not(feature = "use_glib"))]
use std::ptr;
#[cfg(feature = "freetype")]
use std::rc::Rc;

#[cfg(feature = "use_glib")]
use glib::translate::*;

#[cfg(feature = "freetype")]
use crate::FtSynthesize;
use crate::{Error, FontSlant, FontType, FontWeight, ffi, utils::status_to_result};

#[cfg(feature = "freetype")]
static FT_FACE_KEY: crate::UserDataKey<freetype::face::Face> = crate::UserDataKey::new();

#[cfg(feature = "use_glib")]
glib::wrapper! {
    #[derive(Debug)]
    #[doc(alias = "cairo_font_face_t")]
    pub struct FontFace(Shared<ffi::cairo_font_face_t>);

    match fn {
        ref => |ptr| ffi::cairo_font_face_reference(ptr),
        unref => |ptr| ffi::cairo_font_face_destroy(ptr),
        type_ => || ffi::gobject::cairo_gobject_font_face_get_type(),
    }
}

#[cfg(not(feature = "use_glib"))]
#[cfg_attr(docsrs, doc(cfg(not(feature = "use_glib"))))]
#[derive(Debug)]
#[doc(alias = "cairo_font_face_t")]
pub struct FontFace(ptr::NonNull<ffi::cairo_font_face_t>);

impl FontFace {
    #[doc(alias = "cairo_toy_font_face_create")]
    pub fn toy_create(
        family: &str,
        slant: FontSlant,
        weight: FontWeight,
    ) -> Result<FontFace, Error> {
        let font_face: FontFace = unsafe {
            let family = CString::new(family).unwrap();
            FontFace::from_raw_full(ffi::cairo_toy_font_face_create(
                family.as_ptr(),
                slant.into(),
                weight.into(),
            ))
        };
        let status = unsafe { ffi::cairo_font_face_status(font_face.to_raw_none()) };
        status_to_result(status)?;

        Ok(font_face)
    }

    // rustdoc-stripper-ignore-next
    /// Creates a new font face for the FreeType backend from an already opened FreeType face.
    #[cfg(feature = "freetype")]
    #[cfg_attr(docsrs, doc(cfg(feature = "freetype")))]
    #[doc(alias = "cairo_ft_font_face_create_for_ft_face")]
    pub fn create_from_ft(face: &freetype::face::Face) -> Result<FontFace, Error> {
        // Increase reference count of `FT_Face`.
        let mut face = face.clone();

        // SAFETY: The user data entry keeps `freetype::face::Face` alive
        // until the FontFace is dropped.
        let font_face = unsafe {
            FontFace::from_raw_full(ffi::cairo_ft_font_face_create_for_ft_face(
                face.raw_mut() as freetype::ffi::FT_Face as *mut _,
                0,
            ))
        };
        font_face.set_user_data(&FT_FACE_KEY, Rc::new(face))?;
        let status = unsafe { ffi::cairo_font_face_status(font_face.to_raw_none()) };
        status_to_result(status)?;

        Ok(font_face)
    }

    // rustdoc-stripper-ignore-next
    /// Creates a new font face for the FreeType backend from an already opened FreeType face,
    /// additionally allowing you to pass flags to the underlying C API.
    #[cfg(feature = "freetype")]
    #[cfg_attr(docsrs, doc(cfg(feature = "freetype")))]
    #[doc(alias = "cairo_ft_font_face_create_for_ft_face")]
    pub fn create_from_ft_with_flags(
        face: &freetype::face::Face,
        load_flags: libc::c_int,
    ) -> Result<FontFace, Error> {
        // Increase reference count of `FT_Face`.
        let mut face = face.clone();

        // SAFETY: The user data entry keeps `freetype::face::Face` alive
        // until the FontFace is dropped.
        let font_face = unsafe {
            FontFace::from_raw_full(ffi::cairo_ft_font_face_create_for_ft_face(
                face.raw_mut() as freetype::ffi::FT_Face as *mut _,
                load_flags,
            ))
        };
        font_face.set_user_data(&FT_FACE_KEY, Rc::new(face))?;
        let status = unsafe { ffi::cairo_font_face_status(font_face.to_raw_none()) };
        status_to_result(status)?;

        Ok(font_face)
    }

    #[cfg(feature = "use_glib")]
    #[inline]
    pub unsafe fn from_raw_full(ptr: *mut ffi::cairo_font_face_t) -> FontFace {
        unsafe { from_glib_full(ptr) }
    }

    #[cfg(not(feature = "use_glib"))]
    #[inline]
    pub unsafe fn from_raw_full(ptr: *mut ffi::cairo_font_face_t) -> FontFace {
        debug_assert!(!ptr.is_null());
        FontFace(ptr::NonNull::new_unchecked(ptr))
    }

    #[cfg(feature = "use_glib")]
    #[inline]
    pub unsafe fn from_raw_none(ptr: *mut ffi::cairo_font_face_t) -> FontFace {
        unsafe { from_glib_none(ptr) }
    }

    #[cfg(not(feature = "use_glib"))]
    #[inline]
    pub unsafe fn from_raw_none(ptr: *mut ffi::cairo_font_face_t) -> FontFace {
        debug_assert!(!ptr.is_null());
        FontFace(ptr::NonNull::new_unchecked(ptr))
    }

    #[cfg(feature = "use_glib")]
    #[inline]
    pub fn to_raw_none(&self) -> *mut ffi::cairo_font_face_t {
        self.to_glib_none().0
    }

    #[cfg(not(feature = "use_glib"))]
    #[inline]
    pub fn to_raw_none(&self) -> *mut ffi::cairo_font_face_t {
        self.0.as_ptr()
    }

    #[doc(alias = "cairo_toy_font_face_get_family")]
    pub fn toy_get_family(&self) -> Option<String> {
        unsafe { to_optional_string(ffi::cairo_toy_font_face_get_family(self.to_raw_none())) }
    }

    #[doc(alias = "cairo_toy_font_face_get_slant")]
    pub fn toy_get_slant(&self) -> FontSlant {
        unsafe { FontSlant::from(ffi::cairo_toy_font_face_get_slant(self.to_raw_none())) }
    }

    #[doc(alias = "cairo_toy_font_face_get_weight")]
    pub fn toy_get_weight(&self) -> FontWeight {
        unsafe { FontWeight::from(ffi::cairo_toy_font_face_get_weight(self.to_raw_none())) }
    }

    #[doc(alias = "cairo_font_face_get_type")]
    #[doc(alias = "get_type")]
    pub fn type_(&self) -> FontType {
        unsafe { FontType::from(ffi::cairo_font_face_get_type(self.to_raw_none())) }
    }

    #[doc(alias = "cairo_font_face_get_reference_count")]
    #[doc(alias = "get_reference_count")]
    pub fn reference_count(&self) -> usize {
        unsafe { ffi::cairo_font_face_get_reference_count(self.to_raw_none()) as usize }
    }

    #[cfg(feature = "freetype")]
    #[doc(alias = "cairo_ft_font_face_get_synthesize")]
    #[doc(alias = "get_synthesize")]
    pub fn synthesize(&self) -> FtSynthesize {
        unsafe { FtSynthesize::from(ffi::cairo_ft_font_face_get_synthesize(self.to_raw_none())) }
    }

    #[cfg(feature = "freetype")]
    #[doc(alias = "cairo_ft_font_face_set_synthesize")]
    pub fn set_synthesize(&self, synth_flags: FtSynthesize) {
        unsafe { ffi::cairo_ft_font_face_set_synthesize(self.to_raw_none(), synth_flags.into()) }
    }

    #[cfg(feature = "freetype")]
    #[doc(alias = "cairo_ft_font_face_unset_synthesize")]
    pub fn unset_synthesize(&self, synth_flags: FtSynthesize) {
        unsafe { ffi::cairo_ft_font_face_unset_synthesize(self.to_raw_none(), synth_flags.into()) }
    }

    #[doc(alias = "cairo_font_face_status")]
    pub fn status(&self) -> Result<(), Error> {
        let status = unsafe { ffi::cairo_font_face_status(self.to_raw_none()) };
        status_to_result(status)
    }

    user_data_methods! {
        ffi::cairo_font_face_get_user_data,
        ffi::cairo_font_face_set_user_data,
    }
}

#[cfg(not(feature = "use_glib"))]
impl Drop for FontFace {
    #[inline]
    fn drop(&mut self) {
        unsafe {
            ffi::cairo_font_face_destroy(self.to_raw_none());
        }
    }
}

#[cfg(not(feature = "use_glib"))]
impl Clone for FontFace {
    #[inline]
    fn clone(&self) -> FontFace {
        unsafe { FontFace::from_raw_none(self.to_raw_none()) }
    }
}

pub(crate) unsafe fn to_optional_string(str: *const libc::c_char) -> Option<String> {
    unsafe {
        if str.is_null() {
            None
        } else {
            Some(String::from_utf8_lossy(CStr::from_ptr(str).to_bytes()).into_owned())
        }
    }
}
