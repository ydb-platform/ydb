// Take a look at the license at the top of the repository in the LICENSE file.

use std::sync::OnceLock;

use super::{FontExtents, FontFace, ScaledFont, TextCluster, TextClusterFlags, TextExtents};
use crate::{Context, Error, Glyph, ffi, utils::status_to_result};

type BoxInitFunc =
    Box<dyn Fn(&ScaledFont, &Context, &mut FontExtents) -> Result<(), Error> + Send + Sync>;
type BoxRenderGlyphFunc = Box<
    dyn Fn(&ScaledFont, libc::c_ulong, &Context, &mut TextExtents) -> Result<(), Error>
        + Send
        + Sync,
>;
type BoxUnicodeToGlyphFunc =
    Box<dyn Fn(&ScaledFont, libc::c_ulong) -> Result<libc::c_ulong, Error> + Send + Sync>;
type BoxTextToGlyphsFunc = Box<
    dyn Fn(&ScaledFont, &str) -> Result<(Vec<Glyph>, Vec<TextCluster>, TextClusterFlags), Error>
        + Send
        + Sync,
>;

pub struct UserFontFace(FontFace);

impl UserFontFace {
    #[doc(alias = "cairo_user_font_face_create")]
    pub fn create() -> Result<Self, Error> {
        let font_face = unsafe { FontFace::from_raw_full(ffi::cairo_user_font_face_create()) };
        let status = unsafe { ffi::cairo_font_face_status(font_face.to_raw_none()) };
        status_to_result(status)?;
        Ok(Self(font_face))
    }

    #[doc(alias = "cairo_user_font_face_set_init_func")]
    pub fn set_init_func<F>(&self, func: F)
    where
        F: Fn(&ScaledFont, &Context, &mut FontExtents) -> Result<(), Error> + Send + Sync + 'static,
    {
        static INIT_FUNC: OnceLock<BoxInitFunc> = OnceLock::new();
        if INIT_FUNC.set(Box::new(func)).is_err() {
            panic!("Init func can only be set once")
        }
        unsafe extern "C" fn init_trampoline(
            scaled_font: *mut ffi::cairo_scaled_font_t,
            cr: *mut ffi::cairo_t,
            extents: *mut ffi::cairo_font_extents_t,
        ) -> ffi::cairo_status_t {
            unsafe {
                let font_extents = &mut *(extents as *mut FontExtents);
                let init_func = INIT_FUNC.get().unwrap();
                match init_func(
                    &ScaledFont::from_raw_none(scaled_font),
                    &Context::from_raw_none(cr),
                    font_extents,
                ) {
                    Err(err) => err.into(),
                    _ => ffi::STATUS_SUCCESS,
                }
            }
        }
        unsafe {
            ffi::cairo_user_font_face_set_init_func(self.to_raw_none(), Some(init_trampoline));
        }
    }

    #[doc(alias = "cairo_user_font_face_set_render_glyph_func")]
    pub fn set_render_glyph_func<F>(&self, func: F)
    where
        F: Fn(&ScaledFont, libc::c_ulong, &Context, &mut TextExtents) -> Result<(), Error>
            + Send
            + Sync
            + 'static,
    {
        static RENDER_GLYPH_FUNC: OnceLock<BoxRenderGlyphFunc> = OnceLock::new();
        if RENDER_GLYPH_FUNC.set(Box::new(func)).is_err() {
            panic!("RenderGlyph func can only be set once")
        }
        unsafe extern "C" fn render_glyph_trampoline(
            scaled_font: *mut ffi::cairo_scaled_font_t,
            glyph: libc::c_ulong,
            cr: *mut ffi::cairo_t,
            extents: *mut ffi::cairo_text_extents_t,
        ) -> ffi::cairo_status_t {
            unsafe {
                let text_extents = &mut *(extents as *mut TextExtents);
                let render_glyph_func = RENDER_GLYPH_FUNC.get().unwrap();
                match render_glyph_func(
                    &ScaledFont::from_raw_none(scaled_font),
                    glyph,
                    &Context::from_raw_none(cr),
                    text_extents,
                ) {
                    Err(err) => err.into(),
                    _ => ffi::STATUS_SUCCESS,
                }
            }
        }
        unsafe {
            ffi::cairo_user_font_face_set_render_glyph_func(
                self.to_raw_none(),
                Some(render_glyph_trampoline),
            );
        }
    }

    #[doc(alias = "cairo_user_font_face_set_render_color_glyph_func")]
    pub fn set_render_color_glyph_func<F>(&self, func: F)
    where
        F: Fn(&ScaledFont, libc::c_ulong, &Context, &mut TextExtents) -> Result<(), Error>
            + Send
            + Sync
            + 'static,
    {
        static RENDER_COLOR_GLYPH_FUNC: OnceLock<BoxRenderGlyphFunc> = OnceLock::new();
        if RENDER_COLOR_GLYPH_FUNC.set(Box::new(func)).is_err() {
            panic!("RenderColorGlyph func can only be set once")
        }
        unsafe extern "C" fn render_glyph_trampoline(
            scaled_font: *mut ffi::cairo_scaled_font_t,
            glyph: libc::c_ulong,
            cr: *mut ffi::cairo_t,
            extents: *mut ffi::cairo_text_extents_t,
        ) -> ffi::cairo_status_t {
            unsafe {
                let text_extents = &mut *(extents as *mut TextExtents);
                let render_glyph_func = RENDER_COLOR_GLYPH_FUNC.get().unwrap();
                match render_glyph_func(
                    &ScaledFont::from_raw_none(scaled_font),
                    glyph,
                    &Context::from_raw_none(cr),
                    text_extents,
                ) {
                    Err(err) => err.into(),
                    _ => ffi::STATUS_SUCCESS,
                }
            }
        }
        unsafe {
            ffi::cairo_user_font_face_set_render_glyph_func(
                self.to_raw_none(),
                Some(render_glyph_trampoline),
            );
        }
    }

    #[doc(alias = "cairo_user_font_face_set_unicode_to_glyph_func")]
    pub fn set_unicode_to_glyph_func<F>(&self, func: F)
    where
        F: Fn(&ScaledFont, libc::c_ulong) -> Result<libc::c_ulong, Error> + Send + Sync + 'static,
    {
        static UNICODE_TO_GLYPH_FUNC: OnceLock<BoxUnicodeToGlyphFunc> = OnceLock::new();
        if UNICODE_TO_GLYPH_FUNC.set(Box::new(func)).is_err() {
            panic!("UnicodeToGlyph func can only be set once")
        }
        unsafe extern "C" fn unicode_to_glyph_trampoline(
            scaled_font: *mut ffi::cairo_scaled_font_t,
            unicode: libc::c_ulong,
            glyph_index: *mut libc::c_ulong,
        ) -> ffi::cairo_status_t {
            unsafe {
                let unicode_to_glyph_func = UNICODE_TO_GLYPH_FUNC.get().unwrap();
                match unicode_to_glyph_func(&ScaledFont::from_raw_none(scaled_font), unicode) {
                    Err(err) => err.into(),
                    Ok(glyph) => {
                        *glyph_index = glyph;
                        ffi::STATUS_SUCCESS
                    }
                }
            }
        }
        unsafe {
            ffi::cairo_user_font_face_set_unicode_to_glyph_func(
                self.to_raw_none(),
                Some(unicode_to_glyph_trampoline),
            );
        }
    }

    #[doc(alias = "cairo_user_font_face_set_text_to_glyphs_func")]
    pub fn set_text_to_glyphs_func<F>(&self, func: F)
    where
        F: Fn(&ScaledFont, &str) -> Result<(Vec<Glyph>, Vec<TextCluster>, TextClusterFlags), Error>
            + Send
            + Sync
            + 'static,
    {
        static TEXT_TO_GLYPHS_FUNC: OnceLock<BoxTextToGlyphsFunc> = OnceLock::new();
        if TEXT_TO_GLYPHS_FUNC.set(Box::new(func)).is_err() {
            panic!("TextToGlyphs func can only be set once")
        }
        unsafe extern "C" fn text_to_glyphs_trampoline(
            scaled_font: *mut ffi::cairo_scaled_font_t,
            utf8: *const libc::c_char,
            utf8_len: libc::c_int,
            glyphs: *mut *mut ffi::cairo_glyph_t,
            num_glyphs: *mut libc::c_int,
            clusters: *mut *mut ffi::cairo_text_cluster_t,
            num_clusters: *mut libc::c_int,
            cluster_flags: *mut ffi::cairo_text_cluster_flags_t,
        ) -> ffi::cairo_status_t {
            unsafe {
                let text_to_glyphs_func = TEXT_TO_GLYPHS_FUNC.get().unwrap();
                let text = if utf8_len > 0 {
                    let bytes = std::slice::from_raw_parts(utf8 as *const u8, utf8_len as usize);
                    std::str::from_utf8_unchecked(bytes)
                } else {
                    std::ffi::CStr::from_ptr(utf8).to_str().unwrap()
                };
                match text_to_glyphs_func(&ScaledFont::from_raw_none(scaled_font), text) {
                    Err(err) => err.into(),
                    Ok((glyphs_, clusters_, flags)) => {
                        *num_glyphs = glyphs_.len() as _;
                        let c_glyphs = ffi::cairo_glyph_allocate(*num_glyphs);
                        std::ptr::copy_nonoverlapping(
                            glyphs_.as_ptr(),
                            c_glyphs as *mut _,
                            glyphs_.len(),
                        );
                        *glyphs = c_glyphs;

                        *num_clusters = clusters_.len() as _;
                        let c_clusters = ffi::cairo_text_cluster_allocate(*num_clusters);
                        std::ptr::copy_nonoverlapping(
                            clusters_.as_ptr(),
                            c_clusters as *mut _,
                            clusters_.len(),
                        );
                        *clusters = c_clusters;

                        *cluster_flags = flags.into();

                        ffi::STATUS_SUCCESS
                    }
                }
            }
        }
        unsafe {
            ffi::cairo_user_font_face_set_text_to_glyphs_func(
                self.to_raw_none(),
                Some(text_to_glyphs_trampoline),
            );
        }
    }
}

impl std::ops::Deref for UserFontFace {
    type Target = FontFace;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
