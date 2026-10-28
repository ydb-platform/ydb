pub mod compare_surfaces;
pub mod reference_utils;

use cairo;
use gio;
use glib;
use std::env;
use std::sync::Once;

use crate::{
    CairoRenderer, Loader, LoadingError, RenderingError, SvgHandle,
    surface_utils::shared_surface::{SharedImageSurface, SurfaceType},
};

pub fn load_svg(input: &'static [u8]) -> Result<SvgHandle, LoadingError> {
    let bytes = glib::Bytes::from_static(input);
    let stream = gio::MemoryInputStream::from_bytes(&bytes);

    Loader::new().read_stream(&stream, None::<&gio::File>, None::<&gio::Cancellable>)
}

#[derive(Copy, Clone)]
pub struct SurfaceSize(pub i32, pub i32);

pub fn render_document<F: FnOnce(&cairo::Context)>(
    svg: &SvgHandle,
    surface_size: SurfaceSize,
    cr_transform: F,
    viewport: cairo::Rectangle,
) -> Result<SharedImageSurface, RenderingError> {
    let renderer = CairoRenderer::new(svg);

    let SurfaceSize(width, height) = surface_size;

    let output = cairo::ImageSurface::create(cairo::Format::ARgb32, width, height).unwrap();

    let res = {
        let cr = cairo::Context::new(&output).expect("Failed to create a cairo context");
        cr_transform(&cr);
        Ok(renderer.render_document(&cr, &viewport)?)
    };

    res.and_then(|_| Ok(SharedImageSurface::wrap(output, SurfaceType::SRgb)?))
}

#[cfg(all(
    all(not(target_os = "macos"), not(target_os = "windows")),
    system_deps_have_fontconfig,
    system_deps_have_pangoft2
))]
mod pango_ft2 {
    use super::*;
    use glib::prelude::*;
    use glib::translate::*;
    use libc;
    use pangocairo::FontMap;
    use std::ffi::CString;
    use std::path::PathBuf;

    unsafe extern "C" {
        // pango_fc_font_map_set_config (PangoFcFontMap *fcfontmap,
        //                               FcConfig       *fcconfig);
        // This is not bound in gtk-rs, and PangoFcFontMap is not even exposed, so we'll bind it by hand.
        fn pango_fc_font_map_set_config(
            font_map: *mut libc::c_void,
            config: *mut fontconfig_sys::FcConfig,
        );
    }

    pub unsafe fn load_test_fonts() {
        let tests_resources_path: PathBuf = [
            env::var("CARGO_MANIFEST_DIR")
                .expect("Manifest directory unknown")
                .as_str(),
            "tests",
            "resources",
        ]
        .iter()
        .collect();

        unsafe {
            let config = fontconfig_sys::FcConfigCreate();
            if fontconfig_sys::FcConfigSetCurrent(config) == 0 {
                panic!("Could not set a fontconfig configuration");
            }

            let fonts_dot_conf_path = tests_resources_path.clone().join("fonts.conf");
            let fonts_dot_conf_cstring =
                CString::new(fonts_dot_conf_path.to_str().unwrap()).unwrap();
            if fontconfig_sys::FcConfigParseAndLoad(
                config,
                fonts_dot_conf_cstring.as_ptr().cast(),
                1,
            ) == 0
            {
                panic!("Could not parse fontconfig configuration from tests/resources/fonts.conf");
            }

            let tests_resources_cstring =
                CString::new(tests_resources_path.to_str().unwrap()).unwrap();
            if fontconfig_sys::FcConfigAppFontAddDir(
                config,
                tests_resources_cstring.as_ptr().cast(),
            ) == 0
            {
                panic!("Could not load fonts from directory tests/resources");
            }

            let font_map = FontMap::for_font_type(cairo::FontType::FontTypeFt).unwrap();
            let raw_font_map: *mut pango::ffi::PangoFontMap = font_map.to_glib_none().0;

            pango_fc_font_map_set_config(raw_font_map as *mut _, config);
            fontconfig_sys::FcConfigDestroy(config);

            FontMap::set_default(Some(&font_map.downcast::<pangocairo::FontMap>().unwrap()));
        }
    }
}

#[cfg(all(
    all(not(target_os = "macos"), not(target_os = "windows")),
    system_deps_have_fontconfig,
    system_deps_have_pangoft2
))]
pub fn setup_font_map() {
    unsafe {
        self::pango_ft2::load_test_fonts();
    }
}

#[cfg(any(
    any(target_os = "macos", target_os = "windows"),
    not(system_deps_have_fontconfig),
    not(system_deps_have_pangoft2)
))]
pub fn setup_font_map() {}

pub fn setup_language() {
    static ONCE: Once = Once::new();

    ONCE.call_once(|| {
        // For systemLanguage attribute tests.
        // The trailing ":" is intentional to test gitlab#425.
        unsafe {
            env::set_var("LANGUAGE", "de:en_US:en:");
            env::set_var("LC_ALL", "de:en_US:en:");
        }
    });
}
