//! Compute an SVG document's size with the legacy logic.
//!
//! See the documentation for [`LegacySize`].  The legacy C API functions like
//! `rsvg_handle_render_cairo()` do not take a viewport argument: they do not know how big
//! the caller would like to render the document; instead they compute a "natural size"
//! for the document based on its `width`/`height`/`viewBox` and some heuristics for when
//! they are missing.
//!
//! The new style C functions like `rsvg_handle_render_document()` actually take a
//! viewport, which indicates how big the result should be.  This matches the expectations
//! of the web platform, which assumes that all embedded content goes into a viewport.

use float_cmp::approx_eq;

use rsvg::c_api_only::Dpi;
use rsvg::{CairoRenderer, IntrinsicDimensions, RenderingError};

use super::handle::CairoRectangleExt;

/// Extension methods to compute the SVG's size suitable for the legacy C API.
///
/// The legacy C API can compute an SVG document's size from the
/// `width`, `height`, and `viewBox` attributes of the toplevel `<svg>`
/// element.  If these are not available, then the size must be computed
/// by actually measuring the geometries of elements in the document.
///
/// See <https://www.w3.org/TR/css-images-3/#sizing-terms> for terminology and logic.
pub trait LegacySize {
    fn legacy_document_size(&self) -> Result<(f64, f64), RenderingError> {
        let (ink_r, _) = self.legacy_layer_geometry(None)?;
        Ok((ink_r.width(), ink_r.height()))
    }

    fn legacy_layer_geometry(
        &self,
        id: Option<&str>,
    ) -> Result<(cairo::Rectangle, cairo::Rectangle), RenderingError>;
}

impl<'a> LegacySize for CairoRenderer<'a> {
    fn legacy_layer_geometry(
        &self,
        id: Option<&str>,
    ) -> Result<(cairo::Rectangle, cairo::Rectangle), RenderingError> {
        match id {
            Some(id) => Ok(self.geometry_for_layer(Some(id), &unit_rectangle())?),

            None => {
                let size_from_intrinsic_dimensions =
                    self.intrinsic_size_in_pixels().or_else(|| {
                        size_in_pixels_from_percentage_width_and_height(
                            self,
                            &self.intrinsic_dimensions(),
                            self.dpi(),
                        )
                    });

                if let Some((w, h)) = size_from_intrinsic_dimensions {
                    // We have a size directly computed from the <svg> attributes
                    let rect = cairo::Rectangle::from_size(w, h);
                    Ok((rect, rect))
                } else {
                    self.geometry_for_layer(None, &unit_rectangle())
                }
            }
        }
    }
}

fn unit_rectangle() -> cairo::Rectangle {
    cairo::Rectangle::from_size(1.0, 1.0)
}

/// If the width and height are in percentage units, computes a size equal to the
/// `viewBox`'s aspect ratio if it exists, or else returns None.
///
/// For example, a `viewBox="0 0 100 200"` will yield `Some(100.0, 200.0)`.
///
/// Note that this only checks that the width and height are in percentage units, but
/// it actually ignores their values.  This is because at the point this function is
/// called, there is no viewport to embed the SVG document in, so those percentage
/// units cannot be resolved against anything in particular.  The idea is to return
/// some dimensions with the correct aspect ratio.
fn size_in_pixels_from_percentage_width_and_height(
    renderer: &CairoRenderer,
    dim: &IntrinsicDimensions,
    dpi: Dpi,
) -> Option<(f64, f64)> {
    let IntrinsicDimensions {
        width,
        height,
        vbox,
    } = *dim;

    use rsvg::LengthUnit::*;

    // Unwrap or return None if we don't know the aspect ratio -> Let the caller handle it.
    let vbox = vbox?;

    let (w, h) = renderer.width_height_to_user(dpi);

    // Avoid division by zero below.  If the viewBox is zero-sized, there's
    // not much we can do.
    if approx_eq!(f64, vbox.width(), 0.0) || approx_eq!(f64, vbox.height(), 0.0) {
        return Some((0.0, 0.0));
    }

    match (width.unit, height.unit) {
        (Percent, Percent) => Some((vbox.width(), vbox.height())),
        (_, Percent) => Some((w, w * vbox.height() / vbox.width())),
        (Percent, _) => Some((h * vbox.width() / vbox.height(), h)),
        (_, _) => unreachable!("should have been called with percentage units"),
    }
}
