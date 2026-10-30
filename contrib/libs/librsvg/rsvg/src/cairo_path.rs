//! Utilities for dealing with Cairo paths.
//!
//! Librsvg uses Cairo to render Bézier paths, and also depends on Cairo to
//! compute the extents of those paths.  This module holds a number of utilities
//! to convert between librsvg paths and Cairo paths.

use std::f64::consts::PI;
use std::rc::Rc;

use crate::drawing_ctx::Viewport;
use crate::error::InternalRenderingError;
use crate::float_eq_cairo::{CAIRO_FIXED_MAX_DOUBLE, CAIRO_FIXED_MIN_DOUBLE};
use crate::layout::{self, Stroke};
use crate::path_builder::{
    ArcParameterization, CubicBezierCurve, EllipticalArc, Path, PathCommand, arc_segment,
};
use crate::properties::StrokeLinecap;
use crate::rect::Rect;
use crate::transform::Transform;

use cairo::PathSegment;

/// A path that has been validated for being suitable for Cairo.
///
/// As of 2024/Sep/25, Cairo converts path coordinates to fixed point, but it has several problems:
///
/// * For coordinates that are outside of the representable range in
///   fixed point, Cairo just clamps them.  It is not able to return
///   this condition as an error to the caller.
///
/// * Then, it has multiple cases of possible arithmetic overflow
///   while processing the paths for rendering.  Fixing this is an
///   ongoing project.
///
/// While Cairo gets better in these respects, librsvg will try to do
/// some mitigations, mainly about catching problematic coordinates
/// early and not passing them on to Cairo.
pub enum ValidatedPath {
    /// Path that has been checked for being suitable for Cairo.
    ///
    /// Note that this also keeps a reference to the original [crate::path_builder::Path],
    /// in addition to the lowered [CairoPath].  This is because the markers code still
    /// needs the former.
    Validated(layout::Path),

    /// Reason why the path was determined to be not suitable for Cairo.  This
    /// is just used for logging purposes.
    Invalid(String),
}

/// Sees if any of the coordinates in the segment is not representable in Cairo's fixed-point numbers.
///
/// See the documentation for [`CairoPath::has_unsuitable_coordinates`].
fn segment_has_unsuitable_coordinates(segment: &PathSegment, transform: &Transform) -> bool {
    match *segment {
        PathSegment::MoveTo((x, y)) => coordinates_are_unsuitable(x, y, transform),
        PathSegment::LineTo((x, y)) => coordinates_are_unsuitable(x, y, transform),
        PathSegment::CurveTo((x1, y1), (x2, y2), (x3, y3)) => {
            coordinates_are_unsuitable(x1, y1, transform)
                || coordinates_are_unsuitable(x2, y2, transform)
                || coordinates_are_unsuitable(x3, y3, transform)
        }
        PathSegment::ClosePath => false,
    }
}

fn coordinates_are_unsuitable(x: f64, y: f64, transform: &Transform) -> bool {
    let fixed_point_range = CAIRO_FIXED_MIN_DOUBLE..=CAIRO_FIXED_MAX_DOUBLE;

    let (x, y) = transform.transform_point(x, y);

    !(fixed_point_range.contains(&x) && fixed_point_range.contains(&y))
}

/// Our own version of a Cairo path, lower-level than [layout::Path].
///
/// Cairo paths can only represent move_to/line_to/curve_to/close_path, unlike
/// librsvg's, which also have elliptical arcs.  Moreover, not all candidate paths
/// can be represented by Cairo, due to limitations on its fixed-point coordinates.
///
/// This struct represents a path that we have done our best to ensure that Cairo
/// can represent.
///
/// This struct is not just a [cairo::Path] since that type is read-only; it cannot
/// be constructed from raw data and must be first obtained from a [cairo::Context].
/// However, we can reuse [cairo::PathSegment] here which is just a path command.
pub struct CairoPath(Vec<PathSegment>);

impl CairoPath {
    pub fn to_cairo_context(&self, cr: &cairo::Context) -> Result<(), Box<InternalRenderingError>> {
        for segment in &self.0 {
            match *segment {
                PathSegment::MoveTo((x, y)) => cr.move_to(x, y),
                PathSegment::LineTo((x, y)) => cr.line_to(x, y),
                PathSegment::CurveTo((x1, y1), (x2, y2), (x3, y3)) => {
                    cr.curve_to(x1, y1, x2, y2, x3, y3)
                }
                PathSegment::ClosePath => cr.close_path(),
            }
        }

        // We check the cr's status right after feeding it a new path for a few reasons:
        //
        // * Any of the individual path commands may cause the cr to enter an error state, for
        //   example, if they come with coordinates outside of Cairo's supported range.
        //
        // * The *next* call to the cr will probably be something that actually checks the status
        //   (i.e. in cairo-rs), and we don't want to panic there.

        cr.status().map_err(|e| e.into())
    }

    /// Converts a `cairo::Path` to a librsvg `CairoPath`.
    pub fn from_cairo(cairo_path: cairo::Path) -> Self {
        // Cairo has the habit of appending a MoveTo to some paths, but we don't want a
        // path for empty text to generate that lone point.  So, strip out paths composed
        // only of MoveTo.

        if cairo_path_is_only_move_tos(&cairo_path) {
            Self(Vec::new())
        } else {
            Self(cairo_path.iter().collect())
        }
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Sees if any of the coordinates in the path is not representable in Cairo's fixed-point numbers.
    ///
    /// See <https://gitlab.gnome.org/GNOME/librsvg/-/issues/1088> and
    /// for the root cause
    /// <https://gitlab.freedesktop.org/cairo/cairo/-/issues/852>.
    ///
    /// This function does a poor job, but a hopefully serviceable one, of seeing if a path's coordinates
    /// are prone to causing trouble when passed to Cairo.  The caller of this function takes note of
    /// that situation and in the end avoids rendering the path altogether.
    ///
    /// Cairo has trouble when given path coordinates that are outside of the range it can represent
    /// in cairo_fixed_t: 24 bits integer part, and 8 bits fractional part.  Coordinates outside
    /// of ±8 million get clamped.  These, or valid coordinates that are close to the limits,
    /// subsequently cause integer overflow while Cairo does arithmetic on the path's points.
    /// Fixing this in Cairo is a long-term project.
    pub fn has_unsuitable_coordinates(&self, transform: &Transform) -> bool {
        self.0
            .iter()
            .any(|segment| segment_has_unsuitable_coordinates(segment, transform))
    }
}

fn compute_path_extents(path: &Path) -> Result<Option<Rect>, Box<InternalRenderingError>> {
    if path.is_empty() {
        return Ok(None);
    }

    let surface = cairo::RecordingSurface::create(cairo::Content::ColorAlpha, None)?;
    let cr = cairo::Context::new(&surface)?;

    path.to_cairo(&cr, false)?;
    let (x0, y0, x1, y1) = cr.path_extents()?;

    Ok(Some(Rect::new(x0, y0, x1, y1)))
}

impl Path {
    pub fn to_cairo_path(
        &self,
        is_square_linecap: bool,
    ) -> Result<CairoPath, Box<InternalRenderingError>> {
        let mut segments = Vec::new();

        for subpath in self.iter_subpath() {
            // If a subpath is empty and the linecap is a square, then draw a square centered on
            // the origin of the subpath. See #165.
            if is_square_linecap {
                let (x, y) = subpath.origin();
                if subpath.is_zero_length() {
                    let stroke_size = 0.002;

                    segments.push(PathSegment::MoveTo((x - stroke_size / 2., y)));
                    segments.push(PathSegment::LineTo((x + stroke_size / 2., y)));
                }
            }

            for cmd in subpath.iter_commands() {
                cmd.to_path_segments(&mut segments);
            }
        }

        Ok(CairoPath(segments))
    }

    pub fn to_cairo(
        &self,
        cr: &cairo::Context,
        is_square_linecap: bool,
    ) -> Result<(), Box<InternalRenderingError>> {
        let cairo_path = self.to_cairo_path(is_square_linecap)?;
        cairo_path.to_cairo_context(cr)
    }
}

fn cairo_path_is_only_move_tos(path: &cairo::Path) -> bool {
    path.iter()
        .all(|seg| matches!(seg, cairo::PathSegment::MoveTo((_, _))))
}

impl PathCommand {
    fn to_path_segments(&self, segments: &mut Vec<PathSegment>) {
        match *self {
            PathCommand::MoveTo(x, y) => segments.push(PathSegment::MoveTo((x, y))),
            PathCommand::LineTo(x, y) => segments.push(PathSegment::LineTo((x, y))),
            PathCommand::CurveTo(ref curve) => curve.to_path_segments(segments),
            PathCommand::Arc(ref arc) => arc.to_path_segments(segments),
            PathCommand::ClosePath => segments.push(PathSegment::ClosePath),
        }
    }
}

impl EllipticalArc {
    fn to_path_segments(&self, segments: &mut Vec<PathSegment>) {
        match self.center_parameterization() {
            ArcParameterization::CenterParameters {
                center,
                radii,
                theta1,
                delta_theta,
            } => {
                let n_segs = (delta_theta / (PI * 0.5 + 0.001)).abs().ceil() as u32;
                let d_theta = delta_theta / f64::from(n_segs);

                let mut theta = theta1;
                for _ in 0..n_segs {
                    arc_segment(center, radii, self.x_axis_rotation, theta, theta + d_theta)
                        .to_path_segments(segments);
                    theta += d_theta;
                }
            }
            ArcParameterization::LineTo => {
                let (x2, y2) = self.to;
                segments.push(PathSegment::LineTo((x2, y2)));
            }
            ArcParameterization::Omit => {}
        }
    }
}

impl CubicBezierCurve {
    fn to_path_segments(&self, segments: &mut Vec<PathSegment>) {
        let Self { pt1, pt2, to } = *self;
        segments.push(PathSegment::CurveTo(
            (pt1.0, pt1.1),
            (pt2.0, pt2.1),
            (to.0, to.1),
        ));
    }
}

pub fn validate_path(
    path: &Rc<Path>,
    stroke: &Stroke,
    viewport: &Viewport,
) -> Result<ValidatedPath, Box<InternalRenderingError>> {
    let is_square_linecap = stroke.line_cap == StrokeLinecap::Square;
    let cairo_path = path.to_cairo_path(is_square_linecap)?;

    if cairo_path.has_unsuitable_coordinates(&viewport.transform) {
        return Ok(ValidatedPath::Invalid(String::from(
            "path has coordinates that are unsuitable for Cairo",
        )));
    }

    let extents = compute_path_extents(path)?;

    Ok(ValidatedPath::Validated(layout::Path {
        cairo_path,
        path: Rc::clone(path),
        extents,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::path_builder::PathBuilder;

    #[test]
    fn rsvg_path_from_cairo_path() {
        let surface = cairo::ImageSurface::create(cairo::Format::ARgb32, 10, 10).unwrap();
        let cr = cairo::Context::new(&surface).unwrap();

        cr.move_to(1.0, 2.0);
        cr.line_to(3.0, 4.0);
        cr.curve_to(5.0, 6.0, 7.0, 8.0, 9.0, 10.0);
        cr.close_path();

        let cr_path = cr.copy_path().unwrap();
        let cairo_path = CairoPath::from_cairo(cr_path);

        assert_eq!(
            cairo_path.0,
            vec![
                PathSegment::MoveTo((1.0, 2.0)),
                PathSegment::LineTo((3.0, 4.0)),
                PathSegment::CurveTo((5.0, 6.0), (7.0, 8.0), (9.0, 10.0)),
                PathSegment::ClosePath,
                PathSegment::MoveTo((1.0, 2.0)), // cairo inserts a MoveTo after ClosePath
            ],
        );
    }

    #[test]
    fn detects_suitable_coordinates() {
        let mut builder = PathBuilder::default();
        builder.move_to(900000.0, 33.0);
        builder.line_to(-900000.0, 3.0);

        let path = builder.into_path();
        let cairo_path = path.to_cairo_path(false).map_err(|_| ()).unwrap();
        assert!(!cairo_path.has_unsuitable_coordinates(&Transform::identity()));
    }

    #[test]
    fn detects_unsuitable_coordinates() {
        let mut builder = PathBuilder::default();
        builder.move_to(9000000.0, 33.0);
        builder.line_to(-9000000.0, 3.0);

        let path = builder.into_path();
        let cairo_path = path.to_cairo_path(false).map_err(|_| ()).unwrap();
        assert!(cairo_path.has_unsuitable_coordinates(&Transform::identity()));
    }
}
