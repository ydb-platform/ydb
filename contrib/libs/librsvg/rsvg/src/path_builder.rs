//! Representation of Bézier paths.
//!
//! Path data can consume a significant amount of memory in complex SVG documents.  This
//! module deals with this as follows:
//!
//! * The path parser pushes commands into a [`PathBuilder`].  This is a mutable,
//!   temporary storage for path data.
//!
//! * Then, the [`PathBuilder`] gets turned into a long-term, immutable [`Path`] that has
//!   a more compact representation.
//!
//! The code tries to reduce work in the allocator, by using a [`TinyVec`] with space for at
//! least 32 commands on the stack for `PathBuilder`; most paths in SVGs in the wild have
//! fewer than 32 commands, and larger ones will spill to the heap.
//!
//! See these blog posts for details and profiles:
//!
//! * [Compact representation for path data](https://viruta.org/reducing-memory-consumption-in-librsvg-4.html)
//! * [Reducing slack space and allocator work](https://viruta.org/reducing-memory-consumption-in-librsvg-3.html)

use tinyvec::TinyVec;

use std::f64;
use std::f64::consts::*;
use std::slice;

use crate::float_eq_cairo::ApproxEqCairo;
use crate::path_parser::{ParseError, PathParser};
use crate::util::clamp;

/// Whether an arc's sweep should be >= 180 degrees, or smaller.
#[derive(Debug, Copy, Clone, PartialEq)]
pub struct LargeArc(pub bool);

/// Angular direction in which an arc is drawn.
#[derive(Debug, Copy, Clone, PartialEq)]
pub enum Sweep {
    Negative,
    Positive,
}

/// "c" command for paths; describes a cubic Bézier segment.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct CubicBezierCurve {
    /// The (x, y) coordinates of the first control point.
    pub pt1: (f64, f64),
    /// The (x, y) coordinates of the second control point.
    pub pt2: (f64, f64),
    /// The (x, y) coordinates of the end point of this path segment.
    pub to: (f64, f64),
}

impl CubicBezierCurve {
    /// Consumes 6 coordinates and creates a curve segment.
    fn from_coords(coords: &mut slice::Iter<'_, f64>) -> CubicBezierCurve {
        let pt1 = take_two(coords);
        let pt2 = take_two(coords);
        let to = take_two(coords);

        CubicBezierCurve { pt1, pt2, to }
    }

    /// Pushes 6 coordinates to `coords` and returns `PackedCommand::CurveTo`.
    fn to_packed_and_coords(&self, coords: &mut Vec<f64>) -> PackedCommand {
        coords.push(self.pt1.0);
        coords.push(self.pt1.1);
        coords.push(self.pt2.0);
        coords.push(self.pt2.1);
        coords.push(self.to.0);
        coords.push(self.to.1);
        PackedCommand::CurveTo
    }
}

/// Conversion from endpoint parameterization to center parameterization.
///
/// SVG path data specifies elliptical arcs in terms of their endpoints, but
/// they are easier to process if they are converted to a center parameterization.
///
/// When attempting to compute the center parameterization of the arc,
/// out of range parameters may see an arc omitted or treated as a line.
pub enum ArcParameterization {
    /// Center parameterization of the arc.
    CenterParameters {
        /// Center of the ellipse.
        center: (f64, f64),
        /// Radii of the ellipse (corrected).
        radii: (f64, f64),
        /// Angle of the start point.
        theta1: f64,
        /// Delta angle to the end point.
        delta_theta: f64,
    },
    /// Treat the arc as a line to the end point.
    LineTo,
    /// Omit the arc.
    Omit,
}

/// "a" command for paths; describes  an elliptical arc in terms of its endpoints.
#[derive(Debug, Clone, PartialEq)]
pub struct EllipticalArc {
    /// The (x-axis, y-axis) radii for the ellipse.
    pub r: (f64, f64),
    /// The rotation angle in degrees for the ellipse's x-axis
    /// relative to the x-axis of the user coordinate system.
    pub x_axis_rotation: f64,
    /// Flag indicating whether the arc sweep should be
    /// greater than or equal to 180 degrees, or smaller than 180 degrees.
    pub large_arc: LargeArc,
    /// Flag indicating the angular direction in which the arc is drawn.
    pub sweep: Sweep,
    /// The (x, y) coordinates for the start point of this path segment.
    pub from: (f64, f64),
    /// The (x, y) coordinates for the end point of this path segment.
    pub to: (f64, f64),
}

impl EllipticalArc {
    /// Calculates a center parameterization from the endpoint parameterization.
    ///
    /// Radii may be adjusted if there is no solution.
    ///
    /// See section [B.2.4. Conversion from endpoint to center
    /// parameterization](https://www.w3.org/TR/SVG2/implnote.html#ArcConversionEndpointToCenter)
    pub(crate) fn center_parameterization(&self) -> ArcParameterization {
        let Self {
            r: (mut rx, mut ry),
            x_axis_rotation,
            large_arc,
            sweep,
            from: (x1, y1),
            to: (x2, y2),
        } = *self;

        // Ensure radii are non-zero.
        // Otherwise this arc is treated as a line segment joining the end points.
        //
        // A bit further down we divide by the square of the radii.
        // Check that we won't divide by zero.
        // See http://bugs.debian.org/508443
        if rx * rx < f64::EPSILON || ry * ry < f64::EPSILON {
            return ArcParameterization::LineTo;
        }

        let is_large_arc = large_arc.0;
        let is_positive_sweep = sweep == Sweep::Positive;

        let phi = x_axis_rotation * PI / 180.0;
        let (sin_phi, cos_phi) = phi.sin_cos();

        // Ensure radii are positive.
        rx = rx.abs();
        ry = ry.abs();

        // The equations simplify after a translation which places
        // the origin at the midpoint of the line joining (x1, y1) to (x2, y2),
        // followed by a rotation to line up the coordinate axes
        // with the axes of the ellipse.
        // All transformed coordinates will be written with primes.
        //
        // Compute (x1', y1').
        let mid_x = (x1 - x2) / 2.0;
        let mid_y = (y1 - y2) / 2.0;
        let x1_ = cos_phi * mid_x + sin_phi * mid_y;
        let y1_ = -sin_phi * mid_x + cos_phi * mid_y;

        // Ensure radii are large enough.
        let lambda = (x1_ / rx).powi(2) + (y1_ / ry).powi(2);
        if lambda > 1.0 {
            // If not, scale up the ellipse uniformly
            // until there is exactly one solution.
            rx *= lambda.sqrt();
            ry *= lambda.sqrt();
        }

        // Compute the transformed center (cx', cy').
        let d = (rx * y1_).powi(2) + (ry * x1_).powi(2);
        if d == 0.0 {
            return ArcParameterization::Omit;
        }
        let k = {
            let mut k = ((rx * ry).powi(2) / d - 1.0).abs().sqrt();
            if is_positive_sweep == is_large_arc {
                k = -k;
            }
            k
        };
        let cx_ = k * rx * y1_ / ry;
        let cy_ = -k * ry * x1_ / rx;

        // Compute the center (cx, cy).
        let cx = cos_phi * cx_ - sin_phi * cy_ + (x1 + x2) / 2.0;
        let cy = sin_phi * cx_ + cos_phi * cy_ + (y1 + y2) / 2.0;

        // Compute the start angle θ1.
        let ux = (x1_ - cx_) / rx;
        let uy = (y1_ - cy_) / ry;
        let u_len = (ux * ux + uy * uy).abs().sqrt();
        if u_len == 0.0 {
            return ArcParameterization::Omit;
        }
        let cos_theta1 = clamp(ux / u_len, -1.0, 1.0);
        let theta1 = {
            let mut theta1 = cos_theta1.acos();
            if uy < 0.0 {
                theta1 = -theta1;
            }
            theta1
        };

        // Compute the total delta angle Δθ.
        let vx = (-x1_ - cx_) / rx;
        let vy = (-y1_ - cy_) / ry;
        let v_len = (vx * vx + vy * vy).abs().sqrt();
        if v_len == 0.0 {
            return ArcParameterization::Omit;
        }
        let dp_uv = ux * vx + uy * vy;
        let cos_delta_theta = clamp(dp_uv / (u_len * v_len), -1.0, 1.0);
        let delta_theta = {
            let mut delta_theta = cos_delta_theta.acos();
            if ux * vy - uy * vx < 0.0 {
                delta_theta = -delta_theta;
            }
            if is_positive_sweep && delta_theta < 0.0 {
                delta_theta += PI * 2.0;
            } else if !is_positive_sweep && delta_theta > 0.0 {
                delta_theta -= PI * 2.0;
            }
            delta_theta
        };

        ArcParameterization::CenterParameters {
            center: (cx, cy),
            radii: (rx, ry),
            theta1,
            delta_theta,
        }
    }

    /// Consumes 7 coordinates and creates an arc segment.
    fn from_coords(
        large_arc: LargeArc,
        sweep: Sweep,
        coords: &mut slice::Iter<'_, f64>,
    ) -> EllipticalArc {
        let r = take_two(coords);
        let x_axis_rotation = take_one(coords);
        let from = take_two(coords);
        let to = take_two(coords);

        EllipticalArc {
            r,
            x_axis_rotation,
            large_arc,
            sweep,
            from,
            to,
        }
    }

    /// Pushes 7 coordinates to `coords` and returns one of `PackedCommand::Arc*`.
    fn to_packed_and_coords(&self, coords: &mut Vec<f64>) -> PackedCommand {
        coords.push(self.r.0);
        coords.push(self.r.1);
        coords.push(self.x_axis_rotation);
        coords.push(self.from.0);
        coords.push(self.from.1);
        coords.push(self.to.0);
        coords.push(self.to.1);

        match (self.large_arc, self.sweep) {
            (LargeArc(false), Sweep::Negative) => PackedCommand::ArcSmallNegative,
            (LargeArc(false), Sweep::Positive) => PackedCommand::ArcSmallPositive,
            (LargeArc(true), Sweep::Negative) => PackedCommand::ArcLargeNegative,
            (LargeArc(true), Sweep::Positive) => PackedCommand::ArcLargePositive,
        }
    }
}

/// Turns an arc segment into a cubic bezier curve.
///
/// Takes the center, the radii and the x-axis rotation of the ellipse,
/// the angles of the start and end points,
/// and returns cubic bezier curve parameters.
pub(crate) fn arc_segment(
    c: (f64, f64),
    r: (f64, f64),
    x_axis_rotation: f64,
    th0: f64,
    th1: f64,
) -> CubicBezierCurve {
    let (cx, cy) = c;
    let (rx, ry) = r;
    let phi = x_axis_rotation * PI / 180.0;
    let (sin_phi, cos_phi) = phi.sin_cos();
    let (sin_th0, cos_th0) = th0.sin_cos();
    let (sin_th1, cos_th1) = th1.sin_cos();

    let th_half = 0.5 * (th1 - th0);
    let t = (8.0 / 3.0) * (th_half * 0.5).sin().powi(2) / th_half.sin();
    let x1 = rx * (cos_th0 - t * sin_th0);
    let y1 = ry * (sin_th0 + t * cos_th0);
    let x3 = rx * cos_th1;
    let y3 = ry * sin_th1;
    let x2 = x3 + rx * (t * sin_th1);
    let y2 = y3 + ry * (-t * cos_th1);

    CubicBezierCurve {
        pt1: (
            cx + cos_phi * x1 - sin_phi * y1,
            cy + sin_phi * x1 + cos_phi * y1,
        ),
        pt2: (
            cx + cos_phi * x2 - sin_phi * y2,
            cy + sin_phi * x2 + cos_phi * y2,
        ),
        to: (
            cx + cos_phi * x3 - sin_phi * y3,
            cy + sin_phi * x3 + cos_phi * y3,
        ),
    }
}

/// Long-form version of a single path command.
///
/// This is returned from iterators on paths and subpaths.
#[derive(Clone, Default, Debug, PartialEq)]
pub enum PathCommand {
    MoveTo(f64, f64),
    LineTo(f64, f64),
    CurveTo(CubicBezierCurve),
    Arc(EllipticalArc),

    // The #[default] is just so we can use TinyVec, whose type
    // parameter requires T: Default.  There is no actual default for
    // path commands in the SVG spec; this is just our implementation
    // detail.
    #[default]
    ClosePath,
}

impl PathCommand {
    /// Returns the number of coordinate values that this command will generate in a `Path`.
    fn num_coordinates(&self) -> usize {
        match *self {
            PathCommand::MoveTo(..) => 2,
            PathCommand::LineTo(..) => 2,
            PathCommand::CurveTo(_) => 6,
            PathCommand::Arc(_) => 7,
            PathCommand::ClosePath => 0,
        }
    }

    /// Pushes a command's coordinates to `coords` and returns the corresponding `PackedCommand`.
    fn to_packed(&self, coords: &mut Vec<f64>) -> PackedCommand {
        match *self {
            PathCommand::MoveTo(x, y) => {
                coords.push(x);
                coords.push(y);
                PackedCommand::MoveTo
            }

            PathCommand::LineTo(x, y) => {
                coords.push(x);
                coords.push(y);
                PackedCommand::LineTo
            }

            PathCommand::CurveTo(ref c) => c.to_packed_and_coords(coords),

            PathCommand::Arc(ref a) => a.to_packed_and_coords(coords),

            PathCommand::ClosePath => PackedCommand::ClosePath,
        }
    }

    /// Consumes a packed command's coordinates from the `coords` iterator and returns the rehydrated `PathCommand`.
    fn from_packed(packed: PackedCommand, coords: &mut slice::Iter<'_, f64>) -> PathCommand {
        match packed {
            PackedCommand::MoveTo => {
                let x = take_one(coords);
                let y = take_one(coords);
                PathCommand::MoveTo(x, y)
            }

            PackedCommand::LineTo => {
                let x = take_one(coords);
                let y = take_one(coords);
                PathCommand::LineTo(x, y)
            }

            PackedCommand::CurveTo => PathCommand::CurveTo(CubicBezierCurve::from_coords(coords)),

            PackedCommand::ClosePath => PathCommand::ClosePath,

            PackedCommand::ArcSmallNegative => PathCommand::Arc(EllipticalArc::from_coords(
                LargeArc(false),
                Sweep::Negative,
                coords,
            )),

            PackedCommand::ArcSmallPositive => PathCommand::Arc(EllipticalArc::from_coords(
                LargeArc(false),
                Sweep::Positive,
                coords,
            )),

            PackedCommand::ArcLargeNegative => PathCommand::Arc(EllipticalArc::from_coords(
                LargeArc(true),
                Sweep::Negative,
                coords,
            )),

            PackedCommand::ArcLargePositive => PathCommand::Arc(EllipticalArc::from_coords(
                LargeArc(true),
                Sweep::Positive,
                coords,
            )),
        }
    }
}

/// Constructs a path out of commands.
///
/// Create this with `PathBuilder::default`; you can then add commands to it or call the
/// `parse` method.  When you are finished constructing a path builder, turn it into a
/// `Path` with `into_path`.  You can then iterate on that `Path`'s commands with its
/// methods.
#[derive(Default)]
pub struct PathBuilder {
    path_commands: TinyVec<[PathCommand; 32]>,
}

/// An immutable path with a compact representation.
///
/// This is constructed from a `PathBuilder` once it is finished.  You
/// can get an iterator for the path's commands with the `iter`
/// method, or an iterator for its subpaths (subsequences of commands that
/// start with a MoveTo) with the `iter_subpath` method.
///
/// The variants in `PathCommand` have different sizes, so a simple array of `PathCommand`
/// would have a lot of slack space.  We reduce this to a minimum by separating the
/// commands from their coordinates.  Then, we can have two dense arrays: one with a compact
/// representation of commands, and another with a linear list of the coordinates for each
/// command.
///
/// Both `PathCommand` and `PackedCommand` know how many coordinates they ought to
/// produce, with their `num_coordinates` methods.
///
/// This struct implements `Default`, and it yields an empty path.
#[derive(Default)]
pub struct Path {
    commands: Box<[PackedCommand]>,
    coords: Box<[f64]>,
}

/// Packed version of a `PathCommand`, used in `Path`.
///
/// MoveTo/LineTo/CurveTo have only pairs of coordinates, while ClosePath has no coordinates,
/// and EllipticalArc has a bunch of coordinates plus two flags.  Here we represent the flags
/// as four variants.
///
/// This is `repr(u8)` to keep it as small as possible.
#[repr(u8)]
#[derive(Debug, Clone, Copy)]
enum PackedCommand {
    MoveTo,
    LineTo,
    CurveTo,
    ArcSmallNegative,
    ArcSmallPositive,
    ArcLargeNegative,
    ArcLargePositive,
    ClosePath,
}

impl PackedCommand {
    // Returns the number of coordinate values that this command will generate in a `Path`.
    fn num_coordinates(&self) -> usize {
        match *self {
            PackedCommand::MoveTo => 2,
            PackedCommand::LineTo => 2,
            PackedCommand::CurveTo => 6,
            PackedCommand::ArcSmallNegative
            | PackedCommand::ArcSmallPositive
            | PackedCommand::ArcLargeNegative
            | PackedCommand::ArcLargePositive => 7,
            PackedCommand::ClosePath => 0,
        }
    }
}

impl PathBuilder {
    pub fn parse(&mut self, path_str: &str) -> Result<(), ParseError> {
        let mut parser = PathParser::new(self, path_str);
        parser.parse()
    }

    /// Consumes the `PathBuilder` and returns a compact, immutable representation as a `Path`.
    pub fn into_path(self) -> Path {
        let num_coords = self
            .path_commands
            .iter()
            .map(PathCommand::num_coordinates)
            .sum();

        let mut coords = Vec::with_capacity(num_coords);
        let packed_commands: Vec<_> = self
            .path_commands
            .iter()
            .map(|cmd| cmd.to_packed(&mut coords))
            .collect();

        Path {
            commands: packed_commands.into_boxed_slice(),
            coords: coords.into_boxed_slice(),
        }
    }

    /// Adds a MoveTo command to the path.
    pub fn move_to(&mut self, x: f64, y: f64) {
        self.path_commands.push(PathCommand::MoveTo(x, y));
    }

    /// Adds a LineTo command to the path.
    pub fn line_to(&mut self, x: f64, y: f64) {
        self.path_commands.push(PathCommand::LineTo(x, y));
    }

    /// Adds a CurveTo command to the path.
    pub fn curve_to(&mut self, x2: f64, y2: f64, x3: f64, y3: f64, x4: f64, y4: f64) {
        let curve = CubicBezierCurve {
            pt1: (x2, y2),
            pt2: (x3, y3),
            to: (x4, y4),
        };
        self.path_commands.push(PathCommand::CurveTo(curve));
    }

    /// Adds an EllipticalArc command to the path.
    pub fn arc(
        &mut self,
        x1: f64,
        y1: f64,
        rx: f64,
        ry: f64,
        x_axis_rotation: f64,
        large_arc: LargeArc,
        sweep: Sweep,
        x2: f64,
        y2: f64,
    ) {
        let arc = EllipticalArc {
            r: (rx, ry),
            x_axis_rotation,
            large_arc,
            sweep,
            from: (x1, y1),
            to: (x2, y2),
        };
        self.path_commands.push(PathCommand::Arc(arc));
    }

    /// Adds a ClosePath command to the path.
    pub fn close_path(&mut self) {
        self.path_commands.push(PathCommand::ClosePath);
    }
}

/// An iterator over the subpaths of a `Path`.
pub struct SubPathIter<'a> {
    path: &'a Path,
    commands_start: usize,
    coords_start: usize,
}

/// A slice of commands and coordinates with a single `MoveTo` at the beginning.
pub struct SubPath<'a> {
    commands: &'a [PackedCommand],
    coords: &'a [f64],
}

/// An iterator over the commands/coordinates of a subpath.
pub struct SubPathCommandsIter<'a> {
    commands_iter: slice::Iter<'a, PackedCommand>,
    coords_iter: slice::Iter<'a, f64>,
}

impl<'a> SubPath<'a> {
    /// Returns an iterator over the subpath's commands.
    pub fn iter_commands(&self) -> SubPathCommandsIter<'_> {
        SubPathCommandsIter {
            commands_iter: self.commands.iter(),
            coords_iter: self.coords.iter(),
        }
    }

    /// Each subpath starts with a MoveTo; this returns its `(x, y)` coordinates.
    pub fn origin(&self) -> (f64, f64) {
        let first = *self.commands.first().unwrap();
        assert!(matches!(first, PackedCommand::MoveTo));
        let command = PathCommand::from_packed(first, &mut self.coords.iter());

        match command {
            PathCommand::MoveTo(x, y) => (x, y),
            _ => unreachable!(),
        }
    }

    /// Returns whether the length of a subpath is approximately zero.
    pub fn is_zero_length(&self) -> bool {
        let (cur_x, cur_y) = self.origin();

        for cmd in self.iter_commands().skip(1) {
            let (end_x, end_y) = match cmd {
                PathCommand::MoveTo(_, _) => unreachable!(
                    "A MoveTo cannot appear in a subpath if it's not the first element"
                ),
                PathCommand::LineTo(x, y) => (x, y),
                PathCommand::CurveTo(curve) => curve.to,
                PathCommand::Arc(arc) => arc.to,
                // If we get a `ClosePath and haven't returned yet then we haven't moved at all making
                // it an empty subpath`
                PathCommand::ClosePath => return true,
            };

            if !end_x.approx_eq_cairo(cur_x) || !end_y.approx_eq_cairo(cur_y) {
                return false;
            }
        }

        true
    }
}

impl<'a> Iterator for SubPathIter<'a> {
    type Item = SubPath<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        // If we ended on our last command in the previous iteration, we're done here
        if self.commands_start >= self.path.commands.len() {
            return None;
        }

        // Otherwise we have at least one command left, we setup the slice to be all the remaining
        // commands.
        let commands = &self.path.commands[self.commands_start..];

        assert!(matches!(commands.first().unwrap(), PackedCommand::MoveTo));
        let mut num_coords = PackedCommand::MoveTo.num_coordinates();

        // Skip over the initial MoveTo
        for (i, cmd) in commands.iter().enumerate().skip(1) {
            // If we encounter a MoveTo , we ended our current subpath, we
            // return the commands until this command and set commands_start to be the index of the
            // next command
            if let PackedCommand::MoveTo = cmd {
                let subpath_coords_start = self.coords_start;

                self.commands_start += i;
                self.coords_start += num_coords;

                return Some(SubPath {
                    commands: &commands[..i],
                    coords: &self.path.coords
                        [subpath_coords_start..subpath_coords_start + num_coords],
                });
            } else {
                num_coords += cmd.num_coordinates();
            }
        }

        // If we didn't find any MoveTo, we're done here. We return the rest of the path
        // and set commands_start so next iteration will return None.

        self.commands_start = self.path.commands.len();

        let subpath_coords_start = self.coords_start;
        assert!(subpath_coords_start + num_coords == self.path.coords.len());
        self.coords_start = self.path.coords.len();

        Some(SubPath {
            commands,
            coords: &self.path.coords[subpath_coords_start..],
        })
    }
}

impl<'a> Iterator for SubPathCommandsIter<'a> {
    type Item = PathCommand;

    fn next(&mut self) -> Option<Self::Item> {
        self.commands_iter
            .next()
            .map(|packed| PathCommand::from_packed(*packed, &mut self.coords_iter))
    }
}

impl Path {
    /// Get an iterator over a path `Subpath`s.
    pub fn iter_subpath(&self) -> SubPathIter<'_> {
        SubPathIter {
            path: self,
            commands_start: 0,
            coords_start: 0,
        }
    }

    /// Get an iterator over a path's commands.
    pub fn iter(&self) -> impl Iterator<Item = PathCommand> + '_ {
        let commands = self.commands.iter();
        let mut coords = self.coords.iter();

        commands.map(move |cmd| PathCommand::from_packed(*cmd, &mut coords))
    }

    /// Returns whether there are no commands in the path.
    pub fn is_empty(&self) -> bool {
        self.commands.is_empty()
    }
}

fn take_one(iter: &mut slice::Iter<'_, f64>) -> f64 {
    *iter.next().unwrap()
}

fn take_two(iter: &mut slice::Iter<'_, f64>) -> (f64, f64) {
    (take_one(iter), take_one(iter))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_builder() {
        let builder = PathBuilder::default();
        let path = builder.into_path();
        assert!(path.is_empty());
        assert_eq!(path.iter().count(), 0);
    }

    #[test]
    fn empty_path() {
        let path = Path::default();
        assert!(path.is_empty());
        assert_eq!(path.iter().count(), 0);
    }

    #[test]
    fn all_commands() {
        let mut builder = PathBuilder::default();
        builder.move_to(42.0, 43.0);
        builder.line_to(42.0, 43.0);
        builder.curve_to(42.0, 43.0, 44.0, 45.0, 46.0, 47.0);
        builder.arc(
            42.0,
            43.0,
            44.0,
            45.0,
            46.0,
            LargeArc(true),
            Sweep::Positive,
            47.0,
            48.0,
        );
        builder.close_path();
        let path = builder.into_path();
        assert!(path.iter().eq(vec![
            PathCommand::MoveTo(42.0, 43.0),
            PathCommand::LineTo(42.0, 43.0),
            PathCommand::CurveTo(CubicBezierCurve {
                pt1: (42.0, 43.0),
                pt2: (44.0, 45.0),
                to: (46.0, 47.0),
            }),
            PathCommand::Arc(EllipticalArc {
                from: (42.0, 43.0),
                r: (44.0, 45.0),
                to: (47.0, 48.0),
                x_axis_rotation: 46.0,
                large_arc: LargeArc(true),
                sweep: Sweep::Positive,
            }),
            PathCommand::ClosePath,
        ]));
    }

    #[test]
    fn subpath_iter() {
        let mut builder = PathBuilder::default();
        builder.move_to(42.0, 43.0);
        builder.line_to(42.0, 43.0);
        builder.close_path();

        builder.move_to(22.0, 22.0);
        builder.curve_to(22.0, 22.0, 44.0, 45.0, 46.0, 47.0);

        builder.move_to(69.0, 69.0);
        builder.line_to(42.0, 43.0);
        let path = builder.into_path();

        let subpaths = path
            .iter_subpath()
            .map(|subpath| {
                (
                    subpath.origin(),
                    subpath.iter_commands().collect::<Vec<PathCommand>>(),
                )
            })
            .collect::<Vec<((f64, f64), Vec<PathCommand>)>>();

        assert_eq!(
            subpaths,
            vec![
                (
                    (42.0, 43.0),
                    vec![
                        PathCommand::MoveTo(42.0, 43.0),
                        PathCommand::LineTo(42.0, 43.0),
                        PathCommand::ClosePath
                    ]
                ),
                (
                    (22.0, 22.0),
                    vec![
                        PathCommand::MoveTo(22.0, 22.0),
                        PathCommand::CurveTo(CubicBezierCurve {
                            pt1: (22.0, 22.0),
                            pt2: (44.0, 45.0),
                            to: (46.0, 47.0)
                        })
                    ]
                ),
                (
                    (69.0, 69.0),
                    vec![
                        PathCommand::MoveTo(69.0, 69.0),
                        PathCommand::LineTo(42.0, 43.0)
                    ]
                )
            ]
        );
    }

    #[test]
    fn zero_length_subpaths() {
        let mut builder = PathBuilder::default();
        builder.move_to(42.0, 43.0);
        builder.move_to(44.0, 45.0);
        builder.close_path();
        builder.move_to(46.0, 47.0);
        builder.line_to(48.0, 49.0);

        let path = builder.into_path();

        let subpaths = path
            .iter_subpath()
            .map(|subpath| (subpath.is_zero_length(), subpath.origin()))
            .collect::<Vec<(bool, (f64, f64))>>();

        assert_eq!(
            subpaths,
            vec![
                (true, (42.0, 43.0)),
                (true, (44.0, 45.0)),
                (false, (46.0, 47.0)),
            ]
        );
    }
}
