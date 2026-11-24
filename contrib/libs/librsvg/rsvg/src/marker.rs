//! The `marker` element, and geometry computations for markers.

use std::f64::consts::*;
use std::ops::Deref;

use cssparser::Parser;
use markup5ever::{expanded_name, local_name, ns};

use crate::angle::Angle;
use crate::aspect_ratio::*;
use crate::borrow_element_as;
use crate::document::AcquiredNodes;
use crate::drawing_ctx::{DrawingCtx, Viewport};
use crate::element::{DrawResult, ElementTrait, set_attribute};
use crate::error::*;
use crate::float_eq_cairo::ApproxEqCairo;
use crate::layout::{self, Shape, StackingContext};
use crate::length::*;
use crate::node::{CascadedValues, Node, NodeBorrow, NodeDraw};
use crate::parse_identifiers;
use crate::parsers::{Parse, ParseValue};
use crate::path_builder::{ArcParameterization, CubicBezierCurve, Path, PathCommand, arc_segment};
use crate::rect::Rect;
use crate::rsvg_log;
use crate::session::Session;
use crate::transform::Transform;
use crate::viewbox::*;
use crate::xml::Attributes;

// markerUnits attribute: https://www.w3.org/TR/SVG/painting.html#MarkerElement
#[derive(Debug, Default, Copy, Clone, PartialEq)]
enum MarkerUnits {
    UserSpaceOnUse,
    #[default]
    StrokeWidth,
}

impl Parse for MarkerUnits {
    fn parse<'i>(parser: &mut Parser<'i, '_>) -> Result<MarkerUnits, ParseError<'i>> {
        Ok(parse_identifiers!(
            parser,
            "userSpaceOnUse" => MarkerUnits::UserSpaceOnUse,
            "strokeWidth" => MarkerUnits::StrokeWidth,
        )?)
    }
}

// orient attribute: https://www.w3.org/TR/SVG/painting.html#MarkerElement
#[derive(Debug, Copy, Clone, PartialEq)]
enum MarkerOrient {
    Auto,
    AutoStartReverse,
    Angle(Angle),
}

impl Default for MarkerOrient {
    #[inline]
    fn default() -> MarkerOrient {
        MarkerOrient::Angle(Angle::new(0.0))
    }
}

impl Parse for MarkerOrient {
    fn parse<'i>(parser: &mut Parser<'i, '_>) -> Result<MarkerOrient, ParseError<'i>> {
        if parser
            .try_parse(|p| p.expect_ident_matching("auto"))
            .is_ok()
        {
            return Ok(MarkerOrient::Auto);
        }

        if parser
            .try_parse(|p| p.expect_ident_matching("auto-start-reverse"))
            .is_ok()
        {
            Ok(MarkerOrient::AutoStartReverse)
        } else {
            Angle::parse(parser).map(MarkerOrient::Angle)
        }
    }
}

pub struct Marker {
    units: MarkerUnits,
    ref_x: Length<Horizontal>,
    ref_y: Length<Vertical>,
    width: ULength<Horizontal>,
    height: ULength<Vertical>,
    orient: MarkerOrient,
    aspect: AspectRatio,
    vbox: Option<ViewBox>,
}

// From SVG's marker-start, marker-mid, marker-end properties
#[derive(Debug, Copy, Clone, PartialEq)]
enum MarkerType {
    Start,
    Middle,
    End,
}

/// Computed location and orientation for a marker.
#[derive(Debug, PartialEq)]
struct MarkerSpec {
    marker_type: MarkerType,
    x: f64,
    y: f64,
    angle: Angle,
}

impl Default for Marker {
    fn default() -> Marker {
        Marker {
            units: MarkerUnits::default(),
            ref_x: Default::default(),
            ref_y: Default::default(),
            // the following two are per the spec
            width: ULength::<Horizontal>::parse_str("3").unwrap(),
            height: ULength::<Vertical>::parse_str("3").unwrap(),
            orient: MarkerOrient::default(),
            aspect: AspectRatio::default(),
            vbox: None,
        }
    }
}

impl Marker {
    fn render(
        &self,
        node: &Node,
        acquired_nodes: &mut AcquiredNodes<'_>,
        viewport: &Viewport,
        draw_ctx: &mut DrawingCtx,
        spec: &MarkerSpec,
        line_width: f64,
        clipping: bool,
        marker: &layout::Marker,
    ) -> DrawResult {
        let mut cascaded = CascadedValues::new_from_node(node);
        cascaded.context_fill = Some(marker.context_fill.clone());
        cascaded.context_stroke = Some(marker.context_stroke.clone());

        let values = cascaded.get();

        let params = NormalizeParams::new(values, viewport);

        let marker_width = self.width.to_user(&params);
        let marker_height = self.height.to_user(&params);

        if marker_width.approx_eq_cairo(0.0) || marker_height.approx_eq_cairo(0.0) {
            // markerWidth or markerHeight set to 0 disables rendering of the element
            // https://www.w3.org/TR/SVG/painting.html#MarkerWidthAttribute
            return Ok(viewport.empty_bbox());
        }

        let rotation = match self.orient {
            MarkerOrient::Auto => spec.angle,
            MarkerOrient::AutoStartReverse => {
                if spec.marker_type == MarkerType::Start {
                    spec.angle.flip()
                } else {
                    spec.angle
                }
            }
            MarkerOrient::Angle(a) => a,
        };

        let mut transform = Transform::new_translate(spec.x, spec.y).pre_rotate(rotation);

        if self.units == MarkerUnits::StrokeWidth {
            transform = transform.pre_scale(line_width, line_width);
        }

        let content_viewport = if let Some(vbox) = self.vbox {
            if vbox.is_empty() {
                return Ok(viewport.empty_bbox());
            }

            let r = self
                .aspect
                .compute(&vbox, &Rect::from_size(marker_width, marker_height));

            let (vb_width, vb_height) = vbox.size();
            transform = transform.pre_scale(r.width() / vb_width, r.height() / vb_height);

            viewport.with_view_box(vb_width, vb_height)
        } else {
            viewport.with_view_box(marker_width, marker_height)
        };

        let content_params = NormalizeParams::new(values, &content_viewport);

        transform = transform.pre_translate(
            -self.ref_x.to_user(&content_params),
            -self.ref_y.to_user(&content_params),
        );

        // FIXME: This is the only place in the code where we pass a Some(rect) to
        // StackingContext::new() for its clip_rect argument.  The effect is to clip to
        // the viewport that the current marker should establish, but this code for
        // drawing markers does not yet use the layout_viewport argument in the call to
        // with_discrete_layer() below and instead does things by hand.  We should do all
        // this with the layout_viewport instead of clip_rect, and then we can remove the
        // clip_rect field in StackingContext.
        let clip_rect = if values.is_overflow() {
            None
        } else if let Some(vbox) = self.vbox {
            Some(*vbox)
        } else {
            Some(Rect::from_size(marker_width, marker_height))
        };

        let elt = node.borrow_element();
        let stacking_ctx = Box::new(StackingContext::new(
            draw_ctx.session(),
            acquired_nodes,
            &elt,
            transform,
            clip_rect,
            values,
        ));

        draw_ctx.with_discrete_layer(
            &stacking_ctx,
            acquired_nodes,
            &content_viewport,
            None,
            clipping,
            &mut |an, dc, new_viewport| {
                node.draw_children(an, &cascaded, new_viewport, dc, clipping)
            }, // content_viewport
        )
    }
}

impl ElementTrait for Marker {
    fn set_attributes(&mut self, attrs: &Attributes, session: &Session) {
        for (attr, value) in attrs.iter() {
            match attr.expanded() {
                expanded_name!("", "markerUnits") => {
                    set_attribute(&mut self.units, attr.parse(value), session)
                }
                expanded_name!("", "refX") => {
                    set_attribute(&mut self.ref_x, attr.parse(value), session)
                }
                expanded_name!("", "refY") => {
                    set_attribute(&mut self.ref_y, attr.parse(value), session)
                }
                expanded_name!("", "markerWidth") => {
                    set_attribute(&mut self.width, attr.parse(value), session)
                }
                expanded_name!("", "markerHeight") => {
                    set_attribute(&mut self.height, attr.parse(value), session)
                }
                expanded_name!("", "orient") => {
                    set_attribute(&mut self.orient, attr.parse(value), session)
                }
                expanded_name!("", "preserveAspectRatio") => {
                    set_attribute(&mut self.aspect, attr.parse(value), session)
                }
                expanded_name!("", "viewBox") => {
                    set_attribute(&mut self.vbox, attr.parse(value), session)
                }
                _ => (),
            }
        }
    }
}

// Machinery to figure out marker orientations
#[derive(Debug, PartialEq)]
enum Segment {
    Degenerate {
        // A single lone point
        x: f64,
        y: f64,
    },

    LineOrCurve {
        x1: f64,
        y1: f64,
        x2: f64,
        y2: f64,
        x3: f64,
        y3: f64,
        x4: f64,
        y4: f64,
        is_subpath_start: bool,
        is_subpath_end: bool,
    },
}

impl Segment {
    fn degenerate(x: f64, y: f64) -> Segment {
        Segment::Degenerate { x, y }
    }

    fn curve(
        x1: f64,
        y1: f64,
        x2: f64,
        y2: f64,
        x3: f64,
        y3: f64,
        x4: f64,
        y4: f64,
        is_subpath_start: bool,
        is_subpath_end: bool,
    ) -> Segment {
        Segment::LineOrCurve {
            x1,
            y1,
            x2,
            y2,
            x3,
            y3,
            x4,
            y4,
            is_subpath_start,
            is_subpath_end,
        }
    }

    fn line(
        x1: f64,
        y1: f64,
        x2: f64,
        y2: f64,
        is_subpath_start: bool,
        is_subpath_end: bool,
    ) -> Segment {
        Segment::curve(
            x1,
            y1,
            x2,
            y2,
            x1,
            y1,
            x2,
            y2,
            is_subpath_start,
            is_subpath_end,
        )
    }

    // If the segment has directionality, returns two vectors (v1x, v1y, v2x, v2y); otherwise,
    // returns None.  The vectors are the tangents at the beginning and at the end of the segment,
    // respectively.  A segment does not have directionality if it is degenerate (i.e. a single
    // point) or a zero-length segment, i.e. where all four control points are coincident (the first
    // and last control points may coincide, but the others may define a loop - thus nonzero length)
    fn get_directionalities(&self) -> Option<(f64, f64, f64, f64)> {
        match *self {
            Segment::Degenerate { .. } => None,

            Segment::LineOrCurve {
                x1,
                y1,
                x2,
                y2,
                x3,
                y3,
                x4,
                y4,
                ..
            } => {
                let coincide_1_and_2 = points_equal(x1, y1, x2, y2);
                let coincide_1_and_3 = points_equal(x1, y1, x3, y3);
                let coincide_1_and_4 = points_equal(x1, y1, x4, y4);
                let coincide_2_and_3 = points_equal(x2, y2, x3, y3);
                let coincide_2_and_4 = points_equal(x2, y2, x4, y4);
                let coincide_3_and_4 = points_equal(x3, y3, x4, y4);

                if coincide_1_and_2 && coincide_1_and_3 && coincide_1_and_4 {
                    None
                } else if coincide_1_and_2 && coincide_1_and_3 {
                    Some((x4 - x1, y4 - y1, x4 - x3, y4 - y3))
                } else if coincide_1_and_2 && coincide_3_and_4 {
                    Some((x4 - x1, y4 - y1, x4 - x1, y4 - y1))
                } else if coincide_2_and_3 && coincide_2_and_4 {
                    Some((x2 - x1, y2 - y1, x4 - x1, y4 - y1))
                } else if coincide_1_and_2 {
                    Some((x3 - x1, y3 - y1, x4 - x3, y4 - y3))
                } else if coincide_3_and_4 {
                    Some((x2 - x1, y2 - y1, x4 - x2, y4 - y2))
                } else {
                    Some((x2 - x1, y2 - y1, x4 - x3, y4 - y3))
                }
            }
        }
    }
}

fn points_equal(x1: f64, y1: f64, x2: f64, y2: f64) -> bool {
    x1.approx_eq_cairo(x2) && y1.approx_eq_cairo(y2)
}

enum SegmentState {
    Initial,
    NewSubpath,
    InSubpath,
    ClosedSubpath,
}

#[derive(Debug, PartialEq)]
struct Segments(Vec<Segment>);

impl Deref for Segments {
    type Target = [Segment];

    fn deref(&self) -> &[Segment] {
        &self.0
    }
}

// This converts a path builder into a vector of curveto-like segments.
// Each segment can be:
//
// 1. Segment::Degenerate => the segment is actually a single point (x, y)
//
// 2. Segment::LineOrCurve => either a lineto or a curveto (or the effective
// lineto that results from a closepath).
// We have the following points:
//       P1 = (x1, y1)
//       P2 = (x2, y2)
//       P3 = (x3, y3)
//       P4 = (x4, y4)
//
// The start and end points are P1 and P4, respectively.
// The tangent at the start point is given by the vector (P2 - P1).
// The tangent at the end point is given by the vector (P4 - P3).
// The tangents also work if the segment refers to a lineto (they will
// both just point in the same direction).
impl From<&Path> for Segments {
    fn from(path: &Path) -> Segments {
        let mut last_x: f64;
        let mut last_y: f64;

        let mut cur_x: f64 = 0.0;
        let mut cur_y: f64 = 0.0;
        let mut subpath_start_x: f64 = 0.0;
        let mut subpath_start_y: f64 = 0.0;

        let mut segments = Vec::new();
        let mut state = SegmentState::Initial;

        let path_commands: Vec<PathCommand> = path.iter().collect();

        for i in 0..path_commands.len() {
            last_x = cur_x;
            last_y = cur_y;

            let path_command = &path_commands[i];

            let is_subpath_start = matches!(state, SegmentState::NewSubpath);

            let is_subpath_end = if i == path_commands.len() - 1 {
                true
            } else {
                // If the next command is a MoveTo, it means that the next Segment will be disconnected
                // from the current one.  So, make a note to emit a marker for the current segment's
                // endpoint.
                //
                // Normally we only emit markers for the start point of a Segment, since normally
                // Segments are connected to each other.  They are disconnected only if there are
                // subpaths.

                matches!(path_commands[i + 1], PathCommand::MoveTo(..))
            };

            match path_command {
                &PathCommand::MoveTo(x, y) => {
                    cur_x = x;
                    cur_y = y;

                    subpath_start_x = cur_x;
                    subpath_start_y = cur_y;

                    match state {
                        SegmentState::Initial
                        | SegmentState::InSubpath
                        | SegmentState::ClosedSubpath => {
                            // Ignore the very first moveto in a sequence (Initial state),
                            // or if we were already drawing within a subpath, start
                            // a new subpath.
                            state = SegmentState::NewSubpath;
                        }

                        SegmentState::NewSubpath => {
                            // We had just begun a new subpath (i.e. from a moveto) and we got
                            // another moveto?  Output a stray point for the
                            // previous moveto.
                            segments.push(Segment::degenerate(last_x, last_y));
                            state = SegmentState::NewSubpath;
                        }
                    }
                }

                &PathCommand::LineTo(x, y) => {
                    cur_x = x;
                    cur_y = y;

                    segments.push(Segment::line(
                        last_x,
                        last_y,
                        cur_x,
                        cur_y,
                        is_subpath_start,
                        is_subpath_end,
                    ));

                    state = SegmentState::InSubpath;
                }

                PathCommand::CurveTo(curve) => {
                    let CubicBezierCurve {
                        pt1: (x2, y2),
                        pt2: (x3, y3),
                        to,
                    } = *curve;
                    cur_x = to.0;
                    cur_y = to.1;

                    segments.push(Segment::curve(
                        last_x,
                        last_y,
                        x2,
                        y2,
                        x3,
                        y3,
                        cur_x,
                        cur_y,
                        is_subpath_start,
                        is_subpath_end,
                    ));

                    state = SegmentState::InSubpath;
                }

                PathCommand::Arc(arc) => {
                    cur_x = arc.to.0;
                    cur_y = arc.to.1;

                    match arc.center_parameterization() {
                        ArcParameterization::CenterParameters {
                            center,
                            radii,
                            theta1,
                            delta_theta,
                        } => {
                            let rot = arc.x_axis_rotation;
                            let theta2 = theta1 + delta_theta;
                            let n_segs = (delta_theta / (PI * 0.5 + 0.001)).abs().ceil() as u32;
                            let d_theta = delta_theta / f64::from(n_segs);

                            let segment1 =
                                arc_segment(center, radii, rot, theta1, theta1 + d_theta);
                            let segment2 =
                                arc_segment(center, radii, rot, theta2 - d_theta, theta2);

                            let (x2, y2) = segment1.pt1;
                            let (x3, y3) = segment2.pt2;

                            segments.push(Segment::curve(
                                last_x,
                                last_y,
                                x2,
                                y2,
                                x3,
                                y3,
                                cur_x,
                                cur_y,
                                is_subpath_start,
                                is_subpath_end,
                            ));

                            state = SegmentState::InSubpath;
                        }
                        ArcParameterization::LineTo => {
                            segments.push(Segment::line(
                                last_x,
                                last_y,
                                cur_x,
                                cur_y,
                                is_subpath_start,
                                is_subpath_end,
                            ));

                            state = SegmentState::InSubpath;
                        }
                        ArcParameterization::Omit => {}
                    }
                }

                PathCommand::ClosePath => {
                    cur_x = subpath_start_x;
                    cur_y = subpath_start_y;

                    segments.push(Segment::line(
                        last_x,
                        last_y,
                        cur_x,
                        cur_y,
                        is_subpath_start,
                        is_subpath_end,
                    ));

                    state = SegmentState::ClosedSubpath;
                }
            }
        }

        if let SegmentState::NewSubpath = state {
            // Output a lone point if we started a subpath with a moveto
            // command, but there are no subsequent commands.
            segments.push(Segment::degenerate(cur_x, cur_y));
        };

        Segments(segments)
    }
}

// The SVG spec 1.1 says http://www.w3.org/TR/SVG/implnote.html#PathElementImplementationNotes
// Certain line-capping and line-joining situations and markers
// require that a path segment have directionality at its start and
// end points. Zero-length path segments have no directionality. In
// these cases, the following algorithm is used to establish
// directionality:  to determine the directionality of the start
// point of a zero-length path segment, go backwards in the path
// data specification within the current subpath until you find a
// segment which has directionality at its end point (e.g., a path
// segment with non-zero length) and use its ending direction;
// otherwise, temporarily consider the start point to lack
// directionality. Similarly, to determine the directionality of the
// end point of a zero-length path segment, go forwards in the path
// data specification within the current subpath until you find a
// segment which has directionality at its start point (e.g., a path
// segment with non-zero length) and use its starting direction;
// otherwise, temporarily consider the end point to lack
// directionality. If the start point has directionality but the end
// point doesn't, then the end point uses the start point's
// directionality. If the end point has directionality but the start
// point doesn't, then the start point uses the end point's
// directionality. Otherwise, set the directionality for the path
// segment's start and end points to align with the positive x-axis
// in user space.
impl Segments {
    fn find_incoming_angle_backwards(&self, start_index: usize) -> Option<Angle> {
        // "go backwards ... within the current subpath until ... segment which has directionality
        // at its end point"
        for segment in self[..=start_index].iter().rev() {
            match *segment {
                Segment::Degenerate { .. } => {
                    return None; // reached the beginning of the subpath as we ran into a standalone point
                }

                Segment::LineOrCurve { .. } => match segment.get_directionalities() {
                    Some((_, _, v2x, v2y)) => {
                        return Some(Angle::from_vector(v2x, v2y));
                    }
                    None => {
                        continue;
                    }
                },
            }
        }

        None
    }

    fn find_outgoing_angle_forwards(&self, start_index: usize) -> Option<Angle> {
        // "go forwards ... within the current subpath until ... segment which has directionality at
        // its start point"
        for segment in &self[start_index..] {
            match *segment {
                Segment::Degenerate { .. } => {
                    return None; // reached the end of a subpath as we ran into a standalone point
                }

                Segment::LineOrCurve { .. } => match segment.get_directionalities() {
                    Some((v1x, v1y, _, _)) => {
                        return Some(Angle::from_vector(v1x, v1y));
                    }
                    None => {
                        continue;
                    }
                },
            }
        }

        None
    }
}

fn emit_marker_by_node(
    viewport: &Viewport,
    draw_ctx: &mut DrawingCtx,
    acquired_nodes: &mut AcquiredNodes<'_>,
    marker: &layout::Marker,
    spec: &MarkerSpec,
    line_width: f64,
    clipping: bool,
) -> DrawResult {
    match acquired_nodes.acquire_ref(marker.node_ref.as_ref().unwrap()) {
        Ok(acquired) => {
            let node = acquired.get();

            let marker_elt = borrow_element_as!(node, Marker);

            marker_elt.render(
                node,
                acquired_nodes,
                viewport,
                draw_ctx,
                spec,
                line_width,
                clipping,
                marker,
            )
        }

        Err(e) => {
            rsvg_log!(draw_ctx.session(), "could not acquire marker: {}", e);
            Ok(viewport.empty_bbox())
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq)]
enum MarkerEndpoint {
    Start,
    End,
}

fn emit_marker(
    segment: &Segment,
    endpoint: MarkerEndpoint,
    marker_type: MarkerType,
    orient: Angle,
) -> MarkerSpec {
    let (x, y) = match *segment {
        Segment::Degenerate { x, y } => (x, y),

        Segment::LineOrCurve { x1, y1, x4, y4, .. } => match endpoint {
            MarkerEndpoint::Start => (x1, y1),
            MarkerEndpoint::End => (x4, y4),
        },
    };

    MarkerSpec {
        marker_type,
        x,
        y,
        angle: orient,
    }
}

pub fn render_markers_for_shape(
    shape: &Shape,
    viewport: &Viewport,
    draw_ctx: &mut DrawingCtx,
    acquired_nodes: &mut AcquiredNodes<'_>,
    clipping: bool,
) -> DrawResult {
    if shape.stroke.width.approx_eq_cairo(0.0) {
        return Ok(viewport.empty_bbox());
    }

    if shape.marker_start.node_ref.is_none()
        && shape.marker_mid.node_ref.is_none()
        && shape.marker_end.node_ref.is_none()
    {
        return Ok(viewport.empty_bbox());
    }

    let specs = emit_markers_for_path(&shape.path.path);

    for spec in specs {
        let marker = match spec.marker_type {
            MarkerType::Start => &shape.marker_start,
            MarkerType::Middle => &shape.marker_mid,
            MarkerType::End => &shape.marker_end,
        };

        if marker.node_ref.is_some() {
            emit_marker_by_node(
                viewport,
                draw_ctx,
                acquired_nodes,
                marker,
                &spec,
                shape.stroke.width,
                clipping,
            )?;
        }
    }

    Ok(viewport.empty_bbox())
}

fn emit_markers_for_path(path: &Path) -> Vec<MarkerSpec> {
    enum SubpathState {
        NoSubpath,
        InSubpath,
    }

    let mut specs = Vec::new();

    // Convert the path to a list of segments and bare points
    let segments = Segments::from(path);

    let mut subpath_state = SubpathState::NoSubpath;

    for (i, segment) in segments.iter().enumerate() {
        match *segment {
            Segment::Degenerate { .. } => {
                if let SubpathState::InSubpath = subpath_state {
                    assert!(i > 0);

                    // Got a lone point after a subpath; render the subpath's end marker first
                    let angle = segments
                        .find_incoming_angle_backwards(i - 1)
                        .unwrap_or_else(|| Angle::new(0.0));
                    specs.push(emit_marker(
                        &segments[i - 1],
                        MarkerEndpoint::End,
                        MarkerType::End,
                        angle,
                    ));
                }

                // Render marker for the lone point; no directionality
                specs.push(emit_marker(
                    segment,
                    MarkerEndpoint::Start,
                    MarkerType::Middle,
                    Angle::new(0.0),
                ));

                subpath_state = SubpathState::NoSubpath;
            }

            Segment::LineOrCurve { is_subpath_end, .. } => {
                // Not a degenerate segment
                match subpath_state {
                    SubpathState::NoSubpath => {
                        let angle = segments
                            .find_outgoing_angle_forwards(i)
                            .unwrap_or_else(|| Angle::new(0.0));
                        specs.push(emit_marker(
                            segment,
                            MarkerEndpoint::Start,
                            MarkerType::Start,
                            angle,
                        ));

                        subpath_state = SubpathState::InSubpath;
                    }

                    SubpathState::InSubpath => {
                        assert!(i > 0);

                        let incoming = segments.find_incoming_angle_backwards(i - 1);
                        let outgoing = segments.find_outgoing_angle_forwards(i);

                        let angle = match (incoming, outgoing) {
                            (Some(incoming), Some(outgoing)) => incoming.bisect(outgoing),
                            (Some(incoming), _) => incoming,
                            (_, Some(outgoing)) => outgoing,
                            _ => Angle::new(0.0),
                        };

                        specs.push(emit_marker(
                            segment,
                            MarkerEndpoint::Start,
                            MarkerType::Middle,
                            angle,
                        ));

                        if is_subpath_end && i + 1 < segments.len() {
                            specs.push(emit_marker(
                                segment,
                                MarkerEndpoint::End,
                                MarkerType::Middle,
                                outgoing.unwrap_or_else(|| Angle::new(0.0)),
                            ));
                        }
                    }
                }
            }
        }
    }

    // Finally, render the last point
    if !segments.is_empty() {
        let segment = &segments[segments.len() - 1];
        if let Segment::LineOrCurve { .. } = *segment {
            let incoming = segments
                .find_incoming_angle_backwards(segments.len() - 1)
                .unwrap_or_else(|| Angle::new(0.0));

            let angle = {
                if let PathCommand::ClosePath = path.iter().nth(segments.len()).unwrap() {
                    let outgoing = segments
                        .find_outgoing_angle_forwards(0)
                        .unwrap_or_else(|| Angle::new(0.0));
                    incoming.bisect(outgoing)
                } else {
                    incoming
                }
            };

            specs.push(emit_marker(
                segment,
                MarkerEndpoint::End,
                MarkerType::End,
                angle,
            ));
        }
    }

    specs
}

#[cfg(test)]
mod parser_tests {
    use super::*;

    #[test]
    fn parsing_invalid_marker_units_yields_error() {
        assert!(MarkerUnits::parse_str("").is_err());
        assert!(MarkerUnits::parse_str("foo").is_err());
    }

    #[test]
    fn parses_marker_units() {
        assert_eq!(
            MarkerUnits::parse_str("userSpaceOnUse").unwrap(),
            MarkerUnits::UserSpaceOnUse
        );
        assert_eq!(
            MarkerUnits::parse_str("strokeWidth").unwrap(),
            MarkerUnits::StrokeWidth
        );
    }

    #[test]
    fn parsing_invalid_marker_orient_yields_error() {
        assert!(MarkerOrient::parse_str("").is_err());
        assert!(MarkerOrient::parse_str("blah").is_err());
        assert!(MarkerOrient::parse_str("45blah").is_err());
    }

    #[test]
    fn parses_marker_orient() {
        assert_eq!(MarkerOrient::parse_str("auto").unwrap(), MarkerOrient::Auto);
        assert_eq!(
            MarkerOrient::parse_str("auto-start-reverse").unwrap(),
            MarkerOrient::AutoStartReverse
        );

        assert_eq!(
            MarkerOrient::parse_str("0").unwrap(),
            MarkerOrient::Angle(Angle::new(0.0))
        );
        assert_eq!(
            MarkerOrient::parse_str("180").unwrap(),
            MarkerOrient::Angle(Angle::from_degrees(180.0))
        );
        assert_eq!(
            MarkerOrient::parse_str("180deg").unwrap(),
            MarkerOrient::Angle(Angle::from_degrees(180.0))
        );
        assert_eq!(
            MarkerOrient::parse_str("-400grad").unwrap(),
            MarkerOrient::Angle(Angle::from_degrees(-360.0))
        );
        assert_eq!(
            MarkerOrient::parse_str("1rad").unwrap(),
            MarkerOrient::Angle(Angle::new(1.0))
        );
    }
}

#[cfg(test)]
mod directionality_tests {
    use pretty_assertions::assert_eq;

    use super::*;
    use crate::path_builder::PathBuilder;

    // Single open path; the easy case
    fn setup_open_path() -> Segments {
        let mut builder = PathBuilder::default();

        builder.move_to(10.0, 10.0);
        builder.line_to(20.0, 10.0);
        builder.line_to(20.0, 20.0);

        Segments::from(&builder.into_path())
    }

    #[test]
    fn path_to_segments_handles_open_path() {
        let expected_segments: Segments = Segments(vec![
            Segment::line(10.0, 10.0, 20.0, 10.0, true, false),
            Segment::line(20.0, 10.0, 20.0, 20.0, false, true),
        ]);

        assert_eq!(setup_open_path(), expected_segments);
    }

    fn setup_multiple_open_subpaths() -> Segments {
        let mut builder = PathBuilder::default();

        builder.move_to(10.0, 10.0);
        builder.line_to(20.0, 10.0);
        builder.line_to(20.0, 20.0);

        builder.move_to(30.0, 30.0);
        builder.line_to(40.0, 30.0);
        builder.curve_to(50.0, 35.0, 60.0, 60.0, 70.0, 70.0);
        builder.line_to(80.0, 90.0);

        Segments::from(&builder.into_path())
    }

    #[test]
    fn path_to_segments_handles_multiple_open_subpaths() {
        let expected_segments: Segments = Segments(vec![
            Segment::line(10.0, 10.0, 20.0, 10.0, true, false),
            Segment::line(20.0, 10.0, 20.0, 20.0, false, true),
            Segment::line(30.0, 30.0, 40.0, 30.0, true, false),
            Segment::curve(40.0, 30.0, 50.0, 35.0, 60.0, 60.0, 70.0, 70.0, false, false),
            Segment::line(70.0, 70.0, 80.0, 90.0, false, true),
        ]);

        assert_eq!(setup_multiple_open_subpaths(), expected_segments);
    }

    // Closed subpath; must have a line segment back to the first point
    fn setup_closed_subpath() -> Segments {
        let mut builder = PathBuilder::default();

        builder.move_to(10.0, 10.0);
        builder.line_to(20.0, 10.0);
        builder.line_to(20.0, 20.0);
        builder.close_path();

        Segments::from(&builder.into_path())
    }

    #[test]
    fn path_to_segments_handles_closed_subpath() {
        let expected_segments: Segments = Segments(vec![
            Segment::line(10.0, 10.0, 20.0, 10.0, true, false),
            Segment::line(20.0, 10.0, 20.0, 20.0, false, false),
            Segment::line(20.0, 20.0, 10.0, 10.0, false, true),
        ]);

        assert_eq!(setup_closed_subpath(), expected_segments);
    }

    // Multiple closed subpaths; each must have a line segment back to their
    // initial points, with no degenerate segments between subpaths.
    fn setup_multiple_closed_subpaths() -> Segments {
        let mut builder = PathBuilder::default();

        builder.move_to(10.0, 10.0);
        builder.line_to(20.0, 10.0);
        builder.line_to(20.0, 20.0);
        builder.close_path();

        builder.move_to(30.0, 30.0);
        builder.line_to(40.0, 30.0);
        builder.curve_to(50.0, 35.0, 60.0, 60.0, 70.0, 70.0);
        builder.line_to(80.0, 90.0);
        builder.close_path();

        Segments::from(&builder.into_path())
    }

    #[test]
    fn path_to_segments_handles_multiple_closed_subpaths() {
        let expected_segments: Segments = Segments(vec![
            Segment::line(10.0, 10.0, 20.0, 10.0, true, false),
            Segment::line(20.0, 10.0, 20.0, 20.0, false, false),
            Segment::line(20.0, 20.0, 10.0, 10.0, false, true),
            Segment::line(30.0, 30.0, 40.0, 30.0, true, false),
            Segment::curve(40.0, 30.0, 50.0, 35.0, 60.0, 60.0, 70.0, 70.0, false, false),
            Segment::line(70.0, 70.0, 80.0, 90.0, false, false),
            Segment::line(80.0, 90.0, 30.0, 30.0, false, true),
        ]);

        assert_eq!(setup_multiple_closed_subpaths(), expected_segments);
    }

    // A lineto follows the first closed subpath, with no moveto to start the second subpath.
    // The lineto must start at the first point of the first subpath.
    fn setup_no_moveto_after_closepath() -> Segments {
        let mut builder = PathBuilder::default();

        builder.move_to(10.0, 10.0);
        builder.line_to(20.0, 10.0);
        builder.line_to(20.0, 20.0);
        builder.close_path();

        builder.line_to(40.0, 30.0);

        Segments::from(&builder.into_path())
    }

    #[test]
    fn path_to_segments_handles_no_moveto_after_closepath() {
        let expected_segments: Segments = Segments(vec![
            Segment::line(10.0, 10.0, 20.0, 10.0, true, false),
            Segment::line(20.0, 10.0, 20.0, 20.0, false, false),
            Segment::line(20.0, 20.0, 10.0, 10.0, false, false),
            Segment::line(10.0, 10.0, 40.0, 30.0, false, true),
        ]);

        assert_eq!(setup_no_moveto_after_closepath(), expected_segments);
    }

    // Sequence of moveto; should generate degenerate points.
    // This test is not enabled right now!  We create the
    // path fixtures with Cairo, and Cairo compresses
    // sequences of moveto into a single one.  So, we can't
    // really test this, as we don't get the fixture we want.
    //
    // Eventually we'll probably have to switch librsvg to
    // its own internal path representation which should
    // allow for unelided path commands, and which should
    // only build a cairo_path_t for the final rendering step.
    //
    // fn setup_sequence_of_moveto () -> Segments {
    // let mut builder = PathBuilder::default ();
    //
    // builder.move_to (10.0, 10.0);
    // builder.move_to (20.0, 20.0);
    // builder.move_to (30.0, 30.0);
    // builder.move_to (40.0, 40.0);
    //
    // Segments::from(&builder.into_path())
    // }
    //
    // #[test]
    // fn path_to_segments_handles_sequence_of_moveto () {
    // let expected_segments: Segments = Segments(vec! [
    // Segment::degenerate(10.0, 10.0),
    // Segment::degenerate(20.0, 20.0),
    // Segment::degenerate(30.0, 30.0),
    // Segment::degenerate(40.0, 40.0),
    // ]);
    //
    // assert_eq!(setup_sequence_of_moveto(), expected_segments);
    // }

    #[test]
    fn degenerate_segment_has_no_directionality() {
        let s = Segment::degenerate(1.0, 2.0);
        assert!(s.get_directionalities().is_none());
    }

    #[test]
    fn line_segment_has_directionality() {
        let s = Segment::line(1.0, 2.0, 3.0, 4.0, false, false);
        let (v1x, v1y, v2x, v2y) = s.get_directionalities().unwrap();
        assert_eq!((2.0, 2.0), (v1x, v1y));
        assert_eq!((2.0, 2.0), (v2x, v2y));
    }

    #[test]
    fn line_segment_with_coincident_ends_has_no_directionality() {
        let s = Segment::line(1.0, 2.0, 1.0, 2.0, false, false);
        assert!(s.get_directionalities().is_none());
    }

    #[test]
    fn curve_has_directionality() {
        let s = Segment::curve(1.0, 2.0, 3.0, 5.0, 8.0, 13.0, 20.0, 33.0, false, false);
        let (v1x, v1y, v2x, v2y) = s.get_directionalities().unwrap();
        assert_eq!((2.0, 3.0), (v1x, v1y));
        assert_eq!((12.0, 20.0), (v2x, v2y));
    }

    #[test]
    fn curves_with_loops_and_coincident_ends_have_directionality() {
        let s = Segment::curve(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 1.0, 2.0, false, false);
        let (v1x, v1y, v2x, v2y) = s.get_directionalities().unwrap();
        assert_eq!((2.0, 2.0), (v1x, v1y));
        assert_eq!((-4.0, -4.0), (v2x, v2y));

        let s = Segment::curve(1.0, 2.0, 1.0, 2.0, 3.0, 4.0, 1.0, 2.0, false, false);
        let (v1x, v1y, v2x, v2y) = s.get_directionalities().unwrap();
        assert_eq!((2.0, 2.0), (v1x, v1y));
        assert_eq!((-2.0, -2.0), (v2x, v2y));

        let s = Segment::curve(1.0, 2.0, 3.0, 4.0, 1.0, 2.0, 1.0, 2.0, false, false);
        let (v1x, v1y, v2x, v2y) = s.get_directionalities().unwrap();
        assert_eq!((2.0, 2.0), (v1x, v1y));
        assert_eq!((-2.0, -2.0), (v2x, v2y));
    }

    #[test]
    fn curve_with_coincident_control_points_has_no_directionality() {
        let s = Segment::curve(1.0, 2.0, 1.0, 2.0, 1.0, 2.0, 1.0, 2.0, false, false);
        assert!(s.get_directionalities().is_none());
    }

    #[test]
    fn curve_with_123_coincident_has_directionality() {
        let s = Segment::curve(0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 20.0, 40.0, false, false);
        let (v1x, v1y, v2x, v2y) = s.get_directionalities().unwrap();
        assert_eq!((20.0, 40.0), (v1x, v1y));
        assert_eq!((20.0, 40.0), (v2x, v2y));
    }

    #[test]
    fn curve_with_234_coincident_has_directionality() {
        let s = Segment::curve(20.0, 40.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, false, false);
        let (v1x, v1y, v2x, v2y) = s.get_directionalities().unwrap();
        assert_eq!((-20.0, -40.0), (v1x, v1y));
        assert_eq!((-20.0, -40.0), (v2x, v2y));
    }

    #[test]
    fn curve_with_12_34_coincident_has_directionality() {
        let s = Segment::curve(20.0, 40.0, 20.0, 40.0, 60.0, 70.0, 60.0, 70.0, false, false);
        let (v1x, v1y, v2x, v2y) = s.get_directionalities().unwrap();
        assert_eq!((40.0, 30.0), (v1x, v1y));
        assert_eq!((40.0, 30.0), (v2x, v2y));
    }
}

#[cfg(test)]
mod marker_tests {
    use pretty_assertions::assert_eq;

    use super::*;
    use crate::path_builder::PathBuilder;

    #[test]
    fn emits_for_open_subpath() {
        let mut builder = PathBuilder::default();
        builder.move_to(0.0, 0.0);
        builder.line_to(1.0, 0.0);
        builder.line_to(1.0, 1.0);
        builder.line_to(0.0, 1.0);

        let v = emit_markers_for_path(&builder.into_path());

        assert_eq!(
            v,
            vec![
                MarkerSpec {
                    marker_type: MarkerType::Start,
                    x: 0.0,
                    y: 0.0,
                    angle: Angle::new(0.0)
                },
                MarkerSpec {
                    marker_type: MarkerType::Middle,
                    x: 1.0,
                    y: 0.0,
                    angle: Angle::from_vector(1.0, 1.0)
                },
                MarkerSpec {
                    marker_type: MarkerType::Middle,
                    x: 1.0,
                    y: 1.0,
                    angle: Angle::from_vector(-1.0, 1.0)
                },
                MarkerSpec {
                    marker_type: MarkerType::End,
                    x: 0.0,
                    y: 1.0,
                    angle: Angle::from_vector(-1.0, 0.0)
                },
            ]
        );
    }

    #[test]
    fn emits_for_closed_subpath() {
        let mut builder = PathBuilder::default();
        builder.move_to(0.0, 0.0);
        builder.line_to(1.0, 0.0);
        builder.line_to(1.0, 1.0);
        builder.line_to(0.0, 1.0);
        builder.close_path();

        let v = emit_markers_for_path(&builder.into_path());

        assert_eq!(
            v,
            vec![
                MarkerSpec {
                    marker_type: MarkerType::Start,
                    x: 0.0,
                    y: 0.0,
                    angle: Angle::new(0.0)
                },
                MarkerSpec {
                    marker_type: MarkerType::Middle,
                    x: 1.0,
                    y: 0.0,
                    angle: Angle::from_vector(1.0, 1.0)
                },
                MarkerSpec {
                    marker_type: MarkerType::Middle,
                    x: 1.0,
                    y: 1.0,
                    angle: Angle::from_vector(-1.0, 1.0)
                },
                MarkerSpec {
                    marker_type: MarkerType::Middle,
                    x: 0.0,
                    y: 1.0,
                    angle: Angle::from_vector(-1.0, -1.0)
                },
                MarkerSpec {
                    marker_type: MarkerType::End,
                    x: 0.0,
                    y: 0.0,
                    angle: Angle::from_vector(1.0, -1.0)
                },
            ]
        );
    }

    #[test]
    fn bug_1218_missing_end_markers_in_subpaths() {
        let mut builder = PathBuilder::default();
        builder
            .parse("M 0,0 h 10 h 10 M 0,6 h 10 h 10 M 0,12 h 10 h 10")
            .unwrap();
        let path = builder.into_path();

        let specs = emit_markers_for_path(&path);

        assert_eq!(
            specs,
            vec![
                MarkerSpec {
                    marker_type: MarkerType::Start,
                    x: 0.0,
                    y: 0.0,
                    angle: Angle::new(0.0)
                },
                MarkerSpec {
                    marker_type: MarkerType::Middle,
                    x: 10.0,
                    y: 0.0,
                    angle: Angle::new(0.0)
                },
                MarkerSpec {
                    marker_type: MarkerType::Middle,
                    x: 20.0,
                    y: 0.0,
                    angle: Angle::new(0.0)
                },
                MarkerSpec {
                    marker_type: MarkerType::Middle,
                    x: 0.0,
                    y: 6.0,
                    angle: Angle::new(0.0)
                },
                MarkerSpec {
                    marker_type: MarkerType::Middle,
                    x: 10.0,
                    y: 6.0,
                    angle: Angle::new(0.0)
                },
                MarkerSpec {
                    marker_type: MarkerType::Middle,
                    x: 20.0,
                    y: 6.0,
                    angle: Angle::new(0.0)
                },
                MarkerSpec {
                    marker_type: MarkerType::Middle,
                    x: 0.0,
                    y: 12.0,
                    angle: Angle::new(0.0)
                },
                MarkerSpec {
                    marker_type: MarkerType::Middle,
                    x: 10.0,
                    y: 12.0,
                    angle: Angle::new(0.0)
                },
                MarkerSpec {
                    marker_type: MarkerType::End,
                    x: 20.0,
                    y: 12.0,
                    angle: Angle::new(0.0)
                }
            ]
        );
    }

    #[test]
    fn does_not_duplicate_end_marker() {
        let mut builder = PathBuilder::default();
        builder.parse("M 0,0 h 10 v 10 h -10 Z").unwrap();
        let path = builder.into_path();

        let specs = emit_markers_for_path(&path);

        assert_eq!(
            specs,
            vec![
                MarkerSpec {
                    marker_type: MarkerType::Start,
                    x: 0.0,
                    y: 0.0,
                    angle: Angle::new(0.0)
                },
                MarkerSpec {
                    marker_type: MarkerType::Middle,
                    x: 10.0,
                    y: 0.0,
                    angle: Angle::from_degrees(45.0)
                },
                MarkerSpec {
                    marker_type: MarkerType::Middle,
                    x: 10.0,
                    y: 10.0,
                    angle: Angle::from_degrees(135.0)
                },
                MarkerSpec {
                    marker_type: MarkerType::Middle,
                    x: 0.0,
                    y: 10.0,
                    angle: Angle::from_degrees(225.0)
                },
                MarkerSpec {
                    marker_type: MarkerType::End,
                    x: 0.0,
                    y: 0.0,
                    angle: Angle::from_degrees(315.0)
                },
            ]
        );
    }
}
