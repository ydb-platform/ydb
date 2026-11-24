//! Basic SVG shapes: the `path`, `polygon`, `polyline`, `line`,
//! `rect`, `circle`, `ellipse` elements.

use cssparser::{Parser, Token};
use markup5ever::{expanded_name, local_name, ns};
use std::ops::Deref;
use std::rc::Rc;

use crate::cairo_path::{ValidatedPath, validate_path};
use crate::document::AcquiredNodes;
use crate::drawing_ctx::{DrawingCtx, Viewport};
use crate::element::{DrawResult, ElementTrait, set_attribute};
use crate::error::*;
use crate::iri::Iri;
use crate::is_element_of_type;
use crate::layout::{Layer, LayerKind, Marker, Shape, StackingContext, Stroke};
use crate::length::*;
use crate::node::{CascadedValues, Node, NodeBorrow};
use crate::parsers::{Parse, ParseValue, optional_comma};
use crate::path_builder::{LargeArc, Path as SvgPath, PathBuilder, Sweep};
use crate::properties::ComputedValues;
use crate::rsvg_log;
use crate::session::Session;
use crate::xml::Attributes;

#[derive(PartialEq)]
enum Markers {
    No,
    Yes,
}

struct ShapeDef {
    path: Rc<SvgPath>,
    markers: Markers,
}

impl ShapeDef {
    fn new(path: Rc<SvgPath>, markers: Markers) -> ShapeDef {
        ShapeDef { path, markers }
    }
}

trait BasicShape {
    fn make_shape(&self, params: &NormalizeParams, values: &ComputedValues) -> ShapeDef;
}

fn draw_basic_shape(
    basic_shape: &dyn BasicShape,
    node: &Node,
    acquired_nodes: &mut AcquiredNodes<'_>,
    cascaded: &CascadedValues<'_>,
    viewport: &Viewport,
    session: &Session,
) -> Result<Option<Layer>, Box<InternalRenderingError>> {
    let values = cascaded.get();
    let params = NormalizeParams::new(values, viewport);
    let shape_def = basic_shape.make_shape(&params, values);

    let stroke = Stroke::new(values, &params);
    let path = match validate_path(&shape_def.path, &stroke, viewport)? {
        ValidatedPath::Invalid(ref reason) => {
            rsvg_log!(session, "will not render {node}: {reason}");
            return Ok(None);
        }

        ValidatedPath::Validated(path) => path,
    };

    let paint_order = values.paint_order();

    let stroke_paint = values.stroke().0.resolve(
        acquired_nodes,
        values.stroke_opacity().0,
        values.color().0,
        cascaded.context_fill.clone(),
        cascaded.context_stroke.clone(),
        session,
    );

    let fill_paint = values.fill().0.resolve(
        acquired_nodes,
        values.fill_opacity().0,
        values.color().0,
        cascaded.context_fill.clone(),
        cascaded.context_stroke.clone(),
        session,
    );

    let fill_rule = values.fill_rule();
    let clip_rule = values.clip_rule();
    let shape_rendering = values.shape_rendering();

    let marker_start_node;
    let marker_mid_node;
    let marker_end_node;

    if shape_def.markers == Markers::Yes {
        marker_start_node = acquire_marker(session, acquired_nodes, &values.marker_start().0);
        marker_mid_node = acquire_marker(session, acquired_nodes, &values.marker_mid().0);
        marker_end_node = acquire_marker(session, acquired_nodes, &values.marker_end().0);
    } else {
        marker_start_node = None;
        marker_mid_node = None;
        marker_end_node = None;
    }

    let marker_start = Marker {
        node_ref: marker_start_node,
        context_stroke: stroke_paint.clone(),
        context_fill: fill_paint.clone(),
    };

    let marker_mid = Marker {
        node_ref: marker_mid_node,
        context_stroke: stroke_paint.clone(),
        context_fill: fill_paint.clone(),
    };

    let marker_end = Marker {
        node_ref: marker_end_node,
        context_stroke: stroke_paint.clone(),
        context_fill: fill_paint.clone(),
    };

    let normalize_values = NormalizeValues::new(values);

    let stroke_paint_source =
        stroke_paint.to_user_space(&path.extents, viewport, &normalize_values);
    let fill_paint_source = fill_paint.to_user_space(&path.extents, viewport, &normalize_values);

    let shape = Box::new(Shape {
        path,
        paint_order,
        stroke_paint: stroke_paint_source,
        fill_paint: fill_paint_source,
        stroke,
        fill_rule,
        clip_rule,
        shape_rendering,
        marker_start,
        marker_mid,
        marker_end,
    });

    let elt = node.borrow_element();
    let stacking_ctx = StackingContext::new(
        session,
        acquired_nodes,
        &elt,
        values.transform(),
        None,
        values,
    );

    Ok(Some(Layer {
        kind: LayerKind::Shape(shape),
        stacking_ctx,
    }))
}

macro_rules! impl_draw {
    () => {
        fn layout(
            &self,
            node: &Node,
            acquired_nodes: &mut AcquiredNodes<'_>,
            cascaded: &CascadedValues<'_>,
            viewport: &Viewport,
            draw_ctx: &mut DrawingCtx,
            _clipping: bool,
        ) -> Result<Option<Layer>, Box<InternalRenderingError>> {
            draw_basic_shape(
                self,
                node,
                acquired_nodes,
                cascaded,
                viewport,
                &draw_ctx.session().clone(),
            )
        }

        fn draw(
            &self,
            node: &Node,
            acquired_nodes: &mut AcquiredNodes<'_>,
            cascaded: &CascadedValues<'_>,
            viewport: &Viewport,
            draw_ctx: &mut DrawingCtx,
            clipping: bool,
        ) -> DrawResult {
            self.layout(node, acquired_nodes, cascaded, viewport, draw_ctx, clipping)
                .and_then(|layer| {
                    if let Some(layer) = layer {
                        draw_ctx.draw_layer(&layer, acquired_nodes, clipping, viewport)
                    } else {
                        Ok(viewport.empty_bbox())
                    }
                })
        }
    };
}

fn acquire_marker(
    session: &Session,
    acquired_nodes: &mut AcquiredNodes<'_>,
    iri: &Iri,
) -> Option<Node> {
    iri.get().and_then(|id| {
        acquired_nodes
            .acquire(id)
            .map_err(|e| {
                rsvg_log!(session, "cannot render marker: {}", e);
            })
            .ok()
            .and_then(|acquired| {
                let node = acquired.get();

                if is_element_of_type!(node, Marker) {
                    Some(node.clone())
                } else {
                    rsvg_log!(session, "{} is not a marker element", id);
                    None
                }
            })
    })
}

fn make_ellipse(cx: f64, cy: f64, rx: f64, ry: f64) -> SvgPath {
    let mut builder = PathBuilder::default();

    // Per the spec, rx and ry must be nonnegative
    if rx <= 0.0 || ry <= 0.0 {
        return builder.into_path();
    }

    // 4/3 * (1-cos 45°)/sin 45° = 4/3 * sqrt(2) - 1
    let arc_magic: f64 = 0.5522847498;

    // approximate an ellipse using 4 Bézier curves

    builder.move_to(cx + rx, cy);

    builder.curve_to(
        cx + rx,
        cy + arc_magic * ry,
        cx + arc_magic * rx,
        cy + ry,
        cx,
        cy + ry,
    );

    builder.curve_to(
        cx - arc_magic * rx,
        cy + ry,
        cx - rx,
        cy + arc_magic * ry,
        cx - rx,
        cy,
    );

    builder.curve_to(
        cx - rx,
        cy - arc_magic * ry,
        cx - arc_magic * rx,
        cy - ry,
        cx,
        cy - ry,
    );

    builder.curve_to(
        cx + arc_magic * rx,
        cy - ry,
        cx + rx,
        cy - arc_magic * ry,
        cx + rx,
        cy,
    );

    builder.close_path();

    builder.into_path()
}

#[derive(Default)]
pub struct Path {
    path: Rc<SvgPath>,
}

impl ElementTrait for Path {
    fn set_attributes(&mut self, attrs: &Attributes, session: &Session) {
        for (attr, value) in attrs.iter() {
            if attr.expanded() == expanded_name!("", "d") {
                let mut builder = PathBuilder::default();
                if let Err(e) = builder.parse(value) {
                    // Creating a partial path is OK per the spec; we don't throw away the partial
                    // result in case of an error.

                    rsvg_log!(session, "could not parse path: {}", e);
                }
                self.path = Rc::new(builder.into_path());
            }
        }
    }

    impl_draw!();
}

impl BasicShape for Path {
    fn make_shape(&self, _params: &NormalizeParams, _values: &ComputedValues) -> ShapeDef {
        ShapeDef::new(self.path.clone(), Markers::Yes)
    }
}

/// List-of-points for polyline and polygon elements.
///
/// SVG1.1: <https://www.w3.org/TR/SVG/shapes.html#PointsBNF>
///
/// SVG2: <https://www.w3.org/TR/SVG/shapes.html#DataTypePoints>
#[derive(Debug, Default, PartialEq)]
struct Points(Vec<(f64, f64)>);

impl Deref for Points {
    type Target = [(f64, f64)];

    fn deref(&self) -> &[(f64, f64)] {
        &self.0
    }
}

impl Parse for Points {
    fn parse<'i>(parser: &mut Parser<'i, '_>) -> Result<Points, ParseError<'i>> {
        let mut v = Vec::new();

        loop {
            let x = f64::parse(parser)?;
            optional_comma(parser);
            let y = f64::parse(parser)?;

            v.push((x, y));

            if parser.is_exhausted() {
                break;
            }

            match parser.next_including_whitespace() {
                Ok(&Token::WhiteSpace(_)) => (),
                _ => optional_comma(parser),
            }
        }

        Ok(Points(v))
    }
}

fn make_poly(points: &Points, closed: bool) -> SvgPath {
    let mut builder = PathBuilder::default();

    for (i, &(x, y)) in points.iter().enumerate() {
        if i == 0 {
            builder.move_to(x, y);
        } else {
            builder.line_to(x, y);
        }
    }

    if closed && !points.is_empty() {
        builder.close_path();
    }

    builder.into_path()
}

#[derive(Default)]
pub struct Polygon {
    points: Points,
}

impl ElementTrait for Polygon {
    fn set_attributes(&mut self, attrs: &Attributes, session: &Session) {
        for (attr, value) in attrs.iter() {
            if attr.expanded() == expanded_name!("", "points") {
                set_attribute(&mut self.points, attr.parse(value), session);
            }
        }
    }

    impl_draw!();
}

impl BasicShape for Polygon {
    fn make_shape(&self, _params: &NormalizeParams, _values: &ComputedValues) -> ShapeDef {
        ShapeDef::new(Rc::new(make_poly(&self.points, true)), Markers::Yes)
    }
}

#[derive(Default)]
pub struct Polyline {
    points: Points,
}

impl ElementTrait for Polyline {
    fn set_attributes(&mut self, attrs: &Attributes, session: &Session) {
        for (attr, value) in attrs.iter() {
            if attr.expanded() == expanded_name!("", "points") {
                set_attribute(&mut self.points, attr.parse(value), session);
            }
        }
    }

    impl_draw!();
}

impl BasicShape for Polyline {
    fn make_shape(&self, _params: &NormalizeParams, _values: &ComputedValues) -> ShapeDef {
        ShapeDef::new(Rc::new(make_poly(&self.points, false)), Markers::Yes)
    }
}

#[derive(Default)]
pub struct Line {
    x1: Length<Horizontal>,
    y1: Length<Vertical>,
    x2: Length<Horizontal>,
    y2: Length<Vertical>,
}

impl ElementTrait for Line {
    fn set_attributes(&mut self, attrs: &Attributes, session: &Session) {
        for (attr, value) in attrs.iter() {
            match attr.expanded() {
                expanded_name!("", "x1") => set_attribute(&mut self.x1, attr.parse(value), session),
                expanded_name!("", "y1") => set_attribute(&mut self.y1, attr.parse(value), session),
                expanded_name!("", "x2") => set_attribute(&mut self.x2, attr.parse(value), session),
                expanded_name!("", "y2") => set_attribute(&mut self.y2, attr.parse(value), session),
                _ => (),
            }
        }
    }

    impl_draw!();
}

impl BasicShape for Line {
    fn make_shape(&self, params: &NormalizeParams, _values: &ComputedValues) -> ShapeDef {
        let mut builder = PathBuilder::default();

        let x1 = self.x1.to_user(params);
        let y1 = self.y1.to_user(params);
        let x2 = self.x2.to_user(params);
        let y2 = self.y2.to_user(params);

        builder.move_to(x1, y1);
        builder.line_to(x2, y2);

        ShapeDef::new(Rc::new(builder.into_path()), Markers::Yes)
    }
}

/// The `<rect>` element.
///
/// Note that its x/y/width/height/rx/ry are properties in SVG2, so they are
/// defined as part of [the properties machinery](properties.rs).
#[derive(Default)]
pub struct Rect {}

impl ElementTrait for Rect {
    impl_draw!();
}

impl BasicShape for Rect {
    #[allow(clippy::many_single_char_names)]
    fn make_shape(&self, params: &NormalizeParams, values: &ComputedValues) -> ShapeDef {
        let x = values.x().0.to_user(params);
        let y = values.y().0.to_user(params);

        let w = match values.width().0 {
            LengthOrAuto::Length(l) => l.to_user(params),
            LengthOrAuto::Auto => 0.0,
        };
        let h = match values.height().0 {
            LengthOrAuto::Length(l) => l.to_user(params),
            LengthOrAuto::Auto => 0.0,
        };

        let norm_rx = match values.rx().0 {
            LengthOrAuto::Length(l) => Some(l.to_user(params)),
            LengthOrAuto::Auto => None,
        };
        let norm_ry = match values.ry().0 {
            LengthOrAuto::Length(l) => Some(l.to_user(params)),
            LengthOrAuto::Auto => None,
        };

        let mut rx;
        let mut ry;

        match (norm_rx, norm_ry) {
            (None, None) => {
                rx = 0.0;
                ry = 0.0;
            }

            (Some(_rx), None) => {
                rx = _rx;
                ry = _rx;
            }

            (None, Some(_ry)) => {
                rx = _ry;
                ry = _ry;
            }

            (Some(_rx), Some(_ry)) => {
                rx = _rx;
                ry = _ry;
            }
        }

        let mut builder = PathBuilder::default();

        // Per the spec, w,h must be >= 0
        if w <= 0.0 || h <= 0.0 {
            return ShapeDef::new(Rc::new(builder.into_path()), Markers::No);
        }

        let half_w = w / 2.0;
        let half_h = h / 2.0;

        if rx > half_w {
            rx = half_w;
        }

        if ry > half_h {
            ry = half_h;
        }

        if rx == 0.0 {
            ry = 0.0;
        } else if ry == 0.0 {
            rx = 0.0;
        }

        if rx == 0.0 {
            // Easy case, no rounded corners
            builder.move_to(x, y);
            builder.line_to(x + w, y);
            builder.line_to(x + w, y + h);
            builder.line_to(x, y + h);
            builder.line_to(x, y);
        } else {
            /* Hard case, rounded corners
             *
             *      (top_x1, top_y)                   (top_x2, top_y)
             *     *--------------------------------*
             *    /                                  \
             *   * (left_x, left_y1)                  * (right_x, right_y1)
             *   |                                    |
             *   |                                    |
             *   |                                    |
             *   |                                    |
             *   |                                    |
             *   |                                    |
             *   |                                    |
             *   |                                    |
             *   |                                    |
             *   * (left_x, left_y2)                  * (right_x, right_y2)
             *    \                                  /
             *     *--------------------------------*
             *      (bottom_x1, bottom_y)            (bottom_x2, bottom_y)
             */

            let top_x1 = x + rx;
            let top_x2 = x + w - rx;
            let top_y = y;

            let bottom_x1 = top_x1;
            let bottom_x2 = top_x2;
            let bottom_y = y + h;

            let left_x = x;
            let left_y1 = y + ry;
            let left_y2 = y + h - ry;

            let right_x = x + w;
            let right_y1 = left_y1;
            let right_y2 = left_y2;

            builder.move_to(top_x1, top_y);
            builder.line_to(top_x2, top_y);

            builder.arc(
                top_x2,
                top_y,
                rx,
                ry,
                0.0,
                LargeArc(false),
                Sweep::Positive,
                right_x,
                right_y1,
            );

            builder.line_to(right_x, right_y2);

            builder.arc(
                right_x,
                right_y2,
                rx,
                ry,
                0.0,
                LargeArc(false),
                Sweep::Positive,
                bottom_x2,
                bottom_y,
            );

            builder.line_to(bottom_x1, bottom_y);

            builder.arc(
                bottom_x1,
                bottom_y,
                rx,
                ry,
                0.0,
                LargeArc(false),
                Sweep::Positive,
                left_x,
                left_y2,
            );

            builder.line_to(left_x, left_y1);

            builder.arc(
                left_x,
                left_y1,
                rx,
                ry,
                0.0,
                LargeArc(false),
                Sweep::Positive,
                top_x1,
                top_y,
            );
        }

        builder.close_path();

        ShapeDef::new(Rc::new(builder.into_path()), Markers::No)
    }
}

/// The `<circle>` element.
///
/// Note that its cx/cy/r are properties in SVG2, so they are
/// defined as part of [the properties machinery](properties.rs).
#[derive(Default)]
pub struct Circle {}

impl ElementTrait for Circle {
    impl_draw!();
}

impl BasicShape for Circle {
    fn make_shape(&self, params: &NormalizeParams, values: &ComputedValues) -> ShapeDef {
        let cx = values.cx().0.to_user(params);
        let cy = values.cy().0.to_user(params);
        let r = values.r().0.to_user(params);

        ShapeDef::new(Rc::new(make_ellipse(cx, cy, r, r)), Markers::No)
    }
}

/// The `<ellipse>` element.
///
/// Note that its cx/cy/rx/ry are properties in SVG2, so they are
/// defined as part of [the properties machinery](properties.rs).
#[derive(Default)]
pub struct Ellipse {}

impl ElementTrait for Ellipse {
    impl_draw!();
}

impl BasicShape for Ellipse {
    fn make_shape(&self, params: &NormalizeParams, values: &ComputedValues) -> ShapeDef {
        let cx = values.cx().0.to_user(params);
        let cy = values.cy().0.to_user(params);
        let norm_rx = match values.rx().0 {
            LengthOrAuto::Length(l) => Some(l.to_user(params)),
            LengthOrAuto::Auto => None,
        };
        let norm_ry = match values.ry().0 {
            LengthOrAuto::Length(l) => Some(l.to_user(params)),
            LengthOrAuto::Auto => None,
        };

        let rx;
        let ry;

        match (norm_rx, norm_ry) {
            (None, None) => {
                rx = 0.0;
                ry = 0.0;
            }

            (Some(_rx), None) => {
                rx = _rx;
                ry = _rx;
            }

            (None, Some(_ry)) => {
                rx = _ry;
                ry = _ry;
            }

            (Some(_rx), Some(_ry)) => {
                rx = _rx;
                ry = _ry;
            }
        }

        ShapeDef::new(Rc::new(make_ellipse(cx, cy, rx, ry)), Markers::No)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_points() {
        assert_eq!(
            Points::parse_str(" 1 2 ").unwrap(),
            Points(vec![(1.0, 2.0)])
        );
        assert_eq!(
            Points::parse_str("1 2 3 4").unwrap(),
            Points(vec![(1.0, 2.0), (3.0, 4.0)])
        );
        assert_eq!(
            Points::parse_str("1,2,3,4").unwrap(),
            Points(vec![(1.0, 2.0), (3.0, 4.0)])
        );
        assert_eq!(
            Points::parse_str("1,2 3,4").unwrap(),
            Points(vec![(1.0, 2.0), (3.0, 4.0)])
        );
        assert_eq!(
            Points::parse_str("1,2 -3,4").unwrap(),
            Points(vec![(1.0, 2.0), (-3.0, 4.0)])
        );
        assert_eq!(
            Points::parse_str("1,2,-3,4").unwrap(),
            Points(vec![(1.0, 2.0), (-3.0, 4.0)])
        );
    }

    #[test]
    fn errors_on_invalid_points() {
        assert!(Points::parse_str("-1-2-3-4").is_err());
        assert!(Points::parse_str("1 2-3,-4").is_err());
    }
}
