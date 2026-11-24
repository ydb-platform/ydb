//! Text elements: `text`, `tspan`, `tref`.

use markup5ever::{QualName, expanded_name, local_name, ns};
use pango::IsAttribute;
use pango::prelude::FontExt;
use std::cell::RefCell;
use std::convert::TryFrom;
use std::rc::Rc;

use crate::document::{AcquiredNodes, NodeId};
use crate::drawing_ctx::{DrawingCtx, FontOptions, Viewport, create_pango_context};
use crate::element::{DrawResult, ElementData, ElementTrait, set_attribute};
use crate::error::*;
use crate::layout::{self, FontProperties, Layer, LayerKind, StackingContext, Stroke, TextSpan};
use crate::length::*;
use crate::node::{CascadedValues, Node, NodeBorrow};
use crate::paint_server::PaintSource;
use crate::parsers::{CommaSeparatedList, Parse, ParseValue};
use crate::properties::{
    ComputedValues, Direction, DominantBaseline, FontStretch, FontStyle, FontVariant, FontWeight,
    PaintOrder, TextAnchor, TextRendering, UnicodeBidi, WritingMode, XmlLang, XmlSpace,
};
use crate::rect::Rect;
use crate::rsvg_log;
use crate::session::Session;
use crate::space::{NormalizeDefault, XmlSpaceNormalize, xml_space_normalize};
use crate::xml::Attributes;

/// The state of a text layout operation.
struct LayoutContext {
    /// `writing-mode` property from the `<text>` element.
    writing_mode: WritingMode,

    /// Font options from the DrawingCtx.
    font_options: FontOptions,

    /// For normalizing lengths.
    viewport: Viewport,

    /// Session metadata for the document
    session: Session,
}

/// An absolutely-positioned array of `Span`s
///
/// SVG defines a "[text chunk]" to occur when a text-related element
/// has an absolute position adjustment, that is, `x` or `y`
/// attributes.
///
/// A `<text>` element always starts with an absolute position from
/// such attributes, or (0, 0) if they are not specified.
///
/// Subsequent children of the `<text>` element will create new chunks
/// whenever they have `x` or `y` attributes.
///
/// [text chunk]: https://www.w3.org/TR/SVG11/text.html#TextLayoutIntroduction
struct Chunk {
    values: Rc<ComputedValues>,
    x: Option<f64>,
    y: Option<f64>,
    spans: Vec<Span>,
}

struct MeasuredChunk {
    values: Rc<ComputedValues>,
    x: Option<f64>,
    y: Option<f64>,
    dx: f64,
    dy: f64,
    spans: Vec<MeasuredSpan>,
}

struct PositionedChunk {
    next_chunk_x: f64,
    next_chunk_y: f64,
    spans: Vec<PositionedSpan>,
}

struct Span {
    values: Rc<ComputedValues>,
    text: String,
    dx: f64,
    dy: f64,
    _depth: usize,
    link_target: Option<String>,
}

struct MeasuredSpan {
    values: Rc<ComputedValues>,
    layout: pango::Layout,
    layout_size: (f64, f64),
    advance: (f64, f64),
    dx: f64,
    dy: f64,
    link_target: Option<String>,
}

struct PositionedSpan {
    layout: pango::Layout,
    values: Rc<ComputedValues>,
    rendered_position: (f64, f64),
    next_span_position: (f64, f64),
    link_target: Option<String>,
}

/// A laid-out and resolved text span.
///
/// The only thing not in user-space units are the `stroke_paint` and `fill_paint`.
///
/// This is the non-user-space version of `layout::TextSpan`.
struct LayoutSpan {
    layout: pango::Layout,
    gravity: pango::Gravity,
    extents: Option<Rect>,
    is_visible: bool,
    x: f64,
    y: f64,
    paint_order: PaintOrder,
    stroke: Stroke,
    stroke_paint: Rc<PaintSource>,
    fill_paint: Rc<PaintSource>,
    text_rendering: TextRendering,
    link_target: Option<String>,
    values: Rc<ComputedValues>,
}

impl Chunk {
    fn new(values: &ComputedValues, x: Option<f64>, y: Option<f64>) -> Chunk {
        Chunk {
            values: Rc::new(values.clone()),
            x,
            y,
            spans: Vec::new(),
        }
    }
}

impl MeasuredChunk {
    fn from_chunk(layout_context: &LayoutContext, chunk: &Chunk) -> MeasuredChunk {
        let mut measured_spans: Vec<MeasuredSpan> = chunk
            .spans
            .iter()
            .filter_map(|span| MeasuredSpan::from_span(layout_context, span))
            .collect();

        // The first span contains the (dx, dy) that will be applied to the whole chunk.
        // Make them 0 in the span, and extract the values to set them on the chunk.
        // This is a hack until librsvg adds support for multiple dx/dy values per text/tspan.

        let (chunk_dx, chunk_dy) = if let Some(first) = measured_spans.first_mut() {
            let dx = first.dx;
            let dy = first.dy;
            first.dx = 0.0;
            first.dy = 0.0;
            (dx, dy)
        } else {
            (0.0, 0.0)
        };

        MeasuredChunk {
            values: chunk.values.clone(),
            x: chunk.x,
            y: chunk.y,
            dx: chunk_dx,
            dy: chunk_dy,
            spans: measured_spans,
        }
    }
}

impl PositionedChunk {
    fn from_measured(
        layout_context: &LayoutContext,
        measured: &MeasuredChunk,
        chunk_x: f64,
        chunk_y: f64,
    ) -> PositionedChunk {
        let chunk_direction = measured.values.direction();

        // Position the spans relatively to each other, starting at (0, 0)

        let mut positioned = Vec::new();

        // Start position of each span; gets advanced as each span is laid out.
        // This is the text's start position, not the bounding box.
        let mut x = 0.0;
        let mut y = 0.0;

        let mut chunk_bounds: Option<Rect> = None;

        // Find the bounding box of the entire chunk by taking the union of the bounding boxes
        // of each individual span.

        for mspan in &measured.spans {
            let params = NormalizeParams::new(&mspan.values, &layout_context.viewport);

            let layout = mspan.layout.clone();
            let layout_size = mspan.layout_size;
            let values = mspan.values.clone();
            let dx = mspan.dx;
            let dy = mspan.dy;
            let advance = mspan.advance;

            let baseline_offset = compute_baseline_offset(&layout, &values, &params);

            let start_pos = match chunk_direction {
                Direction::Ltr => (x, y),
                Direction::Rtl => (x - advance.0, y),
            };

            let span_advance = match chunk_direction {
                Direction::Ltr => (advance.0, advance.1),
                Direction::Rtl => (-advance.0, advance.1),
            };

            let rendered_position = if layout_context.writing_mode.is_horizontal() {
                (start_pos.0 + dx, start_pos.1 - baseline_offset + dy)
            } else {
                (start_pos.0 + baseline_offset + dx, start_pos.1 + dy)
            };

            let span_bounds =
                Rect::from_size(layout_size.0, layout_size.1).translate(rendered_position);

            // We take the union here

            if let Some(bounds) = chunk_bounds {
                chunk_bounds = Some(bounds.union(&span_bounds));
            } else {
                chunk_bounds = Some(span_bounds);
            }

            x = x + span_advance.0 + dx;
            y = y + span_advance.1 + dy;

            let positioned_span = PositionedSpan {
                layout,
                values,
                rendered_position,
                next_span_position: (x, y),
                link_target: mspan.link_target.clone(),
            };

            positioned.push(positioned_span);
        }

        // Compute the offsets needed to align the chunk per the text-anchor property (start, middle, end):

        let anchor_offset = text_anchor_offset(
            measured.values.text_anchor(),
            chunk_direction,
            layout_context.writing_mode,
            chunk_bounds.unwrap_or_default(),
        );

        // Apply the text-anchor offset to each individually-positioned span, and compute the
        // start position of the next chunk.  Also add in the chunk's dx/dy.

        let mut next_chunk_x = chunk_x;
        let mut next_chunk_y = chunk_y;

        for pspan in &mut positioned {
            // Add the chunk's position, plus the text-anchor offset, plus the chunk's dx/dy.
            // This last term is a hack until librsvg adds support for multiple dx/dy values per text/tspan;
            // see the corresponding part in MeasuredChunk::from_chunk().
            pspan.rendered_position.0 += chunk_x + anchor_offset.0 + measured.dx;
            pspan.rendered_position.1 += chunk_y + anchor_offset.1 + measured.dy;

            next_chunk_x = chunk_x + pspan.next_span_position.0 + anchor_offset.0 + measured.dx;
            next_chunk_y = chunk_y + pspan.next_span_position.1 + anchor_offset.1 + measured.dy;
        }

        PositionedChunk {
            next_chunk_x,
            next_chunk_y,
            spans: positioned,
        }
    }
}

fn compute_baseline_offset(
    layout: &pango::Layout,
    values: &ComputedValues,
    params: &NormalizeParams,
) -> f64 {
    let mut baseline = f64::from(layout.baseline()) / f64::from(pango::SCALE);
    let dominant_baseline = values.dominant_baseline();

    let mut layout_iter = layout.iter();
    loop {
        if let Some(layout_run) = layout_iter.run_readonly() {
            let item = layout_run.item();
            unsafe {
                let analysis = (*item.as_ptr()).analysis;
                if analysis.font.is_null() {
                    break;
                }
            }
            let font = item.analysis().font();

            let metrics = font.metrics(None);
            let ascent = metrics.ascent();
            let descent = metrics.descent();
            let height = metrics.height();

            match dominant_baseline {
                DominantBaseline::Hanging => {
                    baseline -= f64::from(ascent - descent) / f64::from(pango::SCALE);
                }
                DominantBaseline::Middle => {
                    // Approximate meanline using strikethrough position and thickness
                    // https://mail.gnome.org/archives/gtk-i18n-list/2012-December/msg00046.html
                    baseline -= f64::from(
                        metrics.strikethrough_position() + metrics.strikethrough_thickness() / 2,
                    ) / f64::from(pango::SCALE);
                }
                DominantBaseline::Central => {
                    baseline = 0.5 * f64::from(ascent + descent) / f64::from(pango::SCALE);
                }
                DominantBaseline::TextBeforeEdge | DominantBaseline::TextTop => {
                    //baseline -= f64::from(ascent) / f64::from(pango::SCALE);
                    // Bit of a klutch, but leads to better results
                    baseline -= f64::from(2 * ascent - height) / f64::from(pango::SCALE);
                }
                DominantBaseline::TextAfterEdge | DominantBaseline::TextBottom => {
                    baseline += f64::from(descent) / f64::from(pango::SCALE);
                }
                DominantBaseline::Ideographic => {
                    // Approx
                    baseline += f64::from(descent) / f64::from(pango::SCALE);
                }
                DominantBaseline::Mathematical => {
                    // Approx
                    baseline = 0.5 * f64::from(ascent + descent) / f64::from(pango::SCALE);
                }
                _ => (),
            }

            break;
        }

        if !layout_iter.next_run() {
            break;
        }
    }

    let baseline_shift = values.baseline_shift().0.to_user(params);

    baseline + baseline_shift
}

/// Computes the (x, y) offsets to be applied to spans after applying the text-anchor property (start, middle, end).
#[rustfmt::skip]
fn text_anchor_offset(
    anchor: TextAnchor,
    direction: Direction,
    writing_mode: WritingMode,
    chunk_bounds: Rect,
) -> (f64, f64) {
    let (w, h) = (chunk_bounds.width(), chunk_bounds.height());

    let x0 = chunk_bounds.x0;

    if writing_mode.is_horizontal() {
        match (anchor, direction) {
            (TextAnchor::Start,  Direction::Ltr) => (-x0, 0.0),
            (TextAnchor::Start,  Direction::Rtl) => (-x0 - w, 0.0),

            (TextAnchor::Middle, Direction::Ltr) => (-x0 - w / 2.0, 0.0),
            (TextAnchor::Middle, Direction::Rtl) => (-x0 - w / 2.0, 0.0),

            (TextAnchor::End,    Direction::Ltr) => (-x0 - w, 0.0),
            (TextAnchor::End,    Direction::Rtl) => (-x0, 0.0),
        }
    } else {
        // FIXME: we don't deal with text direction for vertical text yet.
        match anchor {
            TextAnchor::Start => (0.0, 0.0),
            TextAnchor::Middle => (0.0, -h / 2.0),
            TextAnchor::End => (0.0, -h),
        }
    }
}

impl Span {
    fn new(
        text: &str,
        values: Rc<ComputedValues>,
        dx: f64,
        dy: f64,
        depth: usize,
        link_target: Option<String>,
    ) -> Span {
        Span {
            values,
            text: text.to_string(),
            dx,
            dy,
            _depth: depth,
            link_target,
        }
    }
}

/// Use as `PangoUnits::from_pixels()` so that we can check for overflow.
struct PangoUnits(i32);

impl PangoUnits {
    fn from_pixels(v: f64) -> Option<Self> {
        // We want (v * f64::from(pango::SCALE) + 0.5) as i32
        //
        // But check for overflow.

        cast::i32(v * f64::from(pango::SCALE) + 0.5)
            .ok()
            .map(PangoUnits)
    }
}

impl MeasuredSpan {
    fn from_span(layout_context: &LayoutContext, span: &Span) -> Option<MeasuredSpan> {
        let values = span.values.clone();

        let params = NormalizeParams::new(&values, &layout_context.viewport);

        let properties = FontProperties::new(&values, &params);

        let bidi_control = BidiControl::from_unicode_bidi_and_direction(
            properties.unicode_bidi,
            properties.direction,
        );

        let with_control_chars = wrap_with_direction_control_chars(&span.text, &bidi_control);

        if let Some(layout) = create_pango_layout(layout_context, &properties, &with_control_chars)
        {
            let (w, h) = layout.size();

            let w = f64::from(w) / f64::from(pango::SCALE);
            let h = f64::from(h) / f64::from(pango::SCALE);

            let advance = if layout_context.writing_mode.is_horizontal() {
                (w, 0.0)
            } else {
                (0.0, w)
            };

            Some(MeasuredSpan {
                values,
                layout,
                layout_size: (w, h),
                advance,
                dx: span.dx,
                dy: span.dy,
                link_target: span.link_target.clone(),
            })
        } else {
            None
        }
    }
}

// FIXME: should the pango crate provide this like PANGO_GRAVITY_IS_VERTICAL() ?
fn gravity_is_vertical(gravity: pango::Gravity) -> bool {
    matches!(gravity, pango::Gravity::East | pango::Gravity::West)
}

fn compute_text_box(
    layout: &pango::Layout,
    x: f64,
    y: f64,
    gravity: pango::Gravity,
) -> Option<Rect> {
    #![allow(clippy::many_single_char_names)]

    let (ink, _) = layout.extents();
    if ink.width() == 0 || ink.height() == 0 {
        return None;
    }

    let ink_x = f64::from(ink.x());
    let ink_y = f64::from(ink.y());
    let ink_width = f64::from(ink.width());
    let ink_height = f64::from(ink.height());
    let pango_scale = f64::from(pango::SCALE);

    let (x, y, w, h) = if gravity_is_vertical(gravity) {
        (
            x + (ink_x - ink_height) / pango_scale,
            y + ink_y / pango_scale,
            ink_height / pango_scale,
            ink_width / pango_scale,
        )
    } else {
        (
            x + ink_x / pango_scale,
            y + ink_y / pango_scale,
            ink_width / pango_scale,
            ink_height / pango_scale,
        )
    };

    Some(Rect::new(x, y, x + w, y + h))
}

impl PositionedSpan {
    fn layout(
        &self,
        layout_context: &LayoutContext,
        acquired_nodes: &mut AcquiredNodes<'_>,
    ) -> LayoutSpan {
        let params = NormalizeParams::new(&self.values, &layout_context.viewport);

        let layout = self.layout.clone();
        let is_visible = self.values.is_visible();
        let (x, y) = self.rendered_position;

        let stroke = Stroke::new(&self.values, &params);

        let gravity = layout.context().gravity();

        let extents = compute_text_box(&layout, x, y, gravity);

        let stroke_paint = self.values.stroke().0.resolve(
            acquired_nodes,
            self.values.stroke_opacity().0,
            self.values.color().0,
            None,
            None,
            &layout_context.session,
        );

        let fill_paint = self.values.fill().0.resolve(
            acquired_nodes,
            self.values.fill_opacity().0,
            self.values.color().0,
            None,
            None,
            &layout_context.session,
        );

        let paint_order = self.values.paint_order();
        let text_rendering = self.values.text_rendering();

        LayoutSpan {
            layout,
            gravity,
            extents,
            is_visible,
            x,
            y,
            paint_order,
            stroke,
            stroke_paint,
            fill_paint,
            text_rendering,
            values: self.values.clone(),
            link_target: self.link_target.clone(),
        }
    }
}

/// Walks the children of a `<text>`, `<tspan>`, or `<tref>` element
/// and appends chunks/spans from them into the specified `chunks`
/// array.
fn children_to_chunks(
    chunks: &mut Vec<Chunk>,
    node: &Node,
    acquired_nodes: &mut AcquiredNodes<'_>,
    cascaded: &CascadedValues<'_>,
    layout_context: &LayoutContext,
    dx: f64,
    dy: f64,
    depth: usize,
    link: Option<String>,
) {
    let mut dx = dx;
    let mut dy = dy;

    for child in node.children() {
        if child.is_chars() {
            let values = cascaded.get();
            child.borrow_chars().to_chunks(
                &child,
                Rc::new(values.clone()),
                chunks,
                dx,
                dy,
                depth,
                link.clone(),
            );
        } else {
            assert!(child.is_element());

            match *child.borrow_element_data() {
                ElementData::TSpan(ref tspan) => {
                    let cascaded = CascadedValues::clone_with_node(cascaded, &child);
                    tspan.to_chunks(
                        &child,
                        acquired_nodes,
                        &cascaded,
                        layout_context,
                        chunks,
                        dx,
                        dy,
                        depth + 1,
                        link.clone(),
                    );
                }

                ElementData::Link(ref link) => {
                    // TSpan::default sets all offsets to 0,
                    // which is what we want in links.
                    //
                    // FIXME: This is the only place in the code where an element's method (TSpan::to_chunks)
                    // is called with a node that is not the element itself: here, `child` is a Link, not a TSpan.
                    //
                    // The code works because the `tspan` is dropped immediately after calling to_chunks and no
                    // references are retained for it.
                    let tspan = TSpan::default();
                    let cascaded = CascadedValues::clone_with_node(cascaded, &child);
                    tspan.to_chunks(
                        &child,
                        acquired_nodes,
                        &cascaded,
                        layout_context,
                        chunks,
                        dx,
                        dy,
                        depth + 1,
                        link.link.clone(),
                    );
                }

                ElementData::TRef(ref tref) => {
                    let cascaded = CascadedValues::clone_with_node(cascaded, &child);
                    tref.to_chunks(
                        &child,
                        acquired_nodes,
                        &cascaded,
                        chunks,
                        depth + 1,
                        layout_context,
                    );
                }

                _ => (),
            }
        }

        // After the first span, we don't need to carry over the parent's dx/dy.
        dx = 0.0;
        dy = 0.0;
    }
}

/// In SVG text elements, we use `Chars` to store character data.  For example,
/// an element like `<text>Foo Bar</text>` will be a `Text` with a single child,
/// and the child will be a `Chars` with "Foo Bar" for its contents.
///
/// Text elements can contain `<tspan>` sub-elements.  In this case,
/// those `tspan` nodes will also contain `Chars` children.
///
/// A text or tspan element can contain more than one `Chars` child, for example,
/// if there is an XML comment that splits the character contents in two:
///
/// ```xml
/// <text>
///   This sentence will create a Chars.
///   <!-- this comment is ignored -->
///   This sentence will cretea another Chars.
/// </text>
/// ```
///
/// When rendering a text element, it will take care of concatenating the strings
/// in its `Chars` children as appropriate, depending on the
/// `xml:space="preserve"` attribute.  A `Chars` stores the characters verbatim
/// as they come out of the XML parser, after ensuring that they are valid UTF-8.

#[derive(Default)]
pub struct Chars {
    string: RefCell<String>,
    space_normalized: RefCell<Option<String>>,
}

impl Chars {
    pub fn new(initial_text: &str) -> Chars {
        Chars {
            string: RefCell::new(String::from(initial_text)),
            space_normalized: RefCell::new(None),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.string.borrow().is_empty()
    }

    pub fn append(&self, s: &str) {
        self.string.borrow_mut().push_str(s);
        *self.space_normalized.borrow_mut() = None;
    }

    fn ensure_normalized_string(&self, node: &Node, values: &ComputedValues) {
        let mut normalized = self.space_normalized.borrow_mut();

        if (*normalized).is_none() {
            let mode = match values.xml_space() {
                XmlSpace::Default => XmlSpaceNormalize::Default(NormalizeDefault {
                    has_element_before: node.previous_sibling().is_some(),
                    has_element_after: node.next_sibling().is_some(),
                }),

                XmlSpace::Preserve => XmlSpaceNormalize::Preserve,
            };

            *normalized = Some(xml_space_normalize(mode, &self.string.borrow()));
        }
    }

    fn make_span(
        &self,
        node: &Node,
        values: Rc<ComputedValues>,
        dx: f64,
        dy: f64,
        depth: usize,
        link_target: Option<String>,
    ) -> Span {
        self.ensure_normalized_string(node, &values);

        Span::new(
            self.space_normalized.borrow().as_ref().unwrap(),
            values,
            dx,
            dy,
            depth,
            link_target,
        )
    }

    fn to_chunks(
        &self,
        node: &Node,
        values: Rc<ComputedValues>,
        chunks: &mut [Chunk],
        dx: f64,
        dy: f64,
        depth: usize,
        link_target: Option<String>,
    ) {
        let span = self.make_span(node, values, dx, dy, depth, link_target);
        let num_chunks = chunks.len();
        assert!(num_chunks > 0);

        chunks[num_chunks - 1].spans.push(span);
    }

    pub fn get_string(&self) -> String {
        self.string.borrow().clone()
    }
}

#[derive(Default)]
pub struct Text {
    x: Length<Horizontal>,
    y: Length<Vertical>,
    dx: Length<Horizontal>,
    dy: Length<Vertical>,
}

impl Text {
    fn make_chunks(
        &self,
        node: &Node,
        acquired_nodes: &mut AcquiredNodes<'_>,
        cascaded: &CascadedValues<'_>,
        layout_context: &LayoutContext,
        x: f64,
        y: f64,
    ) -> Vec<Chunk> {
        let mut chunks = Vec::new();

        let values = cascaded.get();
        let params = NormalizeParams::new(values, &layout_context.viewport);

        chunks.push(Chunk::new(values, Some(x), Some(y)));

        let dx = self.dx.to_user(&params);
        let dy = self.dy.to_user(&params);

        children_to_chunks(
            &mut chunks,
            node,
            acquired_nodes,
            cascaded,
            layout_context,
            dx,
            dy,
            0,
            None,
        );
        chunks
    }
}

// Parse an (optionally) comma-separated list and just return the first element.
//
// From https://gitlab.gnome.org/GNOME/librsvg/-/issues/183, the current implementation
// of text layout only supports a single value for the x/y/dx/dy attributes.  However,
// we need to be able to parse values with multiple lengths.  So, we'll do that, but just
// use the first value from each attribute.
fn parse_list_and_extract_first<T: Copy + Default + Parse>(
    dest: &mut T,
    attr: QualName,
    value: &str,
    session: &Session,
) {
    let mut list: CommaSeparatedList<T, 0, 1024> = CommaSeparatedList(Vec::new());

    set_attribute(&mut list, attr.parse(value), session);
    if list.0.is_empty() {
        *dest = Default::default();
    } else {
        *dest = list.0[0]; // ignore all but the first element
    }
}

impl ElementTrait for Text {
    fn set_attributes(&mut self, attrs: &Attributes, session: &Session) {
        for (attr, value) in attrs.iter() {
            match attr.expanded() {
                expanded_name!("", "x") => {
                    parse_list_and_extract_first(&mut self.x, attr, value, session)
                }
                expanded_name!("", "y") => {
                    parse_list_and_extract_first(&mut self.y, attr, value, session)
                }
                expanded_name!("", "dx") => {
                    parse_list_and_extract_first(&mut self.dx, attr, value, session)
                }
                expanded_name!("", "dy") => {
                    parse_list_and_extract_first(&mut self.dy, attr, value, session)
                }
                _ => (),
            }
        }
    }

    fn layout(
        &self,
        node: &Node,
        acquired_nodes: &mut AcquiredNodes<'_>,
        cascaded: &CascadedValues<'_>,
        viewport: &Viewport,
        draw_ctx: &mut DrawingCtx,
        _clipping: bool,
    ) -> Result<Option<Layer>, Box<InternalRenderingError>> {
        let values = cascaded.get();
        let params = NormalizeParams::new(values, viewport);

        let elt = node.borrow_element();

        let session = draw_ctx.session().clone();

        let stacking_ctx = StackingContext::new(
            &session,
            acquired_nodes,
            &elt,
            values.transform(),
            None,
            values,
        );

        let layout_text = {
            let layout_context = LayoutContext {
                writing_mode: values.writing_mode(),
                font_options: draw_ctx.get_font_options(),
                viewport: *viewport,
                session: session.clone(),
            };

            let mut x = self.x.to_user(&params);
            let mut y = self.y.to_user(&params);

            let chunks = self.make_chunks(node, acquired_nodes, cascaded, &layout_context, x, y);

            let mut measured_chunks = Vec::new();
            for chunk in &chunks {
                measured_chunks.push(MeasuredChunk::from_chunk(&layout_context, chunk));
            }

            let mut positioned_chunks = Vec::new();
            for chunk in &measured_chunks {
                let chunk_x = chunk.x.unwrap_or(x);
                let chunk_y = chunk.y.unwrap_or(y);

                let positioned =
                    PositionedChunk::from_measured(&layout_context, chunk, chunk_x, chunk_y);

                x = positioned.next_chunk_x;
                y = positioned.next_chunk_y;

                positioned_chunks.push(positioned);
            }

            let mut layout_spans = Vec::new();
            for chunk in &positioned_chunks {
                for span in &chunk.spans {
                    layout_spans.push(span.layout(&layout_context, acquired_nodes));
                }
            }

            let text_extents: Option<Rect> = layout_spans
                .iter()
                .map(|span| span.extents)
                .reduce(|a, b| match (a, b) {
                    (None, None) => None,
                    (None, Some(b)) => Some(b),
                    (Some(a), None) => Some(a),
                    (Some(a), Some(b)) => Some(a.union(&b)),
                })
                .flatten();

            let mut text_spans = Vec::new();
            for span in layout_spans {
                let normalize_values = NormalizeValues::new(&span.values);

                let stroke_paint = span.stroke_paint.to_user_space(
                    &text_extents,
                    &layout_context.viewport,
                    &normalize_values,
                );
                let fill_paint = span.fill_paint.to_user_space(
                    &text_extents,
                    &layout_context.viewport,
                    &normalize_values,
                );

                let text_span = TextSpan {
                    layout: span.layout,
                    gravity: span.gravity,
                    extents: span.extents,
                    is_visible: span.is_visible,
                    x: span.x,
                    y: span.y,
                    paint_order: span.paint_order,
                    stroke: span.stroke,
                    stroke_paint,
                    fill_paint,
                    text_rendering: span.text_rendering,
                    link_target: span.link_target,
                };

                text_spans.push(text_span);
            }

            layout::Text {
                spans: text_spans,
                extents: text_extents,
            }
        };

        Ok(Some(Layer {
            kind: LayerKind::Text(Box::new(layout_text)),
            stacking_ctx,
        }))
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
                draw_ctx.draw_layer(layer.as_ref().unwrap(), acquired_nodes, clipping, viewport)
            })
    }
}

#[derive(Default)]
pub struct TRef {
    link: Option<NodeId>,
}

impl TRef {
    fn to_chunks(
        &self,
        node: &Node,
        acquired_nodes: &mut AcquiredNodes<'_>,
        cascaded: &CascadedValues<'_>,
        chunks: &mut Vec<Chunk>,
        depth: usize,
        layout_context: &LayoutContext,
    ) {
        if self.link.is_none() {
            return;
        }

        let link = self.link.as_ref().unwrap();

        let values = cascaded.get();
        if !values.is_displayed() {
            return;
        }

        if let Ok(acquired) = acquired_nodes.acquire(link) {
            let c = acquired.get();
            extract_chars_children_to_chunks_recursively(chunks, c, Rc::new(values.clone()), depth);
        } else {
            rsvg_log!(
                layout_context.session,
                "element {} references a nonexistent text source \"{}\"",
                node,
                link,
            );
        }
    }
}

fn extract_chars_children_to_chunks_recursively(
    chunks: &mut Vec<Chunk>,
    node: &Node,
    values: Rc<ComputedValues>,
    depth: usize,
) {
    for child in node.children() {
        let values = values.clone();

        if child.is_chars() {
            child
                .borrow_chars()
                .to_chunks(&child, values, chunks, 0.0, 0.0, depth, None)
        } else {
            extract_chars_children_to_chunks_recursively(chunks, &child, values, depth + 1)
        }
    }
}

impl ElementTrait for TRef {
    fn set_attributes(&mut self, attrs: &Attributes, _session: &Session) {
        self.link = attrs
            .iter()
            .find(|(attr, _)| attr.expanded() == expanded_name!(xlink "href"))
            // Unlike other elements which use `href` in SVG2 versus `xlink:href` in SVG1.1,
            // the <tref> element got removed in SVG2.  So, here we still use a match
            // against the full namespaced version of the attribute.
            .and_then(|(attr, value)| NodeId::parse(value).attribute(attr).ok());
    }
}

#[derive(Default)]
pub struct TSpan {
    x: Option<Length<Horizontal>>,
    y: Option<Length<Vertical>>,
    dx: Length<Horizontal>,
    dy: Length<Vertical>,
}

impl TSpan {
    fn to_chunks(
        &self,
        node: &Node,
        acquired_nodes: &mut AcquiredNodes<'_>,
        cascaded: &CascadedValues<'_>,
        layout_context: &LayoutContext,
        chunks: &mut Vec<Chunk>,
        dx: f64,
        dy: f64,
        depth: usize,
        link: Option<String>,
    ) {
        let values = cascaded.get();
        if !values.is_displayed() {
            return;
        }

        let params = NormalizeParams::new(values, &layout_context.viewport);

        let x = self.x.map(|l| l.to_user(&params));
        let y = self.y.map(|l| l.to_user(&params));

        let span_dx = dx + self.dx.to_user(&params);
        let span_dy = dy + self.dy.to_user(&params);

        if x.is_some() || y.is_some() {
            chunks.push(Chunk::new(values, x, y));
        }

        children_to_chunks(
            chunks,
            node,
            acquired_nodes,
            cascaded,
            layout_context,
            span_dx,
            span_dy,
            depth,
            link,
        );
    }
}

impl ElementTrait for TSpan {
    fn set_attributes(&mut self, attrs: &Attributes, session: &Session) {
        for (attr, value) in attrs.iter() {
            match attr.expanded() {
                expanded_name!("", "x") => {
                    parse_list_and_extract_first(&mut self.x, attr, value, session)
                }
                expanded_name!("", "y") => {
                    parse_list_and_extract_first(&mut self.y, attr, value, session)
                }
                expanded_name!("", "dx") => {
                    parse_list_and_extract_first(&mut self.dx, attr, value, session)
                }
                expanded_name!("", "dy") => {
                    parse_list_and_extract_first(&mut self.dy, attr, value, session)
                }
                _ => (),
            }
        }
    }
}

impl From<FontStyle> for pango::Style {
    fn from(s: FontStyle) -> pango::Style {
        match s {
            FontStyle::Normal => pango::Style::Normal,
            FontStyle::Italic => pango::Style::Italic,
            FontStyle::Oblique => pango::Style::Oblique,
        }
    }
}

impl From<FontVariant> for pango::Variant {
    fn from(v: FontVariant) -> pango::Variant {
        match v {
            FontVariant::Normal => pango::Variant::Normal,
            FontVariant::SmallCaps => pango::Variant::SmallCaps,
        }
    }
}

impl From<FontStretch> for pango::Stretch {
    fn from(s: FontStretch) -> pango::Stretch {
        match s {
            FontStretch::Normal => pango::Stretch::Normal,
            FontStretch::Wider => pango::Stretch::Expanded, // not quite correct
            FontStretch::Narrower => pango::Stretch::Condensed, // not quite correct
            FontStretch::UltraCondensed => pango::Stretch::UltraCondensed,
            FontStretch::ExtraCondensed => pango::Stretch::ExtraCondensed,
            FontStretch::Condensed => pango::Stretch::Condensed,
            FontStretch::SemiCondensed => pango::Stretch::SemiCondensed,
            FontStretch::SemiExpanded => pango::Stretch::SemiExpanded,
            FontStretch::Expanded => pango::Stretch::Expanded,
            FontStretch::ExtraExpanded => pango::Stretch::ExtraExpanded,
            FontStretch::UltraExpanded => pango::Stretch::UltraExpanded,
        }
    }
}

impl From<FontWeight> for pango::Weight {
    fn from(w: FontWeight) -> pango::Weight {
        pango::Weight::__Unknown(w.numeric_weight().into())
    }
}

impl From<Direction> for pango::Direction {
    fn from(d: Direction) -> pango::Direction {
        match d {
            Direction::Ltr => pango::Direction::Ltr,
            Direction::Rtl => pango::Direction::Rtl,
        }
    }
}

impl From<WritingMode> for pango::Direction {
    fn from(m: WritingMode) -> pango::Direction {
        use WritingMode::*;
        match m {
            HorizontalTb | VerticalRl | VerticalLr | LrTb | Lr | Tb | TbRl => pango::Direction::Ltr,
            RlTb | Rl => pango::Direction::Rtl,
        }
    }
}

impl From<WritingMode> for pango::Gravity {
    fn from(m: WritingMode) -> pango::Gravity {
        use WritingMode::*;
        match m {
            HorizontalTb | LrTb | Lr | RlTb | Rl => pango::Gravity::South,
            VerticalRl | Tb | TbRl => pango::Gravity::East,
            VerticalLr => pango::Gravity::West,
        }
    }
}

/// Constants with Unicode's directional formatting characters
///
/// <https://unicode.org/reports/tr9/#Directional_Formatting_Characters>
pub mod directional_formatting_characters {
    /// Left-to-Right Embedding
    ///
    /// Treat the following text as embedded left-to-right.
    pub const LRE: char = '\u{202a}';

    /// Right-to-Left Embedding
    ///
    /// Treat the following text as embedded right-to-left.
    pub const RLE: char = '\u{202b}';

    /// Left-to-Right Override
    ///
    /// Force following characters to be treated as strong left-to-right characters.
    pub const LRO: char = '\u{202d}';

    /// Right-to-Left Override
    ///
    /// Force following characters to be treated as strong right-to-left characters.
    pub const RLO: char = '\u{202e}';

    /// Pop Directional Formatting
    ///
    /// End the scope of the last LRE, RLE, RLO, or LRO.
    pub const PDF: char = '\u{202c}';

    /// Left-to-Right Isolate
    ///
    /// Treat the following text as isolated and left-to-right.
    pub const LRI: char = '\u{2066}';

    /// Right-to-Left Isolate
    ///
    /// Treat the following text as isolated and right-to-left.
    pub const RLI: char = '\u{2067}';

    /// First Strong Isolate
    ///
    /// Treat the following text as isolated and in the direction of its first strong
    /// directional character that is not inside a nested isolate.
    pub const FSI: char = '\u{2068}';

    /// Pop Directional Isolate
    ///
    /// End the scope of the last LRI, RLI, or FSI.
    pub const PDI: char = '\u{2069}';
}

/// Unicode control characters to be inserted when `unicode-bidi` is specified.
///
/// The `unicode-bidi` property is used to change the embedding of a text span within
/// another.  This struct contains slices with the control characters that must be
/// inserted into the text stream at the span's limits so that the bidi/shaping engine
/// will know what to do.
pub struct BidiControl {
    pub start: &'static [char],
    pub end: &'static [char],
}

impl BidiControl {
    /// Creates a `BidiControl` from the properties that determine it.
    ///
    /// See the table titled "Bidi control codes injected..." in
    /// <https://www.w3.org/TR/css-writing-modes-3/#unicode-bidi>
    #[rustfmt::skip]
    pub fn from_unicode_bidi_and_direction(unicode_bidi: UnicodeBidi, direction: Direction) -> BidiControl {
        use UnicodeBidi::*;
        use Direction::*;
        use directional_formatting_characters::*;

        let (start, end) = match (unicode_bidi, direction) {
            (Normal,          _)   => (&[][..],         &[][..]),
            (Embed,           Ltr) => (&[LRE][..],      &[PDF][..]),
            (Embed,           Rtl) => (&[RLE][..],      &[PDF][..]),
            (Isolate,         Ltr) => (&[LRI][..],      &[PDI][..]),
            (Isolate,         Rtl) => (&[RLI][..],      &[PDI][..]),
            (BidiOverride,    Ltr) => (&[LRO][..],      &[PDF][..]),
            (BidiOverride,    Rtl) => (&[RLO][..],      &[PDF][..]),
            (IsolateOverride, Ltr) => (&[FSI, LRO][..], &[PDF, PDI][..]),
            (IsolateOverride, Rtl) => (&[FSI, RLO][..], &[PDF, PDI][..]),
            (Plaintext,       Ltr) => (&[FSI][..],      &[PDI][..]),
            (Plaintext,       Rtl) => (&[FSI][..],      &[PDI][..]),
        };

        BidiControl { start, end }
    }
}

/// Prepends and appends Unicode directional formatting characters.
fn wrap_with_direction_control_chars(s: &str, bidi_control: &BidiControl) -> String {
    let mut res =
        String::with_capacity(s.len() + bidi_control.start.len() + bidi_control.end.len());

    for &ch in bidi_control.start {
        res.push(ch);
    }

    res.push_str(s);

    for &ch in bidi_control.end {
        res.push(ch);
    }

    res
}

/// Returns `None` if the layout would be invalid due to, for example, out-of-bounds font sizes.
fn create_pango_layout(
    layout_context: &LayoutContext,
    props: &FontProperties,
    text: &str,
) -> Option<pango::Layout> {
    let pango_context = create_pango_context(&layout_context.font_options);

    if let XmlLang(Some(ref lang)) = props.xml_lang {
        pango_context.set_language(Some(&pango::Language::from_string(lang.as_str())));
    }

    pango_context.set_base_gravity(pango::Gravity::from(layout_context.writing_mode));

    match (props.unicode_bidi, props.direction) {
        (UnicodeBidi::BidiOverride, _) | (UnicodeBidi::Embed, _) => {
            pango_context.set_base_dir(pango::Direction::from(props.direction));
        }

        (_, direction) if direction != Direction::Ltr => {
            pango_context.set_base_dir(pango::Direction::from(direction));
        }

        (_, _) => {
            pango_context.set_base_dir(pango::Direction::from(layout_context.writing_mode));
        }
    }

    let layout = pango::Layout::new(&pango_context);

    let font_size = PangoUnits::from_pixels(props.font_size);
    let letter_spacing = PangoUnits::from_pixels(props.letter_spacing);

    if font_size.is_none() {
        rsvg_log!(
            &layout_context.session,
            "font-size {} is out of bounds; ignoring span",
            props.font_size
        );
    }

    if letter_spacing.is_none() {
        rsvg_log!(
            &layout_context.session,
            "letter-spacing {} is out of bounds; ignoring span",
            props.letter_spacing
        );
    }

    if let (Some(font_size), Some(letter_spacing)) = (font_size, letter_spacing) {
        let attr_list = pango::AttrList::new();
        add_pango_attributes(&attr_list, props, 0, text.len(), font_size, letter_spacing);

        layout.set_attributes(Some(&attr_list));
        layout.set_text(text);
        layout.set_auto_dir(false);

        Some(layout)
    } else {
        None
    }
}

/// Adds Pango attributes, suitable for a span of text, to an `AttrList`.
fn add_pango_attributes(
    attr_list: &pango::AttrList,
    props: &FontProperties,
    start_index: usize,
    end_index: usize,
    font_size: PangoUnits,
    letter_spacing: PangoUnits,
) {
    let start_index = u32::try_from(start_index).expect("Pango attribute index must fit in u32");
    let end_index = u32::try_from(end_index).expect("Pango attribute index must fit in u32");
    assert!(start_index <= end_index);

    let mut attributes = Vec::new();

    let mut font_desc = pango::FontDescription::new();
    font_desc.set_family(props.font_family.as_str());
    font_desc.set_style(pango::Style::from(props.font_style));

    font_desc.set_variant(pango::Variant::from(props.font_variant));

    font_desc.set_weight(pango::Weight::from(props.font_weight));
    font_desc.set_stretch(pango::Stretch::from(props.font_stretch));

    font_desc.set_size(font_size.0);

    attributes.push(pango::AttrFontDesc::new(&font_desc).upcast());

    attributes.push(pango::AttrInt::new_letter_spacing(letter_spacing.0).upcast());

    if props.text_decoration.overline {
        attributes.push(pango::AttrInt::new_overline(pango::Overline::Single).upcast());
    }

    if props.text_decoration.underline {
        attributes.push(pango::AttrInt::new_underline(pango::Underline::Single).upcast());
    }

    if props.text_decoration.strike {
        attributes.push(pango::AttrInt::new_strikethrough(true).upcast());
    }

    // Set the range in each attribute

    for attr in &mut attributes {
        attr.set_start_index(start_index);
        attr.set_end_index(end_index);
    }

    // Add the attributes to the attr_list

    for attr in attributes {
        attr_list.insert(attr);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chars_default() {
        let c = Chars::default();
        assert!(c.is_empty());
        assert!(c.space_normalized.borrow().is_none());
    }

    #[test]
    fn chars_new() {
        let example = "Test 123";
        let c = Chars::new(example);
        assert_eq!(c.get_string(), example);
        assert!(c.space_normalized.borrow().is_none());
    }

    // This is called _horizontal because the property value in "CSS Writing Modes 3"
    // is `horizontal-tb`.  Eventually we will support that and this will make more sense.
    #[test]
    fn adjusted_advance_horizontal_ltr() {
        use Direction::*;
        use TextAnchor::*;

        assert_eq!(
            text_anchor_offset(
                Start,
                Ltr,
                WritingMode::Lr,
                Rect::from_size(1.0, 2.0).translate((5.0, 6.0))
            ),
            (-5.0, 0.0)
        );

        assert_eq!(
            text_anchor_offset(
                Middle,
                Ltr,
                WritingMode::Lr,
                Rect::from_size(1.0, 2.0).translate((5.0, 6.0))
            ),
            (-5.5, 0.0)
        );

        assert_eq!(
            text_anchor_offset(
                End,
                Ltr,
                WritingMode::Lr,
                Rect::from_size(1.0, 2.0).translate((5.0, 6.0))
            ),
            (-6.0, 0.0)
        );
    }

    #[test]
    fn adjusted_advance_horizontal_rtl() {
        use Direction::*;
        use TextAnchor::*;

        assert_eq!(
            text_anchor_offset(
                Start,
                Rtl,
                WritingMode::Rl,
                Rect::from_size(1.0, 2.0).translate((5.0, 6.0))
            ),
            (-6.0, 0.0)
        );
        assert_eq!(
            text_anchor_offset(
                Middle,
                Rtl,
                WritingMode::Rl,
                Rect::from_size(1.0, 2.0).translate((5.0, 6.0))
            ),
            (-5.5, 0.0)
        );
        assert_eq!(
            text_anchor_offset(
                TextAnchor::End,
                Direction::Rtl,
                WritingMode::Rl,
                Rect::from_size(1.0, 2.0).translate((5.0, 6.0))
            ),
            (-5.0, 0.0)
        );
    }

    // This is called _vertical because "CSS Writing Modes 3" has both `vertical-rl` (East
    // Asia), and `vertical-lr` (Manchu, Mongolian), but librsvg does not support block
    // flow direction properly yet.  Eventually we will support that and this will make
    // more sense.
    #[test]
    fn adjusted_advance_vertical() {
        use Direction::*;
        use TextAnchor::*;

        assert_eq!(
            text_anchor_offset(Start, Ltr, WritingMode::Tb, Rect::from_size(2.0, 4.0)),
            (0.0, 0.0)
        );

        assert_eq!(
            text_anchor_offset(Middle, Ltr, WritingMode::Tb, Rect::from_size(2.0, 4.0)),
            (0.0, -2.0)
        );

        assert_eq!(
            text_anchor_offset(End, Ltr, WritingMode::Tb, Rect::from_size(2.0, 4.0)),
            (0.0, -4.0)
        );
    }

    #[test]
    fn pango_units_works() {
        assert_eq!(PangoUnits::from_pixels(10.0).unwrap().0, pango::SCALE * 10);
    }

    #[test]
    fn pango_units_detects_overflow() {
        assert!(PangoUnits::from_pixels(1e7).is_none());
    }
}
