//! The main context structure which drives the drawing process.

use float_cmp::approx_eq;
use gio::prelude::*;
use pango::prelude::FontMapExt;
use regex::{Captures, Regex};
use std::cell::RefCell;
use std::convert::TryFrom;
use std::rc::Rc;
use std::{borrow::Cow, sync::OnceLock};

use crate::accept_language::UserLanguage;
use crate::bbox::BoundingBox;
use crate::cairo_path::CairoPath;
use crate::color::{Color, color_to_rgba};
use crate::coord_units::CoordUnits;
use crate::document::{AcquiredNodes, NodeId, RenderingOptions};
use crate::dpi::Dpi;
use crate::element::{DrawResult, Element, ElementData};
use crate::error::{AcquireError, ImplementationLimit, InternalRenderingError, InvalidTransform};
use crate::filters::{self, FilterPlan, FilterSpec, InputRequirements};
use crate::float_eq_cairo::ApproxEqCairo;
use crate::gradient::{GradientVariant, SpreadMethod, UserSpaceGradient};
use crate::layout::{
    Filter, Group, Image, Layer, LayerKind, LayoutViewport, Shape, StackingContext, Stroke, Text,
    TextSpan,
};
use crate::length::*;
use crate::limits;
use crate::marker;
use crate::node::{CascadedValues, Node, NodeBorrow, NodeDraw};
use crate::paint_server::{PaintSource, UserSpacePaintSource};
use crate::pattern::UserSpacePattern;
use crate::properties::{
    ClipRule, ComputedValues, FillRule, ImageRendering, MaskType, MixBlendMode, Opacity,
    PaintTarget, ShapeRendering, StrokeLinecap, StrokeLinejoin, TextRendering,
};
use crate::rect::{IRect, Rect, rect_to_transform};
use crate::rsvg_log;
use crate::session::Session;
use crate::surface_utils::shared_surface::{
    ExclusiveImageSurface, Interpolation, SharedImageSurface, SurfaceType,
};
use crate::transform::{Transform, ValidTransform};
use crate::unit_interval::UnitInterval;
use crate::viewbox::ViewBox;
use crate::{borrow_element_as, is_element_of_type};

/// Opaque font options for a DrawingCtx.
///
/// This is used for DrawingCtx::create_pango_context.
pub struct FontOptions {
    options: cairo::FontOptions,
}

/// Set path on the cairo context, or clear it.
/// This helper object keeps track whether the path has been set already,
/// so that it isn't recalculated every so often.
struct PathHelper<'a> {
    cr: &'a cairo::Context,
    transform: ValidTransform,
    cairo_path: &'a CairoPath,
    has_path: Option<bool>,
}

impl<'a> PathHelper<'a> {
    pub fn new(
        cr: &'a cairo::Context,
        transform: ValidTransform,
        cairo_path: &'a CairoPath,
    ) -> Self {
        PathHelper {
            cr,
            transform,
            cairo_path,
            has_path: None,
        }
    }

    pub fn set(&mut self) -> Result<(), Box<InternalRenderingError>> {
        match self.has_path {
            Some(false) | None => {
                self.has_path = Some(true);
                self.cr.set_matrix(self.transform.into());
                self.cairo_path.to_cairo_context(self.cr)
            }
            Some(true) => Ok(()),
        }
    }

    pub fn unset(&mut self) {
        match self.has_path {
            Some(true) | None => {
                self.has_path = Some(false);
                self.cr.new_path();
            }
            Some(false) => {}
        }
    }
}

/// Holds the size of the current viewport in the user's coordinate system.
#[derive(Clone, Copy)]
pub struct Viewport {
    pub dpi: Dpi,

    /// Corners of the current coordinate space.
    pub vbox: ViewBox,

    /// The viewport's coordinate system, or "user coordinate system" in SVG terms.
    pub transform: ValidTransform,
}

impl Viewport {
    /// FIXME: this is just used in Handle::with_height_to_user(), and in length.rs's test suite.
    /// Find a way to do this without involving a default identity transform.
    pub fn new(dpi: Dpi, view_box_width: f64, view_box_height: f64) -> Viewport {
        Viewport {
            dpi,
            vbox: ViewBox::from(Rect::from_size(view_box_width, view_box_height)),
            transform: Default::default(),
        }
    }

    /// Creates a new viewport suitable for a certain kind of units.
    ///
    /// For `objectBoundingBox`, CSS lengths which are in percentages
    /// refer to the size of the current viewport.  Librsvg implements
    /// that by keeping the same current transformation matrix, and
    /// setting a viewport size of (1.0, 1.0).
    ///
    /// For `userSpaceOnUse`, we just duplicate the current viewport,
    /// since that kind of units means to use the current coordinate
    /// system unchanged.
    pub fn with_units(&self, units: CoordUnits) -> Viewport {
        match units {
            CoordUnits::ObjectBoundingBox => Viewport {
                dpi: self.dpi,
                vbox: ViewBox::from(Rect::from_size(1.0, 1.0)),
                transform: self.transform,
            },

            CoordUnits::UserSpaceOnUse => Viewport {
                dpi: self.dpi,
                vbox: self.vbox,
                transform: self.transform,
            },
        }
    }

    /// Returns a viewport with a new size for normalizing `Length` values.
    pub fn with_view_box(&self, width: f64, height: f64) -> Viewport {
        Viewport {
            dpi: self.dpi,
            vbox: ViewBox::from(Rect::from_size(width, height)),
            transform: self.transform,
        }
    }

    /// Copies the viewport, but just uses a new transform.
    ///
    /// This is used when we are about to draw in a temporary surface: the transform for
    /// that surface is computed independenly of the one in the current viewport.
    pub fn with_explicit_transform(&self, transform: ValidTransform) -> Viewport {
        Viewport {
            dpi: self.dpi,
            vbox: self.vbox,
            transform,
        }
    }

    pub fn with_composed_transform(
        &self,
        transform: ValidTransform,
    ) -> Result<Viewport, InvalidTransform> {
        let composed_transform =
            ValidTransform::try_from((*self.transform).pre_transform(&transform))?;

        Ok(Viewport {
            dpi: self.dpi,
            vbox: self.vbox,
            transform: composed_transform,
        })
    }

    pub fn empty_bbox(&self) -> Box<BoundingBox> {
        Box::new(BoundingBox::new().with_transform(*self.transform))
    }
}

/// Values that stay constant during rendering with a DrawingCtx.
#[derive(Clone)]
pub struct RenderingConfiguration {
    pub dpi: Dpi,
    pub cancellable: Option<gio::Cancellable>,
    pub user_language: UserLanguage,
    pub svg_nesting: SvgNesting,
    pub measuring: bool,
    pub testing: bool,
}

pub struct DrawingCtx {
    session: Session,

    initial_viewport: Viewport,

    cr_stack: Rc<RefCell<Vec<cairo::Context>>>,
    cr: cairo::Context,

    drawsub_stack: Vec<Node>,

    config: RenderingConfiguration,

    /// Depth of nested layers while drawing.
    ///
    /// We use this to set a hard limit on how many nested layers there can be, to avoid
    /// malicious SVGs that would cause unbounded stack consumption.
    recursion_depth: u16,

    /// Cheap hack to monitor stack usage in recursive calls.
    ///
    /// We store the address of a local variable when first creating the DrawingCtx, and
    /// then subtract it from another local variable at trace points.  See the print_stack_depth()
    /// function.
    stack_ptr: *const u8,
}

pub enum DrawingMode {
    LimitToStack { node: Node, root: Node },

    OnlyNode(Node),
}

/// Whether an SVG document is being rendered standalone or referenced from an `<image>` element.
///
/// Normally, the coordinate system used when rendering a toplevel SVG is determined from the
/// initial viewport and the `<svg>` element's `viewBox` and `preserveAspectRatio` attributes.
/// However, when an SVG document is referenced from an `<image>` element, as in `<image href="foo.svg"/>`,
/// its `preserveAspectRatio` needs to be ignored so that the one from the `<image>` element can
/// be used instead.  This lets the parent document (the one with the `<image>` element) specify
/// how it wants the child SVG to be scaled into the viewport.
#[derive(Copy, Clone)]
pub enum SvgNesting {
    Standalone,
    ReferencedFromImageElement,
}

/// The toplevel drawing routine.
///
/// This creates a DrawingCtx internally and starts drawing at the specified `node`.
pub fn draw_tree(
    session: Session,
    mode: DrawingMode,
    cr: &cairo::Context,
    viewport_rect: Rect,
    config: RenderingConfiguration,
    acquired_nodes: &mut AcquiredNodes<'_>,
) -> DrawResult {
    let (drawsub_stack, node) = match mode {
        DrawingMode::LimitToStack { node, root } => (node.ancestors().collect(), root),

        DrawingMode::OnlyNode(node) => (Vec::new(), node),
    };

    let cascaded = CascadedValues::new_from_node(&node);

    // Preserve the user's transform and use it for the outermost bounding box.  All bounds/extents
    // will be converted to this transform in the end.
    let user_transform = Transform::from(cr.matrix());
    let mut user_bbox = Box::new(BoundingBox::new().with_transform(user_transform));

    // https://www.w3.org/TR/SVG2/coords.html#InitialCoordinateSystem
    //
    // "For the outermost svg element, the SVG user agent must
    // determine an initial viewport coordinate system and an
    // initial user coordinate system such that the two
    // coordinates systems are identical. The origin of both
    // coordinate systems must be at the origin of the SVG
    // viewport."
    //
    // "... the initial viewport coordinate system (and therefore
    // the initial user coordinate system) must have its origin at
    // the top/left of the viewport"

    // Translate so (0, 0) is at the viewport's upper-left corner.
    let transform = user_transform.pre_translate(viewport_rect.x0, viewport_rect.y0);

    // Here we exit immediately if the transform is not valid, since we are in the
    // toplevel drawing function.  Downstream cases would simply not render the current
    // element and ignore the error.
    let valid_transform = ValidTransform::try_from(transform)?;
    cr.set_matrix(valid_transform.into());

    // Per the spec, so the viewport has (0, 0) as upper-left.
    let viewport_rect = viewport_rect.translate((-viewport_rect.x0, -viewport_rect.y0));
    let initial_viewport = Viewport {
        dpi: config.dpi,
        vbox: ViewBox::from(viewport_rect),
        transform: valid_transform,
    };

    let mut draw_ctx = DrawingCtx::new(session, cr, &initial_viewport, config, drawsub_stack);

    let content_bbox = draw_ctx.draw_node_from_stack(
        &node,
        acquired_nodes,
        &cascaded,
        &initial_viewport,
        false,
    )?;

    user_bbox.insert(&content_bbox);

    if draw_ctx.is_rendering_cancelled() {
        Err(InternalRenderingError::Cancelled)?
    } else {
        Ok(user_bbox)
    }
}

pub fn with_saved_cr<O, F>(cr: &cairo::Context, f: F) -> Result<O, Box<InternalRenderingError>>
where
    F: FnOnce() -> Result<O, Box<InternalRenderingError>>,
{
    cr.save()?;
    match f() {
        Ok(o) => {
            cr.restore()?;
            Ok(o)
        }

        Err(e) => Err(e),
    }
}

impl Drop for DrawingCtx {
    fn drop(&mut self) {
        self.cr_stack.borrow_mut().pop();
    }
}

const CAIRO_TAG_LINK: &str = "Link";

impl DrawingCtx {
    pub fn new(
        session: Session,
        cr: &cairo::Context,
        initial_viewport: &Viewport,
        config: RenderingConfiguration,
        drawsub_stack: Vec<Node>,
    ) -> DrawingCtx {
        let stack_variable: u8 = 42;

        DrawingCtx {
            session,
            initial_viewport: *initial_viewport,
            cr_stack: Rc::new(RefCell::new(Vec::new())),
            cr: cr.clone(),
            drawsub_stack,
            config,
            recursion_depth: 0,

            // We store this pointer to pinpoint the stack depth at this point in the program.
            // Later, in the print_stack_depth() function, we'll use it to subtract from the
            // current stack pointer.  This is a cheap hack to monitor how much stack space
            // is consumed between recursive calls to the drawing machinery.
            //
            // The pointer is otherwise meaningless and should never be dereferenced.
            stack_ptr: &stack_variable,
        }
    }

    /// Copies a `DrawingCtx` for temporary use on a Cairo surface.
    ///
    /// `DrawingCtx` maintains state using during the drawing process, and sometimes we
    /// would like to use that same state but on a different Cairo surface and context
    /// than the ones being used on `self`.  This function copies the `self` state into a
    /// new `DrawingCtx`, and ties the copied one to the supplied `cr`.
    ///
    /// Note that if this function is called, it means that a temporary surface is being used.
    /// That surface needs a viewport which starts with a special transform; see
    /// [`Viewport::with_explicit_transform`] and how it is used elsewhere.
    fn nested(&self, cr: cairo::Context) -> Box<DrawingCtx> {
        let cr_stack = self.cr_stack.clone();

        cr_stack.borrow_mut().push(self.cr.clone());

        Box::new(DrawingCtx {
            session: self.session.clone(),
            initial_viewport: self.initial_viewport,
            cr_stack,
            cr,
            drawsub_stack: self.drawsub_stack.clone(),
            config: self.config.clone(),
            recursion_depth: self.recursion_depth,
            stack_ptr: self.stack_ptr,
        })
    }

    pub fn session(&self) -> &Session {
        &self.session
    }

    pub fn print_stack_depth(&self, place_name: &str) {
        let stack_variable: u8 = 42;

        let current_stack_ptr = &stack_variable;

        let stack_size = unsafe { self.stack_ptr.byte_offset_from(current_stack_ptr) };
        rsvg_log!(
            self.session,
            "{place_name}: recursion_depth={}, stack_depth={stack_size}",
            self.recursion_depth
        );
    }

    /// Returns the `RenderingOptions` being used for rendering.
    pub fn rendering_options(&self, svg_nesting: SvgNesting) -> RenderingOptions {
        RenderingOptions {
            dpi: self.config.dpi,
            cancellable: self.config.cancellable.clone(),
            user_language: self.config.user_language.clone(),
            svg_nesting,
            testing: self.config.testing,
        }
    }

    pub fn user_language(&self) -> &UserLanguage {
        &self.config.user_language
    }

    pub fn toplevel_viewport(&self) -> Rect {
        *self.initial_viewport.vbox
    }

    /// Gets the transform that will be used on the target surface,
    /// whether using an isolated stacking context or not.
    fn get_transform_for_stacking_ctx(
        &self,
        viewport: &Viewport,
        stacking_ctx: &StackingContext,
        clipping: bool,
    ) -> Result<ValidTransform, Box<InternalRenderingError>> {
        if stacking_ctx.should_isolate() && !clipping {
            let affines = CompositingAffines::new(
                *viewport.transform,
                *self.initial_viewport.transform,
                self.cr_stack.borrow().len(),
            );

            Ok(ValidTransform::try_from(affines.for_temporary_surface)?)
        } else {
            Ok(viewport.transform)
        }
    }

    pub fn svg_nesting(&self) -> SvgNesting {
        self.config.svg_nesting
    }

    pub fn is_measuring(&self) -> bool {
        self.config.measuring
    }

    pub fn is_testing(&self) -> bool {
        self.config.testing
    }

    fn size_for_temporary_surface(&self) -> (i32, i32) {
        let rect = self.toplevel_viewport();

        let (viewport_width, viewport_height) = (rect.width(), rect.height());

        let (width, height) = self
            .initial_viewport
            .transform
            .transform_distance(viewport_width, viewport_height);

        // We need a size in whole pixels, so use ceil() to ensure the whole viewport fits
        // into the temporary surface.
        (width.ceil().abs() as i32, height.ceil().abs() as i32)
    }

    pub fn create_surface_for_toplevel_viewport(
        &self,
    ) -> Result<cairo::ImageSurface, Box<InternalRenderingError>> {
        let (w, h) = self.size_for_temporary_surface();

        Ok(cairo::ImageSurface::create(cairo::Format::ARgb32, w, h)?)
    }

    fn create_similar_surface_for_toplevel_viewport(
        &self,
        surface: &cairo::Surface,
    ) -> Result<cairo::Surface, Box<InternalRenderingError>> {
        let (w, h) = self.size_for_temporary_surface();

        Ok(cairo::Surface::create_similar(
            surface,
            cairo::Content::ColorAlpha,
            w,
            h,
        )?)
    }

    /// Creates a new coordinate space inside a viewport and sets a clipping rectangle.
    ///
    /// Returns the new viewport with the new coordinate space, or `None` if the transform
    /// inside the new viewport turned out to be invalid.  In this case, the caller can simply
    /// not render the object in question.
    fn push_new_viewport(
        &self,
        current_viewport: &Viewport,
        layout_viewport: &LayoutViewport,
    ) -> Option<Viewport> {
        let LayoutViewport {
            geometry,
            vbox,
            preserve_aspect_ratio,
            overflow,
        } = *layout_viewport;

        if !overflow.overflow_allowed() || (vbox.is_some() && preserve_aspect_ratio.is_slice()) {
            clip_to_rectangle(&self.cr, &current_viewport.transform, &geometry);
        }

        preserve_aspect_ratio
            .viewport_to_viewbox_transform(vbox, &geometry)
            .unwrap_or_else(|_e| {
                match vbox {
                    None => unreachable!(
                        "viewport_to_viewbox_transform only returns errors when vbox != None"
                    ),
                    Some(v) => {
                        rsvg_log!(
                            self.session,
                            "ignoring viewBox ({}, {}, {}, {}) since it is not usable",
                            v.x0,
                            v.y0,
                            v.width(),
                            v.height()
                        );
                    }
                }
                None
            })
            .and_then(|t| {
                let transform =
                    ValidTransform::try_from(current_viewport.transform.pre_transform(&t)).ok()?;

                Some(Viewport {
                    dpi: self.config.dpi,
                    vbox: vbox.unwrap_or(current_viewport.vbox),
                    transform,
                })
            })
    }

    fn clip_to_node(
        &mut self,
        clip_node: &Option<Node>,
        acquired_nodes: &mut AcquiredNodes<'_>,
        viewport: &Viewport,
        bbox: &BoundingBox,
    ) -> Result<(), Box<InternalRenderingError>> {
        if clip_node.is_none() {
            return Ok(());
        }

        let node = clip_node.as_ref().unwrap();
        let units = borrow_element_as!(node, ClipPath).get_units();

        if let Ok(transform) = rect_to_transform(&bbox.rect, units) {
            let cascaded = CascadedValues::new_from_node(node);
            let values = cascaded.get();

            let node_transform = values.transform().post_transform(&transform);
            let transform_for_clip = ValidTransform::try_from(node_transform)?;

            let clip_viewport = viewport.with_composed_transform(transform_for_clip)?;

            for child in node.children().filter(|c| {
                c.is_element() && element_can_be_used_inside_clip_path(&c.borrow_element())
            }) {
                child.draw(
                    acquired_nodes,
                    &CascadedValues::clone_with_node(&cascaded, &child),
                    &clip_viewport,
                    self,
                    true,
                )?;
            }

            self.cr.clip();
        }

        Ok(())
    }

    fn generate_cairo_mask(
        &mut self,
        mask_node: &Node,
        viewport: &Viewport,
        transform: Transform,
        bbox: &BoundingBox,
        acquired_nodes: &mut AcquiredNodes<'_>,
    ) -> Result<Option<cairo::ImageSurface>, Box<InternalRenderingError>> {
        if bbox.rect.is_none() {
            // The node being masked is empty / doesn't have a
            // bounding box, so there's nothing to mask!
            return Ok(None);
        }

        let _mask_acquired = match acquired_nodes.acquire_ref(mask_node) {
            Ok(n) => n,

            Err(AcquireError::CircularReference(_)) => {
                rsvg_log!(self.session, "circular reference in element {}", mask_node);
                return Ok(None);
            }

            _ => unreachable!(),
        };

        let mask_element = mask_node.borrow_element();
        let mask = borrow_element_as!(mask_node, Mask);

        let cascaded = CascadedValues::new_from_node(mask_node);
        let values = cascaded.get();

        let mask_units = mask.get_units();

        let mask_rect = {
            let params = NormalizeParams::new(values, &viewport.with_units(mask_units));
            mask.get_rect(&params)
        };

        let transform_for_mask =
            ValidTransform::try_from(values.transform().post_transform(&transform))?;

        let bbtransform = if let Ok(t) = rect_to_transform(&bbox.rect, mask_units)
            .map_err(|_: ()| InvalidTransform)
            .and_then(ValidTransform::try_from)
        {
            t
        } else {
            return Ok(None);
        };

        let mask_content_surface = self.create_surface_for_toplevel_viewport()?;

        // Use a scope because mask_cr needs to release the
        // reference to the surface before we access the pixels
        {
            let mask_cr = cairo::Context::new(&mask_content_surface)?;

            let clip_rect = (*bbtransform).transform_rect(&mask_rect);
            clip_to_rectangle(&mask_cr, &transform_for_mask, &clip_rect);

            let mask_viewport = if mask.get_content_units() == CoordUnits::ObjectBoundingBox {
                viewport
                    .with_units(mask.get_content_units())
                    .with_explicit_transform(transform_for_mask)
                    .with_composed_transform(bbtransform)?
            } else {
                viewport
                    .with_units(mask.get_content_units())
                    .with_explicit_transform(transform_for_mask)
            };

            let mut mask_draw_ctx = self.nested(mask_cr);

            let stacking_ctx = Box::new(StackingContext::new(
                self.session(),
                acquired_nodes,
                &mask_element,
                Transform::identity(),
                None,
                values,
            ));

            rsvg_log!(self.session, "(mask {}", mask_element);

            let res = mask_draw_ctx.with_discrete_layer(
                &stacking_ctx,
                acquired_nodes,
                &mask_viewport,
                None,
                false,
                &mut |an, dc, new_viewport| {
                    mask_node.draw_children(an, &cascaded, new_viewport, dc, false)
                },
            );

            rsvg_log!(self.session, ")");

            res?;
        }

        let tmp = SharedImageSurface::wrap(mask_content_surface, SurfaceType::SRgb)?;

        let mask_result = match values.mask_type() {
            MaskType::Luminance => tmp.to_luminance_mask()?,
            MaskType::Alpha => tmp.extract_alpha(IRect::from_size(tmp.width(), tmp.height()))?,
        };

        let mask = mask_result.into_image_surface()?;

        Ok(Some(mask))
    }

    fn is_rendering_cancelled(&self) -> bool {
        match &self.config.cancellable {
            None => false,
            Some(cancellable) => cancellable.is_cancelled(),
        }
    }

    /// Checks whether the rendering has been cancelled in the middle.
    ///
    /// If so, returns an Err.  This is used from [`DrawingCtx::with_discrete_layer`] to
    /// exit early instead of proceeding with rendering.
    fn check_cancellation(&self) -> Result<(), Box<InternalRenderingError>> {
        if self.is_rendering_cancelled() {
            return Err(Box::new(InternalRenderingError::Cancelled));
        }

        Ok(())
    }

    fn check_layer_nesting_depth(&self) -> Result<(), Box<InternalRenderingError>> {
        if self.recursion_depth > limits::MAX_LAYER_NESTING_DEPTH {
            return Err(Box::new(InternalRenderingError::LimitExceeded(
                ImplementationLimit::MaximumLayerNestingDepthExceeded,
            )));
        }

        Ok(())
    }

    fn filter_current_surface(
        &mut self,
        acquired_nodes: &mut AcquiredNodes<'_>,
        filter: &Filter,
        viewport: &Viewport,
        element_name: &str,
        bbox: &BoundingBox,
    ) -> Result<cairo::Surface, Box<InternalRenderingError>> {
        let surface_to_filter = SharedImageSurface::copy_from_surface(
            &cairo::ImageSurface::try_from(self.cr.target()).unwrap(),
        )?;

        let stroke_paint_source = Rc::new(filter.stroke_paint_source.to_user_space(
            &bbox.rect,
            viewport,
            &filter.normalize_values,
        ));
        let fill_paint_source = Rc::new(filter.fill_paint_source.to_user_space(
            &bbox.rect,
            viewport,
            &filter.normalize_values,
        ));

        // Filter functions (like "blend()", not the <filter> element) require
        // being resolved in userSpaceonUse units, since that is the default
        // for primitive_units.  So, get the corresponding NormalizeParams
        // here and pass them down.
        let user_space_params = NormalizeParams::from_values(
            &filter.normalize_values,
            &viewport.with_units(CoordUnits::UserSpaceOnUse),
        );

        let filtered_surface = self
            .run_filters(
                viewport,
                surface_to_filter,
                filter,
                acquired_nodes,
                element_name,
                &user_space_params,
                stroke_paint_source,
                fill_paint_source,
                bbox,
            )?
            .into_image_surface()?;

        let generic_surface: &cairo::Surface = &filtered_surface; // deref to Surface

        Ok(generic_surface.clone())
    }

    fn draw_in_optional_new_viewport(
        &mut self,
        acquired_nodes: &mut AcquiredNodes<'_>,
        viewport: &Viewport,
        layout_viewport: &Option<LayoutViewport>,
        draw_fn: &mut dyn FnMut(&mut AcquiredNodes<'_>, &mut DrawingCtx, &Viewport) -> DrawResult,
    ) -> DrawResult {
        if let Some(layout_viewport) = layout_viewport.as_ref() {
            if let Some(new_viewport) = self.push_new_viewport(viewport, layout_viewport) {
                draw_fn(acquired_nodes, self, &new_viewport)
            } else {
                Ok(viewport.empty_bbox())
            }
        } else {
            draw_fn(acquired_nodes, self, viewport)
        }
    }

    fn draw_layer_internal(
        &mut self,
        stacking_ctx: &StackingContext,
        acquired_nodes: &mut AcquiredNodes<'_>,
        viewport: &Viewport,
        layout_viewport: Option<LayoutViewport>,
        clipping: bool,
        draw_fn: &mut dyn FnMut(&mut AcquiredNodes<'_>, &mut DrawingCtx, &Viewport) -> DrawResult,
    ) -> DrawResult {
        self.print_stack_depth("DrawingCtx::draw_layer_internal entry");

        let stacking_ctx_transform = ValidTransform::try_from(stacking_ctx.transform)?;

        let viewport = viewport.with_composed_transform(stacking_ctx_transform)?;

        if clipping {
            self.draw_in_optional_new_viewport(acquired_nodes, &viewport, &layout_viewport, draw_fn)
        } else {
            with_saved_cr(&self.cr.clone(), || {
                self.link_tag_begin(&stacking_ctx.link_target);

                if let Some(rect) = stacking_ctx.clip_rect.as_ref() {
                    clip_to_rectangle(&self.cr, &viewport.transform, rect);
                }

                // Here we are clipping in user space, so the bbox doesn't matter
                self.clip_to_node(
                    &stacking_ctx.clip_in_user_space,
                    acquired_nodes,
                    &viewport,
                    &viewport.empty_bbox(),
                )?;

                let res = if stacking_ctx.should_isolate() {
                    self.print_stack_depth("DrawingCtx::draw_layer_internal should_isolate=true");

                    // Compute our assortment of affines

                    let affines = Box::new(CompositingAffines::new(
                        *viewport.transform,
                        *self.initial_viewport.transform,
                        self.cr_stack.borrow().len(),
                    ));

                    // Create temporary surface and its cr

                    let cr = match stacking_ctx.filter {
                        None => cairo::Context::new(
                            &self
                                .create_similar_surface_for_toplevel_viewport(&self.cr.target())?,
                        )?,
                        Some(_) => {
                            cairo::Context::new(self.create_surface_for_toplevel_viewport()?)?
                        }
                    };

                    let transform_for_temporary_surface =
                        ValidTransform::try_from(affines.for_temporary_surface)?;

                    let (source_surface, mut res, bbox) = {
                        let mut temporary_draw_ctx = self.nested(cr.clone());

                        let viewport_for_temporary_surface = Viewport::with_explicit_transform(
                            &viewport,
                            transform_for_temporary_surface,
                        );

                        // Draw!

                        let res = temporary_draw_ctx.draw_in_optional_new_viewport(
                            acquired_nodes,
                            &viewport_for_temporary_surface,
                            &layout_viewport,
                            draw_fn,
                        );

                        let bbox = if let Ok(ref bbox) = res {
                            bbox.clone()
                        } else {
                            Box::new(
                                BoundingBox::new().with_transform(*transform_for_temporary_surface),
                            )
                        };

                        if let Some(ref filter) = stacking_ctx.filter {
                            let filtered_surface = temporary_draw_ctx.filter_current_surface(
                                acquired_nodes,
                                filter,
                                &viewport_for_temporary_surface,
                                &stacking_ctx.element_name,
                                &bbox,
                            )?;

                            // FIXME: "res" was declared mutable above so that we could overwrite it
                            // with the result of filtering, so that if filtering produces an error,
                            // then the masking below wouldn't take place.  Test for that and fix this;
                            // we are *not* modifying res in case of error.
                            (filtered_surface, res, bbox)
                        } else {
                            (temporary_draw_ctx.cr.target(), res, bbox)
                        }
                    };

                    // Set temporary surface as source

                    self.cr
                        .set_matrix(ValidTransform::try_from(affines.compositing)?.into());
                    self.cr.set_source_surface(&source_surface, 0.0, 0.0)?;

                    // Clip

                    let transform_for_clip =
                        ValidTransform::try_from(affines.outside_temporary_surface)?;

                    let viewport_for_clip = viewport.with_explicit_transform(transform_for_clip);
                    self.cr.set_matrix(transform_for_clip.into());

                    self.clip_to_node(
                        &stacking_ctx.clip_in_object_space,
                        acquired_nodes,
                        &viewport_for_clip,
                        &bbox,
                    )?;

                    // Mask

                    if let Some(ref mask_node) = stacking_ctx.mask {
                        self.print_stack_depth("DrawingCtx::draw_layer_internal creating mask");

                        res = res.and_then(|bbox| {
                            self.generate_cairo_mask(
                                mask_node,
                                &viewport,
                                affines.for_temporary_surface,
                                &bbox,
                                acquired_nodes,
                            )
                            .and_then(|mask_surf| {
                                if let Some(surf) = mask_surf {
                                    self.cr.push_group();

                                    self.cr.set_matrix(
                                        ValidTransform::try_from(affines.compositing)?.into(),
                                    );
                                    self.cr.mask_surface(&surf, 0.0, 0.0)?;

                                    Ok(self.cr.pop_group_to_source()?)
                                } else {
                                    Ok(())
                                }
                            })
                            .map(|_: ()| bbox)
                        });
                    }

                    {
                        // Composite the temporary surface

                        self.cr
                            .set_matrix(ValidTransform::try_from(affines.compositing)?.into());
                        self.cr.set_operator(stacking_ctx.mix_blend_mode.into());

                        let Opacity(UnitInterval(opacity)) = stacking_ctx.opacity;

                        if opacity < 1.0 {
                            self.cr.paint_with_alpha(opacity)?;
                        } else {
                            self.cr.paint()?;
                        }
                    }

                    res
                } else {
                    self.draw_in_optional_new_viewport(
                        acquired_nodes,
                        &viewport,
                        &layout_viewport,
                        draw_fn,
                    )
                };

                self.link_tag_end(&stacking_ctx.link_target);

                res
            })
        }
    }

    pub fn with_discrete_layer(
        &mut self,
        stacking_ctx: &StackingContext,
        acquired_nodes: &mut AcquiredNodes<'_>,
        viewport: &Viewport,
        layout_viewport: Option<LayoutViewport>,
        clipping: bool,
        draw_fn: &mut dyn FnMut(&mut AcquiredNodes<'_>, &mut DrawingCtx, &Viewport) -> DrawResult,
    ) -> DrawResult {
        self.check_cancellation()?;

        self.recursion_depth += 1;
        self.print_stack_depth("DrawingCtx::with_discrete_layer");

        match self.check_layer_nesting_depth() {
            Ok(()) => {
                let res = self.draw_layer_internal(
                    stacking_ctx,
                    acquired_nodes,
                    viewport,
                    layout_viewport,
                    clipping,
                    draw_fn,
                );

                self.recursion_depth -= 1;
                res
            }

            Err(e) => Err(e),
        }
    }

    /// Run the drawing function with the specified opacity
    fn with_alpha(
        &mut self,
        opacity: UnitInterval,
        draw_fn: &mut dyn FnMut(&mut DrawingCtx) -> DrawResult,
    ) -> DrawResult {
        let res;
        let UnitInterval(o) = opacity;
        if o < 1.0 {
            self.cr.push_group();
            res = draw_fn(self);
            self.cr.pop_group_to_source()?;
            self.cr.paint_with_alpha(o)?;
        } else {
            res = draw_fn(self);
        }

        res
    }

    /// Start a Cairo tag for PDF links
    fn link_tag_begin(&mut self, link_target: &Option<String>) {
        if let Some(ref link_target) = *link_target {
            let attributes = format!("uri='{}'", escape_link_target(link_target));

            let cr = self.cr.clone();
            cr.tag_begin(CAIRO_TAG_LINK, &attributes);
        }
    }

    /// End a Cairo tag for PDF links
    fn link_tag_end(&mut self, link_target: &Option<String>) {
        if link_target.is_some() {
            self.cr.tag_end(CAIRO_TAG_LINK);
        }
    }

    fn make_filter_plan(
        &mut self,
        acquired_nodes: &mut AcquiredNodes<'_>,
        specs: &[FilterSpec],
        source_image_width: i32,
        source_image_height: i32,
        stroke_paint_source: Rc<UserSpacePaintSource>,
        fill_paint_source: Rc<UserSpacePaintSource>,
        viewport: &Viewport,
    ) -> Result<Rc<FilterPlan>, Box<InternalRenderingError>> {
        let requirements = InputRequirements::new_from_filter_specs(specs);

        let background_image =
            if requirements.needs_background_image || requirements.needs_background_alpha {
                Some(self.get_snapshot(source_image_width, source_image_height)?)
            } else {
                None
            };

        let stroke_paint_image = if requirements.needs_stroke_paint_image {
            Some(self.get_paint_source_surface(
                source_image_width,
                source_image_height,
                acquired_nodes,
                &stroke_paint_source,
                viewport,
            )?)
        } else {
            None
        };

        let fill_paint_image = if requirements.needs_fill_paint_image {
            Some(self.get_paint_source_surface(
                source_image_width,
                source_image_height,
                acquired_nodes,
                &fill_paint_source,
                viewport,
            )?)
        } else {
            None
        };

        Ok(Rc::new(FilterPlan::new(
            self.session(),
            *viewport,
            requirements,
            background_image,
            stroke_paint_image,
            fill_paint_image,
        )?))
    }

    fn run_filters(
        &mut self,
        viewport: &Viewport,
        surface_to_filter: SharedImageSurface,
        filter: &Filter,
        acquired_nodes: &mut AcquiredNodes<'_>,
        node_name: &str,
        user_space_params: &NormalizeParams,
        stroke_paint_source: Rc<UserSpacePaintSource>,
        fill_paint_source: Rc<UserSpacePaintSource>,
        node_bbox: &BoundingBox,
    ) -> Result<SharedImageSurface, Box<InternalRenderingError>> {
        let session = self.session();

        // We try to convert each item in the filter_list to a FilterSpec.
        //
        // However, the spec mentions, "If the filter references a non-existent object or
        // the referenced object is not a filter element, then the whole filter chain is
        // ignored." - https://www.w3.org/TR/filter-effects/#FilterProperty
        //
        // So, run through the filter_list and collect into a Result<Vec<FilterSpec>>.
        // This will return an Err if any of the conversions failed.
        let filter_specs = filter
            .filter_list
            .iter()
            .map(|filter_value| {
                filter_value.to_filter_spec(
                    acquired_nodes,
                    user_space_params,
                    filter.current_color,
                    viewport,
                    session,
                    node_name,
                )
            })
            .collect::<Result<Vec<FilterSpec>, _>>();

        match filter_specs {
            Ok(specs) => {
                let plan = self.make_filter_plan(
                    acquired_nodes,
                    &specs,
                    surface_to_filter.width(),
                    surface_to_filter.height(),
                    stroke_paint_source,
                    fill_paint_source,
                    viewport,
                )?;

                // Start with the surface_to_filter, and apply each filter spec in turn;
                // the final result is our return value.
                specs.iter().try_fold(surface_to_filter, |surface, spec| {
                    filters::render(plan.clone(), spec, surface, acquired_nodes, self, node_bbox)
                })
            }

            Err(e) => {
                rsvg_log!(
                    self.session,
                    "not rendering filter list on node {} because it was in error: {}",
                    node_name,
                    e
                );
                // just return the original surface without filtering it
                Ok(surface_to_filter)
            }
        }
    }

    fn set_pattern(
        &mut self,
        pattern: &UserSpacePattern,
        acquired_nodes: &mut AcquiredNodes<'_>,
        viewport: &Viewport,
    ) -> Result<bool, Box<InternalRenderingError>> {
        // Bail out early if the pattern has zero size, per the spec
        if approx_eq!(f64, pattern.width, 0.0) || approx_eq!(f64, pattern.height, 0.0) {
            return Ok(false);
        }

        // Bail out early if this pattern has a circular reference
        let pattern_node_acquired = match pattern.acquire_pattern_node(acquired_nodes) {
            Ok(n) => n,

            Err(AcquireError::CircularReference(ref node)) => {
                rsvg_log!(self.session, "circular reference in element {}", node);
                return Ok(false);
            }

            _ => unreachable!(),
        };

        let pattern_node = pattern_node_acquired.get();

        let taffine = viewport.transform.pre_transform(&pattern.transform);

        let mut scwscale = (taffine.xx.powi(2) + taffine.xy.powi(2)).sqrt();
        let mut schscale = (taffine.yx.powi(2) + taffine.yy.powi(2)).sqrt();

        let pw: i32 = (pattern.width * scwscale) as i32;
        let ph: i32 = (pattern.height * schscale) as i32;

        if pw < 1 || ph < 1 {
            return Ok(false);
        }

        scwscale = f64::from(pw) / pattern.width;
        schscale = f64::from(ph) / pattern.height;

        // Apply the pattern transform
        let (affine, caffine) = if scwscale.approx_eq_cairo(1.0) && schscale.approx_eq_cairo(1.0) {
            (pattern.coord_transform, pattern.content_transform)
        } else {
            (
                pattern
                    .coord_transform
                    .pre_scale(1.0 / scwscale, 1.0 / schscale),
                pattern.content_transform.post_scale(scwscale, schscale),
            )
        };

        // Draw to another surface
        let surface = self
            .cr
            .target()
            .create_similar(cairo::Content::ColorAlpha, pw, ph)?;

        let cr_pattern = cairo::Context::new(&surface)?;

        // Set up transformations to be determined by the contents units

        let transform = ValidTransform::try_from(caffine)?;
        cr_pattern.set_matrix(transform.into());

        // Draw everything

        {
            let mut pattern_draw_ctx = self.nested(cr_pattern);

            let pattern_viewport = viewport
                .with_view_box(pattern.width, pattern.height)
                .with_explicit_transform(transform);

            let pattern_cascaded = CascadedValues::new_from_node(pattern_node);
            let pattern_values = pattern_cascaded.get();

            let elt = pattern_node.borrow_element();

            let stacking_ctx = Box::new(StackingContext::new(
                self.session(),
                acquired_nodes,
                &elt,
                Transform::identity(),
                None,
                pattern_values,
            ));

            pattern_draw_ctx
                .with_alpha(pattern.opacity, &mut |dc| {
                    dc.with_discrete_layer(
                        &stacking_ctx,
                        acquired_nodes,
                        &pattern_viewport,
                        None,
                        false,
                        &mut |an, dc, new_viewport| {
                            pattern_node.draw_children(
                                an,
                                &pattern_cascaded,
                                new_viewport,
                                dc,
                                false,
                            )
                        },
                    )
                })
                .map(|_| ())?;
        }

        // Set the final surface as a Cairo pattern into the Cairo context
        let pattern = cairo::SurfacePattern::create(&surface);

        if let Some(m) = affine.invert() {
            pattern.set_matrix(ValidTransform::try_from(m)?.into());
            pattern.set_extend(cairo::Extend::Repeat);
            pattern.set_filter(cairo::Filter::Best);
            self.cr.set_source(&pattern)?;
        }

        Ok(true)
    }

    fn set_paint_source(
        &mut self,
        paint_source: &UserSpacePaintSource,
        acquired_nodes: &mut AcquiredNodes<'_>,
        viewport: &Viewport,
    ) -> Result<bool, Box<InternalRenderingError>> {
        match *paint_source {
            UserSpacePaintSource::Gradient(ref gradient, _c) => {
                set_gradient_on_cairo(&self.cr, gradient)?;
                Ok(true)
            }
            UserSpacePaintSource::Pattern(ref pattern, ref c) => {
                if self.set_pattern(pattern, acquired_nodes, viewport)? {
                    Ok(true)
                } else if let Some(c) = c {
                    set_source_color_on_cairo(&self.cr, c);
                    Ok(true)
                } else {
                    Ok(false)
                }
            }
            UserSpacePaintSource::SolidColor(ref c) => {
                set_source_color_on_cairo(&self.cr, c);
                Ok(true)
            }
            UserSpacePaintSource::None => Ok(false),
        }
    }

    /// Computes and returns a surface corresponding to the given paint server.
    pub fn get_paint_source_surface(
        &mut self,
        width: i32,
        height: i32,
        acquired_nodes: &mut AcquiredNodes<'_>,
        paint_source: &UserSpacePaintSource,
        viewport: &Viewport,
    ) -> Result<SharedImageSurface, Box<InternalRenderingError>> {
        let mut surface = ExclusiveImageSurface::new(width, height, SurfaceType::SRgb)?;

        surface.draw(&mut |cr| {
            let mut temporary_draw_ctx = self.nested(cr);

            // FIXME: we are ignoring any error

            let had_paint_server =
                temporary_draw_ctx.set_paint_source(paint_source, acquired_nodes, viewport)?;
            if had_paint_server {
                temporary_draw_ctx.cr.paint()?;
            }

            Ok(())
        })?;

        Ok(surface.share()?)
    }

    pub fn draw_layer(
        &mut self,
        layer: &Layer,
        acquired_nodes: &mut AcquiredNodes<'_>,
        clipping: bool,
        viewport: &Viewport,
    ) -> DrawResult {
        match &layer.kind {
            LayerKind::Shape(shape) => self.draw_shape(
                shape,
                &layer.stacking_ctx,
                acquired_nodes,
                clipping,
                viewport,
            ),
            LayerKind::Text(text) => self.draw_text(
                text,
                &layer.stacking_ctx,
                acquired_nodes,
                clipping,
                viewport,
            ),
            LayerKind::Image(image) => self.draw_image(
                image,
                &layer.stacking_ctx,
                acquired_nodes,
                clipping,
                viewport,
            ),
            LayerKind::Group(group) => self.draw_group(
                group,
                &layer.stacking_ctx,
                acquired_nodes,
                clipping,
                viewport,
            ),
        }
    }

    fn draw_shape(
        &mut self,
        shape: &Shape,
        stacking_ctx: &StackingContext,
        acquired_nodes: &mut AcquiredNodes<'_>,
        clipping: bool,
        viewport: &Viewport,
    ) -> DrawResult {
        self.with_discrete_layer(
            stacking_ctx,
            acquired_nodes,
            viewport,
            None,
            clipping,
            &mut |an, dc, new_viewport| {
                dc.paint_shape(shape, stacking_ctx, an, clipping, new_viewport)
            },
        )
    }

    fn paint_shape(
        &mut self,
        shape: &Shape,
        stacking_ctx: &StackingContext,
        acquired_nodes: &mut AcquiredNodes<'_>,
        clipping: bool,
        viewport: &Viewport,
    ) -> DrawResult {
        let cr = self.cr.clone();

        let transform = self.get_transform_for_stacking_ctx(viewport, stacking_ctx, clipping)?;
        let mut path_helper = PathHelper::new(&cr, transform, &shape.path.cairo_path);

        if clipping {
            if stacking_ctx.is_visible {
                cr.set_fill_rule(cairo::FillRule::from(shape.clip_rule));
                path_helper.set()?;
            }
            return Ok(viewport.empty_bbox());
        }

        cr.set_antialias(cairo::Antialias::from(shape.shape_rendering));

        setup_cr_for_stroke(&cr, &shape.stroke);

        cr.set_fill_rule(cairo::FillRule::from(shape.fill_rule));

        path_helper.set()?;
        let bbox = compute_stroke_and_fill_box(
            &cr,
            &shape.stroke,
            &shape.stroke_paint,
            &self.initial_viewport,
        )?;

        if stacking_ctx.is_visible {
            for &target in &shape.paint_order.targets {
                // fill and stroke operations will preserve the path.
                // markers operation will clear the path.
                match target {
                    PaintTarget::Fill => {
                        path_helper.set()?;
                        let had_paint_server =
                            self.set_paint_source(&shape.fill_paint, acquired_nodes, viewport)?;
                        if had_paint_server {
                            cr.fill_preserve()?;
                        }
                    }

                    PaintTarget::Stroke => {
                        path_helper.set()?;
                        if shape.stroke.non_scaling {
                            cr.set_matrix(self.initial_viewport.transform.into());
                        }

                        let had_paint_server =
                            self.set_paint_source(&shape.stroke_paint, acquired_nodes, viewport)?;
                        if had_paint_server {
                            cr.stroke_preserve()?;
                        }
                    }

                    PaintTarget::Markers => {
                        path_helper.unset();
                        marker::render_markers_for_shape(
                            shape,
                            viewport,
                            self,
                            acquired_nodes,
                            clipping,
                        )?;
                    }
                }
            }
        }

        path_helper.unset();
        Ok(bbox)
    }

    fn paint_surface_from_image(
        &mut self,
        image: &Image,
        viewport: &Viewport,
    ) -> Result<(), cairo::Error> {
        let cr = &self.cr;

        // We need to set extend appropriately, so can't use cr.set_source_surface().
        //
        // If extend is left at its default value (None), then bilinear scaling uses
        // transparency outside of the image producing incorrect results.
        // For example, in svg1.1/filters-blend-01-b.svgthere's a completely
        // opaque 100×1 image of a gradient scaled to 100×98 which ends up
        // transparent almost everywhere without this fix (which it shouldn't).
        let ptn = image.surface.to_cairo_pattern();
        ptn.set_extend(cairo::Extend::Pad);

        let interpolation = Interpolation::from(image.image_rendering);

        ptn.set_filter(cairo::Filter::from(interpolation));
        cr.set_matrix(viewport.transform.into());
        cr.set_source(&ptn)?;

        let image_size_rect = Rect::from_size(
            f64::from(image.surface.width()),
            f64::from(image.surface.height()),
        );

        // Clip is needed due to extend being set to pad.
        clip_to_rectangle(cr, &viewport.transform, &image_size_rect);

        cr.paint()
    }

    fn draw_image(
        &mut self,
        image: &Image,
        stacking_ctx: &StackingContext,
        acquired_nodes: &mut AcquiredNodes<'_>,
        clipping: bool,
        viewport: &Viewport,
    ) -> DrawResult {
        let image_width = image.surface.width();
        let image_height = image.surface.height();
        if clipping || image.rect.is_empty() || image_width == 0 || image_height == 0 {
            return Ok(viewport.empty_bbox());
        }

        let image_size_rect = Rect::from_size(f64::from(image_width), f64::from(image_height));

        let vbox = ViewBox::from(image_size_rect);

        // The bounding box for <image> is decided by the values of the image's x, y, w, h
        // and not by the final computed image bounds.
        let bounds = Box::new(viewport.empty_bbox().with_rect(image.rect));

        let layout_viewport = LayoutViewport {
            vbox: Some(vbox),
            geometry: image.rect,
            preserve_aspect_ratio: image.aspect,
            overflow: image.overflow,
        };

        if stacking_ctx.is_visible {
            self.with_discrete_layer(
                stacking_ctx,
                acquired_nodes,
                viewport,
                Some(layout_viewport),
                clipping,
                &mut |_an, dc, new_viewport| {
                    dc.paint_surface_from_image(image, new_viewport)?;

                    Ok(bounds.clone())
                },
            )
        } else {
            Ok(bounds)
        }
    }

    fn draw_group(
        &mut self,
        _group: &Group,
        _stacking_ctx: &StackingContext,
        _acquired_nodes: &mut AcquiredNodes<'_>,
        _clipping: bool,
        _viewport: &Viewport,
    ) -> DrawResult {
        unimplemented!();
        /*
        self.with_discrete_layer(
            stacking_ctx,
            acquired_nodes,
            viewport,
            group.establish_viewport,
            clipping,
            &mut |an, dc, new_viewport| {
                for layer in &group.children {
                    dc.draw_layer(layer, an, clipping, &new_viewport)?;
                }
            },
        )
        */
    }

    fn draw_text_span(
        &mut self,
        span: &TextSpan,
        acquired_nodes: &mut AcquiredNodes<'_>,
        clipping: bool,
        viewport: &Viewport,
    ) -> DrawResult {
        let path = pango_layout_to_cairo_path(span.x, span.y, &span.layout, span.gravity)?;
        if path.is_empty() {
            // Empty strings, or only-whitespace text, get turned into empty paths.
            // In that case, we really want to return "no bounds" rather than an
            // empty rectangle.
            return Ok(viewport.empty_bbox());
        }

        // #851 - We can't just render all text as paths for PDF; it
        // needs the actual text content so text is selectable by PDF
        // viewers.
        let can_use_text_as_path = self.cr.target().type_() != cairo::SurfaceType::Pdf;

        self.cr
            .set_antialias(cairo::Antialias::from(span.text_rendering));

        setup_cr_for_stroke(&self.cr, &span.stroke);

        self.cr.set_matrix(viewport.transform.into());

        if clipping {
            path.to_cairo_context(&self.cr)?;
            return Ok(viewport.empty_bbox());
        }

        path.to_cairo_context(&self.cr)?;
        let bbox = compute_stroke_and_fill_box(
            &self.cr,
            &span.stroke,
            &span.stroke_paint,
            &self.initial_viewport,
        )?;
        self.cr.new_path();

        if span.is_visible {
            self.link_tag_begin(&span.link_target);

            for &target in &span.paint_order.targets {
                match target {
                    PaintTarget::Fill => {
                        let had_paint_server =
                            self.set_paint_source(&span.fill_paint, acquired_nodes, viewport)?;

                        if had_paint_server {
                            if can_use_text_as_path {
                                path.to_cairo_context(&self.cr)?;
                                self.cr.fill()?;
                                self.cr.new_path();
                            } else {
                                self.cr.move_to(span.x, span.y);

                                let matrix = self.cr.matrix();

                                let rotation_from_gravity = span.gravity.to_rotation();
                                if !rotation_from_gravity.approx_eq_cairo(0.0) {
                                    self.cr.rotate(-rotation_from_gravity);
                                }

                                pangocairo::functions::update_layout(&self.cr, &span.layout);
                                pangocairo::functions::show_layout(&self.cr, &span.layout);

                                self.cr.set_matrix(matrix);
                            }
                        }
                    }

                    PaintTarget::Stroke => {
                        let had_paint_server =
                            self.set_paint_source(&span.stroke_paint, acquired_nodes, viewport)?;

                        if had_paint_server {
                            path.to_cairo_context(&self.cr)?;
                            self.cr.stroke()?;
                            self.cr.new_path();
                        }
                    }

                    PaintTarget::Markers => {}
                }
            }

            self.link_tag_end(&span.link_target);
        }

        Ok(bbox)
    }

    fn draw_text(
        &mut self,
        text: &Text,
        stacking_ctx: &StackingContext,
        acquired_nodes: &mut AcquiredNodes<'_>,
        clipping: bool,
        viewport: &Viewport,
    ) -> DrawResult {
        self.with_discrete_layer(
            stacking_ctx,
            acquired_nodes,
            viewport,
            None,
            clipping,
            &mut |an, dc, new_viewport| dc.paint_text_spans(text, an, clipping, new_viewport),
        )
    }

    fn paint_text_spans(
        &mut self,
        text: &Text,
        acquired_nodes: &mut AcquiredNodes<'_>,
        clipping: bool,
        viewport: &Viewport,
    ) -> DrawResult {
        let mut bbox = viewport.empty_bbox();

        for span in &text.spans {
            let span_bbox = self.draw_text_span(span, acquired_nodes, clipping, viewport)?;
            bbox.insert(&span_bbox);
        }

        Ok(bbox)
    }

    pub fn get_snapshot(
        &self,
        width: i32,
        height: i32,
    ) -> Result<SharedImageSurface, Box<InternalRenderingError>> {
        // TODO: as far as I can tell this should not render elements past the last (topmost) one
        // with enable-background: new (because technically we shouldn't have been caching them).
        // Right now there are no enable-background checks whatsoever.
        //
        // Addendum: SVG 2 has deprecated the enable-background property, and replaced it with an
        // "isolation" property from the CSS Compositing and Blending spec.
        //
        // Deprecation:
        //   https://www.w3.org/TR/filter-effects-1/#AccessBackgroundImage
        //
        // BackgroundImage, BackgroundAlpha in the "in" attribute of filter primitives:
        //   https://www.w3.org/TR/filter-effects-1/#attr-valuedef-in-backgroundimage
        //
        // CSS Compositing and Blending, "isolation" property:
        //   https://www.w3.org/TR/compositing-1/#isolation
        let mut surface = ExclusiveImageSurface::new(width, height, SurfaceType::SRgb)?;

        surface.draw(&mut |cr| {
            // TODO: apparently DrawingCtx.cr_stack is just a way to store pairs of
            // (surface, transform).  Can we turn it into a DrawingCtx.surface_stack
            // instead?  See what CSS isolation would like to call that; are the pairs just
            // stacking contexts instead, or the result of rendering stacking contexts?
            for (depth, draw) in self.cr_stack.borrow().iter().enumerate() {
                let affines = CompositingAffines::new(
                    Transform::from(draw.matrix()),
                    *self.initial_viewport.transform,
                    depth,
                );

                cr.set_matrix(ValidTransform::try_from(affines.for_snapshot)?.into());
                cr.set_source_surface(draw.target(), 0.0, 0.0)?;
                cr.paint()?;
            }

            Ok(())
        })?;

        Ok(surface.share()?)
    }

    pub fn draw_node_to_surface(
        &mut self,
        node: &Node,
        acquired_nodes: &mut AcquiredNodes<'_>,
        cascaded: &CascadedValues<'_>,
        transform: ValidTransform,
        width: i32,
        height: i32,
    ) -> Result<SharedImageSurface, Box<InternalRenderingError>> {
        let surface = cairo::ImageSurface::create(cairo::Format::ARgb32, width, height)?;

        let save_cr = self.cr.clone();

        {
            let cr = cairo::Context::new(&surface)?;
            cr.set_matrix(transform.into());

            self.cr = cr;
            let viewport = Viewport {
                dpi: self.config.dpi,
                transform,
                vbox: ViewBox::from(Rect::from_size(f64::from(width), f64::from(height))),
            };

            // FIXME: if this returns an error, we will not restore the self.cr as per below
            let _ = self.draw_node_from_stack(node, acquired_nodes, cascaded, &viewport, false)?;
        }

        self.cr = save_cr;

        Ok(SharedImageSurface::wrap(surface, SurfaceType::SRgb)?)
    }

    pub fn draw_node_from_stack(
        &mut self,
        node: &Node,
        acquired_nodes: &mut AcquiredNodes<'_>,
        cascaded: &CascadedValues<'_>,
        viewport: &Viewport,
        clipping: bool,
    ) -> DrawResult {
        self.print_stack_depth("DrawingCtx::draw_node_from_stack");

        let stack_top = self.drawsub_stack.pop();

        let draw = if let Some(ref top) = stack_top {
            top == node
        } else {
            true
        };

        let res = if draw {
            node.draw(acquired_nodes, cascaded, viewport, self, clipping)
        } else {
            Ok(viewport.empty_bbox())
        };

        if let Some(top) = stack_top {
            self.drawsub_stack.push(top);
        }

        res
    }

    pub fn draw_from_use_node(
        &mut self,
        node: &Node,
        acquired_nodes: &mut AcquiredNodes<'_>,
        values: &ComputedValues,
        use_rect: Rect,
        link: &NodeId,
        clipping: bool,
        viewport: &Viewport,
        fill_paint: Rc<PaintSource>,
        stroke_paint: Rc<PaintSource>,
    ) -> DrawResult {
        // <use> is an element that is used directly, unlike
        // <pattern>, which is used through a fill="url(#...)"
        // reference.  However, <use> will always reference another
        // element, potentially itself or an ancestor of itself (or
        // another <use> which references the first one, etc.).  So,
        // we acquire the <use> element itself so that circular
        // references can be caught.
        let _self_acquired = match acquired_nodes.acquire_ref(node) {
            Ok(n) => n,

            Err(AcquireError::CircularReference(circular)) => {
                rsvg_log!(self.session, "circular reference in element {}", circular);
                return Err(Box::new(InternalRenderingError::CircularReference(
                    circular,
                )));
            }

            _ => unreachable!(),
        };

        let acquired = match acquired_nodes.acquire(link) {
            Ok(acquired) => acquired,

            Err(AcquireError::CircularReference(circular)) => {
                rsvg_log!(
                    self.session,
                    "circular reference from {} to element {}",
                    node,
                    circular
                );
                return Err(Box::new(InternalRenderingError::CircularReference(
                    circular,
                )));
            }

            Err(AcquireError::MaxReferencesExceeded) => {
                return Err(Box::new(InternalRenderingError::LimitExceeded(
                    ImplementationLimit::TooManyReferencedElements,
                )));
            }

            Err(AcquireError::InvalidLinkType(_)) => unreachable!(),

            Err(AcquireError::LinkNotFound(node_id)) => {
                rsvg_log!(
                    self.session,
                    "element {} references nonexistent \"{}\"",
                    node,
                    node_id
                );
                return Ok(viewport.empty_bbox());
            }
        };

        // width or height set to 0 disables rendering of the element
        // https://www.w3.org/TR/SVG/struct.html#UseElementWidthAttribute
        if use_rect.is_empty() {
            return Ok(viewport.empty_bbox());
        }

        let child = acquired.get();

        if clipping && !element_can_be_used_inside_use_inside_clip_path(&child.borrow_element()) {
            return Ok(viewport.empty_bbox());
        }

        let use_transform = ValidTransform::try_from(values.transform())?;
        let use_viewport = viewport.with_composed_transform(use_transform)?;

        let use_element = node.borrow_element();

        let defines_a_viewport = if is_element_of_type!(child, Symbol) {
            let symbol = borrow_element_as!(child, Symbol);
            Some((symbol.get_viewbox(), symbol.get_preserve_aspect_ratio()))
        } else if is_element_of_type!(child, Svg) {
            let svg = borrow_element_as!(child, Svg);
            Some((svg.get_viewbox(), svg.get_preserve_aspect_ratio()))
        } else {
            None
        };

        let res = if let Some((vbox, preserve_aspect_ratio)) = defines_a_viewport {
            // <symbol> and <svg> define a viewport, as described in the specification:
            // https://www.w3.org/TR/SVG2/struct.html#UseElement
            // https://gitlab.gnome.org/GNOME/librsvg/-/issues/875#note_1482705

            let elt = child.borrow_element();

            let child_values = elt.get_computed_values();

            let stacking_ctx = Box::new(StackingContext::new(
                self.session(),
                acquired_nodes,
                &use_element,
                Transform::identity(),
                None,
                values,
            ));

            let layout_viewport = LayoutViewport {
                vbox,
                geometry: use_rect,
                preserve_aspect_ratio,
                overflow: child_values.overflow(),
            };

            self.with_discrete_layer(
                &stacking_ctx,
                acquired_nodes,
                &use_viewport,
                Some(layout_viewport),
                clipping,
                &mut |an, dc, new_viewport| {
                    child.draw_children(
                        an,
                        &CascadedValues::new_from_values(
                            child,
                            values,
                            Some(fill_paint.clone()),
                            Some(stroke_paint.clone()),
                        ),
                        new_viewport,
                        dc,
                        clipping,
                    )
                },
            )
        } else {
            // otherwise the referenced node is not a <symbol>; process it generically

            let stacking_ctx = Box::new(StackingContext::new(
                self.session(),
                acquired_nodes,
                &use_element,
                Transform::new_translate(use_rect.x0, use_rect.y0),
                None,
                values,
            ));

            self.with_discrete_layer(
                &stacking_ctx,
                acquired_nodes,
                &use_viewport,
                None,
                clipping,
                &mut |an, dc, new_viewport| {
                    child.draw(
                        an,
                        &CascadedValues::new_from_values(
                            child,
                            values,
                            Some(fill_paint.clone()),
                            Some(stroke_paint.clone()),
                        ),
                        new_viewport,
                        dc,
                        clipping,
                    )
                },
            )
        };

        if let Ok(bbox) = res {
            let mut res_bbox = Box::new(BoundingBox::new().with_transform(*viewport.transform));
            res_bbox.insert(&bbox);
            Ok(res_bbox)
        } else {
            res
        }
    }

    /// Extracts the font options for the current state of the DrawingCtx.
    ///
    /// You can use the font options later with create_pango_context().
    pub fn get_font_options(&self) -> FontOptions {
        let mut options = cairo::FontOptions::new().unwrap();
        if self.config.testing {
            options.set_antialias(cairo::Antialias::Gray);
        }

        options.set_hint_style(cairo::HintStyle::None);
        options.set_hint_metrics(cairo::HintMetrics::Off);

        FontOptions { options }
    }
}

impl From<ImageRendering> for Interpolation {
    fn from(r: ImageRendering) -> Interpolation {
        match r {
            ImageRendering::Pixelated
            | ImageRendering::CrispEdges
            | ImageRendering::OptimizeSpeed => Interpolation::Nearest,

            ImageRendering::Smooth
            | ImageRendering::OptimizeQuality
            | ImageRendering::HighQuality
            | ImageRendering::Auto => Interpolation::Smooth,
        }
    }
}

/// Create a Pango context with a particular configuration.
pub fn create_pango_context(font_options: &FontOptions) -> pango::Context {
    let font_map = pangocairo::FontMap::default();
    let context = font_map.create_context();

    context.set_round_glyph_positions(false);

    pangocairo::functions::context_set_font_options(&context, Some(&font_options.options));

    // Pango says this about pango_cairo_context_set_resolution():
    //
    //     Sets the resolution for the context. This is a scale factor between
    //     points specified in a #PangoFontDescription and Cairo units. The
    //     default value is 96, meaning that a 10 point font will be 13
    //     units high. (10 * 96. / 72. = 13.3).
    //
    // I.e. Pango font sizes in a PangoFontDescription are in *points*, not pixels.
    // However, we are normalizing everything to userspace units, which amount to
    // pixels.  So, we will use 72.0 here to make Pango not apply any further scaling
    // to the size values we give it.
    //
    // An alternative would be to divide our font sizes by (dpi_y / 72) to effectively
    // cancel out Pango's scaling, but it's probably better to deal with Pango-isms
    // right here, instead of spreading them out through our Length normalization
    // code.
    pangocairo::functions::context_set_resolution(&context, 72.0);

    context
}

pub fn set_source_color_on_cairo(cr: &cairo::Context, color: &Color) {
    let rgba = color_to_rgba(color);

    cr.set_source_rgba(
        f64::from(rgba.red) / 255.0,
        f64::from(rgba.green) / 255.0,
        f64::from(rgba.blue) / 255.0,
        f64::from(rgba.alpha),
    );
}

fn set_gradient_on_cairo(
    cr: &cairo::Context,
    gradient: &UserSpaceGradient,
) -> Result<(), Box<InternalRenderingError>> {
    let g = match gradient.variant {
        GradientVariant::Linear { x1, y1, x2, y2 } => {
            cairo::Gradient::clone(&cairo::LinearGradient::new(x1, y1, x2, y2))
        }

        GradientVariant::Radial {
            cx,
            cy,
            r,
            fx,
            fy,
            fr,
        } => cairo::Gradient::clone(&cairo::RadialGradient::new(fx, fy, fr, cx, cy, r)),
    };

    g.set_matrix(ValidTransform::try_from(gradient.transform)?.into());
    g.set_extend(cairo::Extend::from(gradient.spread));

    for stop in &gradient.stops {
        let UnitInterval(stop_offset) = stop.offset;

        let rgba = color_to_rgba(&stop.color);

        g.add_color_stop_rgba(
            stop_offset,
            f64::from(rgba.red) / 255.0,
            f64::from(rgba.green) / 255.0,
            f64::from(rgba.blue) / 255.0,
            f64::from(rgba.alpha),
        );
    }

    Ok(cr.set_source(&g)?)
}

/// Converts a Pango layout to a Cairo path on the specified cr starting at (x, y).
/// Does not clear the current path first.
fn pango_layout_to_cairo(
    x: f64,
    y: f64,
    layout: &pango::Layout,
    gravity: pango::Gravity,
    cr: &cairo::Context,
) {
    let rotation_from_gravity = gravity.to_rotation();
    let rotation = if !rotation_from_gravity.approx_eq_cairo(0.0) {
        Some(-rotation_from_gravity)
    } else {
        None
    };

    cr.move_to(x, y);

    let matrix = cr.matrix();
    if let Some(rot) = rotation {
        cr.rotate(rot);
    }

    pangocairo::functions::update_layout(cr, layout);
    pangocairo::functions::layout_path(cr, layout);
    cr.set_matrix(matrix);
}

/// Converts a Pango layout to a CairoPath starting at (x, y).
fn pango_layout_to_cairo_path(
    x: f64,
    y: f64,
    layout: &pango::Layout,
    gravity: pango::Gravity,
) -> Result<CairoPath, Box<InternalRenderingError>> {
    let surface = cairo::RecordingSurface::create(cairo::Content::ColorAlpha, None)?;
    let cr = cairo::Context::new(&surface)?;

    pango_layout_to_cairo(x, y, layout, gravity, &cr);

    let cairo_path = cr.copy_path()?;
    Ok(CairoPath::from_cairo(cairo_path))
}

// https://www.w3.org/TR/css-masking-1/#ClipPathElement
fn element_can_be_used_inside_clip_path(element: &Element) -> bool {
    use ElementData::*;

    matches!(
        element.element_data,
        Circle(_)
            | Ellipse(_)
            | Line(_)
            | Path(_)
            | Polygon(_)
            | Polyline(_)
            | Rect(_)
            | Text(_)
            | Use(_)
    )
}

// https://www.w3.org/TR/css-masking-1/#ClipPathElement
fn element_can_be_used_inside_use_inside_clip_path(element: &Element) -> bool {
    use ElementData::*;

    matches!(
        element.element_data,
        Circle(_) | Ellipse(_) | Line(_) | Path(_) | Polygon(_) | Polyline(_) | Rect(_) | Text(_)
    )
}

#[derive(Debug)]
struct CompositingAffines {
    pub outside_temporary_surface: Transform,
    #[allow(unused)]
    pub initial: Transform,
    pub for_temporary_surface: Transform,
    pub compositing: Transform,
    pub for_snapshot: Transform,
}

impl CompositingAffines {
    fn new(current: Transform, initial: Transform, cr_stack_depth: usize) -> CompositingAffines {
        let is_topmost_temporary_surface = cr_stack_depth == 0;

        let initial_inverse = initial.invert().unwrap();

        let outside_temporary_surface = if is_topmost_temporary_surface {
            current
        } else {
            current.post_transform(&initial_inverse)
        };

        let (scale_x, scale_y) = initial.transform_distance(1.0, 1.0);

        let for_temporary_surface = if is_topmost_temporary_surface {
            current
                .post_transform(&initial_inverse)
                .post_scale(scale_x, scale_y)
        } else {
            current
        };

        let compositing = if is_topmost_temporary_surface {
            initial.pre_scale(1.0 / scale_x, 1.0 / scale_y)
        } else {
            Transform::identity()
        };

        let for_snapshot = compositing.invert().unwrap();

        CompositingAffines {
            outside_temporary_surface,
            initial,
            for_temporary_surface,
            compositing,
            for_snapshot,
        }
    }
}

fn compute_stroke_and_fill_extents(
    cr: &cairo::Context,
    stroke: &Stroke,
    stroke_paint_source: &UserSpacePaintSource,
    initial_viewport: &Viewport,
) -> Result<PathExtents, Box<InternalRenderingError>> {
    // Dropping the precision of cairo's bezier subdivision, yielding 2x
    // _rendering_ time speedups, are these rather expensive operations
    // really needed here? */
    let backup_tolerance = cr.tolerance();
    cr.set_tolerance(1.0);

    // Bounding box for fill
    //
    // Unlike the case for stroke, for fills we always compute the bounding box.
    // In GNOME we have SVGs for symbolic icons where each icon has a bounding
    // rectangle with no fill and no stroke, and inside it there are the actual
    // paths for the icon's shape.  We need to be able to compute the bounding
    // rectangle's extents, even when it has no fill nor stroke.

    let (x0, y0, x1, y1) = cr.fill_extents()?;
    let fill_extents = if x0 != 0.0 || y0 != 0.0 || x1 != 0.0 || y1 != 0.0 {
        Some(Rect::new(x0, y0, x1, y1))
    } else {
        None
    };

    // Bounding box for stroke
    //
    // When presented with a line width of 0, Cairo returns a
    // stroke_extents rectangle of (0, 0, 0, 0).  This would cause the
    // bbox to include a lone point at the origin, which is wrong, as a
    // stroke of zero width should not be painted, per
    // https://www.w3.org/TR/SVG2/painting.html#StrokeWidth
    //
    // So, see if the stroke width is 0 and just not include the stroke in the
    // bounding box if so.

    let stroke_extents = if !stroke.width.approx_eq_cairo(0.0)
        && !matches!(stroke_paint_source, UserSpacePaintSource::None)
    {
        let backup_matrix = if stroke.non_scaling {
            let matrix = cr.matrix();
            cr.set_matrix(initial_viewport.transform.into());
            Some(matrix)
        } else {
            None
        };
        let (x0, y0, x1, y1) = cr.stroke_extents()?;
        if let Some(matrix) = backup_matrix {
            cr.set_matrix(matrix);
        }
        Some(Rect::new(x0, y0, x1, y1))
    } else {
        None
    };

    // objectBoundingBox

    let (x0, y0, x1, y1) = cr.path_extents()?;
    let path_extents = Some(Rect::new(x0, y0, x1, y1));

    // restore tolerance

    cr.set_tolerance(backup_tolerance);

    Ok(PathExtents {
        path_only: path_extents,
        fill: fill_extents,
        stroke: stroke_extents,
    })
}

fn compute_stroke_and_fill_box(
    cr: &cairo::Context,
    stroke: &Stroke,
    stroke_paint_source: &UserSpacePaintSource,
    initial_viewport: &Viewport,
) -> DrawResult {
    let extents =
        compute_stroke_and_fill_extents(cr, stroke, stroke_paint_source, initial_viewport)?;

    let ink_rect = match (extents.fill, extents.stroke) {
        (None, None) => None,
        (Some(f), None) => Some(f),
        (None, Some(s)) => Some(s),
        (Some(f), Some(s)) => Some(f.union(&s)),
    };

    let mut bbox = BoundingBox::new().with_transform(Transform::from(cr.matrix()));

    if let Some(rect) = extents.path_only {
        bbox = bbox.with_rect(rect);
    }

    if let Some(ink_rect) = ink_rect {
        bbox = bbox.with_ink_rect(ink_rect);
    }

    Ok(Box::new(bbox))
}

fn setup_cr_for_stroke(cr: &cairo::Context, stroke: &Stroke) {
    cr.set_line_width(stroke.width);
    cr.set_miter_limit(stroke.miter_limit.0);
    cr.set_line_cap(cairo::LineCap::from(stroke.line_cap));
    cr.set_line_join(cairo::LineJoin::from(stroke.line_join));

    let total_length: f64 = stroke.dashes.iter().sum();

    if total_length > 0.0 {
        cr.set_dash(&stroke.dashes, stroke.dash_offset);
    } else {
        cr.set_dash(&[], 0.0);
    }
}

/// escape quotes and backslashes with backslash
fn escape_link_target(value: &str) -> Cow<'_, str> {
    let regex = {
        static REGEX: OnceLock<Regex> = OnceLock::new();
        REGEX.get_or_init(|| Regex::new(r"['\\]").unwrap())
    };

    regex.replace_all(value, |caps: &Captures<'_>| {
        match caps.get(0).unwrap().as_str() {
            "'" => "\\'".to_owned(),
            "\\" => "\\\\".to_owned(),
            _ => unreachable!(),
        }
    })
}

fn clip_to_rectangle(cr: &cairo::Context, transform: &ValidTransform, r: &Rect) {
    cr.set_matrix((*transform).into());

    cr.rectangle(r.x0, r.y0, r.width(), r.height());
    cr.clip();
}

impl From<SpreadMethod> for cairo::Extend {
    fn from(s: SpreadMethod) -> cairo::Extend {
        match s {
            SpreadMethod::Pad => cairo::Extend::Pad,
            SpreadMethod::Reflect => cairo::Extend::Reflect,
            SpreadMethod::Repeat => cairo::Extend::Repeat,
        }
    }
}

impl From<StrokeLinejoin> for cairo::LineJoin {
    fn from(j: StrokeLinejoin) -> cairo::LineJoin {
        match j {
            StrokeLinejoin::Miter => cairo::LineJoin::Miter,
            StrokeLinejoin::Round => cairo::LineJoin::Round,
            StrokeLinejoin::Bevel => cairo::LineJoin::Bevel,
        }
    }
}

impl From<StrokeLinecap> for cairo::LineCap {
    fn from(j: StrokeLinecap) -> cairo::LineCap {
        match j {
            StrokeLinecap::Butt => cairo::LineCap::Butt,
            StrokeLinecap::Round => cairo::LineCap::Round,
            StrokeLinecap::Square => cairo::LineCap::Square,
        }
    }
}

impl From<MixBlendMode> for cairo::Operator {
    fn from(m: MixBlendMode) -> cairo::Operator {
        use cairo::Operator;

        match m {
            MixBlendMode::Normal => Operator::Over,
            MixBlendMode::Multiply => Operator::Multiply,
            MixBlendMode::Screen => Operator::Screen,
            MixBlendMode::Overlay => Operator::Overlay,
            MixBlendMode::Darken => Operator::Darken,
            MixBlendMode::Lighten => Operator::Lighten,
            MixBlendMode::ColorDodge => Operator::ColorDodge,
            MixBlendMode::ColorBurn => Operator::ColorBurn,
            MixBlendMode::HardLight => Operator::HardLight,
            MixBlendMode::SoftLight => Operator::SoftLight,
            MixBlendMode::Difference => Operator::Difference,
            MixBlendMode::Exclusion => Operator::Exclusion,
            MixBlendMode::Hue => Operator::HslHue,
            MixBlendMode::Saturation => Operator::HslSaturation,
            MixBlendMode::Color => Operator::HslColor,
            MixBlendMode::Luminosity => Operator::HslLuminosity,
        }
    }
}

impl From<ClipRule> for cairo::FillRule {
    fn from(c: ClipRule) -> cairo::FillRule {
        match c {
            ClipRule::NonZero => cairo::FillRule::Winding,
            ClipRule::EvenOdd => cairo::FillRule::EvenOdd,
        }
    }
}

impl From<FillRule> for cairo::FillRule {
    fn from(f: FillRule) -> cairo::FillRule {
        match f {
            FillRule::NonZero => cairo::FillRule::Winding,
            FillRule::EvenOdd => cairo::FillRule::EvenOdd,
        }
    }
}

impl From<ShapeRendering> for cairo::Antialias {
    fn from(sr: ShapeRendering) -> cairo::Antialias {
        match sr {
            ShapeRendering::Auto | ShapeRendering::GeometricPrecision => cairo::Antialias::Default,
            ShapeRendering::OptimizeSpeed | ShapeRendering::CrispEdges => cairo::Antialias::None,
        }
    }
}

impl From<TextRendering> for cairo::Antialias {
    fn from(tr: TextRendering) -> cairo::Antialias {
        match tr {
            TextRendering::Auto
            | TextRendering::OptimizeLegibility
            | TextRendering::GeometricPrecision => cairo::Antialias::Default,
            TextRendering::OptimizeSpeed => cairo::Antialias::None,
        }
    }
}

impl From<cairo::Matrix> for Transform {
    #[inline]
    fn from(m: cairo::Matrix) -> Self {
        Self::new_unchecked(m.xx(), m.yx(), m.xy(), m.yy(), m.x0(), m.y0())
    }
}

impl From<ValidTransform> for cairo::Matrix {
    #[inline]
    fn from(t: ValidTransform) -> cairo::Matrix {
        cairo::Matrix::new(t.xx, t.yx, t.xy, t.yy, t.x0, t.y0)
    }
}

/// Extents for a path in its current coordinate system.
///
/// Normally you'll want to convert this to a BoundingBox, which has knowledge about just
/// what that coordinate system is.
pub struct PathExtents {
    /// Extents of the "plain", unstroked path, or `None` if the path is empty.
    pub path_only: Option<Rect>,

    /// Extents of just the fill, or `None` if the path is empty.
    pub fill: Option<Rect>,

    /// Extents for the stroked path, or `None` if the path is empty or zero-width.
    pub stroke: Option<Rect>,
}
