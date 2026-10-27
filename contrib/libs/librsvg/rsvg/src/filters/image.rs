use markup5ever::{expanded_name, local_name, ns};

use crate::aspect_ratio::AspectRatio;
use crate::document::{AcquiredNodes, Document, NodeId, Resource};
use crate::drawing_ctx::{DrawingCtx, SvgNesting};
use crate::element::{ElementTrait, set_attribute};
use crate::href::{is_href, set_href};
use crate::image::checked_i32;
use crate::node::{CascadedValues, Node};
use crate::parsers::ParseValue;
use crate::properties::ComputedValues;
use crate::rect::Rect;
use crate::rsvg_log;
use crate::session::Session;
use crate::surface_utils::shared_surface::{Interpolation, SharedImageSurface, SurfaceType};
use crate::transform::ValidTransform;
use crate::viewbox::ViewBox;
use crate::xml::Attributes;

use super::bounds::{Bounds, BoundsBuilder};
use super::context::{FilterContext, FilterOutput};
use super::{
    FilterEffect, FilterError, FilterResolveError, InputRequirements, Primitive, PrimitiveParams,
    ResolvedPrimitive,
};

/// The `feImage` filter primitive.
#[derive(Default)]
pub struct FeImage {
    base: Primitive,
    params: ImageParams,
}

#[derive(Clone, Default)]
struct ImageParams {
    aspect: AspectRatio,
    href: Option<String>,
}

/// Resolved `feImage` primitive for rendering.
pub struct Image {
    aspect: AspectRatio,
    source: Source,
    feimage_values: Box<ComputedValues>,
}

/// What a feImage references for rendering.
enum Source {
    /// Nothing is referenced; ignore the filter.
    None,

    /// Reference to a node.
    Node(Node, String),

    /// Reference to an external image.  This is just a URL.
    ExternalImage(String),
}

impl ElementTrait for FeImage {
    fn set_attributes(&mut self, attrs: &Attributes, session: &Session) {
        self.base.parse_no_inputs(attrs, session);

        for (attr, value) in attrs.iter() {
            match attr.expanded() {
                expanded_name!("", "preserveAspectRatio") => {
                    set_attribute(&mut self.params.aspect, attr.parse(value), session);
                }

                // "path" is used by some older Adobe Illustrator versions
                ref a if is_href(a) || *a == expanded_name!("", "path") => {
                    set_href(a, &mut self.params.href, Some(value.to_string()));
                }

                _ => (),
            }
        }
    }
}

impl Image {
    pub fn render(
        &self,
        bounds_builder: BoundsBuilder,
        ctx: &FilterContext,
        acquired_nodes: &mut AcquiredNodes<'_>,
        draw_ctx: &mut DrawingCtx,
    ) -> Result<FilterOutput, FilterError> {
        let bounds = bounds_builder.compute(ctx);

        let surface = match &self.source {
            Source::None => return Err(FilterError::InvalidInput),

            Source::Node(node, name) => {
                if let Ok(acquired) = acquired_nodes.acquire_ref(node) {
                    rsvg_log!(ctx.session(), "(feImage \"{}\"", name);
                    let res = self.render_node(
                        ctx,
                        acquired_nodes,
                        draw_ctx,
                        bounds.clipped,
                        acquired.get(),
                    );
                    rsvg_log!(ctx.session(), ")");
                    res?
                } else {
                    return Err(FilterError::InvalidInput);
                }
            }

            Source::ExternalImage(href) => {
                self.render_external_image(ctx, acquired_nodes, draw_ctx, &bounds, href)?
            }
        };

        Ok(FilterOutput {
            surface,
            bounds: bounds.clipped.into(),
        })
    }

    pub fn get_input_requirements(&self) -> InputRequirements {
        InputRequirements::default()
    }

    /// Renders the filter if the source is an existing node.
    fn render_node(
        &self,
        ctx: &FilterContext,
        acquired_nodes: &mut AcquiredNodes<'_>,
        draw_ctx: &mut DrawingCtx,
        bounds: Rect,
        referenced_node: &Node,
    ) -> Result<SharedImageSurface, FilterError> {
        // https://www.w3.org/TR/filter-effects/#feImageElement
        //
        // The filters spec says, "... otherwise [rendering a referenced object], the
        // referenced resource is rendered according to the behavior of the use element."
        // I think this means that we use the same cascading mode as <use>, i.e. the
        // referenced object inherits its properties from the feImage element.
        let cascaded =
            CascadedValues::new_from_values(referenced_node, &self.feimage_values, None, None);

        let interpolation = Interpolation::from(self.feimage_values.image_rendering());

        let paffine = ValidTransform::try_from(ctx.paffine())?;

        let image = draw_ctx.draw_node_to_surface(
            referenced_node,
            acquired_nodes,
            &cascaded,
            paffine,
            ctx.source_graphic().width(),
            ctx.source_graphic().height(),
        )?;

        let surface = ctx
            .source_graphic()
            .paint_image(bounds, &image, None, interpolation)?;

        Ok(surface)
    }

    /// Renders the filter if the source is an external image.
    fn render_external_image(
        &self,
        ctx: &FilterContext,
        acquired_nodes: &mut AcquiredNodes<'_>,
        draw_ctx: &DrawingCtx,
        bounds: &Bounds,
        url: &str,
    ) -> Result<SharedImageSurface, FilterError> {
        match acquired_nodes.lookup_resource(url) {
            Ok(Resource::Image(surface)) => {
                self.render_surface_from_raster_image(&surface, ctx, bounds)
            }

            Ok(Resource::Document(document)) => {
                self.render_surface_from_svg(&document, ctx, bounds, draw_ctx)
            }

            Err(e) => {
                rsvg_log!(
                    ctx.session(),
                    "could not load image \"{}\" for feImage: {}",
                    url,
                    e
                );
                Err(FilterError::InvalidInput)
            }
        }
    }

    fn render_surface_from_raster_image(
        &self,
        image: &SharedImageSurface,
        ctx: &FilterContext,
        bounds: &Bounds,
    ) -> Result<SharedImageSurface, FilterError> {
        let rect = self.aspect.compute(
            &ViewBox::from(Rect::from_size(
                f64::from(image.width()),
                f64::from(image.height()),
            )),
            &bounds.unclipped,
        );

        // FIXME: overflow is not used but it should be
        // let overflow = self.feimage_values.overflow();
        let interpolation = Interpolation::from(self.feimage_values.image_rendering());

        let surface =
            ctx.source_graphic()
                .paint_image(bounds.clipped, image, Some(rect), interpolation)?;

        Ok(surface)
    }

    fn render_surface_from_svg(
        &self,
        document: &Document,
        ctx: &FilterContext,
        bounds: &Bounds,
        draw_ctx: &DrawingCtx,
    ) -> Result<SharedImageSurface, FilterError> {
        // Strategy:
        //
        // Render the document at the size needed for the filter primitive
        // subregion, and then paste that as if we were handling the case for a raster imge.
        //
        // Note that for feImage, x/y/width/height are *attributes*, not the geometry
        // properties from the normal <image> element , and have special handling:
        //
        // - They don't take "auto" as a value.  The defaults are "0 0 100% 100%" but those
        // are with respect to the filter primitive subregion.

        let x = bounds.x.unwrap_or(0.0);
        let y = bounds.y.unwrap_or(0.0);
        let w = bounds.width.unwrap_or(1.0); // default is 100%
        let h = bounds.height.unwrap_or(1.0);

        // https://www.w3.org/TR/filter-effects/#FilterPrimitiveSubRegion
        // "If the filter primitive subregion has a negative or zero width or height, the
        // effect of the filter primitive is disabled."
        if w <= 0.0 || h < 0.0 {
            // In this case just return an empty image the size of the SourceGraphic
            return Ok(SharedImageSurface::empty(
                ctx.source_graphic().width(),
                ctx.source_graphic().height(),
                SurfaceType::SRgb,
            )?);
        }

        let dest_rect = Rect {
            x0: bounds.clipped.x0 + bounds.clipped.width() * x,
            y0: bounds.clipped.y0 + bounds.clipped.height() * y,
            x1: bounds.clipped.x0 + bounds.clipped.width() * w,
            y1: bounds.clipped.y0 + bounds.clipped.height() * h,
        };

        let dest_size = dest_rect.size();

        let surface_dest_rect = Rect::from_size(dest_size.0, dest_size.1);

        // We use ceil() to avoid chopping off the last pixel if it is partially covered.
        let surface_width = checked_i32(dest_size.0.ceil())?;
        let surface_height = checked_i32(dest_size.1.ceil())?;
        let surface =
            cairo::ImageSurface::create(cairo::Format::ARgb32, surface_width, surface_height)?;

        {
            let cr = cairo::Context::new(&surface)?;

            let options = draw_ctx.rendering_options(SvgNesting::ReferencedFromImageElement);

            document.render_document(&cr, &cairo::Rectangle::from(surface_dest_rect), &options)?;
        }

        // Now paste that image as a normal raster image

        let surface = SharedImageSurface::wrap(surface, SurfaceType::SRgb)?;

        self.render_surface_from_raster_image(&surface, ctx, bounds)
    }
}

impl FilterEffect for FeImage {
    fn resolve(
        &self,
        acquired_nodes: &mut AcquiredNodes<'_>,
        node: &Node,
    ) -> Result<Vec<ResolvedPrimitive>, FilterResolveError> {
        let cascaded = CascadedValues::new_from_node(node);
        let feimage_values = cascaded.get().clone();

        let source = match self.params.href {
            None => Source::None,

            Some(ref s) => {
                if let Ok(node_id) = NodeId::parse(s) {
                    acquired_nodes
                        .acquire(&node_id)
                        .map(|acquired| Source::Node(acquired.get().clone(), s.clone()))
                        .unwrap_or(Source::None)
                } else {
                    Source::ExternalImage(s.to_string())
                }
            }
        };

        Ok(vec![ResolvedPrimitive {
            primitive: self.base.clone(),
            params: PrimitiveParams::Image(Image {
                aspect: self.params.aspect,
                source,
                feimage_values: Box::new(feimage_values),
            }),
        }])
    }
}
