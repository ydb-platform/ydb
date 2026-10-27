//! The `image` element.

use markup5ever::{expanded_name, local_name, ns};

use crate::aspect_ratio::AspectRatio;
use crate::document::{AcquiredNodes, Document, Resource};
use crate::drawing_ctx::{DrawingCtx, SvgNesting, Viewport};
use crate::element::{DrawResult, ElementTrait, set_attribute};
use crate::error::*;
use crate::href::{is_href, set_href};
use crate::layout::{self, Layer, LayerKind, StackingContext};
use crate::length::*;
use crate::node::{CascadedValues, Node, NodeBorrow};
use crate::parsers::ParseValue;
use crate::rect::Rect;
use crate::rsvg_log;
use crate::session::Session;
use crate::surface_utils::shared_surface::{SharedImageSurface, SurfaceType};
use crate::xml::Attributes;

/// The `<image>` element.
///
/// Note that its x/y/width/height are properties in SVG2, so they are
/// defined as part of [the properties machinery](properties.rs).
#[derive(Default)]
pub struct Image {
    aspect: AspectRatio,
    href: Option<String>,
}

impl ElementTrait for Image {
    fn set_attributes(&mut self, attrs: &Attributes, session: &Session) {
        for (attr, value) in attrs.iter() {
            match attr.expanded() {
                expanded_name!("", "preserveAspectRatio") => {
                    set_attribute(&mut self.aspect, attr.parse(value), session)
                }

                // "path" is used by some older Adobe Illustrator versions
                ref a if is_href(a) || *a == expanded_name!("", "path") => {
                    set_href(a, &mut self.href, Some(value.to_string()))
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
        if let Some(ref url) = self.href {
            self.layout_from_url(url, node, acquired_nodes, cascaded, viewport, draw_ctx)
        } else {
            Ok(None)
        }
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
        let layer = self.layout(node, acquired_nodes, cascaded, viewport, draw_ctx, clipping)?;

        if let Some(layer) = layer {
            draw_ctx.draw_layer(&layer, acquired_nodes, clipping, viewport)
        } else {
            Ok(viewport.empty_bbox())
        }
    }
}

impl Image {
    fn layout_from_url(
        &self,
        url: &str,
        node: &Node,
        acquired_nodes: &mut AcquiredNodes<'_>,
        cascaded: &CascadedValues<'_>,
        viewport: &Viewport,
        draw_ctx: &mut DrawingCtx,
    ) -> Result<Option<Layer>, Box<InternalRenderingError>> {
        match acquired_nodes.lookup_resource(url) {
            Ok(Resource::Image(surface)) => self.layout_from_surface(
                &surface,
                node,
                acquired_nodes,
                cascaded,
                viewport,
                draw_ctx,
            ),

            Ok(Resource::Document(document)) => self.layout_from_svg(
                &document,
                node,
                acquired_nodes,
                cascaded,
                viewport,
                draw_ctx,
            ),

            Err(e) => {
                rsvg_log!(
                    draw_ctx.session(),
                    "could not load image \"{}\": {}",
                    url,
                    e
                );
                Ok(None)
            }
        }
    }

    /// Draw an `<image>` from a raster image.
    fn layout_from_surface(
        &self,
        surface: &SharedImageSurface,
        node: &Node,
        acquired_nodes: &mut AcquiredNodes<'_>,
        cascaded: &CascadedValues<'_>,
        viewport: &Viewport,
        draw_ctx: &mut DrawingCtx,
    ) -> Result<Option<Layer>, Box<InternalRenderingError>> {
        let values = cascaded.get();

        let params = NormalizeParams::new(values, viewport);

        let x = values.x().0.to_user(&params);
        let y = values.y().0.to_user(&params);

        let w = match values.width().0 {
            LengthOrAuto::Length(l) => l.to_user(&params),
            LengthOrAuto::Auto => surface.width() as f64,
        };
        let h = match values.height().0 {
            LengthOrAuto::Length(l) => l.to_user(&params),
            LengthOrAuto::Auto => surface.height() as f64,
        };

        let rect = Rect::new(x, y, x + w, y + h);

        let overflow = values.overflow();

        let image = Box::new(layout::Image {
            surface: surface.clone(),
            rect,
            aspect: self.aspect,
            overflow,
            image_rendering: values.image_rendering(),
        });

        let elt = node.borrow_element();
        let stacking_ctx = StackingContext::new(
            draw_ctx.session(),
            acquired_nodes,
            &elt,
            values.transform(),
            None,
            values,
        );

        let layer = Layer {
            kind: LayerKind::Image(image),
            stacking_ctx,
        };

        Ok(Some(layer))
    }

    /// Draw an `<image>` from an SVG image.
    ///
    /// Per the [spec], we need to rasterize the SVG ("The result of processing an ‘image’
    /// is always a four-channel RGBA result.")  and then composite it as if it were a PNG
    /// or JPEG.
    ///
    /// [spec]: https://www.w3.org/TR/SVG2/embedded.html#ImageElement
    fn layout_from_svg(
        &self,
        document: &Document,
        node: &Node,
        acquired_nodes: &mut AcquiredNodes<'_>,
        cascaded: &CascadedValues<'_>,
        viewport: &Viewport,
        draw_ctx: &mut DrawingCtx,
    ) -> Result<Option<Layer>, Box<InternalRenderingError>> {
        let dimensions = document.get_intrinsic_dimensions();

        let values = cascaded.get();

        let params = NormalizeParams::new(values, viewport);

        let x = values.x().0.to_user(&params);
        let y = values.y().0.to_user(&params);

        let w = match values.width().0 {
            LengthOrAuto::Length(l) => l.to_user(&params),
            LengthOrAuto::Auto => dimensions.width.to_user(&params),
        };

        let h = match values.height().0 {
            LengthOrAuto::Length(l) => l.to_user(&params),
            LengthOrAuto::Auto => dimensions.height.to_user(&params),
        };

        let rect = Rect::new(x, y, x + w, y + h);

        let overflow = values.overflow();

        let dest_rect = match dimensions.vbox {
            None => Rect::from_size(w, h),
            Some(vbox) => self.aspect.compute(&vbox, &Rect::new(x, y, x + w, y + h)),
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

        let surface = SharedImageSurface::wrap(surface, SurfaceType::SRgb)?;

        let image = Box::new(layout::Image {
            surface,
            rect,
            aspect: self.aspect,
            overflow,
            image_rendering: values.image_rendering(),
        });

        let elt = node.borrow_element();
        let stacking_ctx = StackingContext::new(
            draw_ctx.session(),
            acquired_nodes,
            &elt,
            values.transform(),
            None,
            values,
        );

        let layer = Layer {
            kind: LayerKind::Image(image),
            stacking_ctx,
        };

        Ok(Some(layer))
    }
}

pub fn checked_i32(x: f64) -> Result<i32, cairo::Error> {
    cast::i32(x).map_err(|_| cairo::Error::InvalidSize)
}
