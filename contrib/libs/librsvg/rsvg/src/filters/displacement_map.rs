use cssparser::Parser;
use markup5ever::{expanded_name, local_name, ns};

use crate::document::AcquiredNodes;
use crate::element::{ElementTrait, set_attribute};
use crate::error::*;
use crate::node::{CascadedValues, Node};
use crate::parse_identifiers;
use crate::parsers::{Parse, ParseValue};
use crate::properties::ColorInterpolationFilters;
use crate::rect::IRect;
use crate::session::Session;
use crate::surface_utils::{iterators::Pixels, shared_surface::ExclusiveImageSurface};
use crate::xml::Attributes;

use super::bounds::BoundsBuilder;
use super::context::{FilterContext, FilterOutput};
use super::{
    FilterEffect, FilterError, FilterResolveError, Input, InputRequirements, Primitive,
    PrimitiveParams, ResolvedPrimitive,
};

/// Enumeration of the color channels the displacement map can source.
#[derive(Default, Clone, Copy)]
enum ColorChannel {
    R,
    G,
    B,
    #[default]
    A,
}

/// The `feDisplacementMap` filter primitive.
#[derive(Default)]
pub struct FeDisplacementMap {
    base: Primitive,
    params: DisplacementMap,
}

/// Resolved `feDisplacementMap` primitive for rendering.
#[derive(Clone, Default)]
pub struct DisplacementMap {
    in1: Input,
    in2: Input,
    scale: f64,
    x_channel_selector: ColorChannel,
    y_channel_selector: ColorChannel,
    color_interpolation_filters: ColorInterpolationFilters,
}

impl ElementTrait for FeDisplacementMap {
    fn set_attributes(&mut self, attrs: &Attributes, session: &Session) {
        let (in1, in2) = self.base.parse_two_inputs(attrs, session);
        self.params.in1 = in1;
        self.params.in2 = in2;

        for (attr, value) in attrs.iter() {
            match attr.expanded() {
                expanded_name!("", "scale") => {
                    set_attribute(&mut self.params.scale, attr.parse(value), session)
                }
                expanded_name!("", "xChannelSelector") => {
                    set_attribute(
                        &mut self.params.x_channel_selector,
                        attr.parse(value),
                        session,
                    );
                }
                expanded_name!("", "yChannelSelector") => {
                    set_attribute(
                        &mut self.params.y_channel_selector,
                        attr.parse(value),
                        session,
                    );
                }
                _ => (),
            }
        }
    }
}

impl DisplacementMap {
    pub fn render(
        &self,
        bounds_builder: BoundsBuilder,
        ctx: &FilterContext,
    ) -> Result<FilterOutput, FilterError> {
        // https://www.w3.org/TR/filter-effects/#feDisplacementMapElement
        // "The color-interpolation-filters property only applies to
        // the in2 source image and does not apply to the in source
        // image. The in source image must remain in its current color
        // space.

        let input_1 = ctx.get_input(&self.in1, ColorInterpolationFilters::Auto)?;
        let displacement_input = ctx.get_input(&self.in2, self.color_interpolation_filters)?;
        let bounds: IRect = bounds_builder
            .add_input(&input_1)
            .add_input(&displacement_input)
            .compute(ctx)
            .clipped
            .into();

        // Displacement map's values need to be non-premultiplied.
        let displacement_surface = displacement_input.surface().unpremultiply(bounds)?;

        let (sx, sy) = ctx.paffine().transform_distance(self.scale, self.scale);

        let mut surface = ExclusiveImageSurface::new(
            ctx.source_graphic().width(),
            ctx.source_graphic().height(),
            input_1.surface().surface_type(),
        )?;

        surface.draw(&mut |cr| {
            for (x, y, displacement_pixel) in Pixels::within(&displacement_surface, bounds) {
                let get_value = |channel| match channel {
                    ColorChannel::R => displacement_pixel.r,
                    ColorChannel::G => displacement_pixel.g,
                    ColorChannel::B => displacement_pixel.b,
                    ColorChannel::A => displacement_pixel.a,
                };

                let process = |x| f64::from(x) / 255.0 - 0.5;

                let dx = process(get_value(self.x_channel_selector));
                let dy = process(get_value(self.y_channel_selector));

                let x = f64::from(x);
                let y = f64::from(y);
                let ox = sx * dx;
                let oy = sy * dy;

                // Doing this in a loop doesn't look too bad performance wise, and allows not to
                // manually implement bilinear or other interpolation.
                cr.rectangle(x, y, 1.0, 1.0);
                cr.reset_clip();
                cr.clip();

                input_1.surface().set_as_source_surface(&cr, -ox, -oy)?;
                cr.paint()?;
            }

            Ok(())
        })?;

        Ok(FilterOutput {
            surface: surface.share()?,
            bounds,
        })
    }

    pub fn get_input_requirements(&self) -> InputRequirements {
        self.in1
            .get_requirements()
            .fold(self.in2.get_requirements())
    }
}

impl FilterEffect for FeDisplacementMap {
    fn resolve(
        &self,
        _acquired_nodes: &mut AcquiredNodes<'_>,
        node: &Node,
    ) -> Result<Vec<ResolvedPrimitive>, FilterResolveError> {
        let cascaded = CascadedValues::new_from_node(node);
        let values = cascaded.get();

        let mut params = self.params.clone();
        params.color_interpolation_filters = values.color_interpolation_filters();

        Ok(vec![ResolvedPrimitive {
            primitive: self.base.clone(),
            params: PrimitiveParams::DisplacementMap(params),
        }])
    }
}

impl Parse for ColorChannel {
    fn parse<'i>(parser: &mut Parser<'i, '_>) -> Result<Self, ParseError<'i>> {
        Ok(parse_identifiers!(
            parser,
            "R" => ColorChannel::R,
            "G" => ColorChannel::G,
            "B" => ColorChannel::B,
            "A" => ColorChannel::A,
        )?)
    }
}
