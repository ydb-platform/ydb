use std::cmp::{max, min};

use cssparser::Parser;
use markup5ever::{expanded_name, local_name, ns};

use crate::document::AcquiredNodes;
use crate::element::{ElementTrait, set_attribute};
use crate::error::*;
use crate::node::Node;
use crate::parse_identifiers;
use crate::parsers::{NumberOptionalNumber, Parse, ParseValue};
use crate::properties::ColorInterpolationFilters;
use crate::rect::IRect;
use crate::session::Session;
use crate::surface_utils::{
    EdgeMode, ImageSurfaceDataExt, Pixel,
    iterators::{PixelRectangle, Pixels},
    shared_surface::ExclusiveImageSurface,
};
use crate::xml::Attributes;

use super::bounds::BoundsBuilder;
use super::context::{FilterContext, FilterOutput};
use super::{
    FilterEffect, FilterError, FilterResolveError, Input, InputRequirements, Primitive,
    PrimitiveParams, ResolvedPrimitive,
};

/// Enumeration of the possible morphology operations.
#[derive(Default, Clone)]
enum Operator {
    #[default]
    Erode,
    Dilate,
}

/// The `feMorphology` filter primitive.
#[derive(Default)]
pub struct FeMorphology {
    base: Primitive,
    params: Morphology,
}

/// Resolved `feMorphology` primitive for rendering.
#[derive(Clone)]
pub struct Morphology {
    in1: Input,
    operator: Operator,
    radius: NumberOptionalNumber<f64>,
}

// We need this because NumberOptionalNumber doesn't impl Default
impl Default for Morphology {
    fn default() -> Morphology {
        Morphology {
            in1: Default::default(),
            operator: Default::default(),
            radius: NumberOptionalNumber(0.0, 0.0),
        }
    }
}

impl ElementTrait for FeMorphology {
    fn set_attributes(&mut self, attrs: &Attributes, session: &Session) {
        self.params.in1 = self.base.parse_one_input(attrs, session);

        for (attr, value) in attrs.iter() {
            match attr.expanded() {
                expanded_name!("", "operator") => {
                    set_attribute(&mut self.params.operator, attr.parse(value), session);
                }
                expanded_name!("", "radius") => {
                    set_attribute(&mut self.params.radius, attr.parse(value), session);
                }
                _ => (),
            }
        }
    }
}

impl Morphology {
    pub fn render(
        &self,
        bounds_builder: BoundsBuilder,
        ctx: &FilterContext,
    ) -> Result<FilterOutput, FilterError> {
        // Although https://www.w3.org/TR/filter-effects/#propdef-color-interpolation-filters does not mention
        // feMorphology as being one of the primitives that does *not* use that property,
        // the SVG1.1 test for filters-morph-01-f.svg fails if we pass the value from the ComputedValues here (that
        // document does not specify the color-interpolation-filters property, so it defaults to linearRGB).
        // So, we pass Auto, which will get resolved to SRGB, and that makes that test pass.
        //
        // I suppose erosion/dilation doesn't care about the color space of the source image?

        let input_1 = ctx.get_input(&self.in1, ColorInterpolationFilters::Auto)?;
        let bounds: IRect = bounds_builder
            .add_input(&input_1)
            .compute(ctx)
            .clipped
            .into();

        let NumberOptionalNumber(rx, ry) = self.radius;

        if rx <= 0.0 && ry <= 0.0 {
            return Ok(FilterOutput {
                surface: input_1.surface().clone(),
                bounds,
            });
        }

        let (rx, ry) = ctx.paffine().transform_distance(rx, ry);

        // The radii can become negative here due to the transform.
        // Additionally The radii being excessively large causes cpu hangups
        let (rx, ry) = (rx.abs().min(10.0), ry.abs().min(10.0));

        let mut surface = ExclusiveImageSurface::new(
            ctx.source_graphic().width(),
            ctx.source_graphic().height(),
            input_1.surface().surface_type(),
        )?;

        surface.modify(&mut |data, stride| {
            for (x, y, _pixel) in Pixels::within(input_1.surface(), bounds) {
                // Compute the kernel rectangle bounds.
                let kernel_bounds = IRect::new(
                    (f64::from(x) - rx).floor() as i32,
                    (f64::from(y) - ry).floor() as i32,
                    (f64::from(x) + rx).ceil() as i32 + 1,
                    (f64::from(y) + ry).ceil() as i32 + 1,
                );

                // Compute the new pixel values.
                let initial = match self.operator {
                    Operator::Erode => u8::MAX,
                    Operator::Dilate => u8::MIN,
                };

                let mut output_pixel = Pixel {
                    r: initial,
                    g: initial,
                    b: initial,
                    a: initial,
                };

                for (_x, _y, pixel) in
                    PixelRectangle::within(input_1.surface(), bounds, kernel_bounds, EdgeMode::None)
                {
                    let op = match self.operator {
                        Operator::Erode => min,
                        Operator::Dilate => max,
                    };

                    output_pixel.r = op(output_pixel.r, pixel.r);
                    output_pixel.g = op(output_pixel.g, pixel.g);
                    output_pixel.b = op(output_pixel.b, pixel.b);
                    output_pixel.a = op(output_pixel.a, pixel.a);
                }

                data.set_pixel(stride, output_pixel, x, y);
            }
        });

        Ok(FilterOutput {
            surface: surface.share()?,
            bounds,
        })
    }

    pub fn get_input_requirements(&self) -> InputRequirements {
        self.in1.get_requirements()
    }
}

impl FilterEffect for FeMorphology {
    fn resolve(
        &self,
        _acquired_nodes: &mut AcquiredNodes<'_>,
        _node: &Node,
    ) -> Result<Vec<ResolvedPrimitive>, FilterResolveError> {
        Ok(vec![ResolvedPrimitive {
            primitive: self.base.clone(),
            params: PrimitiveParams::Morphology(self.params.clone()),
        }])
    }
}

impl Parse for Operator {
    fn parse<'i>(parser: &mut Parser<'i, '_>) -> Result<Self, ParseError<'i>> {
        Ok(parse_identifiers!(
            parser,
            "erode" => Operator::Erode,
            "dilate" => Operator::Dilate,
        )?)
    }
}
