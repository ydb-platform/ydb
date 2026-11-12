use cssparser::Parser;
use markup5ever::{expanded_name, local_name, ns};
use nalgebra::{DMatrix, Dyn, VecStorage};

use crate::bench_only::{
    EdgeMode, ExclusiveImageSurface, ImageSurfaceDataExt, Pixel, PixelRectangle, Pixels,
};
use crate::document::AcquiredNodes;
use crate::element::{ElementTrait, set_attribute};
use crate::error::*;
use crate::node::{CascadedValues, Node};
use crate::parse_identifiers;
use crate::parsers::{CommaSeparatedList, NumberOptionalNumber, Parse, ParseValue};
use crate::properties::ColorInterpolationFilters;
use crate::rect::IRect;
use crate::rsvg_log;
use crate::session::Session;
use crate::util::clamp;
use crate::xml::Attributes;

use super::bounds::BoundsBuilder;
use super::context::{FilterContext, FilterOutput};
use super::{
    FilterEffect, FilterError, FilterResolveError, Input, InputRequirements, Primitive,
    PrimitiveParams, ResolvedPrimitive,
};

/// The `feConvolveMatrix` filter primitive.
#[derive(Default)]
pub struct FeConvolveMatrix {
    base: Primitive,
    params: ConvolveMatrix,
}

/// Resolved `feConvolveMatrix` primitive for rendering.
#[derive(Clone)]
pub struct ConvolveMatrix {
    in1: Input,
    order: NumberOptionalNumber<u32>,
    kernel_matrix: CommaSeparatedList<f64, 0, 400>, // #691: Limit list to 400 (20x20) to mitigate malicious SVGs
    divisor: f64,
    bias: f64,
    target_x: Option<u32>,
    target_y: Option<u32>,
    edge_mode: EdgeMode,
    kernel_unit_length: KernelUnitLength,
    preserve_alpha: bool,
    color_interpolation_filters: ColorInterpolationFilters,
}

#[derive(Clone, Default)]
pub struct KernelUnitLength(pub Option<(f64, f64)>);

impl Parse for KernelUnitLength {
    fn parse<'i>(parser: &mut Parser<'i, '_>) -> Result<Self, ParseError<'i>> {
        let loc = parser.current_source_location();
        let v: Result<NumberOptionalNumber<f64>, _> = NumberOptionalNumber::parse(parser);
        match v {
            Ok(NumberOptionalNumber(x, y)) if x > 0.0 && y > 0.0 => {
                Ok(KernelUnitLength(Some((x, y))))
            } // Only accept positive values
            Ok(_) => {
                Err(loc.new_custom_error(ValueErrorKind::value_error("expected positive values")))
            }
            Err(e) => Err(e),
        }
    }
}

impl Default for ConvolveMatrix {
    /// Constructs a new `ConvolveMatrix` with empty properties.
    #[inline]
    fn default() -> ConvolveMatrix {
        ConvolveMatrix {
            in1: Default::default(),
            order: NumberOptionalNumber(3, 3),
            kernel_matrix: CommaSeparatedList(Vec::new()),
            divisor: 0.0,
            bias: 0.0,
            target_x: None,
            target_y: None,
            // Note that per the spec, `edgeMode` has a different initial value
            // in feConvolveMatrix than feGaussianBlur.
            edge_mode: EdgeMode::Duplicate,
            kernel_unit_length: KernelUnitLength::default(),
            preserve_alpha: false,
            color_interpolation_filters: Default::default(),
        }
    }
}

impl ElementTrait for FeConvolveMatrix {
    fn set_attributes(&mut self, attrs: &Attributes, session: &Session) {
        self.params.in1 = self.base.parse_one_input(attrs, session);

        for (attr, value) in attrs.iter() {
            match attr.expanded() {
                expanded_name!("", "order") => {
                    set_attribute(&mut self.params.order, attr.parse(value), session)
                }
                expanded_name!("", "kernelMatrix") => {
                    set_attribute(&mut self.params.kernel_matrix, attr.parse(value), session)
                }
                expanded_name!("", "divisor") => {
                    set_attribute(&mut self.params.divisor, attr.parse(value), session)
                }
                expanded_name!("", "bias") => {
                    set_attribute(&mut self.params.bias, attr.parse(value), session)
                }
                expanded_name!("", "targetX") => {
                    set_attribute(&mut self.params.target_x, attr.parse(value), session)
                }
                expanded_name!("", "targetY") => {
                    set_attribute(&mut self.params.target_y, attr.parse(value), session)
                }
                expanded_name!("", "edgeMode") => {
                    set_attribute(&mut self.params.edge_mode, attr.parse(value), session)
                }
                expanded_name!("", "kernelUnitLength") => set_attribute(
                    &mut self.params.kernel_unit_length,
                    attr.parse(value),
                    session,
                ),
                expanded_name!("", "preserveAlpha") => {
                    set_attribute(&mut self.params.preserve_alpha, attr.parse(value), session);
                }

                _ => (),
            }
        }
    }
}

impl ConvolveMatrix {
    pub fn render(
        &self,
        bounds_builder: BoundsBuilder,
        ctx: &FilterContext,
    ) -> Result<FilterOutput, FilterError> {
        #![allow(clippy::many_single_char_names)]

        let input_1 = ctx.get_input(&self.in1, self.color_interpolation_filters)?;
        let mut bounds: IRect = bounds_builder
            .add_input(&input_1)
            .compute(ctx)
            .clipped
            .into();
        let original_bounds = bounds;

        let target_x = match self.target_x {
            Some(x) if x >= self.order.0 => {
                return Err(FilterError::InvalidParameter(
                    "targetX must be less than orderX".to_string(),
                ));
            }
            Some(x) => x,
            None => self.order.0 / 2,
        };

        let target_y = match self.target_y {
            Some(y) if y >= self.order.1 => {
                return Err(FilterError::InvalidParameter(
                    "targetY must be less than orderY".to_string(),
                ));
            }
            Some(y) => y,
            None => self.order.1 / 2,
        };

        let mut input_surface = if self.preserve_alpha {
            // preserve_alpha means we need to premultiply and unpremultiply the values.
            input_1.surface().unpremultiply(bounds)?
        } else {
            input_1.surface().clone()
        };

        let scale = self
            .kernel_unit_length
            .0
            .map(|(dx, dy)| ctx.paffine().transform_distance(dx, dy));

        if let Some((ox, oy)) = scale {
            // Scale the input surface to match kernel_unit_length.
            let (new_surface, new_bounds) = input_surface.scale(bounds, 1.0 / ox, 1.0 / oy)?;

            input_surface = new_surface;
            bounds = new_bounds;
        }

        let cols = self.order.0 as usize;
        let rows = self.order.1 as usize;
        let number_of_elements = cols * rows;
        let numbers = self.kernel_matrix.0.clone();

        if numbers.len() != number_of_elements && numbers.len() != 400 {
            // "If the result of orderX * orderY is not equal to the the number of entries
            // in the value list, the filter primitive acts as a pass through filter."
            //
            // https://drafts.fxtf.org/filter-effects/#element-attrdef-feconvolvematrix-kernelmatrix
            rsvg_log!(
                ctx.session(),
                "feConvolveMatrix got {} elements when it expected {}; ignoring it",
                numbers.len(),
                number_of_elements
            );
            return Ok(FilterOutput {
                surface: input_1.surface().clone(),
                bounds: original_bounds,
            });
        }

        let matrix = DMatrix::from_data(VecStorage::new(Dyn(rows), Dyn(cols), numbers));

        let divisor = if self.divisor != 0.0 {
            self.divisor
        } else {
            let d = matrix.iter().sum();

            if d != 0.0 { d } else { 1.0 }
        };

        let mut surface = ExclusiveImageSurface::new(
            input_surface.width(),
            input_surface.height(),
            input_1.surface().surface_type(),
        )?;

        surface.modify(&mut |data, stride| {
            for (x, y, pixel) in Pixels::within(&input_surface, bounds) {
                // Compute the convolution rectangle bounds.
                let kernel_bounds = IRect::new(
                    x as i32 - target_x as i32,
                    y as i32 - target_y as i32,
                    x as i32 - target_x as i32 + self.order.0 as i32,
                    y as i32 - target_y as i32 + self.order.1 as i32,
                );

                // Do the convolution.
                let mut r = 0.0;
                let mut g = 0.0;
                let mut b = 0.0;
                let mut a = 0.0;

                for (x, y, pixel) in
                    PixelRectangle::within(&input_surface, bounds, kernel_bounds, self.edge_mode)
                {
                    let kernel_x = (kernel_bounds.x1 - x - 1) as usize;
                    let kernel_y = (kernel_bounds.y1 - y - 1) as usize;

                    r += f64::from(pixel.r) / 255.0 * matrix[(kernel_y, kernel_x)];
                    g += f64::from(pixel.g) / 255.0 * matrix[(kernel_y, kernel_x)];
                    b += f64::from(pixel.b) / 255.0 * matrix[(kernel_y, kernel_x)];

                    if !self.preserve_alpha {
                        a += f64::from(pixel.a) / 255.0 * matrix[(kernel_y, kernel_x)];
                    }
                }

                // If preserve_alpha is true, set a to the source alpha value.
                if self.preserve_alpha {
                    a = f64::from(pixel.a) / 255.0;
                } else {
                    a = a / divisor + self.bias;
                }

                let clamped_a = clamp(a, 0.0, 1.0);

                let compute = |x| {
                    let x = x / divisor + self.bias * a;

                    let x = if self.preserve_alpha {
                        // Premultiply the output value.
                        clamp(x, 0.0, 1.0) * clamped_a
                    } else {
                        clamp(x, 0.0, clamped_a)
                    };

                    ((x * 255.0) + 0.5) as u8
                };

                let output_pixel = Pixel {
                    r: compute(r),
                    g: compute(g),
                    b: compute(b),
                    a: ((clamped_a * 255.0) + 0.5) as u8,
                };

                data.set_pixel(stride, output_pixel, x, y);
            }
        });

        let mut surface = surface.share()?;

        if let Some((ox, oy)) = scale {
            // Scale the output surface back.
            surface = surface.scale_to(
                ctx.source_graphic().width(),
                ctx.source_graphic().height(),
                original_bounds,
                ox,
                oy,
            )?;

            bounds = original_bounds;
        }

        Ok(FilterOutput { surface, bounds })
    }

    pub fn get_input_requirements(&self) -> InputRequirements {
        self.in1.get_requirements()
    }
}

impl FilterEffect for FeConvolveMatrix {
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
            params: PrimitiveParams::ConvolveMatrix(params),
        }])
    }
}

// Used for the preserveAlpha attribute
impl Parse for bool {
    fn parse<'i>(parser: &mut Parser<'i, '_>) -> Result<Self, ParseError<'i>> {
        Ok(parse_identifiers!(
            parser,
            "false" => false,
            "true" => true,
        )?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kernel_unit_length_expects_positive_numbers() {
        assert!(KernelUnitLength::parse_str("-1").is_err());
        assert!(KernelUnitLength::parse_str("1 -1").is_err());
        assert!(KernelUnitLength::parse_str("-1 -1").is_err());
    }

    #[test]
    fn kernel_unit_length_catches_errors() {
        assert!(KernelUnitLength::parse_str("foo").is_err());
    }
}
