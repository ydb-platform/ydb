use cssparser::Parser;
use markup5ever::{QualName, expanded_name, local_name, ns};
use nalgebra::{Matrix3, Matrix4x5, Matrix5, Vector5};

use crate::document::AcquiredNodes;
use crate::element::{ElementTrait, set_attribute};
use crate::error::*;
use crate::node::{CascadedValues, Node};
use crate::parse_identifiers;
use crate::parsers::{CommaSeparatedList, Parse, ParseValue};
use crate::properties::ColorInterpolationFilters;
use crate::rect::IRect;
use crate::rsvg_log;
use crate::session::Session;
use crate::surface_utils::{
    ImageSurfaceDataExt, Pixel, iterators::Pixels, shared_surface::ExclusiveImageSurface,
};
use crate::util::clamp;
use crate::xml::Attributes;

use super::bounds::BoundsBuilder;
use super::context::{FilterContext, FilterOutput};
use super::{
    FilterEffect, FilterError, FilterResolveError, Input, InputRequirements, Primitive,
    PrimitiveParams, ResolvedPrimitive,
};

/// Color matrix operation types.
#[derive(Debug, Default, Clone, Copy, Eq, PartialEq)]
enum OperationType {
    #[default]
    Matrix,
    Saturate,
    HueRotate,
    LuminanceToAlpha,
}

/// The `feColorMatrix` filter primitive.
#[derive(Default)]
pub struct FeColorMatrix {
    base: Primitive,
    params: ColorMatrix,
}

/// Resolved `feColorMatrix` primitive for rendering.
#[derive(Clone)]
pub struct ColorMatrix {
    pub in1: Input,
    pub matrix: Matrix5<f64>,
    pub color_interpolation_filters: ColorInterpolationFilters,
}

impl Default for ColorMatrix {
    fn default() -> ColorMatrix {
        ColorMatrix {
            in1: Default::default(),
            color_interpolation_filters: Default::default(),

            // nalgebra's Default for Matrix5 is all zeroes, so we actually need this :(
            matrix: Matrix5::identity(),
        }
    }
}

impl ElementTrait for FeColorMatrix {
    fn set_attributes(&mut self, attrs: &Attributes, session: &Session) {
        self.params.in1 = self.base.parse_one_input(attrs, session);

        // First, determine the operation type.
        let mut operation_type = Default::default();
        for (attr, value) in attrs
            .iter()
            .filter(|(attr, _)| attr.expanded() == expanded_name!("", "type"))
        {
            set_attribute(&mut operation_type, attr.parse(value), session);
        }

        // Now read the matrix correspondingly.
        //
        // Here we cannot assume that ColorMatrix::default() has provided the correct
        // initial value for the matrix itself, since the initial value for the matrix
        // (i.e. the value to which it should fall back if the `values` attribute is in
        // error) depends on the operation_type.
        //
        // So, for each operation_type, first initialize the proper default matrix, then
        // try to parse the value.

        use OperationType::*;

        self.params.matrix = match operation_type {
            Matrix => ColorMatrix::default_matrix(),
            Saturate => ColorMatrix::saturate_matrix(1.0),
            HueRotate => ColorMatrix::hue_rotate_matrix(0.0),
            LuminanceToAlpha => ColorMatrix::luminance_to_alpha_matrix(),
        };

        for (attr, value) in attrs
            .iter()
            .filter(|(attr, _)| attr.expanded() == expanded_name!("", "values"))
        {
            match operation_type {
                Matrix => parse_matrix(&mut self.params.matrix, attr, value, session),
                Saturate => parse_saturate_matrix(&mut self.params.matrix, attr, value, session),
                HueRotate => parse_hue_rotate_matrix(&mut self.params.matrix, attr, value, session),
                LuminanceToAlpha => {
                    parse_luminance_to_alpha_matrix(&mut self.params.matrix, attr, value, session)
                }
            }
        }
    }
}

fn parse_matrix(dest: &mut Matrix5<f64>, attr: QualName, value: &str, session: &Session) {
    let parsed: Result<CommaSeparatedList<f64, 20, 20>, _> = attr.parse(value);

    match parsed {
        Ok(CommaSeparatedList(v)) => {
            let matrix = Matrix4x5::from_row_slice(&v);
            let mut matrix = matrix.fixed_resize(0.0);
            matrix[(4, 4)] = 1.0;
            *dest = matrix;
        }

        Err(e) => {
            rsvg_log!(
                session,
                "element feColorMatrix with type=\"matrix\", expected a values attribute with 20 numbers: {}",
                e
            );
        }
    }
}

fn parse_saturate_matrix(dest: &mut Matrix5<f64>, attr: QualName, value: &str, session: &Session) {
    let parsed: Result<f64, _> = attr.parse(value);

    match parsed {
        Ok(s) => {
            *dest = ColorMatrix::saturate_matrix(s);
        }

        Err(e) => {
            rsvg_log!(
                session,
                "element feColorMatrix with type=\"saturate\", expected a values attribute with 1 number: {}",
                e
            );
        }
    }
}

fn parse_hue_rotate_matrix(
    dest: &mut Matrix5<f64>,
    attr: QualName,
    value: &str,
    session: &Session,
) {
    let parsed: Result<f64, _> = attr.parse(value);

    match parsed {
        Ok(degrees) => {
            *dest = ColorMatrix::hue_rotate_matrix(degrees.to_radians());
        }

        Err(e) => {
            rsvg_log!(
                session,
                "element feColorMatrix with type=\"hueRotate\", expected a values attribute with 1 number: {}",
                e
            );
        }
    }
}

fn parse_luminance_to_alpha_matrix(
    _dest: &mut Matrix5<f64>,
    _attr: QualName,
    _value: &str,
    session: &Session,
) {
    // There's nothing to parse, since our caller already supplied the default value,
    // and type="luminanceToAlpha" does not takes a `values` attribute.  So, just warn
    // that the value is being ignored.

    rsvg_log!(
        session,
        "ignoring \"values\" attribute for feColorMatrix with type=\"luminanceToAlpha\""
    );
}

impl ColorMatrix {
    pub fn render(
        &self,
        bounds_builder: BoundsBuilder,
        ctx: &FilterContext,
    ) -> Result<FilterOutput, FilterError> {
        let input_1 = ctx.get_input(&self.in1, self.color_interpolation_filters)?;
        let bounds: IRect = bounds_builder
            .add_input(&input_1)
            .compute(ctx)
            .clipped
            .into();

        let mut surface = ExclusiveImageSurface::new(
            ctx.source_graphic().width(),
            ctx.source_graphic().height(),
            input_1.surface().surface_type(),
        )?;

        surface.modify(&mut |data, stride| {
            for (x, y, pixel) in Pixels::within(input_1.surface(), bounds) {
                let alpha = f64::from(pixel.a) / 255f64;

                let pixel_vec = if alpha == 0.0 {
                    Vector5::new(0.0, 0.0, 0.0, 0.0, 1.0)
                } else {
                    Vector5::new(
                        f64::from(pixel.r) / 255f64 / alpha,
                        f64::from(pixel.g) / 255f64 / alpha,
                        f64::from(pixel.b) / 255f64 / alpha,
                        alpha,
                        1.0,
                    )
                };
                let mut new_pixel_vec = Vector5::zeros();
                self.matrix.mul_to(&pixel_vec, &mut new_pixel_vec);

                let new_alpha = clamp(new_pixel_vec[3], 0.0, 1.0);

                let premultiply = |x: f64| ((clamp(x, 0.0, 1.0) * new_alpha * 255f64) + 0.5) as u8;

                let output_pixel = Pixel {
                    r: premultiply(new_pixel_vec[0]),
                    g: premultiply(new_pixel_vec[1]),
                    b: premultiply(new_pixel_vec[2]),
                    a: ((new_alpha * 255f64) + 0.5) as u8,
                };

                data.set_pixel(stride, output_pixel, x, y);
            }
        });

        Ok(FilterOutput {
            surface: surface.share()?,
            bounds,
        })
    }

    /// Compute a `type="hueRotate"` matrix.
    ///
    /// <https://drafts.fxtf.org/filter-effects/#element-attrdef-fecolormatrix-values>
    #[rustfmt::skip]
    pub fn hue_rotate_matrix(radians: f64) -> Matrix5<f64> {
        let (sin, cos) = radians.sin_cos();

        let a = Matrix3::new(
            0.213, 0.715, 0.072,
            0.213, 0.715, 0.072,
            0.213, 0.715, 0.072,
        );

        let b = Matrix3::new(
             0.787, -0.715, -0.072,
            -0.213,  0.285, -0.072,
            -0.213, -0.715,  0.928,
        );

        let c = Matrix3::new(
            -0.213, -0.715,  0.928,
             0.143,  0.140, -0.283,
            -0.787,  0.715,  0.072,
        );

        let top_left = a + b * cos + c * sin;

        let mut matrix = top_left.fixed_resize(0.0);
        matrix[(3, 3)] = 1.0;
        matrix[(4, 4)] = 1.0;
        matrix
    }

    /// Compute a `type="luminanceToAlpha"` matrix.
    ///
    /// <https://drafts.fxtf.org/filter-effects/#element-attrdef-fecolormatrix-values>
    #[rustfmt::skip]
    fn luminance_to_alpha_matrix() -> Matrix5<f64> {
        Matrix5::new(
            0.0,    0.0,    0.0,    0.0, 0.0,
            0.0,    0.0,    0.0,    0.0, 0.0,
            0.0,    0.0,    0.0,    0.0, 0.0,
            0.2126, 0.7152, 0.0722, 0.0, 0.0,
            0.0,    0.0,    0.0,    0.0, 1.0,
        )
    }

    /// Compute a `type="saturate"` matrix.
    ///
    /// <https://drafts.fxtf.org/filter-effects/#element-attrdef-fecolormatrix-values>
    #[rustfmt::skip]
    fn saturate_matrix(s: f64) -> Matrix5<f64> {
        Matrix5::new(
            0.213 + 0.787 * s, 0.715 - 0.715 * s, 0.072 - 0.072 * s, 0.0, 0.0,
            0.213 - 0.213 * s, 0.715 + 0.285 * s, 0.072 - 0.072 * s, 0.0, 0.0,
            0.213 - 0.213 * s, 0.715 - 0.715 * s, 0.072 + 0.928 * s, 0.0, 0.0,
            0.0,               0.0,               0.0,               1.0, 0.0,
            0.0,               0.0,               0.0,               0.0, 1.0,
        )
    }

    /// Default for `type="matrix"`.
    ///
    /// <https://drafts.fxtf.org/filter-effects/#element-attrdef-fecolormatrix-values>
    fn default_matrix() -> Matrix5<f64> {
        Matrix5::identity()
    }

    pub fn get_input_requirements(&self) -> InputRequirements {
        self.in1.get_requirements()
    }
}

impl FilterEffect for FeColorMatrix {
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
            params: PrimitiveParams::ColorMatrix(params),
        }])
    }
}

impl Parse for OperationType {
    fn parse<'i>(parser: &mut Parser<'i, '_>) -> Result<Self, ParseError<'i>> {
        Ok(parse_identifiers!(
            parser,
            "matrix" => OperationType::Matrix,
            "saturate" => OperationType::Saturate,
            "hueRotate" => OperationType::HueRotate,
            "luminanceToAlpha" => OperationType::LuminanceToAlpha,
        )?)
    }
}
