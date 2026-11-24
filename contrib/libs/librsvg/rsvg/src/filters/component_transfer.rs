use std::cmp::min;

use cssparser::Parser;
use markup5ever::{expanded_name, local_name, ns};

use crate::document::AcquiredNodes;
use crate::element::{ElementData, ElementTrait, set_attribute};
use crate::error::*;
use crate::node::{CascadedValues, Node, NodeBorrow};
use crate::parse_identifiers;
use crate::parsers::{CommaSeparatedList, Parse, ParseValue};
use crate::properties::ColorInterpolationFilters;
use crate::rect::IRect;
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

/// The `feComponentTransfer` filter primitive.
#[derive(Default)]
pub struct FeComponentTransfer {
    base: Primitive,
    params: ComponentTransfer,
}

/// Resolved `feComponentTransfer` primitive for rendering.
#[derive(Clone, Default)]
pub struct ComponentTransfer {
    pub in1: Input,
    pub functions: Functions,
    pub color_interpolation_filters: ColorInterpolationFilters,
}

impl ElementTrait for FeComponentTransfer {
    fn set_attributes(&mut self, attrs: &Attributes, session: &Session) {
        self.params.in1 = self.base.parse_one_input(attrs, session);
    }
}

/// Component transfer function types.
#[derive(Clone, Debug, PartialEq)]
pub enum FunctionType {
    Identity,
    Table,
    Discrete,
    Linear,
    Gamma,
}

impl Parse for FunctionType {
    fn parse<'i>(parser: &mut Parser<'i, '_>) -> Result<Self, ParseError<'i>> {
        Ok(parse_identifiers!(
            parser,
            "identity" => FunctionType::Identity,
            "table" => FunctionType::Table,
            "discrete" => FunctionType::Discrete,
            "linear" => FunctionType::Linear,
            "gamma" => FunctionType::Gamma,
        )?)
    }
}

/// The compute function parameters.
struct FunctionParameters {
    table_values: Vec<f64>,
    slope: f64,
    intercept: f64,
    amplitude: f64,
    exponent: f64,
    offset: f64,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct Functions {
    pub r: FeFuncR,
    pub g: FeFuncG,
    pub b: FeFuncB,
    pub a: FeFuncA,
}

/// The compute function type.
type Function = fn(&FunctionParameters, f64) -> f64;

/// The identity component transfer function.
fn identity(_: &FunctionParameters, value: f64) -> f64 {
    value
}

/// The table component transfer function.
fn table(params: &FunctionParameters, value: f64) -> f64 {
    let n = params.table_values.len() - 1;
    let k = (value * (n as f64)).floor() as usize;

    let k = min(k, n); // Just in case.

    if k == n {
        return params.table_values[k];
    }

    let vk = params.table_values[k];
    let vk1 = params.table_values[k + 1];
    let k = k as f64;
    let n = n as f64;

    vk + (value - k / n) * n * (vk1 - vk)
}

/// The discrete component transfer function.
fn discrete(params: &FunctionParameters, value: f64) -> f64 {
    let n = params.table_values.len();
    let k = (value * (n as f64)).floor() as usize;

    params.table_values[min(k, n - 1)]
}

/// The linear component transfer function.
fn linear(params: &FunctionParameters, value: f64) -> f64 {
    params.slope * value + params.intercept
}

/// The gamma component transfer function.
fn gamma(params: &FunctionParameters, value: f64) -> f64 {
    params.amplitude * value.powf(params.exponent) + params.offset
}

/// Common values for `feFuncX` elements
///
/// The elements `feFuncR`, `feFuncG`, `feFuncB`, `feFuncA` all have the same parameters; this structure
/// contains them.  Later we define newtypes on this struct as [`FeFuncR`], etc.
#[derive(Clone, Debug, PartialEq)]
pub struct FeFuncCommon {
    pub function_type: FunctionType,
    pub table_values: Vec<f64>,
    pub slope: f64,
    pub intercept: f64,
    pub amplitude: f64,
    pub exponent: f64,
    pub offset: f64,
}

impl Default for FeFuncCommon {
    #[inline]
    fn default() -> Self {
        Self {
            function_type: FunctionType::Identity,
            table_values: Vec::new(),
            slope: 1.0,
            intercept: 0.0,
            amplitude: 1.0,
            exponent: 1.0,
            offset: 0.0,
        }
    }
}

// All FeFunc* elements are defined here; they just delegate their attributes
// to the FeFuncCommon inside.
macro_rules! impl_func {
    ($(#[$attr:meta])*
     $name:ident
    ) => {
        #[derive(Clone, Debug, Default, PartialEq)]
        pub struct $name(pub FeFuncCommon);

        impl ElementTrait for $name {
            fn set_attributes(&mut self, attrs: &Attributes, session: &Session) {
                self.0.set_attributes(attrs, session);
            }
        }
    };
}

impl_func!(
    /// The `feFuncR` element.
    FeFuncR
);

impl_func!(
    /// The `feFuncG` element.
    FeFuncG
);

impl_func!(
    /// The `feFuncB` element.
    FeFuncB
);

impl_func!(
    /// The `feFuncA` element.
    FeFuncA
);

impl FeFuncCommon {
    fn set_attributes(&mut self, attrs: &Attributes, session: &Session) {
        for (attr, value) in attrs.iter() {
            match attr.expanded() {
                expanded_name!("", "type") => {
                    set_attribute(&mut self.function_type, attr.parse(value), session)
                }
                expanded_name!("", "tableValues") => {
                    // #691: Limit list to 256 to mitigate malicious SVGs
                    let mut number_list = CommaSeparatedList::<f64, 0, 256>(Vec::new());
                    set_attribute(&mut number_list, attr.parse(value), session);
                    self.table_values = number_list.0;
                }
                expanded_name!("", "slope") => {
                    set_attribute(&mut self.slope, attr.parse(value), session)
                }
                expanded_name!("", "intercept") => {
                    set_attribute(&mut self.intercept, attr.parse(value), session)
                }
                expanded_name!("", "amplitude") => {
                    set_attribute(&mut self.amplitude, attr.parse(value), session)
                }
                expanded_name!("", "exponent") => {
                    set_attribute(&mut self.exponent, attr.parse(value), session)
                }
                expanded_name!("", "offset") => {
                    set_attribute(&mut self.offset, attr.parse(value), session)
                }

                _ => (),
            }
        }

        // The table function type with empty table_values is considered
        // an identity function.
        match self.function_type {
            FunctionType::Table | FunctionType::Discrete => {
                if self.table_values.is_empty() {
                    self.function_type = FunctionType::Identity;
                }
            }
            _ => (),
        }
    }

    fn function_parameters(&self) -> FunctionParameters {
        FunctionParameters {
            table_values: self.table_values.clone(),
            slope: self.slope,
            intercept: self.intercept,
            amplitude: self.amplitude,
            exponent: self.exponent,
            offset: self.offset,
        }
    }

    fn function(&self) -> Function {
        match self.function_type {
            FunctionType::Identity => identity,
            FunctionType::Table => table,
            FunctionType::Discrete => discrete,
            FunctionType::Linear => linear,
            FunctionType::Gamma => gamma,
        }
    }
}

macro_rules! func_or_default {
    ($func_node:ident, $func_type:ident) => {
        match $func_node {
            Some(ref f) => match &*f.borrow_element_data() {
                ElementData::$func_type(e) => (**e).clone(),
                _ => unreachable!(),
            },
            _ => $func_type::default(),
        }
    };
}

macro_rules! get_func_x_node {
    ($func_node:ident, $func_type:ident) => {
        $func_node
            .children()
            .rev()
            .filter(|c| c.is_element())
            .find(|c| matches!(*c.borrow_element_data(), ElementData::$func_type(_)))
    };
}

impl ComponentTransfer {
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

        // Create the output surface.
        let mut surface = ExclusiveImageSurface::new(
            ctx.source_graphic().width(),
            ctx.source_graphic().height(),
            input_1.surface().surface_type(),
        )?;

        fn compute_func(func: &FeFuncCommon) -> impl Fn(u8, f64, f64) -> u8 {
            let compute = func.function();
            let params = func.function_parameters();

            move |value, alpha, new_alpha| {
                let value = f64::from(value) / 255f64;

                let unpremultiplied = if alpha == 0f64 { 0f64 } else { value / alpha };

                let new_value = compute(&params, unpremultiplied);
                let new_value = clamp(new_value, 0f64, 1f64);

                ((new_value * new_alpha * 255f64) + 0.5) as u8
            }
        }

        let compute_r = compute_func(&self.functions.r.0);
        let compute_g = compute_func(&self.functions.g.0);
        let compute_b = compute_func(&self.functions.b.0);

        // Alpha gets special handling since everything else depends on it.
        let compute_a = self.functions.a.0.function();
        let params_a = self.functions.a.0.function_parameters();
        let compute_a = |alpha| compute_a(&params_a, alpha);

        // Do the actual processing.
        surface.modify(&mut |data, stride| {
            for (x, y, pixel) in Pixels::within(input_1.surface(), bounds) {
                let alpha = f64::from(pixel.a) / 255f64;
                let new_alpha = compute_a(alpha);

                let output_pixel = Pixel {
                    r: compute_r(pixel.r, alpha, new_alpha),
                    g: compute_g(pixel.g, alpha, new_alpha),
                    b: compute_b(pixel.b, alpha, new_alpha),
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

    pub fn get_input_requirements(&self) -> InputRequirements {
        self.in1.get_requirements()
    }
}

impl FilterEffect for FeComponentTransfer {
    fn resolve(
        &self,
        _acquired_nodes: &mut AcquiredNodes<'_>,
        node: &Node,
    ) -> Result<Vec<ResolvedPrimitive>, FilterResolveError> {
        let cascaded = CascadedValues::new_from_node(node);
        let values = cascaded.get();

        let mut params = self.params.clone();
        params.functions = get_functions(node)?;
        params.color_interpolation_filters = values.color_interpolation_filters();

        Ok(vec![ResolvedPrimitive {
            primitive: self.base.clone(),
            params: PrimitiveParams::ComponentTransfer(params),
        }])
    }
}

/// Takes a feComponentTransfer and walks its children to produce the feFuncX arguments.
fn get_functions(node: &Node) -> Result<Functions, FilterResolveError> {
    let func_r_node = get_func_x_node!(node, FeFuncR);
    let func_g_node = get_func_x_node!(node, FeFuncG);
    let func_b_node = get_func_x_node!(node, FeFuncB);
    let func_a_node = get_func_x_node!(node, FeFuncA);

    let r = func_or_default!(func_r_node, FeFuncR);
    let g = func_or_default!(func_g_node, FeFuncG);
    let b = func_or_default!(func_b_node, FeFuncB);
    let a = func_or_default!(func_a_node, FeFuncA);

    Ok(Functions { r, g, b, a })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::document::Document;

    #[test]
    fn extracts_functions() {
        let document = Document::load_from_bytes(
            br#"<?xml version="1.0" encoding="UTF-8"?>
<svg xmlns="http://www.w3.org/2000/svg">
  <filter id="filter">
    <feComponentTransfer id="component_transfer">
      <!-- no feFuncR so it should get the defaults -->

      <feFuncG type="table" tableValues="0.0 1.0 2.0"/>

      <feFuncB type="table"/>
      <!-- duplicate this to test that last-one-wins -->
      <feFuncB type="discrete" tableValues="0.0, 1.0" slope="1.0" intercept="2.0" amplitude="3.0" exponent="4.0" offset="5.0"/>

      <!-- no feFuncA so it should get the defaults -->
    </feComponentTransfer>
  </filter>
</svg>
"#
        );

        let component_transfer = document.lookup_internal_node("component_transfer").unwrap();
        let functions = get_functions(&component_transfer).unwrap();

        assert_eq!(
            functions,
            Functions {
                r: FeFuncR::default(),

                g: FeFuncG(FeFuncCommon {
                    function_type: FunctionType::Table,
                    table_values: vec![0.0, 1.0, 2.0],
                    ..FeFuncCommon::default()
                }),

                b: FeFuncB(FeFuncCommon {
                    function_type: FunctionType::Discrete,
                    table_values: vec![0.0, 1.0],
                    slope: 1.0,
                    intercept: 2.0,
                    amplitude: 3.0,
                    exponent: 4.0,
                    offset: 5.0,
                }),

                a: FeFuncA::default(),
            }
        );
    }
}
