use markup5ever::{expanded_name, local_name, ns};

use crate::document::AcquiredNodes;
use crate::element::{ElementTrait, set_attribute};
use crate::node::Node;
use crate::parsers::ParseValue;
use crate::properties::ColorInterpolationFilters;
use crate::rect::IRect;
use crate::rsvg_log;
use crate::session::Session;
use crate::xml::Attributes;

use super::bounds::BoundsBuilder;
use super::context::{FilterContext, FilterOutput};
use super::{
    FilterEffect, FilterError, FilterResolveError, Input, InputRequirements, Primitive,
    PrimitiveParams, ResolvedPrimitive,
};

/// The `feOffset` filter primitive.
#[derive(Default)]
pub struct FeOffset {
    base: Primitive,
    params: Offset,
}

/// Resolved `feOffset` primitive for rendering.
#[derive(Clone, Default)]
pub struct Offset {
    pub in1: Input,
    pub dx: f64,
    pub dy: f64,
}

impl ElementTrait for FeOffset {
    fn set_attributes(&mut self, attrs: &Attributes, session: &Session) {
        self.params.in1 = self.base.parse_one_input(attrs, session);

        for (attr, value) in attrs.iter() {
            match attr.expanded() {
                expanded_name!("", "dx") => {
                    set_attribute(&mut self.params.dx, attr.parse(value), session)
                }
                expanded_name!("", "dy") => {
                    set_attribute(&mut self.params.dy, attr.parse(value), session)
                }
                _ => (),
            }
        }
    }
}

impl Offset {
    pub fn render(
        &self,
        bounds_builder: BoundsBuilder,
        ctx: &FilterContext,
    ) -> Result<FilterOutput, FilterError> {
        // https://www.w3.org/TR/filter-effects/#ColorInterpolationFiltersProperty
        //
        // "Note: The color-interpolation-filters property just has an
        // effect on filter operations. Therefore, it has no effect on
        // filter primitives like feOffset"
        //
        // This is why we pass Auto here.
        let input_1 = ctx.get_input(&self.in1, ColorInterpolationFilters::Auto)?;
        let bounds = bounds_builder.add_input(&input_1).compute(ctx).clipped;

        rsvg_log!(ctx.session(), "(feOffset bounds={:?}", bounds);

        let (dx, dy) = ctx.paffine().transform_distance(self.dx, self.dy);

        let surface = input_1.surface().offset(bounds, dx, dy)?;

        let ibounds: IRect = bounds.into();

        Ok(FilterOutput {
            surface,
            bounds: ibounds,
        })
    }

    pub fn get_input_requirements(&self) -> InputRequirements {
        self.in1.get_requirements()
    }
}

impl FilterEffect for FeOffset {
    fn resolve(
        &self,
        _acquired_nodes: &mut AcquiredNodes<'_>,
        _node: &Node,
    ) -> Result<Vec<ResolvedPrimitive>, FilterResolveError> {
        Ok(vec![ResolvedPrimitive {
            primitive: self.base.clone(),
            params: PrimitiveParams::Offset(self.params.clone()),
        }])
    }
}
