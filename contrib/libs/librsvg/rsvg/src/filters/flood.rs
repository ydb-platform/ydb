use crate::color::{Color, resolve_color};
use crate::document::AcquiredNodes;
use crate::element::ElementTrait;
use crate::node::{CascadedValues, Node};
use crate::rect::IRect;
use crate::rsvg_log;
use crate::session::Session;
use crate::xml::Attributes;

use super::bounds::BoundsBuilder;
use super::context::{FilterContext, FilterOutput};
use super::{
    FilterEffect, FilterError, FilterResolveError, InputRequirements, Primitive, PrimitiveParams,
    ResolvedPrimitive,
};

/// The `feFlood` filter primitive.
#[derive(Default)]
pub struct FeFlood {
    base: Primitive,
}

/// Resolved `feFlood` primitive for rendering.
pub struct Flood {
    pub color: Color,
}

impl ElementTrait for FeFlood {
    fn set_attributes(&mut self, attrs: &Attributes, session: &Session) {
        self.base.parse_no_inputs(attrs, session);
    }
}

impl Flood {
    pub fn render(
        &self,
        bounds_builder: BoundsBuilder,
        ctx: &FilterContext,
    ) -> Result<FilterOutput, FilterError> {
        let bounds: IRect = bounds_builder.compute(ctx).clipped.into();
        rsvg_log!(ctx.session(), "(feFlood bounds={:?}", bounds);

        let surface = ctx.source_graphic().flood(bounds, self.color)?;

        Ok(FilterOutput { surface, bounds })
    }

    pub fn get_input_requirements(&self) -> InputRequirements {
        InputRequirements::default()
    }
}

impl FilterEffect for FeFlood {
    fn resolve(
        &self,
        _acquired_nodes: &mut AcquiredNodes<'_>,
        node: &Node,
    ) -> Result<Vec<ResolvedPrimitive>, FilterResolveError> {
        let cascaded = CascadedValues::new_from_node(node);
        let values = cascaded.get();

        Ok(vec![ResolvedPrimitive {
            primitive: self.base.clone(),
            params: PrimitiveParams::Flood(Flood {
                color: resolve_color(
                    &values.flood_color().0,
                    values.flood_opacity().0,
                    &values.color().0,
                ),
            }),
        }])
    }
}
