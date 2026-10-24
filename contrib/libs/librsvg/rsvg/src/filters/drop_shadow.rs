use markup5ever::{expanded_name, local_name, ns};

use crate::color::resolve_color;
use crate::document::AcquiredNodes;
use crate::element::{ElementTrait, set_attribute};
use crate::filter_func::drop_shadow_primitives;
use crate::node::{CascadedValues, Node};
use crate::parsers::{NumberOptionalNumber, ParseValue};
use crate::session::Session;
use crate::xml::Attributes;

use super::{FilterEffect, FilterResolveError, Input, Primitive, ResolvedPrimitive};

/// The `feDropShadow` element.
#[derive(Default)]
pub struct FeDropShadow {
    base: Primitive,
    params: DropShadow,
}

/// Resolved `feDropShadow` parameters for rendering.
pub struct DropShadow {
    pub in1: Input,
    pub dx: f64,
    pub dy: f64,
    pub std_deviation: NumberOptionalNumber<f64>,
}

impl Default for DropShadow {
    /// Defaults come from <https://www.w3.org/TR/filter-effects/#feDropShadowElement>
    fn default() -> Self {
        Self {
            in1: Default::default(),
            dx: 2.0,
            dy: 2.0,
            std_deviation: NumberOptionalNumber(2.0, 2.0),
        }
    }
}

impl ElementTrait for FeDropShadow {
    fn set_attributes(&mut self, attrs: &Attributes, session: &Session) {
        self.params.in1 = self.base.parse_one_input(attrs, session);

        for (attr, value) in attrs.iter() {
            match attr.expanded() {
                expanded_name!("", "dx") => {
                    set_attribute(&mut self.params.dx, attr.parse(value), session);
                }

                expanded_name!("", "dy") => {
                    set_attribute(&mut self.params.dy, attr.parse(value), session);
                }

                expanded_name!("", "stdDeviation") => {
                    set_attribute(&mut self.params.std_deviation, attr.parse(value), session);
                }

                _ => (),
            }
        }
    }
}

impl FilterEffect for FeDropShadow {
    fn resolve(
        &self,
        _acquired_nodes: &mut AcquiredNodes<'_>,
        node: &Node,
    ) -> Result<Vec<ResolvedPrimitive>, FilterResolveError> {
        let cascaded = CascadedValues::new_from_node(node);
        let values = cascaded.get();

        let color = resolve_color(
            &values.flood_color().0,
            values.flood_opacity().0,
            &values.color().0,
        );

        Ok(drop_shadow_primitives(
            self.params.dx,
            self.params.dy,
            self.params.std_deviation,
            color,
        ))
    }
}
