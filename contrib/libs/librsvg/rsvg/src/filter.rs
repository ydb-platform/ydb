//! The `filter` element.

use cssparser::Parser;
use markup5ever::{expanded_name, local_name, ns};
use std::slice::Iter;

use crate::color::Color;
use crate::coord_units::CoordUnits;
use crate::document::{AcquiredNodes, NodeId};
use crate::drawing_ctx::Viewport;
use crate::element::{ElementData, ElementTrait, set_attribute};
use crate::error::ValueErrorKind;
use crate::filter_func::FilterFunction;
use crate::filters::{FilterResolveError, FilterSpec};
use crate::length::*;
use crate::node::{Node, NodeBorrow};
use crate::parsers::{Parse, ParseValue};
use crate::rect::Rect;
use crate::rsvg_log;
use crate::session::Session;
use crate::xml::Attributes;
use crate::{borrow_element_as, is_element_of_type};

/// The `<filter>` node.
pub struct Filter {
    x: Length<Horizontal>,
    y: Length<Vertical>,
    width: ULength<Horizontal>,
    height: ULength<Vertical>,
    filter_units: CoordUnits,
    primitive_units: CoordUnits,
}

/// A `<filter>` element definition in user-space coordinates.
pub struct UserSpaceFilter {
    pub rect: Rect,
    pub filter_units: CoordUnits,
    pub primitive_units: CoordUnits,
}

impl Default for Filter {
    /// Constructs a new `Filter` with default properties.
    fn default() -> Self {
        Self {
            x: Length::<Horizontal>::parse_str("-10%").unwrap(),
            y: Length::<Vertical>::parse_str("-10%").unwrap(),
            width: ULength::<Horizontal>::parse_str("120%").unwrap(),
            height: ULength::<Vertical>::parse_str("120%").unwrap(),
            filter_units: CoordUnits::ObjectBoundingBox,
            primitive_units: CoordUnits::UserSpaceOnUse,
        }
    }
}

impl Filter {
    pub fn get_filter_units(&self) -> CoordUnits {
        self.filter_units
    }

    pub fn to_user_space(&self, params: &NormalizeParams) -> UserSpaceFilter {
        let x = self.x.to_user(params);
        let y = self.y.to_user(params);
        let w = self.width.to_user(params);
        let h = self.height.to_user(params);

        let rect = Rect::new(x, y, x + w, y + h);

        UserSpaceFilter {
            rect,
            filter_units: self.filter_units,
            primitive_units: self.primitive_units,
        }
    }
}

impl ElementTrait for Filter {
    fn set_attributes(&mut self, attrs: &Attributes, session: &Session) {
        for (attr, value) in attrs.iter() {
            match attr.expanded() {
                expanded_name!("", "filterUnits") => {
                    set_attribute(&mut self.filter_units, attr.parse(value), session)
                }
                expanded_name!("", "x") => set_attribute(&mut self.x, attr.parse(value), session),
                expanded_name!("", "y") => set_attribute(&mut self.y, attr.parse(value), session),
                expanded_name!("", "width") => {
                    set_attribute(&mut self.width, attr.parse(value), session)
                }
                expanded_name!("", "height") => {
                    set_attribute(&mut self.height, attr.parse(value), session)
                }
                expanded_name!("", "primitiveUnits") => {
                    set_attribute(&mut self.primitive_units, attr.parse(value), session)
                }
                _ => (),
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum FilterValue {
    Url(NodeId),
    Function(FilterFunction),
}

impl FilterValue {
    pub fn to_filter_spec(
        &self,
        acquired_nodes: &mut AcquiredNodes<'_>,
        user_space_params: &NormalizeParams,
        current_color: Color,
        viewport: &Viewport,
        session: &Session,
        node_being_filtered_name: &str,
    ) -> Result<FilterSpec, FilterResolveError> {
        match *self {
            FilterValue::Url(ref node_id) => filter_spec_from_filter_node(
                acquired_nodes,
                viewport,
                session,
                node_id,
                node_being_filtered_name,
            ),

            FilterValue::Function(ref func) => {
                Ok(func.to_filter_spec(user_space_params, current_color))
            }
        }
    }
}

/// Holds the viewport parameters for both objectBoundingBox and userSpaceOnUse units.
///
/// When collecting a set of filter primitives (`feFoo`) into a [`FilterSpec`], which is
/// in user space, we need to convert each primitive's units into user space units.  So,
/// pre-compute both cases and pass them around.
///
/// This struct needs a better name; I didn't want to make it seem specific to filters by
/// calling `FiltersViewport` or `FilterCollectionProcessViewport`.  Maybe the
/// original [`Viewport`] should be this struct, with both cases included...
struct ViewportGen {
    object_bounding_box: Viewport,
    user_space_on_use: Viewport,
}

impl ViewportGen {
    pub fn new(viewport: &Viewport) -> Self {
        ViewportGen {
            object_bounding_box: viewport.with_units(CoordUnits::ObjectBoundingBox),
            user_space_on_use: viewport.with_units(CoordUnits::UserSpaceOnUse),
        }
    }

    fn get(&self, units: CoordUnits) -> &Viewport {
        match units {
            CoordUnits::ObjectBoundingBox => &self.object_bounding_box,
            CoordUnits::UserSpaceOnUse => &self.user_space_on_use,
        }
    }
}

fn extract_filter_from_filter_node(
    filter_node: &Node,
    acquired_nodes: &mut AcquiredNodes<'_>,
    session: &Session,
    filter_viewport: &ViewportGen,
) -> Result<FilterSpec, FilterResolveError> {
    assert!(is_element_of_type!(filter_node, Filter));

    let filter_element = filter_node.borrow_element();

    let user_space_filter = {
        let filter_values = filter_element.get_computed_values();

        let filter = borrow_element_as!(filter_node, Filter);

        filter.to_user_space(&NormalizeParams::new(
            filter_values,
            filter_viewport.get(filter.get_filter_units()),
        ))
    };

    let primitive_viewport = filter_viewport.get(user_space_filter.primitive_units);

    let primitive_nodes = filter_node
        .children()
        .filter(|c| c.is_element())
        // Keep only filter primitives (those that implement the Filter trait)
        .filter(|c| c.borrow_element().as_filter_effect().is_some());

    let mut user_space_primitives = Vec::new();

    for primitive_node in primitive_nodes {
        let elt = primitive_node.borrow_element();
        let effect = elt.as_filter_effect().unwrap();

        let primitive_name = format!("{primitive_node}");

        let primitive_values = elt.get_computed_values();
        let params = NormalizeParams::new(primitive_values, primitive_viewport);

        let primitives = match effect.resolve(acquired_nodes, &primitive_node) {
            Ok(primitives) => primitives,
            Err(e) => {
                rsvg_log!(
                    session,
                    "(filter primitive {} returned an error: {})",
                    primitive_name,
                    e
                );
                return Err(e);
            }
        };

        for p in primitives {
            user_space_primitives.push(p.into_user_space(&params));
        }
    }

    Ok(FilterSpec {
        name: filter_element
            .get_id()
            .unwrap_or("(filter element without id)")
            .to_string(),
        user_space_filter,
        primitives: user_space_primitives,
    })
}

fn filter_spec_from_filter_node(
    acquired_nodes: &mut AcquiredNodes<'_>,
    viewport: &Viewport,
    session: &Session,
    node_id: &NodeId,
    node_being_filtered_name: &str,
) -> Result<FilterSpec, FilterResolveError> {
    let filter_viewport = ViewportGen::new(viewport);

    acquired_nodes
        .acquire(node_id)
        .map_err(|e| {
            rsvg_log!(
                *session,
                "element {} will not be filtered with \"{}\": {}",
                node_being_filtered_name,
                node_id,
                e
            );
            FilterResolveError::ReferenceToNonFilterElement
        })
        .and_then(|acquired| {
            let node = acquired.get();

            match *node.borrow_element_data() {
                ElementData::Filter(_) => {
                    extract_filter_from_filter_node(node, acquired_nodes, session, &filter_viewport)
                }

                _ => {
                    rsvg_log!(
                        *session,
                        "element {} will not be filtered since \"{}\" is not a filter",
                        node_being_filtered_name,
                        node_id,
                    );
                    Err(FilterResolveError::ReferenceToNonFilterElement)
                }
            }
        })
}

#[derive(Debug, Default, Clone, PartialEq)]
pub struct FilterValueList(Vec<FilterValue>);

impl FilterValueList {
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn iter(&self) -> Iter<'_, FilterValue> {
        self.0.iter()
    }
}

impl Parse for FilterValueList {
    fn parse<'i>(parser: &mut Parser<'i, '_>) -> Result<Self, crate::error::ParseError<'i>> {
        let mut result = FilterValueList::default();

        loop {
            let loc = parser.current_source_location();

            let filter_value = if let Ok(func) = parser.try_parse(|p| FilterFunction::parse(p)) {
                FilterValue::Function(func)
            } else {
                let url = parser.expect_url()?;
                let node_id = NodeId::parse(&url)
                    .map_err(|e| loc.new_custom_error(ValueErrorKind::from(e)))?;

                FilterValue::Url(node_id)
            };

            result.0.push(filter_value);

            if parser.is_exhausted() {
                break;
            }
        }

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_filter_value_list() {
        let n1 = NodeId::External("foo.svg".to_string(), "bar".to_string());
        let n2 = NodeId::External("test.svg".to_string(), "baz".to_string());
        assert_eq!(
            FilterValueList::parse_str("url(foo.svg#bar) url(test.svg#baz)").unwrap(),
            FilterValueList(vec![FilterValue::Url(n1), FilterValue::Url(n2)])
        );
    }

    #[test]
    fn detects_invalid_filter_value_list() {
        assert!(FilterValueList::parse_str("none").is_err());
        assert!(FilterValueList::parse_str("").is_err());
        assert!(FilterValueList::parse_str("fail").is_err());
        assert!(FilterValueList::parse_str("url(#test) none").is_err());
    }
}
