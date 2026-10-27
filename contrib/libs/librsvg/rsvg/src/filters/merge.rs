use markup5ever::{expanded_name, local_name, ns};

use crate::document::AcquiredNodes;
use crate::element::{ElementData, ElementTrait, set_attribute};
use crate::node::{CascadedValues, Node, NodeBorrow};
use crate::parsers::ParseValue;
use crate::properties::ColorInterpolationFilters;
use crate::rect::IRect;
use crate::session::Session;
use crate::surface_utils::shared_surface::{Operator, SharedImageSurface, SurfaceType};
use crate::xml::Attributes;

use super::bounds::BoundsBuilder;
use super::context::{FilterContext, FilterOutput};
use super::{
    FilterEffect, FilterError, FilterResolveError, Input, InputRequirements, Primitive,
    PrimitiveParams, ResolvedPrimitive,
};

/// The `feMerge` filter primitive.
pub struct FeMerge {
    base: Primitive,
}

/// The `<feMergeNode>` element.
#[derive(Clone, Default)]
pub struct FeMergeNode {
    in1: Input,
}

/// Resolved `feMerge` primitive for rendering.
pub struct Merge {
    pub merge_nodes: Vec<MergeNode>,
}

/// Resolved `feMergeNode` for rendering.
#[derive(Debug, Default, PartialEq)]
pub struct MergeNode {
    pub in1: Input,
    pub color_interpolation_filters: ColorInterpolationFilters,
}

impl Default for FeMerge {
    /// Constructs a new `Merge` with empty properties.
    #[inline]
    fn default() -> FeMerge {
        FeMerge {
            base: Default::default(),
        }
    }
}

impl ElementTrait for FeMerge {
    fn set_attributes(&mut self, attrs: &Attributes, session: &Session) {
        self.base.parse_no_inputs(attrs, session);
    }
}

impl ElementTrait for FeMergeNode {
    fn set_attributes(&mut self, attrs: &Attributes, session: &Session) {
        for (attr, value) in attrs.iter() {
            if let expanded_name!("", "in") = attr.expanded() {
                set_attribute(&mut self.in1, attr.parse(value), session);
            }
        }
    }
}

impl MergeNode {
    fn render(
        &self,
        ctx: &FilterContext,
        bounds: IRect,
        output_surface: Option<SharedImageSurface>,
    ) -> Result<SharedImageSurface, FilterError> {
        let input = ctx.get_input(&self.in1, self.color_interpolation_filters)?;

        if output_surface.is_none() {
            return Ok(input.surface().clone());
        }

        input
            .surface()
            .compose(&output_surface.unwrap(), bounds, Operator::Over)
            .map_err(FilterError::CairoError)
    }
}

impl Merge {
    pub fn render(
        &self,
        bounds_builder: BoundsBuilder,
        ctx: &FilterContext,
    ) -> Result<FilterOutput, FilterError> {
        // Compute the filter bounds, taking each feMergeNode's input into account.
        let mut bounds_builder = bounds_builder;
        for merge_node in &self.merge_nodes {
            let input = ctx.get_input(&merge_node.in1, merge_node.color_interpolation_filters)?;
            bounds_builder = bounds_builder.add_input(&input);
        }

        let bounds: IRect = bounds_builder.compute(ctx).clipped.into();

        // Now merge them all.
        let mut output_surface = None;
        for merge_node in &self.merge_nodes {
            output_surface = merge_node.render(ctx, bounds, output_surface).ok();
        }

        let surface = match output_surface {
            Some(s) => s,
            None => SharedImageSurface::empty(
                ctx.source_graphic().width(),
                ctx.source_graphic().height(),
                SurfaceType::AlphaOnly,
            )?,
        };

        Ok(FilterOutput { surface, bounds })
    }

    pub fn get_input_requirements(&self) -> InputRequirements {
        self.merge_nodes
            .iter()
            .map(|mn| mn.in1.get_requirements())
            .fold(InputRequirements::default(), |a, b| a.fold(b))
    }
}

impl FilterEffect for FeMerge {
    fn resolve(
        &self,
        _acquired_nodes: &mut AcquiredNodes<'_>,
        node: &Node,
    ) -> Result<Vec<ResolvedPrimitive>, FilterResolveError> {
        Ok(vec![ResolvedPrimitive {
            primitive: self.base.clone(),
            params: PrimitiveParams::Merge(Merge {
                merge_nodes: resolve_merge_nodes(node)?,
            }),
        }])
    }
}

/// Takes a feMerge and walks its children to produce a list of feMergeNode arguments.
fn resolve_merge_nodes(node: &Node) -> Result<Vec<MergeNode>, FilterResolveError> {
    let mut merge_nodes = Vec::new();

    for child in node.children().filter(|c| c.is_element()) {
        let cascaded = CascadedValues::new_from_node(&child);
        let values = cascaded.get();

        if let ElementData::FeMergeNode(merge_node) = &*child.borrow_element_data() {
            merge_nodes.push(MergeNode {
                in1: merge_node.in1.clone(),
                color_interpolation_filters: values.color_interpolation_filters(),
            });
        }
    }

    Ok(merge_nodes)
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::borrow_element_as;
    use crate::document::Document;

    #[test]
    fn extracts_parameters() {
        let document = Document::load_from_bytes(
            br#"<?xml version="1.0" encoding="UTF-8"?>
<svg xmlns="http://www.w3.org/2000/svg">
  <filter id="filter">
    <feMerge id="merge">
      <feMergeNode in="SourceGraphic"/>
      <feMergeNode in="SourceAlpha" color-interpolation-filters="sRGB"/>
    </feMerge>
  </filter>
</svg>
"#,
        );
        let mut acquired_nodes = AcquiredNodes::new(&document, None::<gio::Cancellable>);

        let node = document.lookup_internal_node("merge").unwrap();
        let merge = borrow_element_as!(node, FeMerge);
        let resolved = merge.resolve(&mut acquired_nodes, &node).unwrap();
        let ResolvedPrimitive { params, .. } = resolved.first().unwrap();
        let params = match params {
            PrimitiveParams::Merge(m) => m,
            _ => unreachable!(),
        };
        assert_eq!(
            &params.merge_nodes[..],
            vec![
                MergeNode {
                    in1: Input::SourceGraphic,
                    color_interpolation_filters: Default::default(),
                },
                MergeNode {
                    in1: Input::SourceAlpha,
                    color_interpolation_filters: ColorInterpolationFilters::Srgb,
                },
            ]
        );
    }
}
