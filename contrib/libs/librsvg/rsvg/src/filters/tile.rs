use crate::document::AcquiredNodes;
use crate::element::ElementTrait;
use crate::node::Node;
use crate::properties::ColorInterpolationFilters;
use crate::rect::IRect;
use crate::rsvg_log;
use crate::session::Session;
use crate::xml::Attributes;

use super::bounds::BoundsBuilder;
use super::context::{FilterContext, FilterInput, FilterOutput};
use super::{
    FilterEffect, FilterError, FilterResolveError, Input, InputRequirements, Primitive,
    PrimitiveParams, ResolvedPrimitive,
};

/// The `feTile` filter primitive.
#[derive(Default)]
pub struct FeTile {
    base: Primitive,
    params: Tile,
}

/// Resolved `feTile` primitive for rendering.
#[derive(Clone, Default)]
pub struct Tile {
    in1: Input,
}

impl ElementTrait for FeTile {
    fn set_attributes(&mut self, attrs: &Attributes, session: &Session) {
        self.params.in1 = self.base.parse_one_input(attrs, session);
    }
}

impl Tile {
    pub fn render(
        &self,
        bounds_builder: BoundsBuilder,
        ctx: &FilterContext,
    ) -> Result<FilterOutput, FilterError> {
        // https://www.w3.org/TR/filter-effects/#ColorInterpolationFiltersProperty
        //
        // "Note: The color-interpolation-filters property just has an
        // effect on filter operations. Therefore, it has no effect on
        // filter primitives like [...], feTile"
        //
        // This is why we pass Auto here.
        let input_1 = ctx.get_input(&self.in1, ColorInterpolationFilters::Auto)?;

        // feTile doesn't consider its inputs in the filter primitive subregion calculation.
        let bounds: IRect = bounds_builder.compute(ctx).clipped.into();

        let surface = match input_1 {
            FilterInput::StandardInput(input_surface) => input_surface,
            FilterInput::PrimitiveOutput(FilterOutput {
                surface: input_surface,
                bounds: input_bounds,
            }) => {
                if input_bounds.is_empty() {
                    rsvg_log!(
                        ctx.session(),
                        "(feTile with empty input_bounds; returning just the input surface)"
                    );

                    input_surface
                } else {
                    rsvg_log!(
                        ctx.session(),
                        "(feTile bounds={:?}, input_bounds={:?})",
                        bounds,
                        input_bounds
                    );

                    let tile_surface = input_surface.tile(input_bounds)?;

                    ctx.source_graphic().paint_image_tiled(
                        bounds,
                        &tile_surface,
                        input_bounds.x0,
                        input_bounds.y0,
                    )?
                }
            }
        };

        Ok(FilterOutput { surface, bounds })
    }

    pub fn get_input_requirements(&self) -> InputRequirements {
        self.in1.get_requirements()
    }
}

impl FilterEffect for FeTile {
    fn resolve(
        &self,
        _acquired_nodes: &mut AcquiredNodes<'_>,
        _node: &Node,
    ) -> Result<Vec<ResolvedPrimitive>, FilterResolveError> {
        Ok(vec![ResolvedPrimitive {
            primitive: self.base.clone(),
            params: PrimitiveParams::Tile(self.params.clone()),
        }])
    }
}
