use std::collections::HashMap;
use std::rc::Rc;

use crate::bbox::BoundingBox;
use crate::coord_units::CoordUnits;
use crate::filter::UserSpaceFilter;
use crate::parsers::CustomIdent;
use crate::properties::ColorInterpolationFilters;
use crate::rect::{IRect, Rect};
use crate::session::Session;
use crate::surface_utils::shared_surface::{SharedImageSurface, SurfaceType};
use crate::transform::Transform;

use super::error::FilterError;
use super::{FilterPlan, Input};

/// A filter primitive output.
#[derive(Debug, Clone)]
pub struct FilterOutput {
    /// The surface after the filter primitive was applied.
    pub surface: SharedImageSurface,

    /// The filter primitive subregion.
    pub bounds: IRect,
}

/// A filter primitive result.
#[derive(Debug, Clone)]
pub struct FilterResult {
    /// The name of this result: the value of the `result` attribute.
    pub name: Option<CustomIdent>,

    /// The output.
    pub output: FilterOutput,
}

/// An input to a filter primitive.
#[derive(Debug, Clone)]
pub enum FilterInput {
    /// One of the standard inputs.
    StandardInput(SharedImageSurface),
    /// Output of another filter primitive.
    PrimitiveOutput(FilterOutput),
}

/// Context for rendering a single [`crate::filters::FilterSpec`].
///
/// Rendering a [`crate::filters::FilterSpec`] involves keeping track of possibly-named results
/// for each filter primitive (e.g. those that have an `output` attribute).  This struct
/// maintains that information, plus all the extra data used during filtering.
pub struct FilterContext {
    /// Immutable values used during the execution of a `filter` property.
    plan: Rc<FilterPlan>,

    /// The source graphic surface.
    source_surface: SharedImageSurface,

    /// Output of the last filter primitive.
    last_result: Option<FilterOutput>,
    /// Surfaces of the previous filter primitives by name.
    previous_results: HashMap<CustomIdent, FilterOutput>,

    /// Primtive units
    primitive_units: CoordUnits,
    /// The filter effects region.
    effects_region: Rect,

    /// The filter element affine matrix.
    ///
    /// If `filterUnits == userSpaceOnUse`, equal to the drawing context matrix, so, for example,
    /// if the target node is in a group with `transform="translate(30, 20)"`, this will be equal
    /// to a matrix that translates to 30, 20 (and does not scale). Note that the target node
    /// bounding box isn't included in the computations in this case.
    ///
    /// If `filterUnits == objectBoundingBox`, equal to the target node bounding box matrix
    /// multiplied by the drawing context matrix, so, for example, if the target node is in a group
    /// with `transform="translate(30, 20)"` and also has `x="1", y="1", width="50", height="50"`,
    /// this will be equal to a matrix that translates to 31, 21 and scales to 50, 50.
    ///
    /// This is to be used in conjunction with setting the viewbox size to account for the scaling.
    /// For `filterUnits == userSpaceOnUse`, the viewbox will have the actual resolution size, and
    /// for `filterUnits == objectBoundingBox`, the viewbox will have the size of 1, 1.
    _affine: Transform,

    /// The filter primitive affine matrix.
    ///
    /// See the comments for `_affine`, they largely apply here.
    paffine: Transform,
}

impl FilterContext {
    /// Creates a new `FilterContext`.
    pub fn new(
        filter: &UserSpaceFilter,
        plan: Rc<FilterPlan>,
        source_surface: SharedImageSurface,
        node_bbox: BoundingBox,
    ) -> Result<Self, FilterError> {
        // The rect can be empty (for example, if the filter is applied to an empty group).
        // However, with userSpaceOnUse it's still possible to create images with a filter.
        let bbox_rect = node_bbox.rect.unwrap_or_default();

        let affine = match filter.filter_units {
            CoordUnits::UserSpaceOnUse => *plan.viewport.transform,
            CoordUnits::ObjectBoundingBox => Transform::new_unchecked(
                bbox_rect.width(),
                0.0,
                0.0,
                bbox_rect.height(),
                bbox_rect.x0,
                bbox_rect.y0,
            )
            .post_transform(&plan.viewport.transform),
        };

        let paffine = match filter.primitive_units {
            CoordUnits::UserSpaceOnUse => *plan.viewport.transform,
            CoordUnits::ObjectBoundingBox => Transform::new_unchecked(
                bbox_rect.width(),
                0.0,
                0.0,
                bbox_rect.height(),
                bbox_rect.x0,
                bbox_rect.y0,
            )
            .post_transform(&plan.viewport.transform),
        };

        if !(affine.is_invertible() && paffine.is_invertible()) {
            return Err(FilterError::InvalidParameter(
                "transform is not invertible".to_string(),
            ));
        }

        let effects_region = {
            let mut bbox = BoundingBox::new();
            let other_bbox = BoundingBox::new()
                .with_transform(affine)
                .with_rect(filter.rect);

            // At this point all of the previous viewbox and matrix business gets converted to pixel
            // coordinates in the final surface, because bbox is created with an identity transform.
            bbox.insert(&other_bbox);

            // Finally, clip to the width and height of our surface.
            let (width, height) = (source_surface.width(), source_surface.height());
            let rect = Rect::from_size(f64::from(width), f64::from(height));
            let other_bbox = BoundingBox::new().with_rect(rect);
            bbox.clip(&other_bbox);

            bbox.rect.unwrap()
        };

        Ok(Self {
            plan,
            source_surface,
            last_result: None,
            previous_results: HashMap::new(),
            primitive_units: filter.primitive_units,
            effects_region,
            _affine: affine,
            paffine,
        })
    }

    pub fn session(&self) -> &Session {
        &self.plan.session
    }

    /// Returns the surface corresponding to the source graphic.
    #[inline]
    pub fn source_graphic(&self) -> &SharedImageSurface {
        &self.source_surface
    }

    /// Returns the surface corresponding to the background image snapshot.
    fn background_image(&self) -> SharedImageSurface {
        self.plan.background_image.clone().expect(
            "the calling DrawingCtx should have passed a background_image to the FilterPlan",
        )
    }

    /// Returns a surface filled with the current stroke's paint, for `StrokePaint` inputs in primitives.
    ///
    /// Filter Effects 1: <https://www.w3.org/TR/filter-effects/#attr-valuedef-in-strokepaint>
    fn stroke_paint_image(&self) -> SharedImageSurface {
        self.plan.stroke_paint_image.clone().expect(
            "the calling DrawingCtx should have passed a stroke_paint_image to the FilterPlan",
        )
    }

    /// Returns a surface filled with the current fill's paint, for `FillPaint` inputs in primitives.
    ///
    /// Filter Effects 1: <https://www.w3.org/TR/filter-effects/#attr-valuedef-in-fillpaint>
    fn fill_paint_image(&self) -> SharedImageSurface {
        self.plan.fill_paint_image.clone().expect(
            "the calling DrawingCtx should have passed a fill_paint_image to the FilterPlan",
        )
    }

    /// Converts this `FilterContext` into the surface corresponding to the output of the filter
    /// chain.
    ///
    /// The returned surface is in the sRGB color space.
    // TODO: sRGB conversion should probably be done by the caller.
    #[inline]
    pub fn into_output(self) -> Result<SharedImageSurface, cairo::Error> {
        match self.last_result {
            Some(FilterOutput { surface, bounds }) => surface.to_srgb(bounds),
            None => SharedImageSurface::empty(
                self.source_surface.width(),
                self.source_surface.height(),
                SurfaceType::AlphaOnly,
            ),
        }
    }

    /// Stores a filter primitive result into the context.
    #[inline]
    pub fn store_result(&mut self, result: FilterResult) {
        if let Some(name) = result.name {
            self.previous_results.insert(name, result.output.clone());
        }

        self.last_result = Some(result.output);
    }

    /// Returns the paffine matrix.
    #[inline]
    pub fn paffine(&self) -> Transform {
        self.paffine
    }

    /// Returns the primitive units.
    #[inline]
    pub fn primitive_units(&self) -> CoordUnits {
        self.primitive_units
    }

    /// Returns the filter effects region.
    #[inline]
    pub fn effects_region(&self) -> Rect {
        self.effects_region
    }

    /// Get a filter primitive's default input as if its `in=\"...\"` were not specified.
    ///
    /// Per <https://drafts.fxtf.org/filter-effects/#element-attrdef-filter-primitive-in>,
    /// "References to non-existent results will be treated as if no result was
    /// specified".  That is, fall back to the last result in the filter chain, or if this
    /// is the first in the chain, just use SourceGraphic.
    fn get_unspecified_input(&self) -> FilterInput {
        if let Some(output) = self.last_result.as_ref() {
            FilterInput::PrimitiveOutput(output.clone())
        } else {
            FilterInput::StandardInput(self.source_graphic().clone())
        }
    }

    /// Retrieves the filter input surface according to the SVG rules.
    fn get_input_raw(&self, in_: &Input) -> Result<FilterInput, FilterError> {
        match *in_ {
            Input::Unspecified => Ok(self.get_unspecified_input()),

            Input::SourceGraphic => Ok(FilterInput::StandardInput(self.source_graphic().clone())),

            Input::SourceAlpha => self
                .source_graphic()
                .extract_alpha(self.effects_region().into())
                .map_err(FilterError::CairoError)
                .map(FilterInput::StandardInput),

            Input::BackgroundImage => Ok(FilterInput::StandardInput(self.background_image())),

            Input::BackgroundAlpha => self
                .background_image()
                .extract_alpha(self.effects_region().into())
                .map_err(FilterError::CairoError)
                .map(FilterInput::StandardInput),

            Input::FillPaint => Ok(FilterInput::StandardInput(self.fill_paint_image())),

            Input::StrokePaint => Ok(FilterInput::StandardInput(self.stroke_paint_image())),

            Input::FilterOutput(ref name) => {
                let input = match self.previous_results.get(name).cloned() {
                    Some(filter_output) => {
                        // Happy path: we found a previous primitive's named output, so pass it on.
                        FilterInput::PrimitiveOutput(filter_output)
                    }

                    None => {
                        // Fallback path: we didn't find a primitive's output by the
                        // specified name, so fall back to using an unspecified output.
                        // Per the spec, "References to non-existent results will be
                        // treated as if no result was specified." -
                        // https://drafts.fxtf.org/filter-effects/#element-attrdef-filter-primitive-in
                        self.get_unspecified_input()
                    }
                };

                Ok(input)
            }
        }
    }

    /// Retrieves the filter input surface according to the SVG rules.
    ///
    /// The surface will be converted to the color space specified by `color_interpolation_filters`.
    pub fn get_input(
        &self,
        in_: &Input,
        color_interpolation_filters: ColorInterpolationFilters,
    ) -> Result<FilterInput, FilterError> {
        let raw = self.get_input_raw(in_)?;

        // Convert the input surface to the desired format.
        let (surface, bounds) = match raw {
            FilterInput::StandardInput(ref surface) => (surface, self.effects_region().into()),
            FilterInput::PrimitiveOutput(FilterOutput {
                ref surface,
                ref bounds,
            }) => (surface, *bounds),
        };

        let surface = match color_interpolation_filters {
            ColorInterpolationFilters::Auto => Ok(surface.clone()),
            ColorInterpolationFilters::LinearRgb => surface.to_linear_rgb(bounds),
            ColorInterpolationFilters::Srgb => surface.to_srgb(bounds),
        };

        surface
            .map_err(FilterError::CairoError)
            .map(|surface| match raw {
                FilterInput::StandardInput(_) => FilterInput::StandardInput(surface),
                FilterInput::PrimitiveOutput(ref output) => {
                    FilterInput::PrimitiveOutput(FilterOutput { surface, ..*output })
                }
            })
    }
}

impl FilterInput {
    /// Retrieves the surface from `FilterInput`.
    #[inline]
    pub fn surface(&self) -> &SharedImageSurface {
        match *self {
            FilterInput::StandardInput(ref surface) => surface,
            FilterInput::PrimitiveOutput(FilterOutput { ref surface, .. }) => surface,
        }
    }
}
