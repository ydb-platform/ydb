//! Entry point for the CSS filters infrastructure.

use cssparser::{BasicParseError, Parser};
use markup5ever::{expanded_name, local_name, ns};
use std::rc::Rc;
use std::time::Instant;

use crate::bbox::BoundingBox;
use crate::document::AcquiredNodes;
use crate::drawing_ctx::{DrawingCtx, Viewport};
use crate::element::{ElementTrait, set_attribute};
use crate::error::{InternalRenderingError, ParseError};
use crate::filter::UserSpaceFilter;
use crate::length::*;
use crate::node::Node;
use crate::parse_identifiers;
use crate::parsers::{CustomIdent, Parse, ParseValue};
use crate::properties::ColorInterpolationFilters;
use crate::rsvg_log;
use crate::session::Session;
use crate::surface_utils::{
    EdgeMode,
    shared_surface::{SharedImageSurface, SurfaceType},
};
use crate::xml::Attributes;

mod bounds;
use self::bounds::BoundsBuilder;

pub mod context;
use self::context::{FilterContext, FilterOutput, FilterResult};

mod error;
use self::error::FilterError;
pub use self::error::FilterResolveError;

/// A filter primitive interface.
pub trait FilterEffect: ElementTrait {
    fn resolve(
        &self,
        acquired_nodes: &mut AcquiredNodes<'_>,
        node: &Node,
    ) -> Result<Vec<ResolvedPrimitive>, FilterResolveError>;
}

pub mod blend;
pub mod color_matrix;
pub mod component_transfer;
pub mod composite;
pub mod convolve_matrix;
pub mod displacement_map;
pub mod drop_shadow;
pub mod flood;
pub mod gaussian_blur;
pub mod image;
pub mod lighting;
pub mod merge;
pub mod morphology;
pub mod offset;
pub mod tile;
pub mod turbulence;

/// Parameters to apply a list of SVG filter primitives onto a surface.
///
/// This is almost everything needed to take a surface and apply a list of SVG filter
/// primitives to it.
pub struct FilterSpec {
    /// Human-readable identifier for the filter, for logging/debugging purposes.
    pub name: String,

    /// Coordinates and bounds.
    pub user_space_filter: UserSpaceFilter,

    /// List of filter primitives to apply to the surface, in order.
    pub primitives: Vec<UserSpacePrimitive>,
}

/// Parameters using while rendering a whole `filter` property.
///
/// The `filter` property may contain a single primitive, like `filter="blur(2px)"`, or a
/// list of filter specs like `filter="blur(2px) url(#filter_id) drop_shadow(5 5)"`.  Each
/// of those specs may produce more than one primitive; for example, the `url(#filter_id)`
/// there may refer to a `<filter>` element that has several primitives inside it.  Also,
/// the `drop_shadow()` function will expand to the few primitives used to implement a
/// drop shadow.
///
/// Each filter spec will be rendered within a [`FilterContext`], so that the context can maintain
/// the list of named outputs within a `<filter>` element.
///
/// While rendering all those [`FilterContext`]s, there are some immutable parameters.
/// This `FilterPlan` struct contains those parameters.
pub struct FilterPlan {
    session: Session,

    /// Current viewport at the time the filter is invoked.
    pub viewport: Viewport,

    /// Surface corresponding to the background image snapshot, for `in="BackgroundImage"`.
    background_image: Option<SharedImageSurface>,

    /// Surface filled with the current stroke paint, for `in="StrokePaint"`.
    ///
    /// Filter Effects 1: <https://www.w3.org/TR/filter-effects/#attr-valuedef-in-strokepaint>
    stroke_paint_image: Option<SharedImageSurface>,

    /// Surface filled with the current fill paint, for `in="FillPaint"`.
    ///
    /// Filter Effects 1: <https://www.w3.org/TR/filter-effects/#attr-valuedef-in-fillpaint>
    fill_paint_image: Option<SharedImageSurface>,
}

impl FilterPlan {
    pub fn new(
        session: &Session,
        viewport: Viewport,
        requirements: InputRequirements,
        background_image: Option<SharedImageSurface>,
        stroke_paint_image: Option<SharedImageSurface>,
        fill_paint_image: Option<SharedImageSurface>,
    ) -> Result<FilterPlan, Box<InternalRenderingError>> {
        assert_eq!(
            requirements.needs_background_image || requirements.needs_background_alpha,
            background_image.is_some()
        );

        assert_eq!(
            requirements.needs_stroke_paint_image,
            stroke_paint_image.is_some()
        );

        assert_eq!(
            requirements.needs_fill_paint_image,
            fill_paint_image.is_some()
        );

        Ok(FilterPlan {
            session: session.clone(),
            viewport,
            background_image,
            stroke_paint_image,
            fill_paint_image,
        })
    }
}

/// Which surfaces need to be provided as inputs for a [`FilterPlan`].
///
/// The various filters in a `filter` property may require different source images that
/// the calling [`DrawingCtx`] is able to compute.  For example, if a primitive inside a
/// `<filter>` element has `in="FillPaint"`, then the calling [`DrawingCtx`] must supply a
/// surface filled as per the `fill` property of the element being filtered.
///
/// This struct holds the requirements for which such surfaces are needed.  The caller is
/// expected to construct it from an array of [`FilterSpec`], and then to create the
/// corresponding [`Input`] to create a [`FilterPlan`].
#[derive(Debug, Default, PartialEq)]
pub struct InputRequirements {
    pub needs_source_alpha: bool,
    pub needs_background_image: bool,
    pub needs_background_alpha: bool,
    pub needs_stroke_paint_image: bool,
    pub needs_fill_paint_image: bool,
}

impl InputRequirements {
    pub fn new_from_filter_specs(specs: &[FilterSpec]) -> InputRequirements {
        specs
            .iter()
            .flat_map(|spec| spec.primitives.iter())
            .map(|primitive| primitive.params.get_input_requirements())
            .fold(InputRequirements::default(), |a, b| a.fold(b))
    }

    #[rustfmt::skip]
    fn fold(self, r: InputRequirements) -> InputRequirements {
        InputRequirements {
            needs_source_alpha:       self.needs_source_alpha       || r.needs_source_alpha,
            needs_background_image:   self.needs_background_image   || r.needs_background_image,
            needs_background_alpha:   self.needs_background_alpha   || r.needs_background_alpha,
            needs_stroke_paint_image: self.needs_stroke_paint_image || r.needs_stroke_paint_image,
            needs_fill_paint_image:   self.needs_fill_paint_image   || r.needs_fill_paint_image,
        }
    }
}

/// Resolved parameters for each filter primitive.
///
/// These gather all the data that a primitive may need during rendering:
/// the `feFoo` element's attributes, any computed values from its properties,
/// and parameters extracted from the element's children (for example,
/// `feMerge` gathers info from its `feMergNode` children).
pub enum PrimitiveParams {
    Blend(blend::Blend),
    ColorMatrix(color_matrix::ColorMatrix),
    ComponentTransfer(component_transfer::ComponentTransfer),
    Composite(composite::Composite),
    ConvolveMatrix(convolve_matrix::ConvolveMatrix),
    DiffuseLighting(lighting::DiffuseLighting),
    DisplacementMap(displacement_map::DisplacementMap),
    Flood(flood::Flood),
    GaussianBlur(gaussian_blur::GaussianBlur),
    Image(image::Image),
    Merge(merge::Merge),
    Morphology(morphology::Morphology),
    Offset(offset::Offset),
    SpecularLighting(lighting::SpecularLighting),
    Tile(tile::Tile),
    Turbulence(turbulence::Turbulence),
}

impl PrimitiveParams {
    /// Returns a human-readable name for a primitive.
    #[rustfmt::skip]
    fn name(&self) -> &'static str {
        use PrimitiveParams::*;
        match self {
            Blend(..)             => "feBlend",
            ColorMatrix(..)       => "feColorMatrix",
            ComponentTransfer(..) => "feComponentTransfer",
            Composite(..)         => "feComposite",
            ConvolveMatrix(..)    => "feConvolveMatrix",
            DiffuseLighting(..)   => "feDiffuseLighting",
            DisplacementMap(..)   => "feDisplacementMap",
            Flood(..)             => "feFlood",
            GaussianBlur(..)      => "feGaussianBlur",
            Image(..)             => "feImage",
            Merge(..)             => "feMerge",
            Morphology(..)        => "feMorphology",
            Offset(..)            => "feOffset",
            SpecularLighting(..)  => "feSpecularLighting",
            Tile(..)              => "feTile",
            Turbulence(..)        => "feTurbulence",
        }
    }

    #[rustfmt::skip]
    fn get_input_requirements(&self) -> InputRequirements {
        use PrimitiveParams::*;
        match self {
            Blend(p)             => p.get_input_requirements(),
            ColorMatrix(p)       => p.get_input_requirements(),
            ComponentTransfer(p) => p.get_input_requirements(),
            Composite(p)         => p.get_input_requirements(),
            ConvolveMatrix(p)    => p.get_input_requirements(),
            DiffuseLighting(p)   => p.get_input_requirements(),
            DisplacementMap(p)   => p.get_input_requirements(),
            Flood(p)             => p.get_input_requirements(),
            GaussianBlur(p)      => p.get_input_requirements(),
            Image(p)             => p.get_input_requirements(),
            Merge(p)             => p.get_input_requirements(),
            Morphology(p)        => p.get_input_requirements(),
            Offset(p)            => p.get_input_requirements(),
            SpecularLighting(p)  => p.get_input_requirements(),
            Tile(p)              => p.get_input_requirements(),
            Turbulence(p)        => p.get_input_requirements(),
        }
    }
}

/// The base filter primitive node containing common properties.
#[derive(Default, Clone)]
pub struct Primitive {
    pub x: Option<Length<Horizontal>>,
    pub y: Option<Length<Vertical>>,
    pub width: Option<ULength<Horizontal>>,
    pub height: Option<ULength<Vertical>>,
    pub result: Option<CustomIdent>,
}

pub struct ResolvedPrimitive {
    pub primitive: Primitive,
    pub params: PrimitiveParams,
}

/// A fully resolved filter primitive in user-space coordinates.
pub struct UserSpacePrimitive {
    x: Option<f64>,
    y: Option<f64>,
    width: Option<f64>,
    height: Option<f64>,
    result: Option<CustomIdent>,

    params: PrimitiveParams,
}

/// An enumeration of possible inputs for a filter primitive.
#[derive(Debug, Default, Clone, Eq, PartialEq, Hash)]
pub enum Input {
    #[default]
    Unspecified,
    SourceGraphic,
    SourceAlpha,
    BackgroundImage,
    BackgroundAlpha,
    FillPaint,
    StrokePaint,
    FilterOutput(CustomIdent),
}

impl Input {
    pub fn get_requirements(&self) -> InputRequirements {
        use Input::*;

        let mut reqs = InputRequirements::default();

        match self {
            SourceAlpha => reqs.needs_source_alpha = true,
            BackgroundImage => reqs.needs_background_image = true,
            BackgroundAlpha => reqs.needs_background_alpha = true,
            FillPaint => reqs.needs_fill_paint_image = true,
            StrokePaint => reqs.needs_stroke_paint_image = true,
            _ => (),
        }

        reqs
    }
}

impl Parse for Input {
    fn parse<'i>(parser: &mut Parser<'i, '_>) -> Result<Self, ParseError<'i>> {
        parser
            .try_parse(|p| {
                parse_identifiers!(
                    p,
                    "SourceGraphic" => Input::SourceGraphic,
                    "SourceAlpha" => Input::SourceAlpha,
                    "BackgroundImage" => Input::BackgroundImage,
                    "BackgroundAlpha" => Input::BackgroundAlpha,
                    "FillPaint" => Input::FillPaint,
                    "StrokePaint" => Input::StrokePaint,
                )
            })
            .or_else(|_: BasicParseError<'_>| {
                let ident = CustomIdent::parse(parser)?;
                Ok(Input::FilterOutput(ident))
            })
    }
}

impl ResolvedPrimitive {
    pub fn into_user_space(self, params: &NormalizeParams) -> UserSpacePrimitive {
        let x = self.primitive.x.map(|l| l.to_user(params));
        let y = self.primitive.y.map(|l| l.to_user(params));
        let width = self.primitive.width.map(|l| l.to_user(params));
        let height = self.primitive.height.map(|l| l.to_user(params));

        UserSpacePrimitive {
            x,
            y,
            width,
            height,
            result: self.primitive.result,
            params: self.params,
        }
    }
}

impl UserSpacePrimitive {
    /// Validates attributes and returns the `BoundsBuilder` for bounds computation.
    #[inline]
    fn get_bounds(&self, ctx: &FilterContext) -> BoundsBuilder {
        BoundsBuilder::new(self.x, self.y, self.width, self.height, ctx.paffine())
    }
}

impl Primitive {
    fn parse_standard_attributes(
        &mut self,
        attrs: &Attributes,
        session: &Session,
    ) -> (Input, Input) {
        let mut input_1 = Input::Unspecified;
        let mut input_2 = Input::Unspecified;

        for (attr, value) in attrs.iter() {
            match attr.expanded() {
                expanded_name!("", "x") => set_attribute(&mut self.x, attr.parse(value), session),
                expanded_name!("", "y") => set_attribute(&mut self.y, attr.parse(value), session),
                expanded_name!("", "width") => {
                    set_attribute(&mut self.width, attr.parse(value), session)
                }
                expanded_name!("", "height") => {
                    set_attribute(&mut self.height, attr.parse(value), session)
                }
                expanded_name!("", "result") => {
                    set_attribute(&mut self.result, attr.parse(value), session)
                }
                expanded_name!("", "in") => set_attribute(&mut input_1, attr.parse(value), session),
                expanded_name!("", "in2") => {
                    set_attribute(&mut input_2, attr.parse(value), session)
                }
                _ => (),
            }
        }

        (input_1, input_2)
    }

    pub fn parse_no_inputs(&mut self, attrs: &Attributes, session: &Session) {
        let (_, _) = self.parse_standard_attributes(attrs, session);
    }

    pub fn parse_one_input(&mut self, attrs: &Attributes, session: &Session) -> Input {
        let (input_1, _) = self.parse_standard_attributes(attrs, session);
        input_1
    }

    pub fn parse_two_inputs(&mut self, attrs: &Attributes, session: &Session) -> (Input, Input) {
        self.parse_standard_attributes(attrs, session)
    }
}

/// Applies a filter and returns the resulting surface.
pub fn render(
    plan: Rc<FilterPlan>,
    filter: &FilterSpec,
    source_surface: SharedImageSurface,
    acquired_nodes: &mut AcquiredNodes<'_>,
    draw_ctx: &mut DrawingCtx,
    node_bbox: &BoundingBox,
) -> Result<SharedImageSurface, Box<InternalRenderingError>> {
    let session = draw_ctx.session().clone();

    let surface_width = source_surface.width();
    let surface_height = source_surface.height();

    FilterContext::new(&filter.user_space_filter, plan, source_surface, *node_bbox)
        .and_then(|mut filter_ctx| {
            // the message has an unclosed parenthesis; we'll close it below.
            rsvg_log!(
                session,
                "(filter \"{}\" with effects_region={:?}",
                filter.name,
                filter_ctx.effects_region()
            );
            for user_space_primitive in &filter.primitives {
                let start = Instant::now();

                match render_primitive(user_space_primitive, &filter_ctx, acquired_nodes, draw_ctx)
                {
                    Ok(output) => {
                        let elapsed = start.elapsed();
                        rsvg_log!(
                            session,
                            "(rendered filter primitive {} in {} seconds)",
                            user_space_primitive.params.name(),
                            elapsed.as_secs() as f64 + f64::from(elapsed.subsec_nanos()) / 1e9
                        );

                        filter_ctx.store_result(FilterResult {
                            name: user_space_primitive.result.clone(),
                            output,
                        });
                    }

                    Err(err) => {
                        rsvg_log!(
                            session,
                            "(filter primitive {} returned an error: {})",
                            user_space_primitive.params.name(),
                            err
                        );

                        // close the opening parenthesis from the message at the start of this function
                        rsvg_log!(session, ")");

                        // Exit early on Cairo errors. Continue rendering otherwise.
                        if let FilterError::CairoError(status) = err {
                            return Err(FilterError::CairoError(status));
                        }
                    }
                }
            }

            // close the opening parenthesis from the message at the start of this function
            rsvg_log!(session, ")");

            Ok(filter_ctx.into_output()?)
        })
        .or_else(|err| match err {
            FilterError::CairoError(status) => {
                // Exit early on Cairo errors
                Err(Box::new(InternalRenderingError::from(status)))
            }

            _ => {
                // ignore other filter errors and just return an empty surface
                Ok(SharedImageSurface::empty(
                    surface_width,
                    surface_height,
                    SurfaceType::AlphaOnly,
                )?)
            }
        })
}

#[rustfmt::skip]
fn render_primitive(
    primitive: &UserSpacePrimitive,
    ctx: &FilterContext,
    acquired_nodes: &mut AcquiredNodes<'_>,
    draw_ctx: &mut DrawingCtx,
) -> Result<FilterOutput, FilterError> {
    use PrimitiveParams::*;

    let bounds_builder = primitive.get_bounds(ctx);

    // Note that feDropShadow is not handled here.  When its FilterElement::resolve() is called,
    // it returns a series of lower-level primitives (flood, blur, offset, etc.) that make up
    // the drop-shadow effect.

    match primitive.params {
        Blend(ref p)             => p.render(bounds_builder, ctx),
        ColorMatrix(ref p)       => p.render(bounds_builder, ctx),
        ComponentTransfer(ref p) => p.render(bounds_builder, ctx),
        Composite(ref p)         => p.render(bounds_builder, ctx),
        ConvolveMatrix(ref p)    => p.render(bounds_builder, ctx),
        DiffuseLighting(ref p)   => p.render(bounds_builder, ctx),
        DisplacementMap(ref p)   => p.render(bounds_builder, ctx),
        Flood(ref p)             => p.render(bounds_builder, ctx),
        GaussianBlur(ref p)      => p.render(bounds_builder, ctx),
        Image(ref p)             => p.render(bounds_builder, ctx, acquired_nodes, draw_ctx),
        Merge(ref p)             => p.render(bounds_builder, ctx),
        Morphology(ref p)        => p.render(bounds_builder, ctx),
        Offset(ref p)            => p.render(bounds_builder, ctx),
        SpecularLighting(ref p)  => p.render(bounds_builder, ctx),
        Tile(ref p)              => p.render(bounds_builder, ctx),
        Turbulence(ref p)        => p.render(bounds_builder, ctx),
    }
}

impl From<ColorInterpolationFilters> for SurfaceType {
    fn from(c: ColorInterpolationFilters) -> Self {
        match c {
            ColorInterpolationFilters::LinearRgb => SurfaceType::LinearRgb,
            _ => SurfaceType::SRgb,
        }
    }
}

impl Parse for EdgeMode {
    fn parse<'i>(parser: &mut Parser<'i, '_>) -> Result<Self, ParseError<'i>> {
        Ok(parse_identifiers!(
            parser,
            "duplicate" => EdgeMode::Duplicate,
            "wrap" => EdgeMode::Wrap,
            "none" => EdgeMode::None,
        )?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::color::{Color, RGBA};
    use crate::document::Document;
    use crate::dpi::Dpi;
    use crate::node::NodeBorrow;
    use crate::properties::Filter;

    fn get_input_requirements_for_node(document: &Document, node_id: &str) -> InputRequirements {
        let node = document.lookup_internal_node(node_id).unwrap();
        let elt = node.borrow_element();
        let values = elt.get_computed_values();

        let session = Session::default();
        let mut acquired_nodes = AcquiredNodes::new(&document, None::<gio::Cancellable>);

        let viewport = Viewport::new(Dpi::new(96.0, 96.0), 100.0, 100.0);

        let filter = values.filter();

        let filter_list = match filter {
            Filter::None => {
                panic!("the referenced node should have a filter property that is not 'none'")
            }
            Filter::List(filter_list) => filter_list,
        };

        let params = NormalizeParams::new(&values, &viewport);

        let filter_specs = filter_list
            .iter()
            .map(|filter_value| {
                filter_value.to_filter_spec(
                    &mut acquired_nodes,
                    &params,
                    Color::Rgba(RGBA::new(0, 0, 0, 1.0)),
                    &viewport,
                    &session,
                    "rect",
                )
            })
            .collect::<Result<Vec<FilterSpec>, _>>()
            .unwrap();

        InputRequirements::new_from_filter_specs(&filter_specs)
    }

    fn input_requirements_with_only_source_alpha() -> InputRequirements {
        InputRequirements {
            needs_source_alpha: true,
            needs_background_image: false,
            needs_background_alpha: false,
            needs_stroke_paint_image: false,
            needs_fill_paint_image: false,
        }
    }

    #[test]
    fn detects_source_alpha() {
        let document = Document::load_from_bytes(include_bytes!("test_input_requirements.svg"));

        assert_eq!(
            get_input_requirements_for_node(&document, "rect_1"),
            input_requirements_with_only_source_alpha(),
        );

        assert_eq!(
            get_input_requirements_for_node(&document, "rect_2"),
            input_requirements_with_only_source_alpha(),
        );

        assert_eq!(
            get_input_requirements_for_node(&document, "rect_3"),
            InputRequirements {
                needs_source_alpha: false,
                needs_background_image: true,
                needs_background_alpha: true,
                needs_stroke_paint_image: true,
                needs_fill_paint_image: true,
            }
        );
    }
}
