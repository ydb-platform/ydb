//! SVG Elements.

use markup5ever::{QualName, expanded_name, local_name, ns};
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::OnceLock;

use crate::accept_language::UserLanguage;
use crate::bbox::BoundingBox;
use crate::cond::{RequiredExtensions, RequiredFeatures, SystemLanguage};
use crate::css::{Declaration, Origin};
use crate::document::AcquiredNodes;
use crate::drawing_ctx::{DrawingCtx, Viewport};
use crate::error::*;
use crate::filter::Filter;
use crate::filters::{
    FilterEffect,
    blend::FeBlend,
    color_matrix::FeColorMatrix,
    component_transfer::{FeComponentTransfer, FeFuncA, FeFuncB, FeFuncG, FeFuncR},
    composite::FeComposite,
    convolve_matrix::FeConvolveMatrix,
    displacement_map::FeDisplacementMap,
    drop_shadow::FeDropShadow,
    flood::FeFlood,
    gaussian_blur::FeGaussianBlur,
    image::FeImage,
    lighting::{FeDiffuseLighting, FeDistantLight, FePointLight, FeSpecularLighting, FeSpotLight},
    merge::{FeMerge, FeMergeNode},
    morphology::FeMorphology,
    offset::FeOffset,
    tile::FeTile,
    turbulence::FeTurbulence,
};
use crate::gradient::{LinearGradient, RadialGradient, Stop};
use crate::image::Image;
use crate::layout::Layer;
use crate::marker::Marker;
use crate::node::*;
use crate::pattern::Pattern;
use crate::properties::{ComputedValues, SpecifiedValues};
use crate::rsvg_log;
use crate::session::Session;
use crate::shapes::{Circle, Ellipse, Line, Path, Polygon, Polyline, Rect};
use crate::structure::{ClipPath, Group, Link, Mask, NonRendering, Svg, Switch, Symbol, Use};
use crate::style::Style;
use crate::text::{TRef, TSpan, Text};
use crate::text2::Text2;
use crate::xml::Attributes;

pub type DrawResult = Result<Box<BoundingBox>, Box<InternalRenderingError>>;

pub trait ElementTrait {
    /// Sets per-element attributes.
    ///
    /// Each element is supposed to iterate the `attributes`, and parse any ones it needs.
    /// SVG specifies that unknown attributes should be ignored, and known attributes with invalid
    /// values should be ignored so that the attribute ends up with its "initial value".
    ///
    /// You can use the [`set_attribute`] function to do that.
    fn set_attributes(&mut self, _attributes: &Attributes, _session: &Session) {}

    /// Draw an element.
    ///
    /// Each element is supposed to draw itself as needed.
    fn draw(
        &self,
        _node: &Node,
        _acquired_nodes: &mut AcquiredNodes<'_>,
        _cascaded: &CascadedValues<'_>,
        viewport: &Viewport,
        _draw_ctx: &mut DrawingCtx,
        _clipping: bool,
    ) -> DrawResult {
        // by default elements don't draw themselves
        Ok(viewport.empty_bbox())
    }

    /// Create a layout object for the current element.
    ///
    /// This resolves property values, coordinates, lengths, etc. and produces a layout
    /// item for rendering.
    fn layout(
        &self,
        _node: &Node,
        _acquired_nodes: &mut AcquiredNodes<'_>,
        _cascaded: &CascadedValues<'_>,
        _viewport: &Viewport,
        _draw_ctx: &mut DrawingCtx,
        _clipping: bool,
    ) -> Result<Option<Layer>, Box<InternalRenderingError>> {
        Ok(None)
    }
}

/// Sets `dest` if `parse_result` is `Ok()`, otherwise just logs the error.
///
/// Implementations of the [`ElementTrait`] trait generally scan a list of attributes
/// for the ones they can handle, and parse their string values.  Per the SVG spec, an attribute
/// with an invalid value should be ignored, and it should fall back to the default value.
///
/// In librsvg, those default values are set in each element's implementation of the [`Default`] trait:
/// at element creation time, each element gets initialized to its `Default`, and then each attribute
/// gets parsed.  This function will set that attribute's value only if parsing was successful.
///
/// In case the `parse_result` is an error, this function will log an appropriate notice
/// via the [`Session`].
pub fn set_attribute<T>(dest: &mut T, parse_result: Result<T, ElementError>, session: &Session) {
    match parse_result {
        Ok(v) => *dest = v,
        Err(e) => {
            // FIXME: this does not provide a clue of what was the problematic element.
            // We need tracking of the current parsing position to do that.
            rsvg_log!(session, "ignoring attribute with invalid value: {}", e);
        }
    }
}

pub struct Element {
    element_name: QualName,
    attributes: Attributes,
    specified_values: SpecifiedValues,
    important_styles: HashSet<QualName>,
    values: ComputedValues,
    required_extensions: Option<RequiredExtensions>,
    required_features: Option<RequiredFeatures>,
    system_language: Option<SystemLanguage>,
    pub element_data: ElementData,
}

impl fmt::Display for Element {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.element_name().local)?;
        write!(f, " id={}", self.get_id().unwrap_or("None"))?;
        Ok(())
    }
}

/// Parsed contents of an element node in the DOM.
///
/// This enum uses `Box<Foo>` in order to make each variant the size of
/// a pointer.
pub enum ElementData {
    Circle(Box<Circle>),
    ClipPath(Box<ClipPath>),
    Ellipse(Box<Ellipse>),
    Filter(Box<Filter>),
    Group(Box<Group>),
    Image(Box<Image>),
    Line(Box<Line>),
    LinearGradient(Box<LinearGradient>),
    Link(Box<Link>),
    Marker(Box<Marker>),
    Mask(Box<Mask>),
    NonRendering(Box<NonRendering>),
    Path(Box<Path>),
    Pattern(Box<Pattern>),
    Polygon(Box<Polygon>),
    Polyline(Box<Polyline>),
    RadialGradient(Box<RadialGradient>),
    Rect(Box<Rect>),
    Stop(Box<Stop>),
    Style(Box<Style>),
    Svg(Box<Svg>),
    Switch(Box<Switch>),
    Symbol(Box<Symbol>),
    Text(Box<Text>),
    Text2(Box<Text2>),
    TRef(Box<TRef>),
    TSpan(Box<TSpan>),
    Use(Box<Use>),

    // Filter primitives, these start with "Fe" as element names are e.g. "feBlend"
    FeBlend(Box<FeBlend>),
    FeColorMatrix(Box<FeColorMatrix>),
    FeComponentTransfer(Box<FeComponentTransfer>),
    FeComposite(Box<FeComposite>),
    FeConvolveMatrix(Box<FeConvolveMatrix>),
    FeDiffuseLighting(Box<FeDiffuseLighting>),
    FeDisplacementMap(Box<FeDisplacementMap>),
    FeDistantLight(Box<FeDistantLight>),
    FeDropShadow(Box<FeDropShadow>),
    FeFlood(Box<FeFlood>),
    FeFuncA(Box<FeFuncA>),
    FeFuncB(Box<FeFuncB>),
    FeFuncG(Box<FeFuncG>),
    FeFuncR(Box<FeFuncR>),
    FeGaussianBlur(Box<FeGaussianBlur>),
    FeImage(Box<FeImage>),
    FeMerge(Box<FeMerge>),
    FeMergeNode(Box<FeMergeNode>),
    FeMorphology(Box<FeMorphology>),
    FeOffset(Box<FeOffset>),
    FePointLight(Box<FePointLight>),
    FeSpecularLighting(Box<FeSpecularLighting>),
    FeSpotLight(Box<FeSpotLight>),
    FeTile(Box<FeTile>),
    FeTurbulence(Box<FeTurbulence>),
}

#[rustfmt::skip]
fn get_element_creators() -> &'static HashMap<&'static str, (ElementDataCreateFn, ElementCreateFlags)> {
    use ElementCreateFlags::*;

    ELEMENT_CREATORS.get_or_init(|| {
        // Lines in comments are elements that we don't support.
        let creators_table: Vec<(&str, ElementDataCreateFn, ElementCreateFlags)> = vec![
            // name, supports_class, create_fn
            ("a",                   create_link,                  Default),
            /* ("altGlyph",         ), */
            /* ("altGlyphDef",      ), */
            /* ("altGlyphItem",     ), */
            /* ("animate",          ), */
            /* ("animateColor",     ), */
            /* ("animateMotion",    ), */
            /* ("animateTransform", ), */
            ("circle",              create_circle,                Default),
            ("clipPath",            create_clip_path,             Default),
            /* ("color-profile",    ), */
            /* ("cursor",           ), */
            ("defs",                create_defs,                  Default),
            /* ("desc",             ), */
            ("ellipse",             create_ellipse,               Default),
            ("feBlend",             create_fe_blend,              Default),
            ("feColorMatrix",       create_fe_color_matrix,       Default),
            ("feComponentTransfer", create_fe_component_transfer, Default),
            ("feComposite",         create_fe_composite,          Default),
            ("feConvolveMatrix",    create_fe_convolve_matrix,    Default),
            ("feDiffuseLighting",   create_fe_diffuse_lighting,   Default),
            ("feDisplacementMap",   create_fe_displacement_map,   Default),
            ("feDistantLight",      create_fe_distant_light,      IgnoreClass),
            ("feDropShadow",        create_fe_drop_shadow,        Default),
            ("feFuncA",             create_fe_func_a,             IgnoreClass),
            ("feFuncB",             create_fe_func_b,             IgnoreClass),
            ("feFuncG",             create_fe_func_g,             IgnoreClass),
            ("feFuncR",             create_fe_func_r,             IgnoreClass),
            ("feFlood",             create_fe_flood,              Default),
            ("feGaussianBlur",      create_fe_gaussian_blur,      Default),
            ("feImage",             create_fe_image,              Default),
            ("feMerge",             create_fe_merge,              Default),
            ("feMergeNode",         create_fe_merge_node,         IgnoreClass),
            ("feMorphology",        create_fe_morphology,         Default),
            ("feOffset",            create_fe_offset,             Default),
            ("fePointLight",        create_fe_point_light,        IgnoreClass),
            ("feSpecularLighting",  create_fe_specular_lighting,  Default),
            ("feSpotLight",         create_fe_spot_light,         IgnoreClass),
            ("feTile",              create_fe_tile,               Default),
            ("feTurbulence",        create_fe_turbulence,         Default),
            ("filter",              create_filter,                Default),
            /* ("font",             ), */
            /* ("font-face",        ), */
            /* ("font-face-format", ), */
            /* ("font-face-name",   ), */
            /* ("font-face-src",    ), */
            /* ("font-face-uri",    ), */
            /* ("foreignObject",    ), */
            ("g",                   create_group,                 Default),
            /* ("glyph",            ), */
            /* ("glyphRef",         ), */
            /* ("hkern",            ), */
            ("image",               create_image,                 Default),
            ("line",                create_line,                  Default),
            ("linearGradient",      create_linear_gradient,       Default),
            ("marker",              create_marker,                Default),
            ("mask",                create_mask,                  Default),
            /* ("metadata",         ), */
            /* ("missing-glyph",    ), */
            /* ("mpath",            ), */
            /* ("multiImage",       ), */
            ("path",                create_path,                  Default),
            ("pattern",             create_pattern,               Default),
            ("polygon",             create_polygon,               Default),
            ("polyline",            create_polyline,              Default),
            ("radialGradient",      create_radial_gradient,       Default),
            ("rect",                create_rect,                  Default),
            /* ("script",           ), */
            /* ("set",              ), */
            ("stop",                create_stop,                  Default),
            ("style",               create_style,                 IgnoreClass),
            /* ("subImage",         ), */
            /* ("subImageRef",      ), */
            ("svg",                 create_svg,                   Default),
            ("switch",              create_switch,                Default),
            ("symbol",              create_symbol,                Default),
            ("text",                create_text,                  Default),
            ("text2",               create_text2,                 Default),
            /* ("textPath",         ), */
            /* ("title",            ), */
            ("tref",                create_tref,                  Default),
            ("tspan",               create_tspan,                 Default),
            ("use",                 create_use,                   Default),
            /* ("view",             ), */
            /* ("vkern",            ), */
        ];

        creators_table.into_iter().map(|(n, c, f)| (n, (c, f))).collect()
    })
}

impl Element {
    /// Takes an XML element name and consumes a list of attribute/value pairs to create an [`Element`].
    ///
    /// This operation does not fail.  Unknown element names simply produce a [`NonRendering`]
    /// element.
    pub fn new(session: &Session, name: &QualName, mut attributes: Attributes) -> Element {
        let (create_fn, flags): (ElementDataCreateFn, ElementCreateFlags) = if name.ns == ns!(svg) {
            match get_element_creators().get(name.local.as_ref()) {
                // hack in the SVG namespace for supported element names
                Some(&(create_fn, flags)) => (create_fn, flags),

                // Whenever we encounter a element name we don't understand, represent it as a
                // non-rendering element.  This is like a group, but it doesn't do any rendering
                // of children.  The effect is that we will ignore all children of unknown elements.
                None => (create_non_rendering, ElementCreateFlags::Default),
            }
        } else {
            (create_non_rendering, ElementCreateFlags::Default)
        };

        if flags == ElementCreateFlags::IgnoreClass {
            attributes.clear_class();
        };

        let element_data = create_fn(session, &attributes);

        let mut e = Self {
            element_name: name.clone(),
            attributes,
            specified_values: Default::default(),
            important_styles: Default::default(),
            values: Default::default(),
            required_extensions: Default::default(),
            required_features: Default::default(),
            system_language: Default::default(),
            element_data,
        };

        e.set_conditional_processing_attributes(session);
        e.set_presentation_attributes(session);

        e
    }

    pub fn element_name(&self) -> &QualName {
        &self.element_name
    }

    pub fn get_attributes(&self) -> &Attributes {
        &self.attributes
    }

    pub fn get_id(&self) -> Option<&str> {
        self.attributes.get_id()
    }

    pub fn get_class(&self) -> Option<&str> {
        self.attributes.get_class()
    }

    pub fn inherit_xml_lang(&mut self, parent: Option<Node>) {
        self.specified_values
            .inherit_xml_lang(&mut self.values, parent);
    }

    pub fn get_specified_values(&self) -> &SpecifiedValues {
        &self.specified_values
    }

    pub fn get_computed_values(&self) -> &ComputedValues {
        &self.values
    }

    pub fn set_computed_values(&mut self, values: &ComputedValues) {
        self.values = values.clone();
    }

    pub fn get_cond(&self, user_language: &UserLanguage, session: &Session) -> bool {
        self.required_extensions
            .as_ref()
            .map(|v| v.eval())
            .unwrap_or(true)
            && self
                .required_features
                .as_ref()
                .map(|v| v.eval())
                .unwrap_or(true)
            && self
                .system_language
                .as_ref()
                .map(|v| v.eval(user_language, session))
                .unwrap_or(true)
    }

    fn set_conditional_processing_attributes(&mut self, session: &Session) {
        for (attr, value) in self.attributes.iter() {
            match attr.expanded() {
                expanded_name!("", "requiredExtensions") => {
                    self.required_extensions = Some(RequiredExtensions::from_attribute(value));
                }

                expanded_name!("", "requiredFeatures") => {
                    self.required_features = Some(RequiredFeatures::from_attribute(value));
                }

                expanded_name!("", "systemLanguage") => {
                    self.system_language = Some(SystemLanguage::from_attribute(value, session));
                }

                _ => {}
            }
        }
    }

    /// Hands the `attrs` to the node's state, to apply the presentation attributes.
    fn set_presentation_attributes(&mut self, session: &Session) {
        self.specified_values
            .parse_presentation_attributes(session, &self.attributes);
    }

    // Applies a style declaration to the node's specified_values
    pub fn apply_style_declaration(&mut self, declaration: &Declaration, origin: Origin) {
        self.specified_values.set_property_from_declaration(
            declaration,
            origin,
            &mut self.important_styles,
        );
    }

    /// Applies CSS styles from the "style" attribute
    pub fn set_style_attribute(&mut self, session: &Session) {
        let style = self
            .attributes
            .iter()
            .find(|(attr, _)| attr.expanded() == expanded_name!("", "style"))
            .map(|(_, value)| value);

        if let Some(style) = style {
            self.specified_values.parse_style_declarations(
                style,
                Origin::Author,
                &mut self.important_styles,
                session,
            );
        }
    }

    #[rustfmt::skip]
    pub fn as_filter_effect(&self) -> Option<&dyn FilterEffect> {
        use ElementData::*;

        match &self.element_data {
            FeBlend(fe) =>              Some(&**fe),
            FeColorMatrix(fe) =>        Some(&**fe),
            FeComponentTransfer(fe) =>  Some(&**fe),
            FeComposite(fe) =>          Some(&**fe),
            FeConvolveMatrix(fe) =>     Some(&**fe),
            FeDiffuseLighting(fe) =>    Some(&**fe),
            FeDisplacementMap(fe) =>    Some(&**fe),
            FeDropShadow(fe) =>         Some(&**fe),
            FeFlood(fe) =>              Some(&**fe),
            FeGaussianBlur(fe) =>       Some(&**fe),
            FeImage(fe) =>              Some(&**fe),
            FeMerge(fe) =>              Some(&**fe),
            FeMorphology(fe) =>         Some(&**fe),
            FeOffset(fe) =>             Some(&**fe),
            FeSpecularLighting(fe) =>   Some(&**fe),
            FeTile(fe) =>               Some(&**fe),
            FeTurbulence(fe) =>         Some(&**fe),
            _ => None,
        }
    }

    /// Returns whether an element of a particular type is only accessed by reference
    // from other elements' attributes.  The element could in turn cause other nodes
    // to get referenced, potentially causing reference cycles.
    pub fn is_accessed_by_reference(&self) -> bool {
        use ElementData::*;

        matches!(
            self.element_data,
            ClipPath(_)
                | Filter(_)
                | LinearGradient(_)
                | Marker(_)
                | Mask(_)
                | Pattern(_)
                | RadialGradient(_)
        )
    }

    /// The main drawing function for elements.
    pub fn draw(
        &self,
        node: &Node,
        acquired_nodes: &mut AcquiredNodes<'_>,
        cascaded: &CascadedValues<'_>,
        viewport: &Viewport,
        draw_ctx: &mut DrawingCtx,
        clipping: bool,
    ) -> DrawResult {
        let values = cascaded.get();
        if values.is_displayed() {
            self.element_data
                .draw(node, acquired_nodes, cascaded, viewport, draw_ctx, clipping)
        } else {
            Ok(viewport.empty_bbox())
        }
    }

    /// The main layout function for elements.
    pub fn layout(
        &self,
        node: &Node,
        acquired_nodes: &mut AcquiredNodes<'_>,
        cascaded: &CascadedValues<'_>,
        viewport: &Viewport,
        draw_ctx: &mut DrawingCtx,
        clipping: bool,
    ) -> Result<Option<Layer>, Box<InternalRenderingError>> {
        let values = cascaded.get();
        if values.is_displayed() {
            self.element_data
                .layout(node, acquired_nodes, cascaded, viewport, draw_ctx, clipping)
        } else {
            Ok(None)
        }
    }
}

impl ElementData {
    /// Dispatcher for the draw method of concrete element implementations.
    #[rustfmt::skip]
    fn draw(
        &self,
        node: &Node,
        acquired_nodes: &mut AcquiredNodes<'_>,
        cascaded: &CascadedValues<'_>,
        viewport: &Viewport,
        draw_ctx: &mut DrawingCtx,
        clipping: bool,
    ) -> DrawResult {
        use ElementData::*;

        let data: &dyn ElementTrait = match self {
            Circle(d) =>               &**d,
            ClipPath(d) =>             &**d,
            Ellipse(d) =>              &**d,
            Filter(d) =>               &**d,
            Group(d) =>                &**d,
            Image(d) =>                &**d,
            Line(d) =>                 &**d,
            LinearGradient(d) =>       &**d,
            Link(d) =>                 &**d,
            Marker(d) =>               &**d,
            Mask(d) =>                 &**d,
            NonRendering(d) =>         &**d,
            Path(d) =>                 &**d,
            Pattern(d) =>              &**d,
            Polygon(d) =>              &**d,
            Polyline(d) =>             &**d,
            RadialGradient(d) =>       &**d,
            Rect(d) =>                 &**d,
            Stop(d) =>                 &**d,
            Style(d) =>                &**d,
            Svg(d) =>                  &**d,
            Switch(d) =>               &**d,
            Symbol(d) =>               &**d,
            Text(d) =>                 &**d,
            Text2(d) =>                 &**d,
            TRef(d) =>                 &**d,
            TSpan(d) =>                &**d,
            Use(d) =>                  &**d,

            FeBlend(d) =>              &**d,
            FeColorMatrix(d) =>        &**d,
            FeComponentTransfer(d) =>  &**d,
            FeComposite(d) =>          &**d,
            FeConvolveMatrix(d) =>     &**d,
            FeDiffuseLighting(d) =>    &**d,
            FeDisplacementMap(d) =>    &**d,
            FeDistantLight(d) =>       &**d,
            FeDropShadow(d) =>         &**d,
            FeFlood(d) =>              &**d,
            FeFuncA(d) =>              &**d,
            FeFuncB(d) =>              &**d,
            FeFuncG(d) =>              &**d,
            FeFuncR(d) =>              &**d,
            FeGaussianBlur(d) =>       &**d,
            FeImage(d) =>              &**d,
            FeMerge(d) =>              &**d,
            FeMergeNode(d) =>          &**d,
            FeMorphology(d) =>         &**d,
            FeOffset(d) =>             &**d,
            FePointLight(d) =>         &**d,
            FeSpecularLighting(d) =>   &**d,
            FeSpotLight(d) =>          &**d,
            FeTile(d) =>               &**d,
            FeTurbulence(d) =>         &**d,
        };

        data.draw(node, acquired_nodes, cascaded, viewport, draw_ctx, clipping)
    }

    /// Dispatcher for the layout method of concrete element implementations.
    #[rustfmt::skip]
    fn layout(
        &self,
        node: &Node,
        acquired_nodes: &mut AcquiredNodes<'_>,
        cascaded: &CascadedValues<'_>,
        viewport: &Viewport,
        draw_ctx: &mut DrawingCtx,
        clipping: bool,
    ) -> Result<Option<Layer>, Box<InternalRenderingError>> {
        use ElementData::*;

        let data: &dyn ElementTrait = match self {
            Circle(d) =>               &**d,
            ClipPath(d) =>             &**d,
            Ellipse(d) =>              &**d,
            Filter(d) =>               &**d,
            Group(d) =>                &**d,
            Image(d) =>                &**d,
            Line(d) =>                 &**d,
            LinearGradient(d) =>       &**d,
            Link(d) =>                 &**d,
            Marker(d) =>               &**d,
            Mask(d) =>                 &**d,
            NonRendering(d) =>         &**d,
            Path(d) =>                 &**d,
            Pattern(d) =>              &**d,
            Polygon(d) =>              &**d,
            Polyline(d) =>             &**d,
            RadialGradient(d) =>       &**d,
            Rect(d) =>                 &**d,
            Stop(d) =>                 &**d,
            Style(d) =>                &**d,
            Svg(d) =>                  &**d,
            Switch(d) =>               &**d,
            Symbol(d) =>               &**d,
            Text(d) =>                 &**d,
            Text2(d) =>                 &**d,
            TRef(d) =>                 &**d,
            TSpan(d) =>                &**d,
            Use(d) =>                  &**d,

            FeBlend(d) =>              &**d,
            FeColorMatrix(d) =>        &**d,
            FeComponentTransfer(d) =>  &**d,
            FeComposite(d) =>          &**d,
            FeConvolveMatrix(d) =>     &**d,
            FeDiffuseLighting(d) =>    &**d,
            FeDisplacementMap(d) =>    &**d,
            FeDistantLight(d) =>       &**d,
            FeDropShadow(d) =>         &**d,
            FeFlood(d) =>              &**d,
            FeFuncA(d) =>              &**d,
            FeFuncB(d) =>              &**d,
            FeFuncG(d) =>              &**d,
            FeFuncR(d) =>              &**d,
            FeGaussianBlur(d) =>       &**d,
            FeImage(d) =>              &**d,
            FeMerge(d) =>              &**d,
            FeMergeNode(d) =>          &**d,
            FeMorphology(d) =>         &**d,
            FeOffset(d) =>             &**d,
            FePointLight(d) =>         &**d,
            FeSpecularLighting(d) =>   &**d,
            FeSpotLight(d) =>          &**d,
            FeTile(d) =>               &**d,
            FeTurbulence(d) =>         &**d,
        };

        data.layout(node, acquired_nodes, cascaded, viewport, draw_ctx, clipping)
    }
}

macro_rules! e {
    ($name:ident, $element_type:ident) => {
        pub fn $name(session: &Session, attributes: &Attributes) -> ElementData {
            let mut payload = Box::<$element_type>::default();
            payload.set_attributes(attributes, session);

            ElementData::$element_type(payload)
        }
    };
}

#[rustfmt::skip]
mod creators {
    use super::*;

    e!(create_circle,                   Circle);
    e!(create_clip_path,                ClipPath);
    e!(create_defs,                     NonRendering);
    e!(create_ellipse,                  Ellipse);
    e!(create_fe_blend,                 FeBlend);
    e!(create_fe_color_matrix,          FeColorMatrix);
    e!(create_fe_component_transfer,    FeComponentTransfer);
    e!(create_fe_func_a,                FeFuncA);
    e!(create_fe_func_b,                FeFuncB);
    e!(create_fe_func_g,                FeFuncG);
    e!(create_fe_func_r,                FeFuncR);
    e!(create_fe_composite,             FeComposite);
    e!(create_fe_convolve_matrix,       FeConvolveMatrix);
    e!(create_fe_diffuse_lighting,      FeDiffuseLighting);
    e!(create_fe_displacement_map,      FeDisplacementMap);
    e!(create_fe_distant_light,         FeDistantLight);
    e!(create_fe_drop_shadow,           FeDropShadow);
    e!(create_fe_flood,                 FeFlood);
    e!(create_fe_gaussian_blur,         FeGaussianBlur);
    e!(create_fe_image,                 FeImage);
    e!(create_fe_merge,                 FeMerge);
    e!(create_fe_merge_node,            FeMergeNode);
    e!(create_fe_morphology,            FeMorphology);
    e!(create_fe_offset,                FeOffset);
    e!(create_fe_point_light,           FePointLight);
    e!(create_fe_specular_lighting,     FeSpecularLighting);
    e!(create_fe_spot_light,            FeSpotLight);
    e!(create_fe_tile,                  FeTile);
    e!(create_fe_turbulence,            FeTurbulence);
    e!(create_filter,                   Filter);
    e!(create_group,                    Group);
    e!(create_image,                    Image);
    e!(create_line,                     Line);
    e!(create_linear_gradient,          LinearGradient);
    e!(create_link,                     Link);
    e!(create_marker,                   Marker);
    e!(create_mask,                     Mask);
    e!(create_non_rendering,            NonRendering);
    e!(create_path,                     Path);
    e!(create_pattern,                  Pattern);
    e!(create_polygon,                  Polygon);
    e!(create_polyline,                 Polyline);
    e!(create_radial_gradient,          RadialGradient);
    e!(create_rect,                     Rect);
    e!(create_stop,                     Stop);
    e!(create_style,                    Style);
    e!(create_svg,                      Svg);
    e!(create_switch,                   Switch);
    e!(create_symbol,                   Symbol);
    e!(create_text,                     Text);
    e!(create_text2,                    Text2);
    e!(create_tref,                     TRef);
    e!(create_tspan,                    TSpan);
    e!(create_use,                      Use);

    /* Hack to make multiImage sort-of work
     *
     * disabled for now, as markup5ever doesn't have local names for
     * multiImage, subImage, subImageRef.  Maybe we can just... create them ourselves?
     *
     * Is multiImage even in SVG2?
     */
    /*
    e!(create_multi_image,              Switch);
    e!(create_sub_image,                Group);
    e!(create_sub_image_ref,            Image);
    */
}

use creators::*;

type ElementDataCreateFn = fn(session: &Session, attributes: &Attributes) -> ElementData;

#[derive(Copy, Clone, PartialEq)]
enum ElementCreateFlags {
    Default,
    IgnoreClass,
}

static ELEMENT_CREATORS: OnceLock<
    HashMap<&'static str, (ElementDataCreateFn, ElementCreateFlags)>,
> = OnceLock::new();
