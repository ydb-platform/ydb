//! Gradient paint servers; the `linearGradient` and `radialGradient` elements.

use cssparser::Parser;
use markup5ever::{ExpandedName, LocalName, Namespace, expanded_name, local_name, ns};

use crate::color::{Color, resolve_color};
use crate::coord_units;
use crate::coord_units::CoordUnits;
use crate::document::{AcquiredNodes, NodeId, NodeStack};
use crate::drawing_ctx::Viewport;
use crate::element::{ElementData, ElementTrait, set_attribute};
use crate::error::*;
use crate::href::{is_href, set_href};
use crate::length::*;
use crate::node::{CascadedValues, Node, NodeBorrow};
use crate::parse_identifiers;
use crate::parsers::{Parse, ParseValue};
use crate::rect::{Rect, rect_to_transform};
use crate::session::Session;
use crate::transform::{Transform, TransformAttribute};
use crate::unit_interval::UnitInterval;
use crate::xml::Attributes;

/// Contents of a `<stop>` element for gradient color stops
#[derive(Copy, Clone)]
pub struct ColorStop {
    /// `<stop offset="..."/>`
    pub offset: UnitInterval,

    /// `<stop stop-color="..." stop-opacity="..."/>`
    pub color: Color,
}

// gradientUnits attribute; its default is objectBoundingBox
coord_units!(GradientUnits, CoordUnits::ObjectBoundingBox);

/// spreadMethod attribute for gradients
#[derive(Debug, Default, Copy, Clone, PartialEq)]
pub enum SpreadMethod {
    #[default]
    Pad,
    Reflect,
    Repeat,
}

impl Parse for SpreadMethod {
    fn parse<'i>(parser: &mut Parser<'i, '_>) -> Result<SpreadMethod, ParseError<'i>> {
        Ok(parse_identifiers!(
            parser,
            "pad" => SpreadMethod::Pad,
            "reflect" => SpreadMethod::Reflect,
            "repeat" => SpreadMethod::Repeat,
        )?)
    }
}

/// Node for the `<stop>` element
#[derive(Default)]
pub struct Stop {
    /// `<stop offset="..."/>`
    offset: UnitInterval,
    /* stop-color and stop-opacity are not attributes; they are properties, so
     * they go into property_defs.rs */
}

impl ElementTrait for Stop {
    fn set_attributes(&mut self, attrs: &Attributes, session: &Session) {
        for (attr, value) in attrs.iter() {
            if attr.expanded() == expanded_name!("", "offset") {
                set_attribute(&mut self.offset, attr.parse(value), session);
            }
        }
    }
}

/// Parameters specific to each gradient type, before being resolved.
/// These will be composed together with UnreseolvedVariant from fallback
/// nodes (referenced with e.g. `<linearGradient xlink:href="#fallback">`) to form
/// a final, resolved Variant.
#[derive(Copy, Clone)]
enum UnresolvedVariant {
    Linear {
        x1: Option<Length<Horizontal>>,
        y1: Option<Length<Vertical>>,
        x2: Option<Length<Horizontal>>,
        y2: Option<Length<Vertical>>,
    },

    Radial {
        cx: Option<Length<Horizontal>>,
        cy: Option<Length<Vertical>>,
        r: Option<Length<Both>>,
        fx: Option<Length<Horizontal>>,
        fy: Option<Length<Vertical>>,
        fr: Option<Length<Both>>,
    },
}

/// Parameters specific to each gradient type, after resolving.
#[derive(Clone)]
enum ResolvedGradientVariant {
    Linear {
        x1: Length<Horizontal>,
        y1: Length<Vertical>,
        x2: Length<Horizontal>,
        y2: Length<Vertical>,
    },

    Radial {
        cx: Length<Horizontal>,
        cy: Length<Vertical>,
        r: Length<Both>,
        fx: Length<Horizontal>,
        fy: Length<Vertical>,
        fr: Length<Both>,
    },
}

/// Parameters specific to each gradient type, after normalizing to user-space units.
pub enum GradientVariant {
    Linear {
        x1: f64,
        y1: f64,
        x2: f64,
        y2: f64,
    },

    Radial {
        cx: f64,
        cy: f64,
        r: f64,
        fx: f64,
        fy: f64,
        fr: f64,
    },
}

impl UnresolvedVariant {
    fn into_resolved(self) -> ResolvedGradientVariant {
        assert!(self.is_resolved());

        match self {
            UnresolvedVariant::Linear { x1, y1, x2, y2 } => ResolvedGradientVariant::Linear {
                x1: x1.unwrap(),
                y1: y1.unwrap(),
                x2: x2.unwrap(),
                y2: y2.unwrap(),
            },

            UnresolvedVariant::Radial {
                cx,
                cy,
                r,
                fx,
                fy,
                fr,
            } => ResolvedGradientVariant::Radial {
                cx: cx.unwrap(),
                cy: cy.unwrap(),
                r: r.unwrap(),
                fx: fx.unwrap(),
                fy: fy.unwrap(),
                fr: fr.unwrap(),
            },
        }
    }

    fn is_resolved(&self) -> bool {
        match *self {
            UnresolvedVariant::Linear { x1, y1, x2, y2 } => {
                x1.is_some() && y1.is_some() && x2.is_some() && y2.is_some()
            }

            UnresolvedVariant::Radial {
                cx,
                cy,
                r,
                fx,
                fy,
                fr,
            } => {
                cx.is_some()
                    && cy.is_some()
                    && r.is_some()
                    && fx.is_some()
                    && fy.is_some()
                    && fr.is_some()
            }
        }
    }

    fn resolve_from_fallback(&self, fallback: &UnresolvedVariant) -> UnresolvedVariant {
        match (*self, *fallback) {
            (
                UnresolvedVariant::Linear { x1, y1, x2, y2 },
                UnresolvedVariant::Linear {
                    x1: fx1,
                    y1: fy1,
                    x2: fx2,
                    y2: fy2,
                },
            ) => UnresolvedVariant::Linear {
                x1: x1.or(fx1),
                y1: y1.or(fy1),
                x2: x2.or(fx2),
                y2: y2.or(fy2),
            },

            (
                UnresolvedVariant::Radial {
                    cx,
                    cy,
                    r,
                    fx,
                    fy,
                    fr,
                },
                UnresolvedVariant::Radial {
                    cx: f_cx,
                    cy: f_cy,
                    r: f_r,
                    fx: f_fx,
                    fy: f_fy,
                    fr: f_fr,
                },
            ) => UnresolvedVariant::Radial {
                cx: cx.or(f_cx),
                cy: cy.or(f_cy),
                r: r.or(f_r),
                fx: fx.or(f_fx),
                fy: fy.or(f_fy),
                fr: fr.or(f_fr),
            },

            _ => *self, // If variants are of different types, then nothing to resolve
        }
    }

    // https://www.w3.org/TR/SVG/pservers.html#LinearGradients
    // https://www.w3.org/TR/SVG/pservers.html#RadialGradients
    fn resolve_from_defaults(&self) -> UnresolvedVariant {
        match self {
            UnresolvedVariant::Linear { x1, y1, x2, y2 } => UnresolvedVariant::Linear {
                x1: x1.or_else(|| Some(Length::<Horizontal>::parse_str("0%").unwrap())),
                y1: y1.or_else(|| Some(Length::<Vertical>::parse_str("0%").unwrap())),
                x2: x2.or_else(|| Some(Length::<Horizontal>::parse_str("100%").unwrap())),
                y2: y2.or_else(|| Some(Length::<Vertical>::parse_str("0%").unwrap())),
            },

            UnresolvedVariant::Radial {
                cx,
                cy,
                r,
                fx,
                fy,
                fr,
            } => {
                let cx = cx.or_else(|| Some(Length::<Horizontal>::parse_str("50%").unwrap()));
                let cy = cy.or_else(|| Some(Length::<Vertical>::parse_str("50%").unwrap()));
                let r = r.or_else(|| Some(Length::<Both>::parse_str("50%").unwrap()));

                // fx and fy fall back to the presentational value of cx and cy
                let fx = fx.or(cx);
                let fy = fy.or(cy);
                let fr = fr.or_else(|| Some(Length::<Both>::parse_str("0%").unwrap()));

                UnresolvedVariant::Radial {
                    cx,
                    cy,
                    r,
                    fx,
                    fy,
                    fr,
                }
            }
        }
    }
}

/// Fields shared by all gradient nodes
#[derive(Default)]
struct Common {
    units: Option<GradientUnits>,
    transform: Option<TransformAttribute>,
    spread: Option<SpreadMethod>,

    fallback: Option<NodeId>,
}

/// Node for the `<linearGradient>` element
#[derive(Default)]
pub struct LinearGradient {
    common: Common,

    x1: Option<Length<Horizontal>>,
    y1: Option<Length<Vertical>>,
    x2: Option<Length<Horizontal>>,
    y2: Option<Length<Vertical>>,
}

/// Node for the `<radialGradient>` element
#[derive(Default)]
pub struct RadialGradient {
    common: Common,

    cx: Option<Length<Horizontal>>,
    cy: Option<Length<Vertical>>,
    r: Option<Length<Both>>,
    fx: Option<Length<Horizontal>>,
    fy: Option<Length<Vertical>>,
    fr: Option<Length<Both>>,
}

/// Main structure used during gradient resolution.  For unresolved
/// gradients, we store all fields as `Option<T>` - if `None`, it means
/// that the field is not specified; if `Some(T)`, it means that the
/// field was specified.
struct UnresolvedGradient {
    units: Option<GradientUnits>,
    transform: Option<TransformAttribute>,
    spread: Option<SpreadMethod>,
    stops: Option<Vec<ColorStop>>,

    variant: UnresolvedVariant,
}

/// Resolved gradient; this is memoizable after the initial resolution.
#[derive(Clone)]
pub struct ResolvedGradient {
    units: GradientUnits,
    transform: TransformAttribute,
    spread: SpreadMethod,
    stops: Vec<ColorStop>,

    variant: ResolvedGradientVariant,
}

/// Gradient normalized to user-space units.
pub struct UserSpaceGradient {
    pub transform: Transform,
    pub spread: SpreadMethod,
    pub stops: Vec<ColorStop>,

    pub variant: GradientVariant,
}

impl UnresolvedGradient {
    fn into_resolved(self) -> ResolvedGradient {
        assert!(self.is_resolved());

        let UnresolvedGradient {
            units,
            transform,
            spread,
            stops,
            variant,
        } = self;

        match variant {
            UnresolvedVariant::Linear { .. } => ResolvedGradient {
                units: units.unwrap(),
                transform: transform.unwrap(),
                spread: spread.unwrap(),
                stops: stops.unwrap(),

                variant: variant.into_resolved(),
            },

            UnresolvedVariant::Radial { .. } => ResolvedGradient {
                units: units.unwrap(),
                transform: transform.unwrap(),
                spread: spread.unwrap(),
                stops: stops.unwrap(),

                variant: variant.into_resolved(),
            },
        }
    }

    /// Helper for add_color_stops_from_node()
    fn add_color_stop(&mut self, offset: UnitInterval, color: Color) {
        if self.stops.is_none() {
            self.stops = Some(Vec::<ColorStop>::new());
        }

        if let Some(ref mut stops) = self.stops {
            let last_offset = if !stops.is_empty() {
                stops[stops.len() - 1].offset
            } else {
                UnitInterval(0.0)
            };

            let offset = if offset > last_offset {
                offset
            } else {
                last_offset
            };

            stops.push(ColorStop { offset, color });
        } else {
            unreachable!();
        }
    }

    /// Looks for `<stop>` children inside a linearGradient or radialGradient node,
    /// and adds their info to the UnresolvedGradient &self.
    fn add_color_stops_from_node(&mut self, node: &Node, opacity: UnitInterval) {
        assert!(matches!(
            *node.borrow_element_data(),
            ElementData::LinearGradient(_) | ElementData::RadialGradient(_)
        ));

        for child in node.children().filter(|c| c.is_element()) {
            if let ElementData::Stop(stop) = &*child.borrow_element_data() {
                let cascaded = CascadedValues::new_from_node(&child);
                let values = cascaded.get();

                let UnitInterval(stop_opacity) = values.stop_opacity().0;
                let UnitInterval(o) = opacity;

                let composed_opacity = UnitInterval(stop_opacity * o);

                let stop_color =
                    resolve_color(&values.stop_color().0, composed_opacity, &values.color().0);

                self.add_color_stop(stop.offset, stop_color);
            }
        }
    }

    fn is_resolved(&self) -> bool {
        self.units.is_some()
            && self.transform.is_some()
            && self.spread.is_some()
            && self.stops.is_some()
            && self.variant.is_resolved()
    }

    fn resolve_from_fallback(&self, fallback: &UnresolvedGradient) -> UnresolvedGradient {
        let units = self.units.or(fallback.units);
        let transform = self.transform.or(fallback.transform);
        let spread = self.spread.or(fallback.spread);
        let stops = self.stops.clone().or_else(|| fallback.stops.clone());
        let variant = self.variant.resolve_from_fallback(&fallback.variant);

        UnresolvedGradient {
            units,
            transform,
            spread,
            stops,
            variant,
        }
    }

    fn resolve_from_defaults(&self) -> UnresolvedGradient {
        let units = self.units.or_else(|| Some(GradientUnits::default()));
        let transform = self
            .transform
            .or_else(|| Some(TransformAttribute::default()));
        let spread = self.spread.or_else(|| Some(SpreadMethod::default()));
        let stops = self.stops.clone().or_else(|| Some(Vec::<ColorStop>::new()));
        let variant = self.variant.resolve_from_defaults();

        UnresolvedGradient {
            units,
            transform,
            spread,
            stops,
            variant,
        }
    }
}

/// State used during the gradient resolution process
///
/// This is the current node's gradient information, plus the fallback
/// that should be used in case that information is not complete for a
/// resolved gradient yet.
struct Unresolved {
    gradient: UnresolvedGradient,
    fallback: Option<NodeId>,
}

impl LinearGradient {
    fn get_unresolved_variant(&self) -> UnresolvedVariant {
        UnresolvedVariant::Linear {
            x1: self.x1,
            y1: self.y1,
            x2: self.x2,
            y2: self.y2,
        }
    }
}

impl RadialGradient {
    fn get_unresolved_variant(&self) -> UnresolvedVariant {
        UnresolvedVariant::Radial {
            cx: self.cx,
            cy: self.cy,
            r: self.r,
            fx: self.fx,
            fy: self.fy,
            fr: self.fr,
        }
    }
}

impl Common {
    fn set_attributes(&mut self, attrs: &Attributes, session: &Session) {
        for (attr, value) in attrs.iter() {
            match attr.expanded() {
                expanded_name!("", "gradientUnits") => {
                    set_attribute(&mut self.units, attr.parse(value), session)
                }
                expanded_name!("", "gradientTransform") => {
                    set_attribute(&mut self.transform, attr.parse(value), session);
                }
                expanded_name!("", "spreadMethod") => {
                    set_attribute(&mut self.spread, attr.parse(value), session)
                }
                ref a if is_href(a) => {
                    let mut href = None;
                    set_attribute(
                        &mut href,
                        NodeId::parse(value).map(Some).attribute(attr.clone()),
                        session,
                    );
                    set_href(a, &mut self.fallback, href);
                }
                _ => (),
            }
        }
    }
}

impl ElementTrait for LinearGradient {
    fn set_attributes(&mut self, attrs: &Attributes, session: &Session) {
        self.common.set_attributes(attrs, session);

        for (attr, value) in attrs.iter() {
            match attr.expanded() {
                expanded_name!("", "x1") => set_attribute(&mut self.x1, attr.parse(value), session),
                expanded_name!("", "y1") => set_attribute(&mut self.y1, attr.parse(value), session),
                expanded_name!("", "x2") => set_attribute(&mut self.x2, attr.parse(value), session),
                expanded_name!("", "y2") => set_attribute(&mut self.y2, attr.parse(value), session),

                _ => (),
            }
        }
    }
}

macro_rules! impl_gradient {
    ($gradient_type:ident, $other_type:ident) => {
        impl $gradient_type {
            fn get_unresolved(&self, node: &Node, opacity: UnitInterval) -> Unresolved {
                let mut gradient = UnresolvedGradient {
                    units: self.common.units,
                    transform: self.common.transform,
                    spread: self.common.spread,
                    stops: None,
                    variant: self.get_unresolved_variant(),
                };

                gradient.add_color_stops_from_node(node, opacity);

                Unresolved {
                    gradient,
                    fallback: self.common.fallback.clone(),
                }
            }

            pub fn resolve(
                &self,
                node: &Node,
                acquired_nodes: &mut AcquiredNodes<'_>,
                opacity: UnitInterval,
            ) -> Result<ResolvedGradient, AcquireError> {
                let Unresolved {
                    mut gradient,
                    mut fallback,
                } = self.get_unresolved(node, opacity);

                let mut stack = NodeStack::new();

                while !gradient.is_resolved() {
                    if let Some(node_id) = fallback {
                        let acquired = acquired_nodes.acquire(&node_id)?;
                        let acquired_node = acquired.get();

                        if stack.contains(acquired_node) {
                            return Err(AcquireError::CircularReference(acquired_node.clone()));
                        }

                        let unresolved = match *acquired_node.borrow_element_data() {
                            ElementData::$gradient_type(ref g) => {
                                g.get_unresolved(&acquired_node, opacity)
                            }
                            ElementData::$other_type(ref g) => {
                                g.get_unresolved(&acquired_node, opacity)
                            }
                            _ => return Err(AcquireError::InvalidLinkType(node_id.clone())),
                        };

                        gradient = gradient.resolve_from_fallback(&unresolved.gradient);
                        fallback = unresolved.fallback;

                        stack.push(acquired_node);
                    } else {
                        gradient = gradient.resolve_from_defaults();
                        break;
                    }
                }

                Ok(gradient.into_resolved())
            }
        }
    };
}

impl_gradient!(LinearGradient, RadialGradient);
impl_gradient!(RadialGradient, LinearGradient);

impl ElementTrait for RadialGradient {
    fn set_attributes(&mut self, attrs: &Attributes, session: &Session) {
        self.common.set_attributes(attrs, session);

        // Create a local expanded name for "fr" because markup5ever doesn't have built-in
        let expanded_name_fr = ExpandedName {
            ns: &Namespace::from(""),
            local: &LocalName::from("fr"),
        };

        for (attr, value) in attrs.iter() {
            let attr_expanded = attr.expanded();
            match attr_expanded {
                expanded_name!("", "cx") => set_attribute(&mut self.cx, attr.parse(value), session),
                expanded_name!("", "cy") => set_attribute(&mut self.cy, attr.parse(value), session),
                expanded_name!("", "r") => set_attribute(&mut self.r, attr.parse(value), session),
                expanded_name!("", "fx") => set_attribute(&mut self.fx, attr.parse(value), session),
                expanded_name!("", "fy") => set_attribute(&mut self.fy, attr.parse(value), session),
                a if a == expanded_name_fr => {
                    set_attribute(&mut self.fr, attr.parse(value), session)
                }

                _ => (),
            }
        }
    }
}

impl ResolvedGradient {
    pub fn to_user_space(
        &self,
        object_bbox: &Option<Rect>,
        viewport: &Viewport,
        values: &NormalizeValues,
    ) -> Option<UserSpaceGradient> {
        let units = self.units.0;
        let transform = rect_to_transform(object_bbox, units).ok()?;
        let viewport = viewport.with_units(units);
        let params = NormalizeParams::from_values(values, &viewport);

        let gradient_transform = self.transform.to_transform();
        let transform = transform.pre_transform(&gradient_transform).invert()?;

        let variant = match self.variant {
            ResolvedGradientVariant::Linear { x1, y1, x2, y2 } => GradientVariant::Linear {
                x1: x1.to_user(&params),
                y1: y1.to_user(&params),
                x2: x2.to_user(&params),
                y2: y2.to_user(&params),
            },

            ResolvedGradientVariant::Radial {
                cx,
                cy,
                r,
                fx,
                fy,
                fr,
            } => GradientVariant::Radial {
                cx: cx.to_user(&params),
                cy: cy.to_user(&params),
                r: r.to_user(&params),
                fx: fx.to_user(&params),
                fy: fy.to_user(&params),
                fr: fr.to_user(&params),
            },
        };

        Some(UserSpaceGradient {
            transform,
            spread: self.spread,
            stops: self.stops.clone(),
            variant,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use markup5ever::{QualName, ns};

    use crate::borrow_element_as;
    use crate::node::{Node, NodeData};

    #[test]
    fn parses_spread_method() {
        assert_eq!(SpreadMethod::parse_str("pad").unwrap(), SpreadMethod::Pad);
        assert_eq!(
            SpreadMethod::parse_str("reflect").unwrap(),
            SpreadMethod::Reflect
        );
        assert_eq!(
            SpreadMethod::parse_str("repeat").unwrap(),
            SpreadMethod::Repeat
        );
        assert!(SpreadMethod::parse_str("foobar").is_err());
    }

    #[test]
    fn gradient_resolved_from_defaults_is_really_resolved() {
        let session = Session::default();

        let node = Node::new(NodeData::new_element(
            &session,
            &QualName::new(None, ns!(svg), local_name!("linearGradient")),
            Attributes::new(),
        ));

        let unresolved = borrow_element_as!(node, LinearGradient)
            .get_unresolved(&node, UnitInterval::clamp(1.0));
        let gradient = unresolved.gradient.resolve_from_defaults();
        assert!(gradient.is_resolved());

        let node = Node::new(NodeData::new_element(
            &session,
            &QualName::new(None, ns!(svg), local_name!("radialGradient")),
            Attributes::new(),
        ));

        let unresolved = borrow_element_as!(node, RadialGradient)
            .get_unresolved(&node, UnitInterval::clamp(1.0));
        let gradient = unresolved.gradient.resolve_from_defaults();
        assert!(gradient.is_resolved());
    }
}
