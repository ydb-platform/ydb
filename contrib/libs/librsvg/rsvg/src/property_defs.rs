//! Definitions for CSS property types.
//!
//! Do not import things directly from this module; use the `properties` module instead,
//! which re-exports things from here.
//!
//! This module defines most of the CSS property types that librsvg supports.  Each
//! property requires a Rust type that will hold its values, and that type should
//! implement a few traits, as follows.
//!
//! # Requirements for a property type
//!
//! You should call the [`make_property`] macro to take care of most of these requirements
//! automatically:
//!
//! * A name for the type.  For example, the `fill` property has a [`Fill`] type defined
//!   in this module.
//!
//! * An initial value per the CSS or SVG specs, given through an implementation of the
//!   [`Default`] trait.
//!
//! * Whether the property's computed value inherits to child elements, given through an
//!   implementation of the [`Property`] trait and its
//!   [`inherits_automatically`][Property::inherits_automatically] method.
//!
//! * A way to derive the CSS *computed value* for the property, given through an
//!   implementation of the [`Property`] trait and its [`compute`][Property::compute] method.
//!
//! * The actual underlying type.  For example, the [`make_property`] macro can generate a
//!   field-less enum for properties like the `clip-rule` property, which just has
//!   identifier-based values like `nonzero` and `evenodd`.  For general-purpose types like
//!   [`Length`], the macro can wrap them in a newtype like `struct`
//!   [`StrokeWidth`]`(`[`Length`]`)`.  For custom types, the macro call can be used just to
//!   define the initial/default value and whether the property inherits automatically; you
//!   should provide the other required trait implementations separately.
//!
//! * An implementation of the [`Parse`] trait for the underlying type.
use std::convert::TryInto;
use std::str::FromStr;

use cssparser::{Parser, Token};
use language_tags::LanguageTag;

use crate::dasharray::Dasharray;
use crate::error::*;
use crate::filter::FilterValueList;
use crate::font_props::{
    Font, FontFamily, FontSize, FontWeight, GlyphOrientationVertical, LetterSpacing, LineHeight,
};
use crate::iri::Iri;
use crate::length::*;
use crate::paint_server::PaintServer;
use crate::parse_identifiers;
use crate::parsers::Parse;
use crate::properties::ComputedValues;
use crate::property_macros::Property;
use crate::rect::Rect;
use crate::transform::TransformProperty;
use crate::unit_interval::UnitInterval;
use crate::{impl_default, impl_property, make_property};

make_property!(
    /// `baseline-shift` property.
    ///
    /// SVG1.1: <https://www.w3.org/TR/SVG11/text.html#BaselineShiftProperty>
    ///
    /// SVG2: <https://www.w3.org/TR/SVG2/text.html#BaselineShiftProperty>
    BaselineShift,
    default: Length::<Both>::default(),
    newtype: Length<Both>,
    property_impl: {
        impl Property for BaselineShift {
            fn inherits_automatically() -> bool {
                false
            }

            fn compute(&self, v: &ComputedValues) -> Self {
                let font_size = v.font_size().value();
                let parent = v.baseline_shift();

                match (self.0.unit, parent.0.unit) {
                    (LengthUnit::Percent, _) => {
                        BaselineShift(Length::<Both>::new(self.0.length * font_size.length + parent.0.length, font_size.unit))
                    }

                    (x, y) if x == y || parent.0.length == 0.0 => {
                        BaselineShift(Length::<Both>::new(self.0.length + parent.0.length, self.0.unit))
                    }

                    _ => {
                        // FIXME: the limitation here is that the parent's baseline_shift
                        // and ours have different units.  We should be able to normalize
                        // the lengths and add them even if they have different units, but
                        // at the moment that requires access to the draw_ctx, which we
                        // don't have here.
                        //
                        // So for now we won't add to the parent's baseline_shift.

                        parent
                    }
                }
            }
        }
    },
    parse_impl: {
        impl Parse for BaselineShift {
            // These values come from Inkscape's SP_CSS_BASELINE_SHIFT_(SUB/SUPER/BASELINE);
            // see sp_style_merge_baseline_shift_from_parent()
            fn parse<'i>(parser: &mut Parser<'i, '_>) -> Result<BaselineShift, crate::error::ParseError<'i>> {
                parser.try_parse(|p| Ok(BaselineShift(Length::<Both>::parse(p)?)))
                    .or_else(|_: ParseError<'_>| {
                        Ok(parse_identifiers!(
                            parser,
                            "baseline" => BaselineShift(Length::<Both>::new(0.0, LengthUnit::Percent)),
                            "sub" => BaselineShift(Length::<Both>::new(-0.2, LengthUnit::Percent)),

                            "super" => BaselineShift(Length::<Both>::new(0.4, LengthUnit::Percent)),
                        )?)
                    })
            }
        }
    }
);

make_property!(
    /// `clip-path` property.
    ///
    /// SVG1.1: <https://www.w3.org/TR/SVG11/masking.html#ClipPathProperty>
    ///
    /// CSS Masking 1: <https://www.w3.org/TR/css-masking-1/#the-clip-path>
    ClipPath,
    default: Iri::None,
    inherits_automatically: false,
    newtype_parse: Iri,
);

make_property!(
    /// `clip-rule` property.
    ///
    /// SVG1.1: <https://www.w3.org/TR/SVG11/masking.html#ClipRuleProperty>
    ///
    /// CSS Masking 1: <https://www.w3.org/TR/css-masking-1/#the-clip-rule>
    ClipRule,
    default: NonZero,
    inherits_automatically: true,

    identifiers:
    "nonzero" => NonZero,
    "evenodd" => EvenOdd,
);

make_property!(
    /// `color` property, the fallback for `currentColor` values.
    ///
    /// SVG1.1: <https://www.w3.org/TR/SVG11/color.html#ColorProperty>
    ///
    /// SVG2: <https://www.w3.org/TR/SVG2/painting.html#ColorProperty>
    ///
    /// The SVG spec allows the user agent to choose its own initial value for the "color"
    /// property.  Here we start with opaque black for the initial value.  Clients can
    /// override this by specifing a custom CSS stylesheet.
    ///
    /// Most of the time the `color` property is used to call
    /// [`crate::color::resolve_color`].
    Color,
    default: crate::color::Color::Rgba(crate::color::RGBA::new(0, 0, 0, 1.0)),
    inherits_automatically: true,
    newtype_parse: crate::color::Color,
);

make_property!(
    /// `color-interpolation-filters` property.
    ///
    /// SVG1.1: <https://www.w3.org/TR/SVG11/painting.html#ColorInterpolationFiltersProperty>
    ///
    /// Filter Effects 1: <https://www.w3.org/TR/filter-effects/#propdef-color-interpolation-filters>
    ColorInterpolationFilters,
    default: LinearRgb,
    inherits_automatically: true,

    identifiers:
    "auto" => Auto,
    "linearRGB" => LinearRgb,
    "sRGB" => Srgb,
);

make_property!(
    /// `cx` property.
    ///
    /// SVG2: <https://www.w3.org/TR/SVG2/geometry.html#CX>
    ///
    /// Note that in SVG1.1, this was an attribute, not a property.
    CX,
    default: Length::<Horizontal>::default(),
    inherits_automatically: false,
    newtype_parse: Length<Horizontal>,
);

make_property!(
    /// `cy` attribute.
    ///
    /// SVG2: <https://www.w3.org/TR/SVG2/geometry.html#CY>
    ///
    /// Note that in SVG1.1, this was an attribute, not a property.
    CY,
    default: Length::<Vertical>::default(),
    inherits_automatically: false,
    newtype_parse: Length<Vertical>,
);

make_property!(
    /// `direction` property.
    ///
    /// SVG1.1: <https://www.w3.org/TR/SVG11/text.html#DirectionProperty>
    ///
    /// SVG2: <https://www.w3.org/TR/SVG2/text.html#DirectionProperty>
    Direction,
    default: Ltr,
    inherits_automatically: true,

    identifiers:
    "ltr" => Ltr,
    "rtl" => Rtl,
);

make_property!(
    /// `display` property.
    ///
    /// SVG1.1: <https://www.w3.org/TR/CSS2/visuren.html#display-prop>
    ///
    /// SVG2: <https://www.w3.org/TR/SVG2/render.html#VisibilityControl>
    Display,
    default: Inline,
    inherits_automatically: false,

    identifiers:
    "inline" => Inline,
    "block" => Block,
    "list-item" => ListItem,
    "run-in" => RunIn,
    "compact" => Compact,
    "marker" => Marker,
    "table" => Table,
    "inline-table" => InlineTable,
    "table-row-group" => TableRowGroup,
    "table-header-group" => TableHeaderGroup,
    "table-footer-group" => TableFooterGroup,
    "table-row" => TableRow,
    "table-column-group" => TableColumnGroup,
    "table-column" => TableColumn,
    "table-cell" => TableCell,
    "table-caption" => TableCaption,
    "none" => None,
);

make_property!(
    /// `dominant-baseline` property.
    ///
    /// SVG1.1: <https://www.w3.org/TR/SVG11/text.html#BaselineAlignmentProperties>
    DominantBaseline,
    default: Auto,
    inherits_automatically: true,

    identifiers:
    "auto" => Auto,
    "ideographic" => Ideographic,
    "alphabetic" => Alphabetic,
    "hanging" => Hanging,
    "mathematical" => Mathematical,
    "central" => Central,
    "middle" => Middle,
    "text-after-edge" => TextAfterEdge,
    "text-before-edge" => TextBeforeEdge,
    // CSS3
    "text-top" => TextTop,
    "text-bottom" => TextBottom,
    // No longer supported in SVG2 (https://www.w3.org/TR/SVG2/text.html#DominantBaselineProperty):
    // use-script, no-change and reset-size
);

/// `enable-background` property.
///
/// SVG1.1: <https://www.w3.org/TR/SVG11/filters.html#EnableBackgroundProperty>
///
/// This is deprecated in SVG2.  We just have a parser for it to avoid setting elements in
/// error if they have this property.  Librsvg does not use the value of this property.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum EnableBackground {
    Accumulate,
    New(Option<Rect>),
}

make_property!(
    EnableBackground,
    default: EnableBackground::Accumulate,
    inherits_automatically: false,

    parse_impl: {
        impl Parse for EnableBackground {
            fn parse<'i>(parser: &mut Parser<'i, '_>) -> Result<Self, crate::error::ParseError<'i>> {
                let loc = parser.current_source_location();

                if parser
                    .try_parse(|p| p.expect_ident_matching("accumulate"))
                    .is_ok()
                {
                    return Ok(EnableBackground::Accumulate);
                }

                if parser.try_parse(|p| p.expect_ident_matching("new")).is_ok() {
                    parser.try_parse(|p| -> Result<_, ParseError<'_>> {
                        let x = f64::parse(p)?;
                        let y = f64::parse(p)?;
                        let w = f64::parse(p)?;
                        let h = f64::parse(p)?;

                        Ok(EnableBackground::New(Some(Rect::new(x, y, x + w, y + h))))
                    }).or(Ok(EnableBackground::New(None)))
                } else {
                    Err(loc.new_custom_error(ValueErrorKind::parse_error("invalid syntax for 'enable-background' property")))
                }
            }
        }

    }
);

#[cfg(test)]
#[test]
fn parses_enable_background() {
    assert_eq!(
        EnableBackground::parse_str("accumulate").unwrap(),
        EnableBackground::Accumulate
    );

    assert_eq!(
        EnableBackground::parse_str("new").unwrap(),
        EnableBackground::New(None)
    );

    assert_eq!(
        EnableBackground::parse_str("new 1 2 3 4").unwrap(),
        EnableBackground::New(Some(Rect::new(1.0, 2.0, 4.0, 6.0)))
    );

    assert!(EnableBackground::parse_str("new foo").is_err());

    assert!(EnableBackground::parse_str("plonk").is_err());
}

make_property!(
    /// `fill` property.
    ///
    /// SVG1.1: <https://www.w3.org/TR/SVG11/painting.html#FillProperty>
    ///
    /// SVG2: <https://www.w3.org/TR/SVG2/painting.html#FillProperty>
    Fill,
    default: PaintServer::SolidColor(crate::color::Color::Rgba(
        crate::color::RGBA::new(0, 0, 0, 1.0)
    )),
    inherits_automatically: true,
    newtype_parse: PaintServer,
);

make_property!(
    /// `fill-opacity` property.
    ///
    /// SVG1.1: <https://www.w3.org/TR/SVG11/painting.html#FillOpacityProperty>
    ///
    /// SVG2: <https://www.w3.org/TR/SVG2/painting.html#FillOpacity>
    FillOpacity,
    default: UnitInterval(1.0),
    inherits_automatically: true,
    newtype_parse: UnitInterval,
);

make_property!(
    /// `fill-rule` property.
    ///
    /// SVG1.1: <https://www.w3.org/TR/SVG11/painting.html#FillRuleProperty>
    ///
    /// SVG2: <https://www.w3.org/TR/SVG2/painting.html#WindingRule>
    FillRule,
    default: NonZero,
    inherits_automatically: true,

    identifiers:
    "nonzero" => NonZero,
    "evenodd" => EvenOdd,
);

/// `filter` property.
///
/// SVG1.1: <https://www.w3.org/TR/SVG11/filters.html#FilterProperty>
///
/// Filter Effects 1: <https://www.w3.org/TR/filter-effects/#FilterProperty>
///
/// Note that in SVG2, the filters got offloaded to the [Filter Effects Module Level
/// 1](https://www.w3.org/TR/filter-effects/) specification.
#[derive(Debug, Clone, PartialEq)]
pub enum Filter {
    None,
    List(FilterValueList),
}

make_property!(
    Filter,
    default: Filter::None,
    inherits_automatically: false,
    parse_impl: {
        impl Parse for Filter {
            fn parse<'i>(parser: &mut Parser<'i, '_>) -> Result<Self, crate::error::ParseError<'i>> {

                if parser
                    .try_parse(|p| p.expect_ident_matching("none"))
                    .is_ok()
                {
                    return Ok(Filter::None);
                }

                Ok(Filter::List(FilterValueList::parse(parser)?))
            }
        }
    }
);

make_property!(
    /// `flood-color` property, for `feFlood` and `feDropShadow` filter elements.
    ///
    /// SVG1.1: <https://www.w3.org/TR/SVG11/filters.html#feFloodElement>
    ///
    /// Filter Effects 1: <https://www.w3.org/TR/filter-effects/#FloodColorProperty>
    FloodColor,
    default: crate::color::Color::Rgba(crate::color::RGBA::new(0, 0, 0, 1.0)),
    inherits_automatically: false,
    newtype_parse: crate::color::Color,
);

make_property!(
    /// `flood-opacity` property, for `feFlood` and `feDropShadow` filter elements.
    ///
    /// SVG1.1: <https://www.w3.org/TR/SVG11/filters.html#feFloodElement>
    ///
    /// Filter Effects 1: <https://www.w3.org/TR/filter-effects/#FloodOpacityProperty>
    FloodOpacity,
    default: UnitInterval(1.0),
    inherits_automatically: false,
    newtype_parse: UnitInterval,
);

make_property!(
    // docs are in font_props.rs
    Font,
    default: Font::Spec(Default::default()),
    inherits_automatically: true,
);

make_property!(
    // docs are in font_props.rs
    FontFamily,
    default: FontFamily("Times New Roman".to_string()),
    inherits_automatically: true,
);

make_property!(
    // docs are in font_props.rs
    FontSize,
    default: FontSize::Value(Length::<Both>::new(12.0, LengthUnit::Px)),
    property_impl: {
        impl Property for FontSize {
            fn inherits_automatically() -> bool {
                true
            }

            fn compute(&self, v: &ComputedValues) -> Self {
                self.compute(v)
            }
        }
    }
);

make_property!(
    /// `font-stretch` property.
    ///
    /// SVG1.1: <https://www.w3.org/TR/SVG11/text.html#FontStretchProperty>
    ///
    /// CSS Fonts 3: <https://www.w3.org/TR/css-fonts-3/#font-size-propstret>
    FontStretch,
    default: Normal,
    inherits_automatically: true,

    identifiers:
    "normal" => Normal,
    "wider" => Wider,
    "narrower" => Narrower,
    "ultra-condensed" => UltraCondensed,
    "extra-condensed" => ExtraCondensed,
    "condensed" => Condensed,
    "semi-condensed" => SemiCondensed,
    "semi-expanded" => SemiExpanded,
    "expanded" => Expanded,
    "extra-expanded" => ExtraExpanded,
    "ultra-expanded" => UltraExpanded,
);

make_property!(
    /// `font-style` property.
    ///
    /// SVG1.1: <https://www.w3.org/TR/SVG11/text.html#FontStyleProperty>
    ///
    /// CSS Fonts 3: <https://www.w3.org/TR/css-fonts-3/#font-size-propstret>
    FontStyle,
    default: Normal,
    inherits_automatically: true,

    identifiers:
    "normal" => Normal,
    "italic" => Italic,
    "oblique" => Oblique,
);

make_property!(
    /// `font-variant` property.
    ///
    /// SVG1.1: <https://www.w3.org/TR/SVG11/text.html#FontVariantProperty>
    ///
    /// CSS Fonts 3: <https://www.w3.org/TR/css-fonts-3/#propdef-font-variant>
    ///
    /// Note that in CSS3, this is a lot more complex than CSS2.1 / SVG1.1.
    FontVariant,
    default: Normal,
    inherits_automatically: true,

    identifiers:
    "normal" => Normal,
    "small-caps" => SmallCaps,
);

make_property!(
    // docs are in font_props.rs
    FontWeight,
    default: FontWeight::Normal,
    property_impl: {
        impl Property for FontWeight {
            fn inherits_automatically() -> bool {
                true
            }

            fn compute(&self, v: &ComputedValues) -> Self {
                self.compute(&v.font_weight())
            }
        }
    }
);

make_property!(
    // docs are in font_props.rs
    //
    // Although https://www.w3.org/TR/css-writing-modes-3/#propdef-glyph-orientation-vertical specifies
    // "n/a" for both the initial value (default) and inheritance, we'll use Auto here for the default,
    // since it translates to TextOrientation::Mixed - which is text-orientation's initial value.
    GlyphOrientationVertical,
    default: GlyphOrientationVertical::Auto,
    inherits_automatically: false,
);

make_property!(
    /// `height` property.
    ///
    /// SVG2: <https://www.w3.org/TR/SVG2/geometry.html#Sizing>
    ///
    /// Note that in SVG1.1, this was an attribute, not a property.
    Height,
    default: LengthOrAuto::<Vertical>::Auto,
    inherits_automatically: false,
    newtype_parse: LengthOrAuto<Vertical>,
);

make_property!(
    /// `image-rendering` property.
    ///
    /// CSS Images Module Level 3: <https://www.w3.org/TR/css-images-3/#the-image-rendering>
    ///
    /// Note that this property previously accepted the values optimizeSpeed and optimizeQuality.
    /// These are now deprecated; a user agent must accept them as valid values but must treat
    /// them as having the same behavior as crisp-edges and smooth respectively.
    ImageRendering,
    default: Auto,
    inherits_automatically: true,

    identifiers:
    "auto" => Auto,
    "smooth" => Smooth,
    "optimizeQuality" => OptimizeQuality,
    "high-quality" => HighQuality,
    "crisp-edges" => CrispEdges,
    "optimizeSpeed" => OptimizeSpeed,
    "pixelated" => Pixelated,
);

make_property!(
    /// `isolation` property.
    ///
    /// CSS Compositing and Blending 1: <https://www.w3.org/TR/compositing-1/#isolation>
    Isolation,
    default: Auto,
    inherits_automatically: false,

    identifiers:
    "auto" => Auto,
    "isolate" => Isolate,
);

make_property!(
    // docs are in font_props.rs
    LetterSpacing,
    default: LetterSpacing::Normal,
    property_impl: {
        impl Property for LetterSpacing {
            fn inherits_automatically() -> bool {
                true
            }

            fn compute(&self, _v: &ComputedValues) -> Self {
                self.compute()
            }
        }
    }
);

make_property!(
    // docs are in font_props.rs
    LineHeight,
    default: LineHeight::Normal,
    inherits_automatically: true,
);

make_property!(
    /// `lighting-color` property for `feDiffuseLighting` and `feSpecularLighting` filter elements.
    ///
    /// SVG1.1: <https://www.w3.org/TR/SVG11/filters.html#LightingColorProperty>
    ///
    /// Filter Effects 1: <https://www.w3.org/TR/filter-effects/#LightingColorProperty>
    LightingColor,
    default: crate::color::Color::Rgba(crate::color::RGBA::new(255, 255, 255, 1.0)),
    inherits_automatically: false,
    newtype_parse: crate::color::Color,
);

make_property!(
    /// `marker` shorthand property.
    ///
    /// SVG2: <https://www.w3.org/TR/SVG2/painting.html#MarkerShorthand>
    ///
    /// This is a shorthand, which expands to the `marker-start`, `marker-mid`,
    /// `marker-end` longhand properties.
    Marker,
    default: Iri::None,
    inherits_automatically: true,
    newtype_parse: Iri,
);

make_property!(
    /// `marker-end` property.
    ///
    /// SVG2: <https://www.w3.org/TR/SVG2/painting.html#VertexMarkerProperties>
    MarkerEnd,
    default: Iri::None,
    inherits_automatically: true,
    newtype_parse: Iri,
);

make_property!(
    /// `marker-mid` property.
    ///
    /// SVG2: <https://www.w3.org/TR/SVG2/painting.html#VertexMarkerProperties>
    MarkerMid,
    default: Iri::None,
    inherits_automatically: true,
    newtype_parse: Iri,
);

make_property!(
    /// `marker-start` property.
    ///
    /// SVG2: <https://www.w3.org/TR/SVG2/painting.html#VertexMarkerProperties>
    MarkerStart,
    default: Iri::None,
    inherits_automatically: true,
    newtype_parse: Iri,
);

make_property!(
    /// `mask` shorthand property.
    ///
    /// SVG1.1: <https://www.w3.org/TR/SVG11/masking.html#MaskProperty>
    ///
    /// CSS Masking 1: <https://www.w3.org/TR/css-masking-1/#the-mask>
    ///
    /// Note that librsvg implements SVG1.1 semantics, where this is not a shorthand.
    Mask,
    default: Iri::None,
    inherits_automatically: false,
    newtype_parse: Iri,
);

make_property!(
    /// `mask-type` property.
    ///
    /// CSS Masking 1: <https://www.w3.org/TR/css-masking-1/#the-mask-type>
    MaskType,
    default: Luminance,
    inherits_automatically: false,

    identifiers:
    "luminance" => Luminance,
    "alpha" => Alpha,
);

make_property!(
    /// `mix-blend-mode` property.
    ///
    /// Compositing and Blending 1: <https://www.w3.org/TR/compositing/#mix-blend-mode>
    MixBlendMode,
    default: Normal,
    inherits_automatically: false,

    identifiers:
    "normal" => Normal,
    "multiply" => Multiply,
    "screen" => Screen,
    "overlay" => Overlay,
    "darken" => Darken,
    "lighten" => Lighten,
    "color-dodge" => ColorDodge,
    "color-burn" => ColorBurn,
    "hard-light" => HardLight,
    "soft-light" => SoftLight,
    "difference" => Difference,
    "exclusion" => Exclusion,
    "hue" => Hue,
    "saturation" => Saturation,
    "color" => Color,
    "luminosity" => Luminosity,
);

make_property!(
    /// `opacity` property.
    ///
    /// CSS Color 3: <https://www.w3.org/TR/css-color-3/#opacity>
    Opacity,
    default: UnitInterval(1.0),
    inherits_automatically: false,
    newtype_parse: UnitInterval,
);

make_property!(
    /// `overflow` shorthand property.
    ///
    /// CSS2: <https://www.w3.org/TR/CSS2/visufx.html#overflow>
    ///
    /// CSS Overflow 3: <https://www.w3.org/TR/css-overflow-3/#propdef-overflow>
    ///
    /// Note that librsvg implements SVG1.1 semantics, where this is not a shorthand.
    Overflow,
    default: Visible,
    inherits_automatically: false,

    identifiers:
    "visible" => Visible,
    "hidden" => Hidden,
    "scroll" => Scroll,
    "auto" => Auto,
);

impl Overflow {
    pub fn overflow_allowed(&self) -> bool {
        matches!(*self, Overflow::Auto | Overflow::Visible)
    }
}

/// One of the three operations for the `paint-order` property; see [`PaintOrder`].
#[repr(u8)]
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum PaintTarget {
    Fill,
    Stroke,
    Markers,
}

make_property!(
    /// `paint-order` property.
    ///
    /// SVG2: <https://www.w3.org/TR/SVG2/painting.html#PaintOrder>
    ///
    /// The `targets` field specifies the order in which graphic elements should be filled/stroked.
    /// Instead of hard-coding an order of fill/stroke/markers, use the order specified by the `targets`.
    PaintOrder,
    inherits_automatically: true,
    fields: {
        targets: [PaintTarget; 3], default: [PaintTarget::Fill, PaintTarget::Stroke, PaintTarget::Markers],
    }

    parse_impl: {
        impl Parse for PaintOrder {
            fn parse<'i>(parser: &mut Parser<'i, '_>) -> Result<PaintOrder, ParseError<'i>> {
                let allowed_targets = 3;
                let mut targets = Vec::with_capacity(allowed_targets);

                if parser.try_parse(|p| p.expect_ident_matching("normal")).is_ok() {
                    return Ok(PaintOrder::default());
                }

                while !parser.is_exhausted() {
                    let loc = parser.current_source_location();
                    let token = parser.next()?;

                    let value = match token {
                        Token::Ident(cow) if cow.eq_ignore_ascii_case("fill") && !targets.contains(&PaintTarget::Fill) => PaintTarget::Fill,
                        Token::Ident(cow) if cow.eq_ignore_ascii_case("stroke") && !targets.contains(&PaintTarget::Stroke) => PaintTarget::Stroke,
                        Token::Ident(cow) if cow.eq_ignore_ascii_case("markers") && !targets.contains(&PaintTarget::Markers) => PaintTarget::Markers,
                        _ => return Err(loc.new_basic_unexpected_token_error(token.clone()).into()),
                    };

                    targets.push(value);
                };

                // any values which were not specfied should be painted in default order
                // (fill, stroke, markers) following the values which were explicitly specified.
                for &target in &[PaintTarget::Fill, PaintTarget::Stroke, PaintTarget::Markers] {
                    if !targets.contains(&target) {
                        targets.push(target);
                    }
                }
                Ok(PaintOrder {
                    targets: targets[..].try_into().expect("Incorrect number of targets in paint-order")
                })
            }
        }
    }
);

#[cfg(test)]
#[test]
fn parses_paint_order() {
    assert_eq!(
        PaintOrder::parse_str("normal").unwrap(),
        PaintOrder {
            targets: [PaintTarget::Fill, PaintTarget::Stroke, PaintTarget::Markers]
        }
    );

    assert_eq!(
        PaintOrder::parse_str("markers fill").unwrap(),
        PaintOrder {
            targets: [PaintTarget::Markers, PaintTarget::Fill, PaintTarget::Stroke]
        }
    );

    assert_eq!(
        PaintOrder::parse_str("stroke").unwrap(),
        PaintOrder {
            targets: [PaintTarget::Stroke, PaintTarget::Fill, PaintTarget::Markers]
        }
    );

    assert!(PaintOrder::parse_str("stroke stroke").is_err());
    assert!(PaintOrder::parse_str("markers stroke fill hello").is_err());
}

make_property!(
    /// `r` property.
    ///
    /// SVG2: <https://www.w3.org/TR/SVG2/geometry.html#R>
    ///
    /// Note that in SVG1.1, this was an attribute, not a property.
    R,
    default: ULength::<Both>::default(),
    inherits_automatically: false,
    newtype_parse: ULength<Both>,
);

make_property!(
    /// `rx` property.
    ///
    /// SVG2: <https://www.w3.org/TR/SVG2/geometry.html#RX>
    ///
    /// Note that in SVG1.1, this was an attribute, not a property.
    RX,
    default: LengthOrAuto::<Horizontal>::Auto,
    inherits_automatically: false,
    newtype_parse: LengthOrAuto<Horizontal>,
);

make_property!(
    /// `ry` property.
    ///
    /// SVG2: <https://www.w3.org/TR/SVG2/geometry.html#RY>
    ///
    /// Note that in SVG1.1, this was an attribute, not a property.
    RY,
    default: LengthOrAuto::<Vertical>::Auto,
    inherits_automatically: false,
    newtype_parse: LengthOrAuto<Vertical>,
);

make_property!(
    /// `shape-rendering` property.
    ///
    /// SVG2: <https://www.w3.org/TR/SVG2/painting.html#ShapeRendering>
    ShapeRendering,
    default: Auto,
    inherits_automatically: true,

    identifiers:
    "auto" => Auto,
    "optimizeSpeed" => OptimizeSpeed,
    "geometricPrecision" => GeometricPrecision,
    "crispEdges" => CrispEdges,
);

make_property!(
    /// `stop-color` property for gradient stops.
    ///
    /// SVG2: <https://www.w3.org/TR/SVG2/pservers.html#StopColorProperty>
    StopColor,
    default: crate::color::Color::Rgba(crate::color::RGBA::new(0, 0, 0, 1.0)),
    inherits_automatically: false,
    newtype_parse: crate::color::Color,
);

make_property!(
    /// `stop-opacity` property for gradient stops.
    ///
    /// SVG2: <https://www.w3.org/TR/SVG2/pservers.html#StopOpacityProperty>
    StopOpacity,
    default: UnitInterval(1.0),
    inherits_automatically: false,
    newtype_parse: UnitInterval,
);

make_property!(
    /// `stroke` property.
    ///
    /// SVG2: <https://www.w3.org/TR/SVG2/painting.html#SpecifyingStrokePaint>
    Stroke,
    default: PaintServer::None,
    inherits_automatically: true,
    newtype_parse: PaintServer,
);

make_property!(
    /// `stroke-dasharray` property.
    ///
    /// SVG2: <https://www.w3.org/TR/SVG2/painting.html#StrokeDashing>
    StrokeDasharray,
    default: Dasharray::default(),
    inherits_automatically: true,
    newtype_parse: Dasharray,
);

make_property!(
    /// `stroke-dashoffset` property.
    ///
    /// SVG2: <https://www.w3.org/TR/SVG2/painting.html#StrokeDashingdas>
    StrokeDashoffset,
    default: Length::<Both>::default(),
    inherits_automatically: true,
    newtype_parse: Length<Both>,
);

make_property!(
    /// `stroke-linecap` property.
    ///
    /// SVG2: <https://www.w3.org/TR/SVG2/painting.html#LineCaps>
    StrokeLinecap,
    default: Butt,
    inherits_automatically: true,

    identifiers:
    "butt" => Butt,
    "round" => Round,
    "square" => Square,
);

make_property!(
    /// `stroke-linejoin` property.
    ///
    /// SVG2: <https://www.w3.org/TR/SVG2/painting.html#LineJoin>
    StrokeLinejoin,
    default: Miter,
    inherits_automatically: true,

    identifiers:
    "miter" => Miter,
    "round" => Round,
    "bevel" => Bevel,
);

make_property!(
    /// `stroke-miterlimit` property.
    ///
    /// SVG2: <https://www.w3.org/TR/SVG2/painting.html#StrokeMiterlimitProperty>
    StrokeMiterlimit,
    default: 4f64,
    inherits_automatically: true,
    newtype_parse: f64,
);

make_property!(
    /// `stroke-opacity` property.
    ///
    /// SVG2: <https://www.w3.org/TR/SVG2/painting.html#StrokeOpacity>
    StrokeOpacity,
    default: UnitInterval(1.0),
    inherits_automatically: true,
    newtype_parse: UnitInterval,
);

make_property!(
    /// `stroke-width` property.
    ///
    /// SVG2: <https://www.w3.org/TR/SVG2/painting.html#StrokeWidth>
    StrokeWidth,
    default: Length::<Both>::new(1.0, LengthUnit::Px),
    inherits_automatically: true,
    newtype_parse: Length::<Both>,
);

make_property!(
    /// `text-anchor` property.
    ///
    /// SVG1.1: <https://www.w3.org/TR/SVG11/text.html#TextAnchorProperty>
    TextAnchor,
    default: Start,
    inherits_automatically: true,

    identifiers:
    "start" => Start,
    "middle" => Middle,
    "end" => End,
);

make_property!(
    /// `text-decoration` shorthand property.
    ///
    /// SVG1.1: <https://www.w3.org/TR/SVG11/text.html#TextDecorationProperty>
    ///
    /// CSS Text Decoration 3: <https://www.w3.org/TR/css-text-decor-3/#text-decoration-property>
    ///
    /// Note that librsvg implements SVG1.1 semantics, where this is not a shorthand.
    TextDecoration,
    inherits_automatically: false,

    fields: {
        overline: bool, default: false,
        underline: bool, default: false,
        strike: bool, default: false,
    }

    parse_impl: {
        impl Parse for TextDecoration {
            fn parse<'i>(parser: &mut Parser<'i, '_>) -> Result<TextDecoration, ParseError<'i>> {
                let mut overline = false;
                let mut underline = false;
                let mut strike = false;

                if parser.try_parse(|p| p.expect_ident_matching("none")).is_ok() {
                    return Ok(TextDecoration::default());
                }

                while !parser.is_exhausted() {
                    let loc = parser.current_source_location();
                    let token = parser.next()?;

                    match token {
                        Token::Ident(cow) if cow.eq_ignore_ascii_case("overline") => overline = true,
                        Token::Ident(cow) if cow.eq_ignore_ascii_case("underline") => underline = true,
                        Token::Ident(cow) if cow.eq_ignore_ascii_case("line-through") => strike = true,
                        _ => return Err(loc.new_basic_unexpected_token_error(token.clone()).into()),
                    }
                }

                Ok(TextDecoration {
                    overline,
                    underline,
                    strike,
                })
            }
        }
    }
);

#[cfg(test)]
#[test]
fn parses_text_decoration() {
    assert_eq!(
        TextDecoration::parse_str("none").unwrap(),
        TextDecoration {
            overline: false,
            underline: false,
            strike: false,
        }
    );

    assert_eq!(
        TextDecoration::parse_str("overline").unwrap(),
        TextDecoration {
            overline: true,
            underline: false,
            strike: false,
        }
    );

    assert_eq!(
        TextDecoration::parse_str("underline").unwrap(),
        TextDecoration {
            overline: false,
            underline: true,
            strike: false,
        }
    );

    assert_eq!(
        TextDecoration::parse_str("line-through").unwrap(),
        TextDecoration {
            overline: false,
            underline: false,
            strike: true,
        }
    );

    assert_eq!(
        TextDecoration::parse_str("underline overline").unwrap(),
        TextDecoration {
            overline: true,
            underline: true,
            strike: false,
        }
    );

    assert!(TextDecoration::parse_str("airline").is_err())
}

make_property!(
    /// `text-orientation` property.
    ///
    /// CSS Writing Modes 3: <https://www.w3.org/TR/css-writing-modes-3/#propdef-text-orientation>
    TextOrientation,
    default: Mixed,
    inherits_automatically: true,

    identifiers:
    "mixed" => Mixed,
    "upright" => Upright,
    "sideways" => Sideways,
);

impl From<GlyphOrientationVertical> for TextOrientation {
    /// Converts the `glyph-orientation-vertical` shorthand to a `text-orientation` longhand.
    ///
    /// See <https://www.w3.org/TR/css-writing-modes-3/#propdef-glyph-orientation-vertical> for the conversion table.
    fn from(o: GlyphOrientationVertical) -> TextOrientation {
        match o {
            GlyphOrientationVertical::Auto => TextOrientation::Mixed,
            GlyphOrientationVertical::Angle0 => TextOrientation::Upright,
            GlyphOrientationVertical::Angle90 => TextOrientation::Sideways,
        }
    }
}

make_property!(
    /// `text-rendering` property.
    ///
    /// SVG1.1: <https://www.w3.org/TR/SVG11/painting.html#TextRenderingProperty>
    TextRendering,
    default: Auto,
    inherits_automatically: true,

    identifiers:
    "auto" => Auto,
    "optimizeSpeed" => OptimizeSpeed,
    "optimizeLegibility" => OptimizeLegibility,
    "geometricPrecision" => GeometricPrecision,
);

// FIXME: this is marked as allow(unused) because this `Transform` newtype is never
// constructed.  Instead, properties.rs uses `TransformProperty` directly to handle the
// distinction between the "transform" attribute from SVG1.1, and the "transform" CSS
// property in SVG2.
//
// I think we can play a bit with the naming or namespacing of things to make this
// `Transform` newtype actually work for the machinery in properties.rs - mainly being
// careful about properties::Transform (which is a re-export of property_defs::Transform)
// versus transform::Transform (which is just an affine).
make_property!(
    /// `transform` property.
    ///
    /// CSS Transforms 1: <https://www.w3.org/TR/css-transforms-1/#transform-property>
    #[allow(unused)]
    Transform,
    default: TransformProperty::None,
    inherits_automatically: false,
    newtype_parse: TransformProperty,
);

make_property!(
    /// `unicode-bidi` property.
    ///
    /// CSS Writing Modes 3: <https://www.w3.org/TR/css-writing-modes-3/#unicode-bidi>
    UnicodeBidi,
    default: Normal,
    inherits_automatically: false,

    identifiers:
    "normal" => Normal,
    "embed" => Embed,
    "isolate" => Isolate,
    "bidi-override" => BidiOverride,
    "isolate-override" => IsolateOverride,
    "plaintext" => Plaintext,
);

make_property!(
    /// `vector-effect` property.
    ///
    /// SVG2: <https://svgwg.org/svg2-draft/coords.html#VectorEffectProperty>
    VectorEffect,
    default: None,
    inherits_automatically: false,

    identifiers:
    "none" => None,
    "non-scaling-stroke" => NonScalingStroke,
    // non-scaling-size, non-rotation, fixed-position not implemented
);

make_property!(
    /// `visibility` property.
    ///
    /// CSS2: <https://www.w3.org/TR/CSS2/visufx.html#visibility>
    Visibility,
    default: Visible,
    inherits_automatically: true,

    identifiers:
    "visible" => Visible,
    "hidden" => Hidden,
    "collapse" => Collapse,
);

make_property!(
    /// `width` property.
    ///
    /// SVG2: <https://www.w3.org/TR/SVG2/geometry.html#Sizing>
    ///
    /// Note that in SVG1.1, this was an attribute, not a property.
    Width,
    default: LengthOrAuto::<Horizontal>::Auto,
    inherits_automatically: false,
    newtype_parse: LengthOrAuto<Horizontal>,
);

make_property!(
    /// `writing-mode` property.
    ///
    /// SVG1.1: <https://www.w3.org/TR/SVG11/text.html#WritingModeProperty>
    ///
    /// SVG2: <https://svgwg.org/svg2-draft/text.html#WritingModeProperty>
    ///
    /// CSS Writing Modes 3: <https://www.w3.org/TR/css-writing-modes-3/#block-flow>
    ///
    /// See the comments in the SVG2 spec for how the SVG1.1 values must be translated
    /// into CSS Writing Modes 3 values.
    WritingMode,
    default: HorizontalTb,
    identifiers: {
        "horizontal-tb" => HorizontalTb,
        "vertical-rl" => VerticalRl,
        "vertical-lr" => VerticalLr,
        "lr" => Lr,
        "lr-tb" => LrTb,
        "rl" => Rl,
        "rl-tb" => RlTb,
        "tb" => Tb,
        "tb-rl" => TbRl,
    },
    property_impl: {
        impl Property for WritingMode {
            fn inherits_automatically() -> bool {
                true
            }

            fn compute(&self, _v: &ComputedValues) -> Self {
                use WritingMode::*;

                // Translate SVG1.1 compatibility values to SVG2 / CSS Writing Modes 3.
                match *self {
                    Lr | LrTb | Rl | RlTb => HorizontalTb,
                    Tb | TbRl => VerticalRl,
                    _ => *self,
                }
            }
        }
    }
);

impl WritingMode {
    pub fn is_horizontal(self) -> bool {
        use WritingMode::*;

        matches!(self, HorizontalTb | Lr | LrTb | Rl | RlTb)
    }
}

make_property!(
    /// `x` property.
    ///
    /// SVG2: <https://www.w3.org/TR/SVG2/geometry.html#X>
    ///
    /// Note that in SVG1.1, this was an attribute, not a property.
    X,
    default: Length::<Horizontal>::default(),
    inherits_automatically: false,
    newtype_parse: Length<Horizontal>,
);

make_property!(
    /// `xml:lang` attribute.
    ///
    /// XML1.0: <https://www.w3.org/TR/xml/#sec-lang-tag>
    ///
    /// Similar to `XmlSpace`, this is a hack in librsvg: the `xml:lang` attribute is
    /// supposed to apply to an element and all its children.  This more or less matches
    /// CSS property inheritance, so librsvg reuses the machinery for property inheritance
    /// to propagate down the value of the `xml:lang` attribute to an element's children.
    XmlLang,
    default: None,
    inherits_automatically: true,
    newtype: Option<Box<LanguageTag>>,
    parse_impl: {
        impl Parse for XmlLang {
            fn parse<'i>(
                parser: &mut Parser<'i, '_>,
            ) -> Result<XmlLang, ParseError<'i>> {
                let language_tag = parser.expect_ident()?;
                let language_tag = LanguageTag::from_str(language_tag).map_err(|_| {
                    parser.new_custom_error(ValueErrorKind::parse_error("invalid syntax for 'xml:lang' parameter"))
                })?;
                Ok(XmlLang(Some(Box::new(language_tag))))
            }
        }
    },
);

#[cfg(test)]
#[test]
fn parses_xml_lang() {
    assert_eq!(
        XmlLang::parse_str("es-MX").unwrap(),
        XmlLang(Some(Box::new(LanguageTag::from_str("es-MX").unwrap())))
    );

    assert!(XmlLang::parse_str("").is_err());
}

make_property!(
    /// `xml:space` attribute.
    ///
    /// XML1.0: <https://www.w3.org/TR/xml/#sec-white-space>
    ///
    /// Similar to `XmlLang`, this is a hack in librsvg.  The `xml:space` attribute is
    /// supposed to be applied to all the children of the element in which it appears, so
    /// it works more or less the same as CSS property inheritance.  Librsvg reuses the
    /// machinery for CSS property inheritance to propagate down the value of `xml:space`
    /// to an element's children.
    XmlSpace,
    default: Default,
    inherits_automatically: true,

    identifiers:
    "default" => Default,
    "preserve" => Preserve,
);

make_property!(
    /// `y` property.
    ///
    /// SVG2: <https://www.w3.org/TR/SVG2/geometry.html#Y>
    ///
    /// Note that in SVG1.1, this was an attribute, not a property.
    Y,
    default: Length::<Vertical>::default(),
    inherits_automatically: false,
    newtype_parse: Length<Vertical>,
);

make_property!(
    /// `whitespace` properties
    ///
    /// <https://www.w3.org/TR/css-text-3/#white-space-property>
    WhiteSpace,

    default: Normal,
    inherits_automatically: true,

    identifiers:
    "normal" => Normal,
    "pre" => Pre,
    "nowrap" => NoWrap,
    "pre-wrap" => PreWrap,
    "break-spaces" => BreakSpaces,
    "pre-line" => PreLine,
);
