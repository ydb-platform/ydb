//! Handling of transform values.
//!
//! This module contains the following:
//!
//! * [`Transform`] to represent 2D transforms in general; it's just a matrix.
//!
//! * [`TransformProperty`] for the [`transform` property][prop] in SVG2/CSS3.
//!
//! * [`Transform`] also handles the [`transform` attribute][attr] in SVG1.1, which has a different
//!   grammar than the `transform` property from SVG2.
//!
//! [prop]: https://www.w3.org/TR/css-transforms-1/#transform-property
//! [attr]: https://www.w3.org/TR/SVG11/coords.html#TransformAttribute

use cssparser::{Parser, Token};
use std::ops::Deref;

use crate::angle::Angle;
use crate::error::*;
use crate::length::*;
use crate::parsers::{Parse, optional_comma};
use crate::properties::ComputedValues;
use crate::property_macros::Property;
use crate::rect::Rect;

/// A transform that has been checked to be invertible.
///
/// We need to validate user-supplied transforms before setting them on Cairo,
/// so we use this type for that.
#[derive(Debug, Default, Copy, Clone, PartialEq)]
pub struct ValidTransform(Transform);

impl TryFrom<Transform> for ValidTransform {
    type Error = InvalidTransform;

    /// Validates a [`Transform`] before converting it to a [`ValidTransform`].
    ///
    /// A transform is valid if it is invertible.  For example, a
    /// matrix with all-zeros is not invertible, and it is invalid.
    fn try_from(t: Transform) -> Result<ValidTransform, InvalidTransform> {
        if t.is_invertible() {
            Ok(ValidTransform(t))
        } else {
            Err(InvalidTransform)
        }
    }
}

impl Deref for ValidTransform {
    type Target = Transform;

    fn deref(&self) -> &Transform {
        &self.0
    }
}

/// A 2D transformation matrix.
#[derive(Debug, Copy, Clone, PartialEq)]
pub struct Transform {
    pub xx: f64,
    pub yx: f64,
    pub xy: f64,
    pub yy: f64,
    pub x0: f64,
    pub y0: f64,
}

/// The `transform` property from the CSS Transforms Module Level 1.
///
/// CSS Transforms 1: <https://www.w3.org/TR/css-transforms-1/#transform-property>
#[derive(Debug, Default, Clone, PartialEq)]
pub enum TransformProperty {
    #[default]
    None,
    List(Vec<TransformFunction>),
}

/// The `transform` attribute from SVG1.1
///
/// SVG1.1: <https://www.w3.org/TR/SVG11/coords.html#TransformAttribute>
#[derive(Copy, Clone, Default, Debug, PartialEq)]
pub struct TransformAttribute(Transform);

impl Property for TransformProperty {
    fn inherits_automatically() -> bool {
        false
    }

    fn compute(&self, _v: &ComputedValues) -> Self {
        self.clone()
    }
}

impl TransformProperty {
    pub fn to_transform(&self) -> Transform {
        // From the spec (https://www.w3.org/TR/css-transforms-1/#current-transformation-matrix):
        // Start with the identity matrix.
        // TODO: implement (#685) - Translate by the computed X and Y of transform-origin
        // Multiply by each of the transform functions in transform property from left to right
        // TODO: implement - Translate by the negated computed X and Y values of transform-origin

        match self {
            TransformProperty::None => Transform::identity(),

            TransformProperty::List(l) => {
                let mut final_transform = Transform::identity();

                for f in l.iter() {
                    use TransformFunction::*;

                    let transform_matrix = match f {
                        Matrix(trans_matrix) => *trans_matrix,
                        Translate(h, v) => Transform::new_translate(h.length, v.length),
                        TranslateX(h) => Transform::new_translate(h.length, 0.0),
                        TranslateY(v) => Transform::new_translate(0.0, v.length),
                        Scale(x, y) => Transform::new_scale(*x, *y),
                        ScaleX(x) => Transform::new_scale(*x, 1.0),
                        ScaleY(y) => Transform::new_scale(1.0, *y),
                        Rotate(a) => Transform::new_rotate(*a),
                        Skew(ax, ay) => Transform::new_skew(*ax, *ay),
                        SkewX(ax) => Transform::new_skew(*ax, Angle::new(0.0)),
                        SkewY(ay) => Transform::new_skew(Angle::new(0.0), *ay),
                    };
                    final_transform = transform_matrix.post_transform(&final_transform);
                }

                final_transform
            }
        }
    }
}

// https://www.w3.org/TR/css-transforms-1/#typedef-transform-function
#[derive(Debug, Clone, PartialEq)]
pub enum TransformFunction {
    Matrix(Transform),
    Translate(Length<Horizontal>, Length<Vertical>),
    TranslateX(Length<Horizontal>),
    TranslateY(Length<Vertical>),
    Scale(f64, f64),
    ScaleX(f64),
    ScaleY(f64),
    Rotate(Angle),
    Skew(Angle, Angle),
    SkewX(Angle),
    SkewY(Angle),
}

impl Parse for TransformProperty {
    fn parse<'i>(parser: &mut Parser<'i, '_>) -> Result<TransformProperty, ParseError<'i>> {
        if parser
            .try_parse(|p| p.expect_ident_matching("none"))
            .is_ok()
        {
            Ok(TransformProperty::None)
        } else {
            let t = parse_transform_prop_function_list(parser)?;

            Ok(TransformProperty::List(t))
        }
    }
}

fn parse_transform_prop_function_list<'i>(
    parser: &mut Parser<'i, '_>,
) -> Result<Vec<TransformFunction>, ParseError<'i>> {
    let mut v = Vec::<TransformFunction>::new();

    loop {
        v.push(parse_transform_prop_function_command(parser)?);

        if parser.is_exhausted() {
            break;
        }
    }

    Ok(v)
}

fn parse_transform_prop_function_command<'i>(
    parser: &mut Parser<'i, '_>,
) -> Result<TransformFunction, ParseError<'i>> {
    let loc = parser.current_source_location();

    match parser.next()?.clone() {
        Token::Function(ref name) => parse_transform_prop_function_internal(name, parser),
        tok => Err(loc.new_unexpected_token_error(tok.clone())),
    }
}

fn parse_transform_prop_function_internal<'i>(
    name: &str,
    parser: &mut Parser<'i, '_>,
) -> Result<TransformFunction, ParseError<'i>> {
    let loc = parser.current_source_location();

    match name {
        "matrix" => parse_prop_matrix_args(parser),
        "translate" => parse_prop_translate_args(parser),
        "translateX" => parse_prop_translate_x_args(parser),
        "translateY" => parse_prop_translate_y_args(parser),
        "scale" => parse_prop_scale_args(parser),
        "scaleX" => parse_prop_scale_x_args(parser),
        "scaleY" => parse_prop_scale_y_args(parser),
        "rotate" => parse_prop_rotate_args(parser),
        "skew" => parse_prop_skew_args(parser),
        "skewX" => parse_prop_skew_x_args(parser),
        "skewY" => parse_prop_skew_y_args(parser),
        _ => Err(loc.new_custom_error(ValueErrorKind::parse_error(
            "expected matrix|translate|translateX|translateY|scale|scaleX|scaleY|rotate|skewX|skewY",
        ))),
    }
}

fn parse_prop_matrix_args<'i>(
    parser: &mut Parser<'i, '_>,
) -> Result<TransformFunction, ParseError<'i>> {
    parser.parse_nested_block(|p| {
        let xx = f64::parse(p)?;
        p.expect_comma()?;
        let yx = f64::parse(p)?;
        p.expect_comma()?;
        let xy = f64::parse(p)?;
        p.expect_comma()?;
        let yy = f64::parse(p)?;
        p.expect_comma()?;
        let x0 = f64::parse(p)?;
        p.expect_comma()?;
        let y0 = f64::parse(p)?;

        Ok(TransformFunction::Matrix(Transform::new_unchecked(
            xx, yx, xy, yy, x0, y0,
        )))
    })
}

fn length_is_in_pixels<N: Normalize>(l: &Length<N>) -> bool {
    l.unit == LengthUnit::Px
}

fn only_pixels_error<'i>(loc: cssparser::SourceLocation) -> ParseError<'i> {
    loc.new_custom_error(ValueErrorKind::parse_error(
        "only translations in pixels are supported for now",
    ))
}

fn parse_prop_translate_args<'i>(
    parser: &mut Parser<'i, '_>,
) -> Result<TransformFunction, ParseError<'i>> {
    parser.parse_nested_block(|p| {
        let loc = p.current_source_location();

        let tx: Length<Horizontal> = Length::parse(p)?;

        let ty: Length<Vertical> = if p.try_parse(|p| p.expect_comma()).is_ok() {
            Length::parse(p)?
        } else {
            Length::new(0.0, LengthUnit::Px)
        };

        if !(length_is_in_pixels(&tx) && length_is_in_pixels(&ty)) {
            return Err(only_pixels_error(loc));
        }

        Ok(TransformFunction::Translate(tx, ty))
    })
}

fn parse_prop_translate_x_args<'i>(
    parser: &mut Parser<'i, '_>,
) -> Result<TransformFunction, ParseError<'i>> {
    parser.parse_nested_block(|p| {
        let loc = p.current_source_location();

        let tx: Length<Horizontal> = Length::parse(p)?;

        if !length_is_in_pixels(&tx) {
            return Err(only_pixels_error(loc));
        }

        Ok(TransformFunction::TranslateX(tx))
    })
}

fn parse_prop_translate_y_args<'i>(
    parser: &mut Parser<'i, '_>,
) -> Result<TransformFunction, ParseError<'i>> {
    parser.parse_nested_block(|p| {
        let loc = p.current_source_location();

        let ty: Length<Vertical> = Length::parse(p)?;

        if !length_is_in_pixels(&ty) {
            return Err(only_pixels_error(loc));
        }

        Ok(TransformFunction::TranslateY(ty))
    })
}

fn parse_prop_scale_args<'i>(
    parser: &mut Parser<'i, '_>,
) -> Result<TransformFunction, ParseError<'i>> {
    parser.parse_nested_block(|p| {
        let x = f64::parse(p)?;

        let y = if p.try_parse(|p| p.expect_comma()).is_ok() {
            f64::parse(p)?
        } else {
            x
        };

        Ok(TransformFunction::Scale(x, y))
    })
}

fn parse_prop_scale_x_args<'i>(
    parser: &mut Parser<'i, '_>,
) -> Result<TransformFunction, ParseError<'i>> {
    parser.parse_nested_block(|p| {
        let x = f64::parse(p)?;

        Ok(TransformFunction::ScaleX(x))
    })
}

fn parse_prop_scale_y_args<'i>(
    parser: &mut Parser<'i, '_>,
) -> Result<TransformFunction, ParseError<'i>> {
    parser.parse_nested_block(|p| {
        let y = f64::parse(p)?;

        Ok(TransformFunction::ScaleY(y))
    })
}

fn parse_prop_rotate_args<'i>(
    parser: &mut Parser<'i, '_>,
) -> Result<TransformFunction, ParseError<'i>> {
    parser.parse_nested_block(|p| {
        let angle = Angle::parse(p)?;

        Ok(TransformFunction::Rotate(angle))
    })
}

fn parse_prop_skew_args<'i>(
    parser: &mut Parser<'i, '_>,
) -> Result<TransformFunction, ParseError<'i>> {
    parser.parse_nested_block(|p| {
        let ax = Angle::parse(p)?;

        let ay = if p.try_parse(|p| p.expect_comma()).is_ok() {
            Angle::parse(p)?
        } else {
            Angle::from_degrees(0.0)
        };

        Ok(TransformFunction::Skew(ax, ay))
    })
}

fn parse_prop_skew_x_args<'i>(
    parser: &mut Parser<'i, '_>,
) -> Result<TransformFunction, ParseError<'i>> {
    parser.parse_nested_block(|p| {
        let angle = Angle::parse(p)?;
        Ok(TransformFunction::SkewX(angle))
    })
}

fn parse_prop_skew_y_args<'i>(
    parser: &mut Parser<'i, '_>,
) -> Result<TransformFunction, ParseError<'i>> {
    parser.parse_nested_block(|p| {
        let angle = Angle::parse(p)?;
        Ok(TransformFunction::SkewY(angle))
    })
}

impl Transform {
    #[inline]
    pub fn new_unchecked(xx: f64, yx: f64, xy: f64, yy: f64, x0: f64, y0: f64) -> Self {
        Self {
            xx,
            yx,
            xy,
            yy,
            x0,
            y0,
        }
    }

    #[inline]
    pub fn identity() -> Self {
        Self::new_unchecked(1.0, 0.0, 0.0, 1.0, 0.0, 0.0)
    }

    #[inline]
    pub fn new_translate(tx: f64, ty: f64) -> Self {
        Self::new_unchecked(1.0, 0.0, 0.0, 1.0, tx, ty)
    }

    #[inline]
    pub fn new_scale(sx: f64, sy: f64) -> Self {
        Self::new_unchecked(sx, 0.0, 0.0, sy, 0.0, 0.0)
    }

    #[inline]
    pub fn new_rotate(a: Angle) -> Self {
        let (s, c) = a.radians().sin_cos();
        Self::new_unchecked(c, s, -s, c, 0.0, 0.0)
    }

    #[inline]
    pub fn new_skew(ax: Angle, ay: Angle) -> Self {
        Self::new_unchecked(1.0, ay.radians().tan(), ax.radians().tan(), 1.0, 0.0, 0.0)
    }

    #[must_use]
    pub fn multiply(t1: &Transform, t2: &Transform) -> Self {
        #[allow(clippy::suspicious_operation_groupings)]
        Transform {
            xx: t1.xx * t2.xx + t1.yx * t2.xy,
            yx: t1.xx * t2.yx + t1.yx * t2.yy,
            xy: t1.xy * t2.xx + t1.yy * t2.xy,
            yy: t1.xy * t2.yx + t1.yy * t2.yy,
            x0: t1.x0 * t2.xx + t1.y0 * t2.xy + t2.x0,
            y0: t1.x0 * t2.yx + t1.y0 * t2.yy + t2.y0,
        }
    }

    #[inline]
    pub fn pre_transform(&self, t: &Transform) -> Self {
        Self::multiply(t, self)
    }

    #[inline]
    pub fn post_transform(&self, t: &Transform) -> Self {
        Self::multiply(self, t)
    }

    #[inline]
    pub fn pre_translate(&self, x: f64, y: f64) -> Self {
        self.pre_transform(&Transform::new_translate(x, y))
    }

    #[inline]
    pub fn pre_scale(&self, sx: f64, sy: f64) -> Self {
        self.pre_transform(&Transform::new_scale(sx, sy))
    }

    #[inline]
    pub fn pre_rotate(&self, angle: Angle) -> Self {
        self.pre_transform(&Transform::new_rotate(angle))
    }

    #[inline]
    pub fn post_translate(&self, x: f64, y: f64) -> Self {
        self.post_transform(&Transform::new_translate(x, y))
    }

    #[inline]
    pub fn post_scale(&self, sx: f64, sy: f64) -> Self {
        self.post_transform(&Transform::new_scale(sx, sy))
    }

    #[inline]
    pub fn post_rotate(&self, angle: Angle) -> Self {
        self.post_transform(&Transform::new_rotate(angle))
    }

    #[inline]
    fn determinant(&self) -> f64 {
        self.xx * self.yy - self.xy * self.yx
    }

    #[inline]
    pub fn is_invertible(&self) -> bool {
        let det = self.determinant();

        det != 0.0 && det.is_finite()
    }

    #[must_use]
    pub fn invert(&self) -> Option<Self> {
        let det = self.determinant();

        if det == 0.0 || !det.is_finite() {
            return None;
        }

        let inv_det = 1.0 / det;

        Some(Transform::new_unchecked(
            inv_det * self.yy,
            inv_det * (-self.yx),
            inv_det * (-self.xy),
            inv_det * self.xx,
            inv_det * (self.xy * self.y0 - self.yy * self.x0),
            inv_det * (self.yx * self.x0 - self.xx * self.y0),
        ))
    }

    #[inline]
    pub fn transform_distance(&self, dx: f64, dy: f64) -> (f64, f64) {
        (dx * self.xx + dy * self.xy, dx * self.yx + dy * self.yy)
    }

    #[inline]
    pub fn transform_point(&self, px: f64, py: f64) -> (f64, f64) {
        let (x, y) = self.transform_distance(px, py);
        (x + self.x0, y + self.y0)
    }

    pub fn transform_rect(&self, rect: &Rect) -> Rect {
        let points = [
            self.transform_point(rect.x0, rect.y0),
            self.transform_point(rect.x1, rect.y0),
            self.transform_point(rect.x0, rect.y1),
            self.transform_point(rect.x1, rect.y1),
        ];

        let (mut xmin, mut ymin, mut xmax, mut ymax) = {
            let (x, y) = points[0];

            (x, y, x, y)
        };

        for &(x, y) in points.iter().skip(1) {
            if x < xmin {
                xmin = x;
            }

            if x > xmax {
                xmax = x;
            }

            if y < ymin {
                ymin = y;
            }

            if y > ymax {
                ymax = y;
            }
        }

        Rect {
            x0: xmin,
            y0: ymin,
            x1: xmax,
            y1: ymax,
        }
    }
}

impl Default for Transform {
    #[inline]
    fn default() -> Transform {
        Transform::identity()
    }
}

impl TransformAttribute {
    pub fn to_transform(self) -> Transform {
        self.0
    }
}

impl Parse for TransformAttribute {
    fn parse<'i>(parser: &mut Parser<'i, '_>) -> Result<TransformAttribute, ParseError<'i>> {
        Ok(TransformAttribute(parse_transform_list(parser)?))
    }
}

fn parse_transform_list<'i>(parser: &mut Parser<'i, '_>) -> Result<Transform, ParseError<'i>> {
    let mut t = Transform::identity();

    loop {
        if parser.is_exhausted() {
            break;
        }

        t = parse_transform_command(parser)?.post_transform(&t);
        optional_comma(parser);
    }

    Ok(t)
}

fn parse_transform_command<'i>(parser: &mut Parser<'i, '_>) -> Result<Transform, ParseError<'i>> {
    let loc = parser.current_source_location();

    match parser.next()?.clone() {
        Token::Function(ref name) => parse_transform_function(name, parser),

        Token::Ident(ref name) => {
            parser.expect_parenthesis_block()?;
            parse_transform_function(name, parser)
        }

        tok => Err(loc.new_unexpected_token_error(tok.clone())),
    }
}

fn parse_transform_function<'i>(
    name: &str,
    parser: &mut Parser<'i, '_>,
) -> Result<Transform, ParseError<'i>> {
    let loc = parser.current_source_location();

    match name {
        "matrix" => parse_matrix_args(parser),
        "translate" => parse_translate_args(parser),
        "scale" => parse_scale_args(parser),
        "rotate" => parse_rotate_args(parser),
        "skewX" => parse_skew_x_args(parser),
        "skewY" => parse_skew_y_args(parser),
        _ => Err(loc.new_custom_error(ValueErrorKind::parse_error(
            "expected matrix|translate|scale|rotate|skewX|skewY",
        ))),
    }
}

fn parse_matrix_args<'i>(parser: &mut Parser<'i, '_>) -> Result<Transform, ParseError<'i>> {
    parser.parse_nested_block(|p| {
        let xx = f64::parse(p)?;
        optional_comma(p);

        let yx = f64::parse(p)?;
        optional_comma(p);

        let xy = f64::parse(p)?;
        optional_comma(p);

        let yy = f64::parse(p)?;
        optional_comma(p);

        let x0 = f64::parse(p)?;
        optional_comma(p);

        let y0 = f64::parse(p)?;

        Ok(Transform::new_unchecked(xx, yx, xy, yy, x0, y0))
    })
}

fn parse_translate_args<'i>(parser: &mut Parser<'i, '_>) -> Result<Transform, ParseError<'i>> {
    parser.parse_nested_block(|p| {
        let tx = f64::parse(p)?;

        let ty = p
            .try_parse(|p| {
                optional_comma(p);
                f64::parse(p)
            })
            .unwrap_or(0.0);

        Ok(Transform::new_translate(tx, ty))
    })
}

fn parse_scale_args<'i>(parser: &mut Parser<'i, '_>) -> Result<Transform, ParseError<'i>> {
    parser.parse_nested_block(|p| {
        let x = f64::parse(p)?;

        let y = p
            .try_parse(|p| {
                optional_comma(p);
                f64::parse(p)
            })
            .unwrap_or(x);

        Ok(Transform::new_scale(x, y))
    })
}

fn parse_rotate_args<'i>(parser: &mut Parser<'i, '_>) -> Result<Transform, ParseError<'i>> {
    parser.parse_nested_block(|p| {
        let angle = Angle::from_degrees(f64::parse(p)?);

        let (tx, ty) = p
            .try_parse(|p| -> Result<_, ParseError<'_>> {
                optional_comma(p);
                let tx = f64::parse(p)?;

                optional_comma(p);
                let ty = f64::parse(p)?;

                Ok((tx, ty))
            })
            .unwrap_or((0.0, 0.0));

        Ok(Transform::new_translate(tx, ty)
            .pre_rotate(angle)
            .pre_translate(-tx, -ty))
    })
}

fn parse_skew_x_args<'i>(parser: &mut Parser<'i, '_>) -> Result<Transform, ParseError<'i>> {
    parser.parse_nested_block(|p| {
        let angle = Angle::from_degrees(f64::parse(p)?);
        Ok(Transform::new_skew(angle, Angle::new(0.0)))
    })
}

fn parse_skew_y_args<'i>(parser: &mut Parser<'i, '_>) -> Result<Transform, ParseError<'i>> {
    parser.parse_nested_block(|p| {
        let angle = Angle::from_degrees(f64::parse(p)?);
        Ok(Transform::new_skew(Angle::new(0.0), angle))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use float_cmp::ApproxEq;
    use std::f64;

    fn rotation_transform(deg: f64, tx: f64, ty: f64) -> Transform {
        Transform::new_translate(tx, ty)
            .pre_rotate(Angle::from_degrees(deg))
            .pre_translate(-tx, -ty)
    }

    fn parse_transform_attr(s: &str) -> Result<Transform, ParseError<'_>> {
        let transform_attr = TransformAttribute::parse_str(s)?;
        Ok(transform_attr.to_transform())
    }

    fn parse_transform_prop(s: &str) -> Result<TransformProperty, ParseError<'_>> {
        TransformProperty::parse_str(s)
    }

    fn assert_transform_eq(t1: &Transform, t2: &Transform) {
        let epsilon = 8.0 * f64::EPSILON; // kind of arbitrary, but allow for some sloppiness

        assert!(t1.xx.approx_eq(t2.xx, (epsilon, 1)));
        assert!(t1.yx.approx_eq(t2.yx, (epsilon, 1)));
        assert!(t1.xy.approx_eq(t2.xy, (epsilon, 1)));
        assert!(t1.yy.approx_eq(t2.yy, (epsilon, 1)));
        assert!(t1.x0.approx_eq(t2.x0, (epsilon, 1)));
        assert!(t1.y0.approx_eq(t2.y0, (epsilon, 1)));
    }

    #[test]
    fn test_multiply() {
        let t1 = Transform::identity();
        let t2 = Transform::new_unchecked(1.0, 2.0, 3.0, 4.0, 5.0, 6.0);
        assert_transform_eq(&Transform::multiply(&t1, &t2), &t2);
        assert_transform_eq(&Transform::multiply(&t2, &t1), &t2);

        let t1 = Transform::new_unchecked(1.0, 2.0, 3.0, 4.0, 5.0, 6.0);
        let t2 = Transform::new_unchecked(0.0, 0.0, 0.0, 0.0, 0.0, 0.0);
        let r = Transform::new_unchecked(0.0, 0.0, 0.0, 0.0, 5.0, 6.0);
        assert_transform_eq(&Transform::multiply(&t1, &t2), &t2);
        assert_transform_eq(&Transform::multiply(&t2, &t1), &r);

        let t1 = Transform::new_unchecked(0.5, 0.0, 0.0, 0.5, 10.0, 10.0);
        let t2 = Transform::new_unchecked(1.0, 0.0, 0.0, 1.0, -10.0, -10.0);
        let r1 = Transform::new_unchecked(0.5, 0.0, 0.0, 0.5, 0.0, 0.0);
        let r2 = Transform::new_unchecked(0.5, 0.0, 0.0, 0.5, 5.0, 5.0);
        assert_transform_eq(&Transform::multiply(&t1, &t2), &r1);
        assert_transform_eq(&Transform::multiply(&t2, &t1), &r2);
    }

    #[test]
    fn test_invert() {
        let t = Transform::new_unchecked(2.0, 0.0, 0.0, 0.0, 0.0, 0.0);
        assert!(!t.is_invertible());
        assert!(t.invert().is_none());

        let t = Transform::identity();
        assert!(t.is_invertible());
        assert!(t.invert().is_some());
        let i = t.invert().unwrap();
        assert_transform_eq(&i, &Transform::identity());

        let t = Transform::new_unchecked(1.0, 2.0, 3.0, 4.0, 5.0, 6.0);
        assert!(t.is_invertible());
        assert!(t.invert().is_some());
        let i = t.invert().unwrap();
        assert_transform_eq(&t.pre_transform(&i), &Transform::identity());
        assert_transform_eq(&t.post_transform(&i), &Transform::identity());
    }

    #[test]
    pub fn test_transform_point() {
        let t = Transform::new_translate(10.0, 10.0);
        assert_eq!((11.0, 11.0), t.transform_point(1.0, 1.0));
    }

    #[test]
    pub fn test_transform_distance() {
        let t = Transform::new_translate(10.0, 10.0).pre_scale(2.0, 1.0);
        assert_eq!((2.0, 1.0), t.transform_distance(1.0, 1.0));
    }

    #[test]
    fn parses_valid_transform() {
        let t = Transform::new_unchecked(1.0, 0.0, 0.0, 1.0, 20.0, 30.0);
        let s = Transform::new_unchecked(10.0, 0.0, 0.0, 10.0, 0.0, 0.0);
        let r = rotation_transform(30.0, 10.0, 10.0);

        let a = Transform::multiply(&s, &t);
        assert_transform_eq(
            &parse_transform_attr("translate(20, 30), scale (10) rotate (30 10 10)").unwrap(),
            &Transform::multiply(&r, &a),
        );
    }

    fn assert_parse_error(s: &str) {
        assert!(parse_transform_attr(s).is_err());
    }

    #[test]
    fn syntax_error_yields_parse_error() {
        assert_parse_error("foo");
        assert_parse_error("matrix (1 2 3 4 5)");
        assert_parse_error("translate(1 2 3 4 5)");
        assert_parse_error("translate (1,)");
        assert_parse_error("scale (1,)");
        assert_parse_error("skewX (1,2)");
        assert_parse_error("skewY ()");
        assert_parse_error("skewY");
    }

    #[test]
    fn parses_matrix() {
        assert_transform_eq(
            &parse_transform_attr("matrix (1 2 3 4 5 6)").unwrap(),
            &Transform::new_unchecked(1.0, 2.0, 3.0, 4.0, 5.0, 6.0),
        );

        assert_transform_eq(
            &parse_transform_attr("matrix(1,2,3,4 5 6)").unwrap(),
            &Transform::new_unchecked(1.0, 2.0, 3.0, 4.0, 5.0, 6.0),
        );

        assert_transform_eq(
            &parse_transform_attr("matrix (1,2.25,-3.25e2,4 5 6)").unwrap(),
            &Transform::new_unchecked(1.0, 2.25, -325.0, 4.0, 5.0, 6.0),
        );
    }

    #[test]
    fn parses_translate() {
        assert_transform_eq(
            &parse_transform_attr("translate(-1 -2)").unwrap(),
            &Transform::new_unchecked(1.0, 0.0, 0.0, 1.0, -1.0, -2.0),
        );

        assert_transform_eq(
            &parse_transform_attr("translate(-1, -2)").unwrap(),
            &Transform::new_unchecked(1.0, 0.0, 0.0, 1.0, -1.0, -2.0),
        );

        assert_transform_eq(
            &parse_transform_attr("translate(-1)").unwrap(),
            &Transform::new_unchecked(1.0, 0.0, 0.0, 1.0, -1.0, 0.0),
        );
    }

    #[test]
    fn parses_scale() {
        assert_transform_eq(
            &parse_transform_attr("scale (-1)").unwrap(),
            &Transform::new_unchecked(-1.0, 0.0, 0.0, -1.0, 0.0, 0.0),
        );

        assert_transform_eq(
            &parse_transform_attr("scale(-1 -2)").unwrap(),
            &Transform::new_unchecked(-1.0, 0.0, 0.0, -2.0, 0.0, 0.0),
        );

        assert_transform_eq(
            &parse_transform_attr("scale(-1, -2)").unwrap(),
            &Transform::new_unchecked(-1.0, 0.0, 0.0, -2.0, 0.0, 0.0),
        );
    }

    #[test]
    fn parses_rotate() {
        assert_transform_eq(
            &parse_transform_attr("rotate (30)").unwrap(),
            &rotation_transform(30.0, 0.0, 0.0),
        );
        assert_transform_eq(
            &parse_transform_attr("rotate (30,-1,-2)").unwrap(),
            &rotation_transform(30.0, -1.0, -2.0),
        );
        assert_transform_eq(
            &parse_transform_attr("rotate(30, -1, -2)").unwrap(),
            &rotation_transform(30.0, -1.0, -2.0),
        );
    }

    #[test]
    fn parses_skew_x() {
        assert_transform_eq(
            &parse_transform_attr("skewX (30)").unwrap(),
            &Transform::new_skew(Angle::from_degrees(30.0), Angle::new(0.0)),
        );
    }

    #[test]
    fn parses_skew_y() {
        assert_transform_eq(
            &parse_transform_attr("skewY (30)").unwrap(),
            &Transform::new_skew(Angle::new(0.0), Angle::from_degrees(30.0)),
        );
    }

    #[test]
    fn parses_transform_list() {
        let t = Transform::new_unchecked(1.0, 0.0, 0.0, 1.0, 20.0, 30.0);
        let s = Transform::new_unchecked(10.0, 0.0, 0.0, 10.0, 0.0, 0.0);
        let r = rotation_transform(30.0, 10.0, 10.0);

        assert_transform_eq(
            &parse_transform_attr("scale(10)rotate(30, 10, 10)").unwrap(),
            &Transform::multiply(&r, &s),
        );

        assert_transform_eq(
            &parse_transform_attr("translate(20, 30), scale (10)").unwrap(),
            &Transform::multiply(&s, &t),
        );

        let a = Transform::multiply(&s, &t);
        assert_transform_eq(
            &parse_transform_attr("translate(20, 30), scale (10) rotate (30 10 10)").unwrap(),
            &Transform::multiply(&r, &a),
        );
    }

    #[test]
    fn parses_empty() {
        assert_transform_eq(&parse_transform_attr("").unwrap(), &Transform::identity());
    }

    #[test]
    fn garbage_transform_attr_is_error() {
        assert!(parse_transform_attr("123").is_err());
        assert!(parse_transform_attr("translate").is_err());
        assert!(parse_transform_attr("&").is_err());
        assert!(parse_transform_attr("plonk()").is_err());
        assert!(parse_transform_attr("plonk ()").is_err());
    }

    #[test]
    fn test_parse_transform_property_none() {
        assert_eq!(
            parse_transform_prop("none").unwrap(),
            TransformProperty::None
        );
    }

    #[test]
    fn none_transform_is_identity() {
        assert_eq!(
            parse_transform_prop("none").unwrap().to_transform(),
            Transform::identity()
        );
    }

    #[test]
    fn empty_transform_property_is_error() {
        // https://www.w3.org/TR/css-transforms-1/#transform-property
        //
        // <transform-list> = <transform-function>+
        //                                        ^ one or more required
        assert!(parse_transform_prop("").is_err());
    }

    #[test]
    fn garbage_transform_property_is_error() {
        assert!(parse_transform_prop("123").is_err());
        assert!(parse_transform_prop("translate").is_err());
        assert!(parse_transform_prop("&").is_err());
        assert!(parse_transform_prop("plonk()").is_err());
    }

    #[test]
    fn test_parse_transform_property_matrix() {
        let tp = TransformProperty::List(vec![TransformFunction::Matrix(
            Transform::new_unchecked(1.0, 2.0, 3.0, 4.0, 5.0, 6.0),
        )]);

        assert_eq!(&tp, &parse_transform_prop("matrix(1,2,3,4,5,6)").unwrap());
        assert_eq!(
            tp.to_transform(),
            Transform::new_unchecked(1.0, 2.0, 3.0, 4.0, 5.0, 6.0)
        );

        assert!(parse_transform_prop("matrix(1 2 3 4 5 6)").is_err());
        assert!(parse_transform_prop("Matrix(1,2,3,4,5,6)").is_err());
    }

    #[test]
    fn test_parse_transform_property_translate() {
        let tpt = TransformProperty::List(vec![TransformFunction::Translate(
            Length::<Horizontal>::new(100.0, LengthUnit::Px),
            Length::<Vertical>::new(200.0, LengthUnit::Px),
        )]);

        assert_eq!(
            &tpt,
            &parse_transform_prop("translate(100px,200px)").unwrap()
        );

        assert_eq!(
            parse_transform_prop("translate(1)").unwrap(),
            parse_transform_prop("translate(1, 0)").unwrap()
        );

        assert_eq!(tpt.to_transform(), Transform::new_translate(100.0, 200.0));

        assert!(parse_transform_prop("translate(100, foo)").is_err());
        assert!(parse_transform_prop("translate(100, )").is_err());
        assert!(parse_transform_prop("translate(100 200)").is_err());
        assert!(parse_transform_prop("translate(1px,2px,3px,4px)").is_err());
    }

    #[test]
    fn test_parse_transform_property_translate_x() {
        let tptx = TransformProperty::List(vec![TransformFunction::TranslateX(
            Length::<Horizontal>::new(100.0, LengthUnit::Px),
        )]);

        assert_eq!(&tptx, &parse_transform_prop("translateX(100px)").unwrap());

        assert_eq!(tptx.to_transform(), Transform::new_translate(100.0, 0.0));

        assert!(parse_transform_prop("translateX(1)").is_ok());
        assert!(parse_transform_prop("translateX(100 100)").is_err());
        assert!(parse_transform_prop("translatex(1px)").is_err());
        assert!(parse_transform_prop("translatex(1rad)").is_err());
    }

    #[test]
    fn test_parse_transform_property_translate_y() {
        let tpty = TransformProperty::List(vec![TransformFunction::TranslateY(
            Length::<Vertical>::new(100.0, LengthUnit::Px),
        )]);

        assert_eq!(&tpty, &parse_transform_prop("translateY(100px)").unwrap());

        assert_eq!(tpty.to_transform(), Transform::new_translate(0.0, 100.0));

        assert!(parse_transform_prop("translateY(1)").is_ok());
        assert!(parse_transform_prop("translateY(100 100)").is_err());
        assert!(parse_transform_prop("translatey(1px)").is_err());
    }

    #[test]
    fn test_translate_only_supports_pixel_units() {
        assert!(parse_transform_prop("translate(1in, 2)").is_err());
        assert!(parse_transform_prop("translate(1, 2in)").is_err());
        assert!(parse_transform_prop("translateX(1cm)").is_err());
        assert!(parse_transform_prop("translateY(1cm)").is_err());
    }

    #[test]
    fn test_parse_transform_property_scale() {
        let tps = TransformProperty::List(vec![TransformFunction::Scale(1.0, 10.0)]);

        assert_eq!(&tps, &parse_transform_prop("scale(1,10)").unwrap());

        assert_eq!(
            parse_transform_prop("scale(2)").unwrap(),
            parse_transform_prop("scale(2, 2)").unwrap()
        );

        assert_eq!(tps.to_transform(), Transform::new_scale(1.0, 10.0));

        assert!(parse_transform_prop("scale(100, foo)").is_err());
        assert!(parse_transform_prop("scale(100, )").is_err());
        assert!(parse_transform_prop("scale(1 10)").is_err());
        assert!(parse_transform_prop("scale(1px,10px)").is_err());
        assert!(parse_transform_prop("scale(1%)").is_err());
    }

    #[test]
    fn test_parse_transform_property_scale_x() {
        let tpsx = TransformProperty::List(vec![TransformFunction::ScaleX(10.0)]);

        assert_eq!(&tpsx, &parse_transform_prop("scaleX(10)").unwrap());

        assert_eq!(tpsx.to_transform(), Transform::new_scale(10.0, 1.0));

        assert!(parse_transform_prop("scaleX(100 100)").is_err());
        assert!(parse_transform_prop("scalex(10)").is_err());
        assert!(parse_transform_prop("scaleX(10px)").is_err());
    }

    #[test]
    fn test_parse_transform_property_scale_y() {
        let tpsy = TransformProperty::List(vec![TransformFunction::ScaleY(10.0)]);

        assert_eq!(&tpsy, &parse_transform_prop("scaleY(10)").unwrap());

        assert_eq!(tpsy.to_transform(), Transform::new_scale(1.0, 10.0));

        assert!(parse_transform_prop("scaleY(10 1)").is_err());
        assert!(parse_transform_prop("scaleY(1px)").is_err());
    }

    #[test]
    fn test_parse_transform_property_rotate() {
        let tpr =
            TransformProperty::List(vec![TransformFunction::Rotate(Angle::from_degrees(100.0))]);

        assert_eq!(&tpr, &parse_transform_prop("rotate(100deg)").unwrap());

        assert_eq!(
            tpr.to_transform(),
            Transform::new_rotate(Angle::from_degrees(100.0))
        );

        assert!(parse_transform_prop("rotate(100deg 100)").is_err());
        assert!(parse_transform_prop("rotate(3px)").is_err());
    }

    #[test]
    fn test_parse_transform_property_skew() {
        let tpsk = TransformProperty::List(vec![TransformFunction::Skew(
            Angle::from_degrees(90.0),
            Angle::from_degrees(120.0),
        )]);

        assert_eq!(&tpsk, &parse_transform_prop("skew(90deg,120deg)").unwrap());

        assert_eq!(
            parse_transform_prop("skew(45deg)").unwrap(),
            parse_transform_prop("skew(45deg, 0)").unwrap()
        );

        assert!(parse_transform_prop("skew(1.0,1.0)").is_ok());
        assert!(parse_transform_prop("skew(1rad,1rad)").is_ok());

        assert_eq!(
            tpsk.to_transform(),
            Transform::new_skew(Angle::from_degrees(90.0), Angle::from_degrees(120.0))
        );

        assert!(parse_transform_prop("skew(100, foo)").is_err());
        assert!(parse_transform_prop("skew(100, )").is_err());
        assert!(parse_transform_prop("skew(1.0px)").is_err());
        assert!(parse_transform_prop("skew(1.0,1.0,1deg)").is_err());
    }

    #[test]
    fn test_parse_transform_property_skew_x() {
        let tpskx =
            TransformProperty::List(vec![TransformFunction::SkewX(Angle::from_degrees(90.0))]);

        assert_eq!(&tpskx, &parse_transform_prop("skewX(90deg)").unwrap());
        assert!(parse_transform_prop("skewX(1.0)").is_ok());
        assert!(parse_transform_prop("skewX(1rad)").is_ok());

        assert_eq!(
            tpskx.to_transform(),
            Transform::new_skew(Angle::from_degrees(90.0), Angle::from_degrees(0.0))
        );

        assert!(parse_transform_prop("skewx(1.0)").is_err());
        assert!(parse_transform_prop("skewX(1.0,1.0)").is_err());
    }

    #[test]
    fn test_parse_transform_property_skew_y() {
        let tpsky =
            TransformProperty::List(vec![TransformFunction::SkewY(Angle::from_degrees(90.0))]);

        assert_eq!(&tpsky, &parse_transform_prop("skewY(90deg)").unwrap());
        assert!(parse_transform_prop("skewY(1.0)").is_ok());
        assert!(parse_transform_prop("skewY(1rad)").is_ok());

        assert_eq!(
            tpsky.to_transform(),
            Transform::new_skew(Angle::from_degrees(0.0), Angle::from_degrees(90.0))
        );

        assert!(parse_transform_prop("skewy(1.0)").is_err());
        assert!(parse_transform_prop("skewY(1.0,1.0)").is_err());
    }
}
