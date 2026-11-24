//! The `Parse` trait for CSS properties, and utilities for parsers.

use cssparser::{Parser, ParserInput, Token};
use markup5ever::QualName;
use std::str;

use crate::error::*;

/// Trait to parse values using `cssparser::Parser`.
pub trait Parse: Sized {
    /// Parses a value out of the `parser`.
    ///
    /// All value types should implement this for composability.
    fn parse<'i>(parser: &mut Parser<'i, '_>) -> Result<Self, ParseError<'i>>;

    /// Convenience function to parse a value out of a `&str`.
    ///
    /// This is useful mostly for tests which want to avoid creating a
    /// `cssparser::Parser` by hand.  Property types do not need to reimplement this.
    fn parse_str(s: &str) -> Result<Self, ParseError<'_>> {
        let mut input = ParserInput::new(s);
        let mut parser = Parser::new(&mut input);

        let res = Self::parse(&mut parser)?;
        parser.expect_exhausted()?;

        Ok(res)
    }
}

/// Consumes a comma if it exists, or does nothing.
pub fn optional_comma(parser: &mut Parser<'_, '_>) {
    let _ = parser.try_parse(|p| p.expect_comma());
}

/// Parses an `f32` and ensures that it is not an infinity or NaN.
pub fn finite_f32(n: f32) -> Result<f32, ValueErrorKind> {
    if n.is_finite() {
        Ok(n)
    } else {
        Err(ValueErrorKind::Value("expected finite number".to_string()))
    }
}

pub trait ParseValue<T: Parse> {
    /// Parses a `value` string into a type `T`.
    fn parse(&self, value: &str) -> Result<T, ElementError>;
}

impl<T: Parse> ParseValue<T> for QualName {
    /// Parse a value from an XML attribute.
    ///
    /// Say we have an attribute `bar` like in `<foo bar="42"/>`.  If `attr` is a [`QualName`]
    /// for the attribute `bar`, then we'll parse it like `attr.parse("42")`.
    ///
    /// The reason for doing things that way is so that, in case of a parse error, this
    /// function can annotate the error result with the attribute's name.
    ///
    /// Note that attribute values are parsed entirely, thus the call to
    /// `expect_exhausted()` below.  We don't want to allow garbage in the string after
    /// the initial value has been parsed.
    fn parse(&self, value: &str) -> Result<T, ElementError> {
        let mut input = ParserInput::new(value);
        let mut parser = Parser::new(&mut input);

        T::parse(&mut parser)
            .and_then(|v| {
                parser.expect_exhausted()?;
                Ok(v)
            })
            .attribute(self.clone())
    }
}

impl<T: Parse> Parse for Option<T> {
    fn parse<'i>(parser: &mut Parser<'i, '_>) -> Result<Self, ParseError<'i>> {
        T::parse(parser).map(Some)
    }
}

impl Parse for f64 {
    fn parse<'i>(parser: &mut Parser<'i, '_>) -> Result<Self, ParseError<'i>> {
        let loc = parser.current_source_location();
        let n = parser.expect_number()?;
        if n.is_finite() {
            Ok(f64::from(n))
        } else {
            Err(loc.new_custom_error(ValueErrorKind::value_error("expected finite number")))
        }
    }
}

/// Non-Negative number
#[derive(Debug, Copy, Clone, PartialEq)]
pub struct NonNegative(pub f64);

impl Parse for NonNegative {
    fn parse<'i>(parser: &mut Parser<'i, '_>) -> Result<Self, ParseError<'i>> {
        let loc = parser.current_source_location();
        let n = Parse::parse(parser)?;
        if n >= 0.0 {
            Ok(NonNegative(n))
        } else {
            Err(loc.new_custom_error(ValueErrorKind::value_error("expected non negative number")))
        }
    }
}

/// CSS number-optional-number
///
/// SVG1.1: <https://www.w3.org/TR/SVG11/types.html#DataTypeNumberOptionalNumber>
#[derive(Debug, Copy, Clone, PartialEq)]
pub struct NumberOptionalNumber<T: Parse>(pub T, pub T);

impl<T: Parse + Copy> Parse for NumberOptionalNumber<T> {
    fn parse<'i>(parser: &mut Parser<'i, '_>) -> Result<Self, ParseError<'i>> {
        let x = Parse::parse(parser)?;

        if !parser.is_exhausted() {
            optional_comma(parser);
            let y = Parse::parse(parser)?;
            Ok(NumberOptionalNumber(x, y))
        } else {
            Ok(NumberOptionalNumber(x, x))
        }
    }
}

/// CSS number-percentage
///
/// CSS Values and Units 3: <https://www.w3.org/TR/css3-values/#typedef-number-percentage>
#[derive(Debug, Copy, Clone, PartialEq)]
pub struct NumberOrPercentage {
    pub value: f64,
}

impl Parse for NumberOrPercentage {
    fn parse<'i>(parser: &mut Parser<'i, '_>) -> Result<Self, ParseError<'i>> {
        let loc = parser.current_source_location();

        let value = match parser.next()? {
            Token::Number { value, .. } => Ok(*value),
            Token::Percentage { unit_value, .. } => Ok(*unit_value),
            tok => Err(loc.new_unexpected_token_error(tok.clone())),
        }?;

        let v = finite_f32(value).map_err(|e| parser.new_custom_error(e))?;
        Ok(NumberOrPercentage {
            value: f64::from(v),
        })
    }
}

impl Parse for i32 {
    /// CSS integer
    ///
    /// SVG1.1: <https://www.w3.org/TR/SVG11/types.html#DataTypeInteger>
    fn parse<'i>(parser: &mut Parser<'i, '_>) -> Result<Self, ParseError<'i>> {
        Ok(parser.expect_integer()?)
    }
}

impl Parse for u32 {
    fn parse<'i>(parser: &mut Parser<'i, '_>) -> Result<Self, ParseError<'i>> {
        let loc = parser.current_source_location();
        let n = parser.expect_integer()?;
        if n >= 0 {
            Ok(n as u32)
        } else {
            Err(loc.new_custom_error(ValueErrorKind::value_error("expected unsigned number")))
        }
    }
}

/// List separated by optional commas, with bounds for the required and maximum number of items.
#[derive(Clone, Debug, PartialEq)]
pub struct CommaSeparatedList<T: Parse, const REQUIRED: usize, const MAX: usize>(pub Vec<T>);

impl<T: Parse, const REQUIRED: usize, const MAX: usize> Parse
    for CommaSeparatedList<T, REQUIRED, MAX>
{
    fn parse<'i>(parser: &mut Parser<'i, '_>) -> Result<Self, ParseError<'i>> {
        let loc = parser.current_source_location();
        let mut v = Vec::<T>::with_capacity(MAX);
        for i in 0..MAX {
            if i != 0 {
                optional_comma(parser);
            }

            v.push(T::parse(parser)?);

            if parser.is_exhausted() {
                break;
            }
        }

        if REQUIRED > 0 && v.len() < REQUIRED {
            Err(loc.new_custom_error(ValueErrorKind::value_error("expected more values")))
        } else {
            v.shrink_to_fit();
            Ok(CommaSeparatedList(v))
        }
    }
}

/// Parses a list of identifiers from a `cssparser::Parser`
///
/// # Example
///
/// ```
/// # use cssparser::{ParserInput, Parser};
/// # use rsvg::parse_identifiers;
/// # fn main() -> Result<(), cssparser::BasicParseError<'static>> {
/// # let mut input = ParserInput::new("true");
/// # let mut parser = Parser::new(&mut input);
/// let my_boolean = parse_identifiers!(
///     parser,
///     "true" => true,
///     "false" => false,
/// )?;
/// # Ok(())
/// # }
/// ```
#[doc(hidden)]
#[macro_export]
macro_rules! parse_identifiers {
    ($parser:expr,
     $($str:expr => $val:expr,)+) => {
        {
            let loc = $parser.current_source_location();
            let token = $parser.next()?;

            match token {
                $(&cssparser::Token::Ident(ref cow) if cow.eq_ignore_ascii_case($str) => Ok($val),)+

                _ => Err(loc.new_basic_unexpected_token_error(token.clone()))
            }
        }
    };
}

/// CSS Custom identifier.
///
/// CSS Values and Units 4: <https://www.w3.org/TR/css-values-4/#custom-idents>
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CustomIdent(pub String);

impl Parse for CustomIdent {
    fn parse<'i>(parser: &mut Parser<'i, '_>) -> Result<Self, ParseError<'i>> {
        let loc = parser.current_source_location();
        let token = parser.next()?;

        match token {
            // CSS-wide keywords and "default" are errors here
            // https://www.w3.org/TR/css-values-4/#css-wide-keywords
            Token::Ident(cow) => {
                for s in &["initial", "inherit", "unset", "default"] {
                    if cow.eq_ignore_ascii_case(s) {
                        return Err(loc.new_basic_unexpected_token_error(token.clone()).into());
                    }
                }

                Ok(CustomIdent(cow.as_ref().to_string()))
            }

            _ => Err(loc.new_basic_unexpected_token_error(token.clone()).into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use markup5ever::{QualName, local_name, ns};

    #[test]
    fn parses_number_optional_number() {
        assert_eq!(
            NumberOptionalNumber::parse_str("1, 2").unwrap(),
            NumberOptionalNumber(1.0, 2.0)
        );
        assert_eq!(
            NumberOptionalNumber::parse_str("1 2").unwrap(),
            NumberOptionalNumber(1.0, 2.0)
        );
        assert_eq!(
            NumberOptionalNumber::parse_str("1").unwrap(),
            NumberOptionalNumber(1.0, 1.0)
        );

        assert_eq!(
            NumberOptionalNumber::parse_str("-1, -2").unwrap(),
            NumberOptionalNumber(-1.0, -2.0)
        );
        assert_eq!(
            NumberOptionalNumber::parse_str("-1 -2").unwrap(),
            NumberOptionalNumber(-1.0, -2.0)
        );
        assert_eq!(
            NumberOptionalNumber::parse_str("-1").unwrap(),
            NumberOptionalNumber(-1.0, -1.0)
        );
    }

    #[test]
    fn invalid_number_optional_number() {
        assert!(NumberOptionalNumber::<f64>::parse_str("").is_err());
        assert!(NumberOptionalNumber::<f64>::parse_str("1x").is_err());
        assert!(NumberOptionalNumber::<f64>::parse_str("x1").is_err());
        assert!(NumberOptionalNumber::<f64>::parse_str("1 x").is_err());
        assert!(NumberOptionalNumber::<f64>::parse_str("1 , x").is_err());
        assert!(NumberOptionalNumber::<f64>::parse_str("1 , 2x").is_err());
        assert!(NumberOptionalNumber::<f64>::parse_str("1 2 x").is_err());
    }

    #[test]
    fn parses_integer() {
        assert_eq!(i32::parse_str("0").unwrap(), 0);
        assert_eq!(i32::parse_str("1").unwrap(), 1);
        assert_eq!(i32::parse_str("-1").unwrap(), -1);

        assert_eq!(u32::parse_str("0").unwrap(), 0);
        assert_eq!(u32::parse_str("1").unwrap(), 1);
    }

    #[test]
    fn invalid_integer() {
        assert!(i32::parse_str("").is_err());
        assert!(i32::parse_str("1x").is_err());
        assert!(i32::parse_str("1.5").is_err());

        assert!(u32::parse_str("").is_err());
        assert!(u32::parse_str("1x").is_err());
        assert!(u32::parse_str("1.5").is_err());
        assert!(u32::parse_str("-1").is_err());
    }

    #[test]
    fn parses_integer_optional_integer() {
        assert_eq!(
            NumberOptionalNumber::parse_str("1, 2").unwrap(),
            NumberOptionalNumber(1, 2)
        );
        assert_eq!(
            NumberOptionalNumber::parse_str("1 2").unwrap(),
            NumberOptionalNumber(1, 2)
        );
        assert_eq!(
            NumberOptionalNumber::parse_str("1").unwrap(),
            NumberOptionalNumber(1, 1)
        );

        assert_eq!(
            NumberOptionalNumber::parse_str("-1, -2").unwrap(),
            NumberOptionalNumber(-1, -2)
        );
        assert_eq!(
            NumberOptionalNumber::parse_str("-1 -2").unwrap(),
            NumberOptionalNumber(-1, -2)
        );
        assert_eq!(
            NumberOptionalNumber::parse_str("-1").unwrap(),
            NumberOptionalNumber(-1, -1)
        );
    }

    #[test]
    fn invalid_integer_optional_integer() {
        assert!(NumberOptionalNumber::<i32>::parse_str("").is_err());
        assert!(NumberOptionalNumber::<i32>::parse_str("1x").is_err());
        assert!(NumberOptionalNumber::<i32>::parse_str("x1").is_err());
        assert!(NumberOptionalNumber::<i32>::parse_str("1 x").is_err());
        assert!(NumberOptionalNumber::<i32>::parse_str("1 , x").is_err());
        assert!(NumberOptionalNumber::<i32>::parse_str("1 , 2x").is_err());
        assert!(NumberOptionalNumber::<i32>::parse_str("1 2 x").is_err());
        assert!(NumberOptionalNumber::<i32>::parse_str("1.5").is_err());
        assert!(NumberOptionalNumber::<i32>::parse_str("1 2.5").is_err());
        assert!(NumberOptionalNumber::<i32>::parse_str("1, 2.5").is_err());
    }

    #[test]
    fn parses_comma_separated_list() {
        assert_eq!(
            CommaSeparatedList::<f64, 1, 1>::parse_str("5").unwrap(),
            CommaSeparatedList(vec![5.0])
        );

        assert_eq!(
            CommaSeparatedList::<f64, 4, 4>::parse_str("1 2 3 4").unwrap(),
            CommaSeparatedList(vec![1.0, 2.0, 3.0, 4.0])
        );

        assert_eq!(
            CommaSeparatedList::<f64, 0, 5>::parse_str("1 2 3 4 5").unwrap(),
            CommaSeparatedList(vec![1.0, 2.0, 3.0, 4.0, 5.0])
        );

        assert_eq!(
            CommaSeparatedList::<f64, 0, 5>::parse_str("1 2 3").unwrap(),
            CommaSeparatedList(vec![1.0, 2.0, 3.0])
        );
    }

    #[test]
    fn errors_on_invalid_comma_separated_list() {
        // empty
        assert!(CommaSeparatedList::<f64, 1, 1>::parse_str("").is_err());
        assert!(CommaSeparatedList::<f64, 0, 1>::parse_str("").is_err());

        // garbage
        assert!(CommaSeparatedList::<f64, 1, 1>::parse_str("foo").is_err());
        assert!(CommaSeparatedList::<f64, 2, 2>::parse_str("1foo").is_err());
        assert!(CommaSeparatedList::<f64, 2, 2>::parse_str("1 foo").is_err());
        assert!(CommaSeparatedList::<f64, 2, 2>::parse_str("1 foo 2").is_err());
        assert!(CommaSeparatedList::<f64, 2, 2>::parse_str("1,foo").is_err());

        // too many
        assert!(CommaSeparatedList::<f64, 1, 1>::parse_str("1 2").is_err());

        // extra token
        assert!(CommaSeparatedList::<f64, 1, 1>::parse_str("1,").is_err());
        assert!(CommaSeparatedList::<f64, 0, 1>::parse_str("1,").is_err());

        // too few
        assert!(CommaSeparatedList::<f64, 2, 2>::parse_str("1").is_err());
        assert!(CommaSeparatedList::<f64, 3, 3>::parse_str("1 2").is_err());
    }

    #[test]
    fn detects_too_many_numbers_bug_1138() {
        // The root cause of this bug is that we didn't check for token exhaustion when
        // parsing attribute values in `impl<T: Parse> ParseValue<T> for QualName`.  So,
        // for this test, we actually invoke the parser for CommaSeparatedList via that impl.

        let attribute = QualName::new(None, ns!(svg), local_name!("matrix"));

        // should parse 20 numbers and error out on the 21st
        let r: Result<CommaSeparatedList<f64, 20, 20>, _> =
            attribute.parse("1 0 0 0 0 0 1 0 0 0 0 0 1 0 0 0 0 0 0,500000 0 ");
        assert!(r.is_err());
    }

    #[test]
    fn parses_custom_ident() {
        assert_eq!(
            CustomIdent::parse_str("hello").unwrap(),
            CustomIdent("hello".to_string())
        );
    }

    #[test]
    fn invalid_custom_ident_yields_error() {
        assert!(CustomIdent::parse_str("initial").is_err());
        assert!(CustomIdent::parse_str("inherit").is_err());
        assert!(CustomIdent::parse_str("unset").is_err());
        assert!(CustomIdent::parse_str("default").is_err());
        assert!(CustomIdent::parse_str("").is_err());
    }
}
