//! Parser for the `stroke-dasharray` property.

use cssparser::Parser;

use crate::error::*;
use crate::length::*;
use crate::parsers::{Parse, optional_comma};

#[derive(Debug, Default, PartialEq, Clone)]
pub enum Dasharray {
    #[default]
    None,
    Array(Box<[ULength<Both>]>),
}

impl Parse for Dasharray {
    fn parse<'i>(parser: &mut Parser<'i, '_>) -> Result<Dasharray, ParseError<'i>> {
        if parser
            .try_parse(|p| p.expect_ident_matching("none"))
            .is_ok()
        {
            return Ok(Dasharray::None);
        }

        let mut dasharray = Vec::new();

        loop {
            let d = ULength::<Both>::parse(parser)?;
            dasharray.push(d);

            if parser.is_exhausted() {
                break;
            }

            optional_comma(parser);
        }

        Ok(Dasharray::Array(dasharray.into_boxed_slice()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dasharray(l: &[ULength<Both>]) -> Dasharray {
        Dasharray::Array(l.to_vec().into_boxed_slice())
    }

    #[test]
    fn parses_dash_array() {
        // helper to cut down boilderplate
        let length_parse = |s| ULength::<Both>::parse_str(s).unwrap();

        let expected = dasharray(&[
            length_parse("1"),
            length_parse("2in"),
            length_parse("3"),
            length_parse("4%"),
        ]);

        let sample_1 = dasharray(&[length_parse("10"), length_parse("6")]);

        let sample_2 = dasharray(&[length_parse("5"), length_parse("5"), length_parse("20")]);

        let sample_3 = dasharray(&[
            length_parse("10px"),
            length_parse("20px"),
            length_parse("20px"),
        ]);

        let sample_4 = dasharray(&[
            length_parse("25"),
            length_parse("5"),
            length_parse("5"),
            length_parse("5"),
        ]);

        let sample_5 = dasharray(&[length_parse("3.1415926"), length_parse("8")]);
        let sample_6 = dasharray(&[length_parse("5"), length_parse("3.14")]);
        let sample_7 = dasharray(&[length_parse("2")]);

        assert_eq!(Dasharray::parse_str("none").unwrap(), Dasharray::None);
        assert_eq!(Dasharray::parse_str("1 2in,3 4%").unwrap(), expected);
        assert_eq!(Dasharray::parse_str("10,6").unwrap(), sample_1);
        assert_eq!(Dasharray::parse_str("5,5,20").unwrap(), sample_2);
        assert_eq!(Dasharray::parse_str("10px 20px 20px").unwrap(), sample_3);
        assert_eq!(Dasharray::parse_str("25  5 , 5 5").unwrap(), sample_4);
        assert_eq!(Dasharray::parse_str("3.1415926,8").unwrap(), sample_5);
        assert_eq!(Dasharray::parse_str("5, 3.14").unwrap(), sample_6);
        assert_eq!(Dasharray::parse_str("2").unwrap(), sample_7);

        // Negative numbers
        assert!(Dasharray::parse_str("20,40,-20").is_err());

        // Empty dash_array
        assert!(Dasharray::parse_str("").is_err());
        assert!(Dasharray::parse_str("\t  \n     ").is_err());
        assert!(Dasharray::parse_str(",,,").is_err());
        assert!(Dasharray::parse_str("10,  \t, 20 \n").is_err());

        // No trailing commas allowed, parse error
        assert!(Dasharray::parse_str("10,").is_err());

        // A comma should be followed by a number
        assert!(Dasharray::parse_str("20,,10").is_err());
    }
}
