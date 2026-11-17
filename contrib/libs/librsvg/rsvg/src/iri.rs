//! CSS funciri values.

use cssparser::Parser;

use crate::document::NodeId;
use crate::error::*;
use crate::parsers::Parse;

/// Used where style properties take a funciri or "none"
///
/// This is not to be used for values which don't come from properties.
/// For example, the `xlink:href` attribute in the `<image>` element
/// does not take a funciri value (which looks like `url(...)`), but rather
/// it takes a plain URL.
#[derive(Debug, Clone, PartialEq)]
pub enum Iri {
    None,
    Resource(Box<NodeId>),
}

impl Iri {
    /// Returns the contents of an `IRI::Resource`, or `None`
    pub fn get(&self) -> Option<&NodeId> {
        match *self {
            Iri::None => None,
            Iri::Resource(ref f) => Some(f),
        }
    }
}

impl Parse for Iri {
    fn parse<'i>(parser: &mut Parser<'i, '_>) -> Result<Iri, ParseError<'i>> {
        if parser
            .try_parse(|i| i.expect_ident_matching("none"))
            .is_ok()
        {
            Ok(Iri::None)
        } else {
            let loc = parser.current_source_location();
            let url = parser.expect_url()?;
            let node_id =
                NodeId::parse(&url).map_err(|e| loc.new_custom_error(ValueErrorKind::from(e)))?;

            Ok(Iri::Resource(Box::new(node_id)))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_none() {
        assert_eq!(Iri::parse_str("none").unwrap(), Iri::None);
    }

    #[test]
    fn parses_url() {
        assert_eq!(
            Iri::parse_str("url(#bar)").unwrap(),
            Iri::Resource(Box::new(NodeId::Internal("bar".to_string())))
        );

        assert_eq!(
            Iri::parse_str("url(foo#bar)").unwrap(),
            Iri::Resource(Box::new(NodeId::External(
                "foo".to_string(),
                "bar".to_string()
            )))
        );

        // be permissive if the closing ) is missing
        assert_eq!(
            Iri::parse_str("url(#bar").unwrap(),
            Iri::Resource(Box::new(NodeId::Internal("bar".to_string())))
        );
        assert_eq!(
            Iri::parse_str("url(foo#bar").unwrap(),
            Iri::Resource(Box::new(NodeId::External(
                "foo".to_string(),
                "bar".to_string()
            )))
        );

        assert!(Iri::parse_str("").is_err());
        assert!(Iri::parse_str("foo").is_err());
        assert!(Iri::parse_str("url(foo)bar").is_err());
    }
}
