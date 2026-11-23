//! Processing of the `xml:space` attribute.

use itertools::Itertools;

pub struct NormalizeDefault {
    pub has_element_before: bool,
    pub has_element_after: bool,
}

pub enum XmlSpaceNormalize {
    Default(NormalizeDefault),
    Preserve,
}

/// Implements `xml:space` handling per the SVG spec
///
/// Normalizes a string as it comes out of the XML parser's handler
/// for character data according to the SVG rules in
/// <https://www.w3.org/TR/SVG/text.html#WhiteSpace>
pub fn xml_space_normalize(mode: XmlSpaceNormalize, s: &str) -> String {
    match mode {
        XmlSpaceNormalize::Default(d) => normalize_default(d, s),
        XmlSpaceNormalize::Preserve => normalize_preserve(s),
    }
}

// From https://www.w3.org/TR/SVG/text.html#WhiteSpace
//
// When xml:space="default", the SVG user agent will do the following
// using a copy of the original character data content. First, it will
// remove all newline characters. Then it will convert all tab
// characters into space characters. Then, it will strip off all
// leading and trailing space characters. Then, all contiguous space
// characters will be consolidated.
fn normalize_default(elements: NormalizeDefault, mut s: &str) -> String {
    if !elements.has_element_before {
        s = s.trim_start();
    }

    if !elements.has_element_after {
        s = s.trim_end();
    }

    s.chars()
        .filter(|ch| *ch != '\n')
        .map(|ch| match ch {
            '\t' => ' ',
            c => c,
        })
        .coalesce(|current, next| match (current, next) {
            (' ', ' ') => Ok(' '),
            (_, _) => Err((current, next)),
        })
        .collect::<String>()
}

// From https://www.w3.org/TR/SVG/text.html#WhiteSpace
//
// When xml:space="preserve", the SVG user agent will do the following
// using a copy of the original character data content. It will
// convert all newline and tab characters into space characters. Then,
// it will draw all space characters, including leading, trailing and
// multiple contiguous space characters. Thus, when drawn with
// xml:space="preserve", the string "a   b" (three spaces between "a"
// and "b") will produce a larger separation between "a" and "b" than
// "a b" (one space between "a" and "b").
fn normalize_preserve(s: &str) -> String {
    s.chars()
        .map(|ch| match ch {
            '\n' | '\t' => ' ',

            c => c,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn xml_space_default() {
        assert_eq!(
            xml_space_normalize(
                XmlSpaceNormalize::Default(NormalizeDefault {
                    has_element_before: false,
                    has_element_after: false,
                }),
                "\n    WS example\n    indented lines\n  "
            ),
            "WS example indented lines"
        );
        assert_eq!(
            xml_space_normalize(
                XmlSpaceNormalize::Default(NormalizeDefault {
                    has_element_before: false,
                    has_element_after: false,
                }),
                "\n  \t  \tWS \t\t\texample\n  \t  indented lines\t\t  \n  "
            ),
            "WS example indented lines"
        );
        assert_eq!(
            xml_space_normalize(
                XmlSpaceNormalize::Default(NormalizeDefault {
                    has_element_before: false,
                    has_element_after: false,
                }),
                "\n  \t  \tWS \t\t\texample\n  \t  duplicate letters\t\t  \n  "
            ),
            "WS example duplicate letters"
        );
        assert_eq!(
            xml_space_normalize(
                XmlSpaceNormalize::Default(NormalizeDefault {
                    has_element_before: false,
                    has_element_after: false,
                }),
                "\nWS example\nnon-indented lines\n  "
            ),
            "WS examplenon-indented lines"
        );
        assert_eq!(
            xml_space_normalize(
                XmlSpaceNormalize::Default(NormalizeDefault {
                    has_element_before: false,
                    has_element_after: false,
                }),
                "\nWS example\tnon-indented lines\n  "
            ),
            "WS example non-indented lines"
        );
    }

    #[test]
    fn xml_space_default_with_elements() {
        assert_eq!(
            xml_space_normalize(
                XmlSpaceNormalize::Default(NormalizeDefault {
                    has_element_before: true,
                    has_element_after: false,
                }),
                " foo \n\t  bar "
            ),
            " foo bar"
        );

        assert_eq!(
            xml_space_normalize(
                XmlSpaceNormalize::Default(NormalizeDefault {
                    has_element_before: false,
                    has_element_after: true,
                }),
                " foo   \nbar "
            ),
            "foo bar "
        );
    }

    #[test]
    fn xml_space_preserve() {
        assert_eq!(
            xml_space_normalize(
                XmlSpaceNormalize::Preserve,
                "\n    WS example\n    indented lines\n  "
            ),
            "     WS example     indented lines   "
        );
        assert_eq!(
            xml_space_normalize(
                XmlSpaceNormalize::Preserve,
                "\n  \t  \tWS \t\t\texample\n  \t  indented lines\t\t  \n  "
            ),
            "       WS    example      indented lines       "
        );
        assert_eq!(
            xml_space_normalize(
                XmlSpaceNormalize::Preserve,
                "\n  \t  \tWS \t\t\texample\n  \t  duplicate letters\t\t  \n  "
            ),
            "       WS    example      duplicate letters       "
        );
    }
}
