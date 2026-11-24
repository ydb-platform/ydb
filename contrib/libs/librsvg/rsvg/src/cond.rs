//! Conditional processing attributes: `requiredExtensions`, `requiredFeatures`, `systemLanguage`.

#[allow(unused_imports, deprecated)]
use std::ascii::AsciiExt;

use std::str::FromStr;

use language_tags::LanguageTag;

use crate::accept_language::{LanguageTags, UserLanguage};
use crate::error::*;
use crate::rsvg_log;
use crate::session::Session;

// No extensions at the moment.
static IMPLEMENTED_EXTENSIONS: &[&str] = &[];

#[derive(Debug, PartialEq)]
pub struct RequiredExtensions(pub bool);

impl RequiredExtensions {
    /// Parse a requiredExtensions attribute.
    ///
    /// <http://www.w3.org/TR/SVG/struct.html#RequiredExtensionsAttribute>
    pub fn from_attribute(s: &str) -> RequiredExtensions {
        RequiredExtensions(
            s.split_whitespace()
                .all(|f| IMPLEMENTED_EXTENSIONS.binary_search(&f).is_ok()),
        )
    }

    /// Evaluate a requiredExtensions value for conditional processing.
    pub fn eval(&self) -> bool {
        self.0
    }
}

// Keep these sorted alphabetically for binary_search.
static IMPLEMENTED_FEATURES: &[&str] = &[
    "http://www.w3.org/TR/SVG11/feature#BasicFilter",
    "http://www.w3.org/TR/SVG11/feature#BasicGraphicsAttribute",
    "http://www.w3.org/TR/SVG11/feature#BasicPaintAttribute",
    "http://www.w3.org/TR/SVG11/feature#BasicStructure",
    "http://www.w3.org/TR/SVG11/feature#BasicText",
    "http://www.w3.org/TR/SVG11/feature#ConditionalProcessing",
    "http://www.w3.org/TR/SVG11/feature#ContainerAttribute",
    "http://www.w3.org/TR/SVG11/feature#Filter",
    "http://www.w3.org/TR/SVG11/feature#Gradient",
    "http://www.w3.org/TR/SVG11/feature#Image",
    "http://www.w3.org/TR/SVG11/feature#Marker",
    "http://www.w3.org/TR/SVG11/feature#Mask",
    "http://www.w3.org/TR/SVG11/feature#OpacityAttribute",
    "http://www.w3.org/TR/SVG11/feature#Pattern",
    "http://www.w3.org/TR/SVG11/feature#SVG",
    "http://www.w3.org/TR/SVG11/feature#SVG-static",
    "http://www.w3.org/TR/SVG11/feature#Shape",
    "http://www.w3.org/TR/SVG11/feature#Structure",
    "http://www.w3.org/TR/SVG11/feature#Style",
    "http://www.w3.org/TR/SVG11/feature#View",
    "org.w3c.svg.static", // deprecated SVG 1.0 feature string
];

#[derive(Debug, PartialEq)]
pub struct RequiredFeatures(pub bool);

impl RequiredFeatures {
    // Parse a requiredFeatures attribute
    // http://www.w3.org/TR/SVG/struct.html#RequiredFeaturesAttribute
    pub fn from_attribute(s: &str) -> RequiredFeatures {
        RequiredFeatures(
            s.split_whitespace()
                .all(|f| IMPLEMENTED_FEATURES.binary_search(&f).is_ok()),
        )
    }

    /// Evaluate a requiredFeatures value for conditional processing.
    pub fn eval(&self) -> bool {
        self.0
    }
}

/// The systemLanguage attribute inside `<cond>` element's children.
///
/// Parsing the value of a `systemLanguage` attribute may fail if the document supplies
/// invalid BCP47 language tags.  In that case, we store an `Invalid` variant.
///
/// That variant is used later, during [`SystemLanguage::eval`], to see whether the
/// `<cond>` should match or not.
#[derive(Debug, PartialEq)]
pub enum SystemLanguage {
    Valid(LanguageTags),
    Invalid,
}

impl SystemLanguage {
    /// Parse a `systemLanguage` attribute and match it against a given `Locale`
    ///
    /// The [`systemLanguage`] conditional attribute is a
    /// comma-separated list of [BCP47] Language Tags.  This function
    /// parses the attribute and matches the result against a given
    /// `locale`.  If there is a match, i.e. if the given locale
    /// supports one of the languages listed in the `systemLanguage`
    /// attribute, then the `SystemLanguage.0` will be `true`;
    /// otherwise it will be `false`.
    ///
    /// Normally, calling code will pass `&Locale::current()` for the
    /// `locale` attribute; this is the user's current locale.
    ///
    /// [`systemLanguage`]: https://www.w3.org/TR/SVG/struct.html#ConditionalProcessingSystemLanguageAttribute
    /// [BCP47]: http://www.ietf.org/rfc/bcp/bcp47.txt
    pub fn from_attribute(s: &str, session: &Session) -> SystemLanguage {
        let attribute_tags = s
            .split(',')
            .map(str::trim)
            .map(|s| {
                LanguageTag::from_str(s).map_err(|e| {
                    ValueErrorKind::parse_error(&format!("invalid language tag \"{s}\": {e}"))
                })
            })
            .collect::<Result<Vec<LanguageTag>, _>>();

        match attribute_tags {
            Ok(tags) => SystemLanguage::Valid(LanguageTags::from(tags)),

            Err(e) => {
                rsvg_log!(
                    session,
                    "ignoring systemLanguage attribute with invalid value: {}",
                    e
                );
                SystemLanguage::Invalid
            }
        }
    }

    /// Evaluate a systemLanguage value for conditional processing.
    pub fn eval(&self, user_language: &UserLanguage, session: &Session) -> bool {
        match *self {
            SystemLanguage::Valid(ref tags) => user_language.any_matches(tags, session),
            SystemLanguage::Invalid => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use locale_config::Locale;

    use super::*;

    #[test]
    fn required_extensions() {
        assert_eq!(
            RequiredExtensions::from_attribute("http://test.org/NotExisting/1.0"),
            RequiredExtensions(false)
        );
    }

    #[test]
    fn required_features() {
        assert_eq!(
            RequiredFeatures::from_attribute("http://www.w3.org/TR/SVG11/feature#NotExisting"),
            RequiredFeatures(false)
        );

        assert_eq!(
            RequiredFeatures::from_attribute("http://www.w3.org/TR/SVG11/feature#BasicFilter"),
            RequiredFeatures(true)
        );

        assert_eq!(
            RequiredFeatures::from_attribute(
                "http://www.w3.org/TR/SVG11/feature#BasicFilter \
                 http://www.w3.org/TR/SVG11/feature#NotExisting",
            ),
            RequiredFeatures(false)
        );

        assert_eq!(
            RequiredFeatures::from_attribute(
                "http://www.w3.org/TR/SVG11/feature#BasicFilter \
                 http://www.w3.org/TR/SVG11/feature#BasicText",
            ),
            RequiredFeatures(true)
        );
    }

    #[test]
    fn system_language() {
        let session = Session::new_for_test_suite();

        let locale = Locale::new("de,en-US").unwrap();
        let user_language = UserLanguage::LanguageTags(LanguageTags::from_locale(&locale).unwrap());

        assert!(matches!(
            SystemLanguage::from_attribute("", &session),
            SystemLanguage::Invalid
        ));

        assert!(matches!(
            SystemLanguage::from_attribute("12345", &session),
            SystemLanguage::Invalid
        ));

        assert!(!SystemLanguage::from_attribute("fr", &session).eval(&user_language, &session));

        assert!(!SystemLanguage::from_attribute("en", &session).eval(&user_language, &session));

        assert!(SystemLanguage::from_attribute("de", &session).eval(&user_language, &session));

        assert!(SystemLanguage::from_attribute("en-US", &session).eval(&user_language, &session));

        assert!(!SystemLanguage::from_attribute("en-GB", &session).eval(&user_language, &session));

        assert!(SystemLanguage::from_attribute("DE", &session).eval(&user_language, &session));

        assert!(SystemLanguage::from_attribute("de-LU", &session).eval(&user_language, &session));

        assert!(SystemLanguage::from_attribute("fr, de", &session).eval(&user_language, &session));
    }
}
