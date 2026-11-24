//! Parser for an Accept-Language HTTP header.

use language_tags::{LanguageTag, ParseError};
use locale_config::{LanguageRange, Locale};

use std::error;
use std::fmt;
use std::str::FromStr;
use std::sync::{Arc, OnceLock};

#[cfg(doc)]
use crate::api::CairoRenderer;
use crate::rsvg_log;
use crate::session::Session;

/// Used to set the language for rendering.
///
/// SVG documents can use the `<switch>` element, whose children have a `systemLanguage`
/// attribute; only the first child which has a `systemLanguage` that matches the
/// preferred languages will be rendered.
///
/// This enum, used with [`CairoRenderer::with_language`], configures how to obtain the
/// user's prefererred languages.
pub enum Language {
    /// Use the Unix environment variables `LANGUAGE`, `LC_ALL`, `LC_MESSAGES` and `LANG` to obtain the
    /// user's language.
    ///
    /// This uses [`g_get_language_names()`][ggln] underneath.
    ///
    /// [ggln]: https://docs.gtk.org/glib/func.get_language_names.html
    FromEnvironment,

    /// Use a list of languages in the form of an HTTP Accept-Language header, like `es, en;q=0.8`.
    ///
    /// This is convenient when you want to select an explicit set of languages, instead of
    /// assuming that the Unix environment has the language you want.
    AcceptLanguage(AcceptLanguage),
}

/// `Language` but with the environment's locale converted to something we can use.
#[derive(Clone)]
pub enum UserLanguage {
    LanguageTags(LanguageTags),
    AcceptLanguage(AcceptLanguage),
    FromEnvironment(Arc<OnceLock<LanguageTags>>),
}

#[derive(Clone, Debug, PartialEq)]
struct Weight(Option<f32>);

impl Weight {
    fn numeric(&self) -> f32 {
        self.0.unwrap_or(1.0)
    }
}

#[derive(Clone, Debug, PartialEq)]
struct Item {
    tag: LanguageTag,
    weight: Weight,
}

/// Stores a parsed version of an HTTP Accept-Language header.
///
/// RFC 7231: <https://datatracker.ietf.org/doc/html/rfc7231#section-5.3.5>
#[derive(Clone, Debug, PartialEq)]
pub struct AcceptLanguage(Box<[Item]>);

/// Errors when parsing an `AcceptLanguage`.
#[derive(Debug, PartialEq)]
enum AcceptLanguageError {
    NoElements,
    InvalidCharacters,
    InvalidLanguageTag(ParseError),
    InvalidWeight,
}

impl error::Error for AcceptLanguageError {}

impl fmt::Display for AcceptLanguageError {
    // Skipped for mutation testing.  This is just an error formatter.  In the public API,
    // `Language::AcceptLanguage(AcceptLanguage)` is constructible via
    // `AcceptLanguage::parse()`, which returns `Result<Self, String>`.  These strings go
    // there, but librsvg makes no promises about their contents.  Below, we do have tests
    // for `AcceptLanguageError` being computed correctly.
    #[mutants::skip]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoElements => write!(f, "no language tags in list"),
            Self::InvalidCharacters => write!(f, "invalid characters in language list"),
            Self::InvalidLanguageTag(e) => write!(f, "invalid language tag: {e}"),
            Self::InvalidWeight => write!(f, "invalid q= weight"),
        }
    }
}

/// Optional whitespace, Space or Tab, per RFC 7230.
///
/// RFC 7230: <https://datatracker.ietf.org/doc/html/rfc7230#section-3.2.3>
const OWS: [char; 2] = ['\x20', '\x09'];

impl AcceptLanguage {
    /// Parses the payload of an HTTP Accept-Language header.
    ///
    /// For example, a valid header looks like `es, en;q=0.8`, and means, "I prefer Spanish,
    /// but will also accept English".
    ///
    /// Use this function to construct a [`Language::AcceptLanguage`]
    /// variant to pass to the [`CairoRenderer::with_language`] function.
    ///
    /// See RFC 7231 for details: <https://datatracker.ietf.org/doc/html/rfc7231#section-5.3.5>
    pub fn parse(s: &str) -> Result<AcceptLanguage, String> {
        AcceptLanguage::parse_internal(s).map_err(|e| format!("{}", e))
    }

    /// Internal constructor.  We don't expose [`AcceptLanguageError`] in the public API;
    /// there we just use a [`String`].
    fn parse_internal(s: &str) -> Result<AcceptLanguage, AcceptLanguageError> {
        if !s.is_ascii() {
            return Err(AcceptLanguageError::InvalidCharacters);
        }

        let mut items = Vec::new();

        for val in s.split(',') {
            let trimmed = val.trim_matches(&OWS[..]);
            if trimmed.is_empty() {
                continue;
            }

            items.push(Item::parse(trimmed)?);
        }

        if items.is_empty() {
            Err(AcceptLanguageError::NoElements)
        } else {
            Ok(AcceptLanguage(items.into_boxed_slice()))
        }
    }

    fn iter(&self) -> impl Iterator<Item = (&LanguageTag, f32)> {
        self.0.iter().map(|item| (&item.tag, item.weight.numeric()))
    }

    fn any_matches(&self, tag: &LanguageTag) -> bool {
        self.iter().any(|(self_tag, _weight)| tag.matches(self_tag))
    }
}

impl Item {
    fn parse(s: &str) -> Result<Item, AcceptLanguageError> {
        let semicolon_pos = s.find(';');

        let (before_semicolon, after_semicolon) = if let Some(semi) = semicolon_pos {
            (&s[..semi], Some(&s[semi + 1..]))
        } else {
            (s, None)
        };

        let tag = LanguageTag::parse(before_semicolon)
            .map_err(AcceptLanguageError::InvalidLanguageTag)?;

        let weight = if let Some(quality) = after_semicolon {
            let quality = quality.trim_start_matches(&OWS[..]);

            let number = if let Some(qvalue) = quality.strip_prefix("q=") {
                if qvalue.starts_with(&['0', '1'][..]) {
                    let first_digit = qvalue.chars().next().unwrap();

                    if let Some(decimals) = qvalue[1..].strip_prefix('.') {
                        if (first_digit == '0'
                            && decimals.len() <= 3
                            && decimals.chars().all(|c| c.is_ascii_digit()))
                            || (first_digit == '1'
                                && decimals.len() <= 3
                                && decimals.chars().all(|c| c == '0'))
                        {
                            qvalue
                        } else {
                            return Err(AcceptLanguageError::InvalidWeight);
                        }
                    } else {
                        qvalue
                    }
                } else {
                    return Err(AcceptLanguageError::InvalidWeight);
                }
            } else {
                return Err(AcceptLanguageError::InvalidWeight);
            };

            Weight(Some(
                f32::from_str(number).map_err(|_| AcceptLanguageError::InvalidWeight)?,
            ))
        } else {
            Weight(None)
        };

        Ok(Item { tag, weight })
    }
}

/// A list of BCP47 language tags.
///
/// RFC 5664: <https://www.rfc-editor.org/info/rfc5664>
#[derive(Debug, Clone, PartialEq)]
pub struct LanguageTags(Box<[LanguageTag]>);

impl LanguageTags {
    pub fn empty() -> Self {
        LanguageTags(Box::new([]))
    }

    /// Converts a `Locale` to a set of language tags.
    pub fn from_locale(locale: &Locale) -> Result<LanguageTags, String> {
        let mut tags = Vec::new();

        for locale_range in locale.tags_for("messages") {
            if locale_range == LanguageRange::invariant() {
                continue;
            }

            let str_locale_range = locale_range.as_ref();

            let locale_tag = LanguageTag::from_str(str_locale_range).map_err(|e| {
                format!("invalid language tag \"{str_locale_range}\" in locale: {e}")
            })?;

            if !locale_tag.is_language_range() {
                return Err(format!(
                    "language tag \"{locale_tag}\" is not a language range"
                ));
            }

            tags.push(locale_tag);
        }

        Ok(LanguageTags(Box::from(tags)))
    }

    pub fn from(tags: Vec<LanguageTag>) -> LanguageTags {
        LanguageTags(Box::from(tags))
    }

    pub fn iter(&self) -> impl Iterator<Item = &LanguageTag> {
        self.0.iter()
    }

    pub fn any_matches(&self, language_tag: &LanguageTag) -> bool {
        self.0.iter().any(|tag| tag.matches(language_tag))
    }
}

impl UserLanguage {
    pub fn new(language: &crate::api::Language) -> UserLanguage {
        match *language {
            crate::api::Language::FromEnvironment => {
                UserLanguage::FromEnvironment(Arc::new(OnceLock::new()))
            }

            crate::api::Language::AcceptLanguage(ref a) => UserLanguage::AcceptLanguage(a.clone()),
        }
    }

    pub fn any_matches(&self, tags: &LanguageTags, session: &Session) -> bool {
        match *self {
            UserLanguage::LanguageTags(ref language_tags) => {
                tags.iter().any(|tag| language_tags.any_matches(tag))
            }
            UserLanguage::AcceptLanguage(ref accept_language) => {
                tags.iter().any(|tag| accept_language.any_matches(tag))
            }
            UserLanguage::FromEnvironment(ref once_lock) => {
                let language_tags =
                    once_lock.get_or_init(|| get_language_tags_from_environment(session));
                tags.iter().any(|tag| language_tags.any_matches(tag))
            }
        }
    }
}

/// Gets language tags from the environment.
///
/// This function is thread-safe.
fn get_language_tags_from_environment(session: &Session) -> LanguageTags {
    LanguageTags::from_locale(&locale_from_environment())
        .map_err(|s| {
            rsvg_log!(session, "could not convert locale to language tags: {}", s);
        })
        .unwrap_or_else(|_| LanguageTags::empty())
}

/// Gets the user's preferred locale from the environment and
/// translates it to a `Locale` with `LanguageRange` fallbacks.
fn locale_from_environment() -> Locale {
    let mut locale = Locale::invariant();

    for name in glib::language_names() {
        let name = name.as_str();
        if let Ok(range) = LanguageRange::from_unix(name) {
            locale.add(&range);
        }
    }

    locale
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_accept_language() {
        // plain tag
        assert_eq!(
            AcceptLanguage::parse_internal("es-MX").unwrap(),
            AcceptLanguage(
                vec![Item {
                    tag: LanguageTag::parse("es-MX").unwrap(),
                    weight: Weight(None)
                }]
                .into_boxed_slice()
            )
        );

        // with quality
        assert_eq!(
            AcceptLanguage::parse_internal("es-MX;q=1").unwrap(),
            AcceptLanguage(
                vec![Item {
                    tag: LanguageTag::parse("es-MX").unwrap(),
                    weight: Weight(Some(1.0))
                }]
                .into_boxed_slice()
            )
        );

        // with quality
        assert_eq!(
            AcceptLanguage::parse_internal("es-MX;q=0").unwrap(),
            AcceptLanguage(
                vec![Item {
                    tag: LanguageTag::parse("es-MX").unwrap(),
                    weight: Weight(Some(0.0))
                }]
                .into_boxed_slice()
            )
        );

        // zero decimals are allowed
        assert_eq!(
            AcceptLanguage::parse_internal("es-MX;q=0.").unwrap(),
            AcceptLanguage(
                vec![Item {
                    tag: LanguageTag::parse("es-MX").unwrap(),
                    weight: Weight(Some(0.0))
                }]
                .into_boxed_slice()
            )
        );

        // zero decimals are allowed
        assert_eq!(
            AcceptLanguage::parse_internal("es-MX;q=1.").unwrap(),
            AcceptLanguage(
                vec![Item {
                    tag: LanguageTag::parse("es-MX").unwrap(),
                    weight: Weight(Some(1.0))
                }]
                .into_boxed_slice()
            )
        );

        // one decimal
        assert_eq!(
            AcceptLanguage::parse_internal("es-MX;q=1.0").unwrap(),
            AcceptLanguage(
                vec![Item {
                    tag: LanguageTag::parse("es-MX").unwrap(),
                    weight: Weight(Some(1.0))
                }]
                .into_boxed_slice()
            )
        );

        // two decimals
        assert_eq!(
            AcceptLanguage::parse_internal("es-MX;q=1.00").unwrap(),
            AcceptLanguage(
                vec![Item {
                    tag: LanguageTag::parse("es-MX").unwrap(),
                    weight: Weight(Some(1.0))
                }]
                .into_boxed_slice()
            )
        );

        // three decimals
        assert_eq!(
            AcceptLanguage::parse_internal("es-MX;q=1.000").unwrap(),
            AcceptLanguage(
                vec![Item {
                    tag: LanguageTag::parse("es-MX").unwrap(),
                    weight: Weight(Some(1.0))
                }]
                .into_boxed_slice()
            )
        );

        // multiple elements
        assert_eq!(
            AcceptLanguage::parse_internal("es-MX, en; q=0.5").unwrap(),
            AcceptLanguage(
                vec![
                    Item {
                        tag: LanguageTag::parse("es-MX").unwrap(),
                        weight: Weight(None)
                    },
                    Item {
                        tag: LanguageTag::parse("en").unwrap(),
                        weight: Weight(Some(0.5))
                    },
                ]
                .into_boxed_slice()
            )
        );

        // superfluous whitespace
        assert_eq!(
            AcceptLanguage::parse_internal(",es-MX;q=1.000  , en; q=0.125  ,  ,").unwrap(),
            AcceptLanguage(
                vec![
                    Item {
                        tag: LanguageTag::parse("es-MX").unwrap(),
                        weight: Weight(Some(1.0))
                    },
                    Item {
                        tag: LanguageTag::parse("en").unwrap(),
                        weight: Weight(Some(0.125))
                    },
                ]
                .into_boxed_slice()
            )
        );
    }

    #[test]
    fn empty_lists() {
        assert!(matches!(
            AcceptLanguage::parse_internal(""),
            Err(AcceptLanguageError::NoElements)
        ));

        assert!(matches!(
            AcceptLanguage::parse_internal(","),
            Err(AcceptLanguageError::NoElements)
        ));

        assert!(matches!(
            AcceptLanguage::parse_internal(", , ,,,"),
            Err(AcceptLanguageError::NoElements)
        ));
    }

    #[test]
    fn ascii_only() {
        assert!(matches!(
            AcceptLanguage::parse_internal("ës"),
            Err(AcceptLanguageError::InvalidCharacters)
        ));
    }

    #[test]
    fn invalid_tag() {
        assert!(matches!(
            AcceptLanguage::parse_internal("no_underscores"),
            Err(AcceptLanguageError::InvalidLanguageTag(_))
        ));
    }

    #[test]
    fn invalid_weight() {
        assert!(matches!(
            AcceptLanguage::parse_internal("es;"),
            Err(AcceptLanguageError::InvalidWeight)
        ));
        assert!(matches!(
            AcceptLanguage::parse_internal("es;q"),
            Err(AcceptLanguageError::InvalidWeight)
        ));
        assert!(matches!(
            AcceptLanguage::parse_internal("es;q="),
            Err(AcceptLanguageError::InvalidWeight)
        ));
        assert!(matches!(
            AcceptLanguage::parse_internal("es;q=2"),
            Err(AcceptLanguageError::InvalidWeight)
        ));
        assert!(matches!(
            AcceptLanguage::parse_internal("es;q=1.1"),
            Err(AcceptLanguageError::InvalidWeight)
        ));
        assert!(matches!(
            AcceptLanguage::parse_internal("es;q=1.12"),
            Err(AcceptLanguageError::InvalidWeight)
        ));
        assert!(matches!(
            AcceptLanguage::parse_internal("es;q=1.123"),
            Err(AcceptLanguageError::InvalidWeight)
        ));

        // Up to three decimals allowed per RFC 7231
        assert!(matches!(
            AcceptLanguage::parse_internal("es;q=0.1234"),
            Err(AcceptLanguageError::InvalidWeight)
        ));
    }

    #[test]
    fn iter() {
        let accept_language = AcceptLanguage::parse_internal("es-MX, en; q=0.5").unwrap();
        let mut iter = accept_language.iter();

        let (tag, weight) = iter.next().unwrap();
        assert_eq!(*tag, LanguageTag::parse("es-MX").unwrap());
        assert_eq!(weight, 1.0);

        let (tag, weight) = iter.next().unwrap();
        assert_eq!(*tag, LanguageTag::parse("en").unwrap());
        assert_eq!(weight, 0.5);

        assert!(iter.next().is_none());
    }

    #[test]
    fn matches() {
        let accept_language = AcceptLanguage::parse_internal("es-MX, en; q=0.5").unwrap();

        assert!(accept_language.any_matches(&LanguageTag::parse("es-MX").unwrap()));
        assert!(accept_language.any_matches(&LanguageTag::parse("en").unwrap()));

        assert!(!accept_language.any_matches(&LanguageTag::parse("fr").unwrap()));
    }
}
