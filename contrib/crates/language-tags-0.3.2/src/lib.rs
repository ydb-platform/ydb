#![deny(
    future_incompatible,
    nonstandard_style,
    rust_2018_idioms,
    missing_docs,
    trivial_casts,
    trivial_numeric_casts,
    unused_qualifications
)]
#![cfg_attr(test, deny(warnings))]

//! Language tags can be used identify human languages, scripts e.g. Latin script, countries and
//! other regions.
//!
//! Language tags are defined in [BCP47](http://tools.ietf.org/html/bcp47), an introduction is
//! ["Language tags in HTML and XML"](http://www.w3.org/International/articles/language-tags/) by
//! the W3C. They are commonly used in HTML and HTTP `Content-Language` and `Accept-Language`
//! header fields.
//!
//! This package currently supports parsing (fully conformant parser), validation, canonicalization,
//! formatting and comparing language tags.
//!
//! # Examples
//! Create a simple language tag representing the French language as spoken
//! in Belgium and print it:
//!
//! ```rust
//! use language_tags::LanguageTag;
//!
//! let langtag = LanguageTag::parse("fr-BE").unwrap();
//! assert_eq!(langtag.to_string(), "fr-BE");
//! ```
//!
//! Parse a tag representing a special type of English specified by private agreement:
//!
//! ```rust
//! use language_tags::LanguageTag;
//! use std::iter::FromIterator;
//!
//! let langtag: LanguageTag = "en-x-twain".parse().unwrap();
//! assert_eq!(langtag.primary_language(), "en");
//! assert_eq!(Vec::from_iter(langtag.private_use_subtags()), vec!["twain"]);
//! ```
//!
//! You can check for equality, but more often you should test if two tags match.
//! In this example we check if the resource in German language is suitable for
//! a user from Austria. While people speaking Austrian German normally understand
//! standard German the opposite is not always true. So the resource can be presented
//! to the user but if the resource was in `de-AT` and a user asked for a representation
//! in `de` the request should be rejected.
//!
//!
//! ```rust
//! use language_tags::LanguageTag;
//!
//! let mut langtag_server = LanguageTag::parse("de-AT").unwrap();
//! let mut langtag_user = LanguageTag::parse("de").unwrap();
//! assert!(langtag_user.matches(&langtag_server));
//! ```

mod iana_registry;
#[cfg(feature = "serde")]
mod serde;

use crate::iana_registry::*;
use std::error::Error;
use std::fmt;
use std::iter::once;
use std::ops::Deref;
use std::str::FromStr;
use std::str::Split;

/// A language tag as described in [RFC 5646](https://tools.ietf.org/html/rfc5646).
///
/// Language tags are used to help identify languages, whether spoken,
/// written, signed, or otherwise signaled, for the purpose of
/// communication.  This includes constructed and artificial languages
/// but excludes languages not intended primarily for human
/// communication, such as programming languages.
#[derive(Eq, PartialEq, Debug, Clone, Hash)]
pub struct LanguageTag {
    /// Syntax described in [RFC 5646 2.1](https://tools.ietf.org/html/rfc5646#section-2.1)
    serialization: String,
    language_end: usize,
    extlang_end: usize,
    script_end: usize,
    region_end: usize,
    variant_end: usize,
    extension_end: usize,
}

impl LanguageTag {
    /// Return the serialization of this language tag.
    ///
    /// This is fast since that serialization is already stored in the `LanguageTag` struct.
    #[inline]
    pub fn as_str(&self) -> &str {
        &self.serialization
    }

    /// Return the serialization of this language tag.
    ///
    /// This consumes the `LanguageTag` and takes ownership of the `String` stored in it.
    #[inline]
    pub fn into_string(self) -> String {
        self.serialization
    }

    /// Return the [primary language subtag](https://tools.ietf.org/html/rfc5646#section-2.2.1).
    ///
    /// ```
    /// use language_tags::LanguageTag;
    ///
    /// let language_tag = LanguageTag::parse("zh-cmn-Hans-CN").unwrap();
    /// assert_eq!(language_tag.primary_language(), "zh");
    /// ```
    #[inline]
    pub fn primary_language(&self) -> &str {
        &self.serialization[..self.language_end]
    }

    /// Return the [extended language subtags](https://tools.ietf.org/html/rfc5646#section-2.2.2).
    ///
    /// Valid language tags have at most one extended language.
    ///
    /// ```
    /// use language_tags::LanguageTag;
    ///
    /// let language_tag = LanguageTag::parse("zh-cmn-Hans-CN").unwrap();
    /// assert_eq!(language_tag.extended_language(), Some("cmn"));
    /// ```
    #[inline]
    pub fn extended_language(&self) -> Option<&str> {
        if self.language_end == self.extlang_end {
            None
        } else {
            Some(&self.serialization[self.language_end + 1..self.extlang_end])
        }
    }

    /// Iterate on the [extended language subtags](https://tools.ietf.org/html/rfc5646#section-2.2.2).
    ///
    /// Valid language tags have at most one extended language.
    ///
    /// ```
    /// use language_tags::LanguageTag;
    ///
    /// let language_tag = LanguageTag::parse("zh-cmn-Hans-CN").unwrap();
    /// assert_eq!(language_tag.extended_language_subtags().collect::<Vec<_>>(), vec!["cmn"]);
    /// ```
    #[inline]
    pub fn extended_language_subtags(&self) -> impl Iterator<Item = &str> {
        self.extended_language().unwrap_or("").split_terminator('-')
    }

    /// Return the [primary language subtag](https://tools.ietf.org/html/rfc5646#section-2.2.1)
    /// and its [extended language subtags](https://tools.ietf.org/html/rfc5646#section-2.2.2).
    ///
    /// ```
    /// use language_tags::LanguageTag;
    ///
    /// let language_tag = LanguageTag::parse("zh-cmn-Hans-CN").unwrap();
    /// assert_eq!(language_tag.full_language(), "zh-cmn");
    /// ```
    #[inline]
    pub fn full_language(&self) -> &str {
        &self.serialization[..self.extlang_end]
    }

    /// Return the [script subtag](https://tools.ietf.org/html/rfc5646#section-2.2.3).
    ///
    /// ```
    /// use language_tags::LanguageTag;
    ///
    /// let language_tag = LanguageTag::parse("zh-cmn-Hans-CN").unwrap();
    /// assert_eq!(language_tag.script(), Some("Hans"));
    /// ```
    #[inline]
    pub fn script(&self) -> Option<&str> {
        if self.extlang_end == self.script_end {
            None
        } else {
            Some(&self.serialization[self.extlang_end + 1..self.script_end])
        }
    }

    /// Return the [region subtag](https://tools.ietf.org/html/rfc5646#section-2.2.4).
    ///
    ///
    /// ```
    /// use language_tags::LanguageTag;
    ///
    /// let language_tag = LanguageTag::parse("zh-cmn-Hans-CN").unwrap();
    /// assert_eq!(language_tag.region(), Some("CN"));
    /// ```
    #[inline]
    pub fn region(&self) -> Option<&str> {
        if self.script_end == self.region_end {
            None
        } else {
            Some(&self.serialization[self.script_end + 1..self.region_end])
        }
    }

    /// Return the [variant subtags](https://tools.ietf.org/html/rfc5646#section-2.2.5).
    ///
    /// ```
    /// use language_tags::LanguageTag;
    ///
    /// let language_tag = LanguageTag::parse("zh-Latn-TW-pinyin").unwrap();
    /// assert_eq!(language_tag.variant(), Some("pinyin"));
    /// ```
    #[inline]
    pub fn variant(&self) -> Option<&str> {
        if self.region_end == self.variant_end {
            None
        } else {
            Some(&self.serialization[self.region_end + 1..self.variant_end])
        }
    }

    /// Iterate on the [variant subtags](https://tools.ietf.org/html/rfc5646#section-2.2.5).
    ///
    /// ```
    /// use language_tags::LanguageTag;
    ///
    /// let language_tag = LanguageTag::parse("zh-Latn-TW-pinyin").unwrap();
    /// assert_eq!(language_tag.variant_subtags().collect::<Vec<_>>(), vec!["pinyin"]);
    /// ```
    #[inline]
    pub fn variant_subtags(&self) -> impl Iterator<Item = &str> {
        self.variant().unwrap_or("").split_terminator('-')
    }

    /// Return the [extension subtags](https://tools.ietf.org/html/rfc5646#section-2.2.6).
    ///
    /// ```
    /// use language_tags::LanguageTag;
    ///
    /// let language_tag = LanguageTag::parse("de-DE-u-co-phonebk").unwrap();
    /// assert_eq!(language_tag.extension(), Some("u-co-phonebk"));
    /// ```
    #[inline]
    pub fn extension(&self) -> Option<&str> {
        if self.variant_end == self.extension_end {
            None
        } else {
            Some(&self.serialization[self.variant_end + 1..self.extension_end])
        }
    }

    /// Iterate on the [extension subtags](https://tools.ietf.org/html/rfc5646#section-2.2.6).
    ///
    /// ```
    /// use language_tags::LanguageTag;
    ///
    /// let language_tag = LanguageTag::parse("de-DE-u-co-phonebk").unwrap();
    /// assert_eq!(language_tag.extension_subtags().collect::<Vec<_>>(), vec![('u', "co-phonebk")]);
    /// ```
    #[inline]
    pub fn extension_subtags(&self) -> impl Iterator<Item = (char, &str)> {
        match self.extension() {
            Some(parts) => ExtensionsIterator::new(parts),
            None => ExtensionsIterator::new(""),
        }
    }

    /// Return the [private use subtags](https://tools.ietf.org/html/rfc5646#section-2.2.7).
    ///
    ///
    /// ```
    /// use language_tags::LanguageTag;
    ///
    /// let language_tag = LanguageTag::parse("de-x-foo-bar").unwrap();
    /// assert_eq!(language_tag.private_use(), Some("x-foo-bar"));
    /// ```
    #[inline]
    pub fn private_use(&self) -> Option<&str> {
        if self.serialization.starts_with("x-") {
            Some(&self.serialization)
        } else if self.extension_end == self.serialization.len() {
            None
        } else {
            Some(&self.serialization[self.extension_end + 1..])
        }
    }

    /// Iterate on the [private use subtags](https://tools.ietf.org/html/rfc5646#section-2.2.7).
    ///
    /// ```
    /// use language_tags::LanguageTag;
    ///
    /// let language_tag = LanguageTag::parse("de-x-foo-bar").unwrap();
    /// assert_eq!(language_tag.private_use_subtags().collect::<Vec<_>>(), vec!["foo", "bar"]);
    /// ```
    #[inline]
    pub fn private_use_subtags(&self) -> impl Iterator<Item = &str> {
        self.private_use()
            .map(|part| &part[2..])
            .unwrap_or("")
            .split_terminator('-')
    }

    /// Create a `LanguageTag` from its serialization.
    ///
    /// This parser accepts the language tags that are "well-formed" according to
    /// [RFC 5646](https://tools.ietf.org/html/rfc5646#section-2.2.9).
    /// Full validation could be done with the `validate` method.
    ///
    /// ```rust
    /// use language_tags::LanguageTag;
    ///
    /// let language_tag = LanguageTag::parse("en-us").unwrap();
    /// assert_eq!(language_tag.into_string(), "en-US")
    /// ```
    ///
    /// # Errors
    ///
    /// If the language tag is not "well-formed" a `ParseError` variant will be returned.
    pub fn parse(input: &str) -> Result<Self, ParseError> {
        //grandfathered tags
        if let Some(tag) = GRANDFATHEREDS
            .iter()
            .find(|record| record.eq_ignore_ascii_case(input))
        {
            // grandfathered tag
            Ok(tag_from_primary_language(*tag))
        } else if input.starts_with("x-") || input.starts_with("X-") {
            // private use
            if !is_alphanumeric_or_dash(input) {
                Err(ParseError::ForbiddenChar)
            } else if input.len() == 2 {
                Err(ParseError::EmptyPrivateUse)
            } else {
                Ok(tag_from_primary_language(input.to_ascii_lowercase()))
            }
        } else {
            parse_language_tag(input)
        }
    }

    /// Check if the language tag is "valid" according to
    /// [RFC 5646](https://tools.ietf.org/html/rfc5646#section-2.2.9).
    ///
    /// It applies the following steps:
    ///
    /// * grandfathereds and private use tags are valid
    /// * There should be no more than one extended language subtag
    ///   (c.f. [errata 5457](https://www.rfc-editor.org/errata/eid5457)).
    /// * Primary language, extended language, script, region and variants should appear
    ///   in the IANA Language Subtag Registry.
    /// * Extended language and variants should have a correct prefix as set
    ///   in the IANA Language Subtag Registry.
    /// * There should be no duplicate variant and singleton (extension) subtags.
    ///
    ///
    /// # Errors
    ///
    /// If the language tag is not "valid" a `ValidationError` variant will be returned.
    pub fn validate(&self) -> Result<(), ValidationError> {
        // The tag is well-formed.
        // always ok

        // Private tag
        if self.serialization.starts_with("x-") {
            return Ok(());
        }

        // The tag is in the list of grandfathered tags
        if is_in_str_slice_set(&GRANDFATHEREDS, &self.serialization) {
            return Ok(());
        }

        // There is no more than one extended language subtag.
        // From [errata 5457](https://www.rfc-editor.org/errata/eid5457).
        if let Some(extended_language) = self.extended_language() {
            if extended_language.contains('-') {
                return Err(ValidationError::MultipleExtendedLanguageSubtags);
            }
        }

        // all of its primary language, extended language, script, region, and variant
        // subtags appear in the IANA Language Subtag Registry as of the
        // particular registry date.
        let primary_language = self.primary_language();
        if !between(primary_language, "qaa", "qtz")
            && !is_in_from_str_slice_set(&LANGUAGES, primary_language)
        {
            return Err(ValidationError::PrimaryLanguageNotInRegistry);
        }
        if let Some(extended_language) = self.extended_language() {
            if let Some(extended_language_prefix) =
                find_in_from_str_slice_map(&EXTLANGS, extended_language)
            {
                if !self.serialization.starts_with(extended_language_prefix) {
                    return Err(ValidationError::WrongExtendedLanguagePrefix);
                }
            } else {
                return Err(ValidationError::ExtendedLanguageNotInRegistry);
            }
        }
        if let Some(script) = self.script() {
            if !between(script, "Qaaa", "Qabx") && !is_in_from_str_slice_set(&SCRIPTS, script) {
                return Err(ValidationError::ScriptNotInRegistry);
            }
        }
        if let Some(region) = self.region() {
            if !between(region, "QM", "QZ")
                && !between(region, "XA", "XZ")
                && !is_in_from_str_slice_set(&REGIONS, region)
            {
                return Err(ValidationError::RegionNotInRegistry);
            }
        }
        for variant in self.variant_subtags() {
            if let Some(variant_prefixes) = find_in_str_slice_map(&VARIANTS, variant) {
                if !variant_prefixes
                    .split(' ')
                    .any(|prefix| self.serialization.starts_with(prefix))
                {
                    return Err(ValidationError::WrongVariantPrefix);
                }
            } else {
                return Err(ValidationError::VariantNotInRegistry);
            }
        }

        // There are no duplicate variant subtags.
        let with_duplicate_variant = self.variant_subtags().enumerate().any(|(id1, variant1)| {
            self.variant_subtags()
                .enumerate()
                .any(|(id2, variant2)| id1 != id2 && variant1 == variant2)
        });
        if with_duplicate_variant {
            return Err(ValidationError::DuplicateVariant);
        }

        // There are no duplicate singleton (extension) subtags.
        if let Some(extension) = self.extension() {
            let mut seen_extensions = AlphanumericLowerCharSet::new();
            let with_duplicate_extension = extension.split('-').any(|subtag| {
                if subtag.len() == 1 {
                    let extension = subtag.chars().next().unwrap();
                    if seen_extensions.contains(extension) {
                        true
                    } else {
                        seen_extensions.insert(extension);
                        false
                    }
                } else {
                    false
                }
            });
            if with_duplicate_extension {
                return Err(ValidationError::DuplicateExtension);
            }
        }

        Ok(())
    }

    /// Check if the language tag is valid according to
    /// [RFC 5646](https://tools.ietf.org/html/rfc5646#section-2.2.9).
    pub fn is_valid(&self) -> bool {
        self.validate().is_ok()
    }

    /// Returns the canonical version of the language tag following
    /// [RFC 5646 4.5](https://tools.ietf.org/html/rfc5646#section-4.5).
    ///
    /// It currently applies the following steps:
    ///
    /// * Grandfathered tags are replaced with the canonical version if possible.
    /// * Redundant tags are replaced with the canonical version if possible.
    /// * Extension languages are promoted to primary language.
    /// * Deprecated languages, scripts, regions and variants are replaced with modern equivalents.
    /// * Suppress-Script is applied to remove default script for a language (e.g. "en-Latn" is canonicalized as "en").
    /// * Variants are deduplicated
    ///
    ///
    /// # Errors
    ///
    /// If there is not a unique way to canonicalize the language tag
    /// a `ValidationError` variant will be returned.
    pub fn canonicalize(&self) -> Result<LanguageTag, ValidationError> {
        //We could not do anything for private use
        if self.serialization.starts_with("x-") {
            return Ok(self.clone());
        }

        // 2 Redundant or grandfathered tags are replaced by their 'Preferred-Value', if there is one.
        if is_in_str_slice_set(&GRANDFATHEREDS, &self.serialization) {
            return Ok(
                if let Some(preferred_value) =
                    find_in_str_slice_map(&GRANDFATHEREDS_PREFERRED_VALUE, &self.serialization)
                {
                    Self::parse(preferred_value).unwrap()
                } else {
                    self.clone()
                },
            );
        }
        if let Some(preferred_value) =
            find_in_str_slice_map(&REDUNDANTS_PREFERRED_VALUE, &self.serialization)
        {
            return Ok(Self::parse(preferred_value).unwrap());
        }
        //TODO: what if a redundant has a some extensions/private use?

        // 3.  Subtags are replaced by their 'Preferred-Value', if there is one.
        // Primary language
        let mut primary_language = self.primary_language();
        if let Some(preferred_value) =
            find_in_from_str_slice_map(&LANGUAGES_PREFERRED_VALUE, primary_language)
        {
            primary_language = preferred_value;
        }

        // Extended language
        // For extlangs, the original primary language subtag is also replaced if there is a primary language subtag in the 'Preferred-Value'.
        let mut extended_language = None;
        if let Some(extlang) = self.extended_language() {
            // We fail if there is more than one (no single possible canonicalization)
            if extlang.contains('-') {
                return Err(ValidationError::MultipleExtendedLanguageSubtags);
            }
            if let Some(preferred_value) =
                find_in_from_str_slice_map(&EXTLANGS_PREFERRED_VALUE, extlang)
            {
                primary_language = preferred_value;
            } else {
                extended_language = Some(extlang);
            }
        }

        let mut serialization = String::with_capacity(self.serialization.len());
        serialization.push_str(primary_language);
        let language_end = serialization.len();
        if let Some(extended_language) = extended_language {
            serialization.push('-');
            serialization.push_str(extended_language);
        }
        let extlang_end = serialization.len();

        // Script
        if let Some(script) = self.script() {
            let script =
                find_in_from_str_slice_map(&SCRIPTS_PREFERRED_VALUE, script).unwrap_or(script);

            // Suppress-Script
            let match_suppress_script =
                find_in_from_str_slice_map(&LANGUAGES_SUPPRESS_SCRIPT, primary_language)
                    .filter(|suppress_script| *suppress_script == script)
                    .is_some();
            if !match_suppress_script {
                serialization.push('-');
                serialization.push_str(script);
            }
        }
        let script_end = serialization.len();

        // Region
        if let Some(region) = self.region() {
            serialization.push('-');
            serialization.push_str(
                find_in_from_str_slice_map(&REGIONS_PREFERRED_VALUE, region).unwrap_or(region),
            );
        }
        let region_end = serialization.len();

        // Variant
        for variant in self.variant_subtags() {
            let variant =
                *find_in_str_slice_map(&VARIANTS_PREFERRED_VALUE, variant).unwrap_or(&variant);
            let variant_already_exists = serialization.split('-').any(|subtag| subtag == variant);
            if !variant_already_exists {
                serialization.push('-');
                serialization.push_str(variant);
            }
        }
        let variant_end = serialization.len();

        //Extension
        // 1.  Extension sequences are ordered into case-insensitive ASCII order by singleton subtags
        if self.extension().is_some() {
            let mut extensions: Vec<_> = self.extension_subtags().collect();
            extensions.sort_unstable();
            for (k, v) in extensions {
                serialization.push('-');
                serialization.push(k);
                serialization.push('-');
                serialization.push_str(v);
            }
        }
        let extension_end = serialization.len();

        //Private use
        if let Some(private_use) = self.private_use() {
            serialization.push('-');
            serialization.push_str(private_use);
        }

        Ok(LanguageTag {
            serialization,
            language_end,
            extlang_end,
            script_end,
            region_end,
            variant_end,
            extension_end,
        })
    }

    /// Matches language tags. The first language acts as a language range, the second one is used
    /// as a normal language tag. None fields in the language range are ignored. If the language
    /// tag has more extlangs than the range these extlangs are ignored. Matches are
    /// case-insensitive.
    ///
    /// For example the range `en-GB` matches only `en-GB` and `en-Arab-GB` but not `en`.
    /// The range `en` matches all language tags starting with `en` including `en`, `en-GB`,
    /// `en-Arab` and `en-Arab-GB`.
    ///
    /// # Panics
    /// If the language range has extensions or private use tags.
    ///
    /// # Examples
    /// ```rust
    /// use language_tags::LanguageTag;
    ///
    /// let range_italian = LanguageTag::parse("it").unwrap();
    /// let tag_german = LanguageTag::parse("de").unwrap();
    /// let tag_italian_switzerland = LanguageTag::parse("it-CH").unwrap();
    /// assert!(!range_italian.matches(&tag_german));
    /// assert!(range_italian.matches(&tag_italian_switzerland));
    ///
    /// let range_spanish_brazil = LanguageTag::parse("es-BR").unwrap();
    /// let tag_spanish = LanguageTag::parse("es").unwrap();
    /// assert!(!range_spanish_brazil.matches(&tag_spanish));
    /// ```
    pub fn matches(&self, other: &LanguageTag) -> bool {
        fn matches_option(a: Option<&str>, b: Option<&str>) -> bool {
            match (a, b) {
                (Some(a), Some(b)) => a == b,
                (None, _) => true,
                (_, None) => false,
            }
        }
        fn matches_iter<'a>(
            a: impl Iterator<Item = &'a str>,
            b: impl Iterator<Item = &'a str>,
        ) -> bool {
            a.zip(b).all(|(x, y)| x == y)
        }
        assert!(self.is_language_range());
        self.full_language() == other.full_language()
            && matches_option(self.script(), other.script())
            && matches_option(self.region(), other.region())
            && matches_iter(self.variant_subtags(), other.variant_subtags())
    }

    /// Checks if it is a language range, meaning that there are no extension and privateuse tags.
    pub fn is_language_range(&self) -> bool {
        self.extension().is_none() && self.private_use().is_none()
    }
}

impl FromStr for LanguageTag {
    type Err = ParseError;

    #[inline]
    fn from_str(input: &str) -> Result<Self, ParseError> {
        Self::parse(input)
    }
}

impl fmt::Display for LanguageTag {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Builds a tag from its primary language
fn tag_from_primary_language(tag: impl Into<String>) -> LanguageTag {
    let serialization = tag.into();
    let end = serialization.len();
    LanguageTag {
        serialization,
        language_end: end,
        extlang_end: end,
        script_end: end,
        region_end: end,
        variant_end: end,
        extension_end: end,
    }
}

/// Handles normal tags.
fn parse_language_tag(input: &str) -> Result<LanguageTag, ParseError> {
    #[derive(PartialEq, Eq)]
    enum State {
        Start,
        AfterLanguage,
        AfterExtLang,
        AfterScript,
        AfterRegion,
        InExtension { expected: bool },
        InPrivateUse { expected: bool },
    }

    let mut serialization = String::with_capacity(input.len());

    let mut state = State::Start;
    let mut language_end = 0;
    let mut extlang_end = 0;
    let mut script_end = 0;
    let mut region_end = 0;
    let mut variant_end = 0;
    let mut extension_end = 0;
    let mut extlangs_count = 0;
    for (subtag, end) in SubTagIterator::new(input) {
        if subtag.is_empty() {
            // All subtags have a maximum length of eight characters.
            return Err(ParseError::EmptySubtag);
        }
        if subtag.len() > 8 {
            // All subtags have a maximum length of eight characters.
            return Err(ParseError::SubtagTooLong);
        }
        if state == State::Start {
            // Primary language
            if subtag.len() < 2 || !is_alphabetic(subtag) {
                return Err(ParseError::InvalidLanguage);
            }
            language_end = end;
            serialization.extend(to_lowercase(subtag));
            if subtag.len() < 4 {
                // extlangs are only allowed for short language tags
                state = State::AfterLanguage;
            } else {
                state = State::AfterExtLang;
            }
        } else if let State::InPrivateUse { .. } = state {
            if !is_alphanumeric(subtag) {
                return Err(ParseError::InvalidSubtag);
            }
            serialization.push('-');
            serialization.extend(to_lowercase(subtag));
            state = State::InPrivateUse { expected: false };
        } else if subtag == "x" || subtag == "X" {
            // We make sure extension is found
            if let State::InExtension { expected: true } = state {
                return Err(ParseError::EmptyExtension);
            }
            serialization.push('-');
            serialization.push('x');
            state = State::InPrivateUse { expected: true };
        } else if subtag.len() == 1 && is_alphanumeric(subtag) {
            // We make sure extension is found
            if let State::InExtension { expected: true } = state {
                return Err(ParseError::EmptyExtension);
            }
            let extension_tag = subtag.chars().next().unwrap().to_ascii_lowercase();
            serialization.push('-');
            serialization.push(extension_tag);
            state = State::InExtension { expected: true };
        } else if let State::InExtension { .. } = state {
            if !is_alphanumeric(subtag) {
                return Err(ParseError::InvalidSubtag);
            }
            extension_end = end;
            serialization.push('-');
            serialization.extend(to_lowercase(subtag));
            state = State::InExtension { expected: false };
        } else if state == State::AfterLanguage && subtag.len() == 3 && is_alphabetic(subtag) {
            extlangs_count += 1;
            if extlangs_count > 3 {
                return Err(ParseError::TooManyExtlangs);
            }
            // valid extlangs
            extlang_end = end;
            serialization.push('-');
            serialization.extend(to_lowercase(subtag));
        } else if (state == State::AfterLanguage || state == State::AfterExtLang)
            && subtag.len() == 4
            && is_alphabetic(subtag)
        {
            // Script
            script_end = end;
            serialization.push('-');
            serialization.extend(to_uppercase_first(subtag));
            state = State::AfterScript;
        } else if (state == State::AfterLanguage
            || state == State::AfterExtLang
            || state == State::AfterScript)
            && (subtag.len() == 2 && is_alphabetic(subtag)
                || subtag.len() == 3 && is_numeric(subtag))
        {
            // Region
            region_end = end;
            serialization.push('-');
            serialization.extend(to_uppercase(subtag));
            state = State::AfterRegion;
        } else if (state == State::AfterLanguage
            || state == State::AfterExtLang
            || state == State::AfterScript
            || state == State::AfterRegion)
            && is_alphanumeric(subtag)
            && (subtag.len() >= 5 && is_alphabetic(&subtag[0..1])
                || subtag.len() >= 4 && is_numeric(&subtag[0..1]))
        {
            // Variant
            variant_end = end;
            serialization.push('-');
            serialization.extend(to_lowercase(subtag));
            state = State::AfterRegion;
        } else {
            return Err(ParseError::InvalidSubtag);
        }
    }

    //We make sure we are in a correct final state
    if let State::InExtension { expected: true } = state {
        return Err(ParseError::EmptyExtension);
    }
    if let State::InPrivateUse { expected: true } = state {
        return Err(ParseError::EmptyPrivateUse);
    }

    //We make sure we have not skipped anyone
    if extlang_end < language_end {
        extlang_end = language_end;
    }
    if script_end < extlang_end {
        script_end = extlang_end;
    }
    if region_end < script_end {
        region_end = script_end;
    }
    if variant_end < region_end {
        variant_end = region_end;
    }
    if extension_end < variant_end {
        extension_end = variant_end;
    }

    Ok(LanguageTag {
        serialization,
        language_end,
        extlang_end,
        script_end,
        region_end,
        variant_end,
        extension_end,
    })
}

struct ExtensionsIterator<'a> {
    input: &'a str,
}

impl<'a> ExtensionsIterator<'a> {
    fn new(input: &'a str) -> Self {
        Self { input }
    }
}

impl<'a> Iterator for ExtensionsIterator<'a> {
    type Item = (char, &'a str);

    fn next(&mut self) -> Option<(char, &'a str)> {
        let mut parts_iterator = self.input.split_terminator('-');
        let singleton = parts_iterator.next()?.chars().next().unwrap();
        let mut content_size: usize = 2;
        for part in parts_iterator {
            if part.len() == 1 {
                let content = &self.input[2..content_size - 1];
                self.input = &self.input[content_size..];
                return Some((singleton, content));
            } else {
                content_size += part.len() + 1;
            }
        }
        let result = self.input.get(2..).map(|content| (singleton, content));
        self.input = "";
        result
    }
}

struct SubTagIterator<'a> {
    split: Split<'a, char>,
    position: usize,
}

impl<'a> SubTagIterator<'a> {
    fn new(input: &'a str) -> Self {
        Self {
            split: input.split('-'),
            position: 0,
        }
    }
}

impl<'a> Iterator for SubTagIterator<'a> {
    type Item = (&'a str, usize);

    fn next(&mut self) -> Option<(&'a str, usize)> {
        let tag = self.split.next()?;
        let tag_end = self.position + tag.len();
        self.position = tag_end + 1;
        Some((tag, tag_end))
    }
}

struct AlphanumericLowerCharSet {
    alphabetic_set: [bool; 26],
    numeric_set: [bool; 10],
}

impl AlphanumericLowerCharSet {
    fn new() -> Self {
        Self {
            alphabetic_set: [false; 26],
            numeric_set: [false; 10],
        }
    }

    fn contains(&mut self, c: char) -> bool {
        if c.is_ascii_digit() {
            self.numeric_set[char_sub(c, '0')]
        } else if c.is_ascii_lowercase() {
            self.alphabetic_set[char_sub(c, 'a')]
        } else if c.is_ascii_uppercase() {
            self.alphabetic_set[char_sub(c, 'A')]
        } else {
            false
        }
    }

    fn insert(&mut self, c: char) {
        if c.is_ascii_digit() {
            self.numeric_set[char_sub(c, '0')] = true
        } else if c.is_ascii_lowercase() {
            self.alphabetic_set[char_sub(c, 'a')] = true
        } else if c.is_ascii_uppercase() {
            self.alphabetic_set[char_sub(c, 'A')] = true
        }
    }
}

fn char_sub(c1: char, c2: char) -> usize {
    (c1 as usize) - (c2 as usize)
}

fn is_alphabetic(s: &str) -> bool {
    s.chars().all(|x| x.is_ascii_alphabetic())
}

fn is_numeric(s: &str) -> bool {
    s.chars().all(|x| x.is_ascii_digit())
}

fn is_alphanumeric(s: &str) -> bool {
    s.chars().all(|x| x.is_ascii_alphanumeric())
}

fn is_alphanumeric_or_dash(s: &str) -> bool {
    s.chars().all(|x| x.is_ascii_alphanumeric() || x == '-')
}

fn to_uppercase(s: &'_ str) -> impl Iterator<Item = char> + '_ {
    s.chars().map(|c| c.to_ascii_uppercase())
}

// Beware: panics if s.len() == 0 (should never happen in our code)
fn to_uppercase_first(s: &'_ str) -> impl Iterator<Item = char> + '_ {
    let mut chars = s.chars();
    once(chars.next().unwrap().to_ascii_uppercase()).chain(chars.map(|c| c.to_ascii_lowercase()))
}

fn to_lowercase(s: &'_ str) -> impl Iterator<Item = char> + '_ {
    s.chars().map(|c| c.to_ascii_lowercase())
}

/// Errors returned by `LanguageTag` parsing
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ParseError {
    /// If an extension subtag is present, it must not be empty.
    EmptyExtension,
    /// If the `x` subtag is present, it must not be empty.
    EmptyPrivateUse,
    /// The langtag contains a char that is not A-Z, a-z, 0-9 or the dash.
    ForbiddenChar,
    /// A subtag fails to parse, it does not match any other subtags.
    InvalidSubtag,
    /// The given language subtag is invalid.
    InvalidLanguage,
    /// A subtag may be eight characters in length at maximum.
    SubtagTooLong,
    /// A subtag should not be empty.
    EmptySubtag,
    /// At maximum three extlangs are allowed, but zero to one extlangs are preferred.
    TooManyExtlangs,
}

impl Error for ParseError {}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::EmptyExtension => "if an extension subtag is present, it must not be empty",
            Self::EmptyPrivateUse => "if the `x` subtag is present, it must not be empty",
            Self::ForbiddenChar => "the langtag contains a char not allowed",
            Self::InvalidSubtag => "a subtag fails to parse, it does not match any other subtags",
            Self::InvalidLanguage => "the given language subtag is invalid",
            Self::SubtagTooLong => "a subtag may be eight characters in length at maximum",
            Self::EmptySubtag => "a subtag should not be empty",
            Self::TooManyExtlangs => "at maximum three extlangs are allowed",
        })
    }
}

/// Errors returned by the `LanguageTag` validation
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ValidationError {
    /// The same variant subtag is only allowed once in a tag.
    DuplicateVariant,
    /// The same extension subtag is only allowed once in a tag before the private use part.
    DuplicateExtension,
    /// only one extended language subtag is allowed
    MultipleExtendedLanguageSubtags,
    /// The primary language is not in the IANA Language Subtag Registry
    PrimaryLanguageNotInRegistry,
    /// The extended language is not in the IANA Language Subtag Registry
    ExtendedLanguageNotInRegistry,
    /// The script is not in the IANA Language Subtag Registry
    ScriptNotInRegistry,
    /// The region is not in the IANA Language Subtag Registry
    RegionNotInRegistry,
    /// A variant is not in the IANA Language Subtag Registry
    VariantNotInRegistry,
    /// The primary language is not the expected extended language prefix from the IANA Language Subtag Registry
    WrongExtendedLanguagePrefix,
    /// The language tag has not one of the expected variant prefix from the IANA Language Subtag Registry
    WrongVariantPrefix,
}

impl Error for ValidationError {}

impl fmt::Display for ValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::DuplicateVariant => {
                "the same variant subtag is only allowed once in a tag"
            }
            Self::DuplicateExtension => {
                "the same extension subtag is only allowed once in a tag"
            }
            Self::MultipleExtendedLanguageSubtags => {
                "only one extended language subtag is allowed"
            }
            Self::PrimaryLanguageNotInRegistry => {
                "the primary language is not in the IANA Language Subtag Registry"
            }
            Self::ExtendedLanguageNotInRegistry => {
                "the extended language is not in the IANA Language Subtag Registry"
            }
            Self::ScriptNotInRegistry => {
                "the script is not in the IANA Language Subtag Registry"
            }
            Self::RegionNotInRegistry => {
                "the region is not in the IANA Language Subtag Registry"
            }
            Self::VariantNotInRegistry => {
                "a variant is not in the IANA Language Subtag Registry"
            }
            Self::WrongExtendedLanguagePrefix => {
                "the primary language is not the expected extended language prefix from the IANA Language Subtag Registry"
            }
            Self::WrongVariantPrefix => {
                "the language tag has not one of the expected variant prefix from the IANA Language Subtag Registry"
            }
        })
    }
}

fn between<T: Ord>(value: T, start: T, end: T) -> bool {
    start <= value && value <= end
}

fn is_in_str_slice_set(slice: &[&'static str], value: &str) -> bool {
    slice.binary_search(&value).is_ok()
}

fn is_in_from_str_slice_set<T: Copy + Ord + FromStr>(slice: &[T], value: &str) -> bool {
    match T::from_str(value) {
        Ok(key) => slice.binary_search(&key).is_ok(),
        Err(_) => false,
    }
}

fn find_in_str_slice_map<'a, V>(slice: &'a [(&'static str, V)], value: &str) -> Option<&'a V> {
    if let Ok(position) = slice.binary_search_by_key(&value, |t| t.0) {
        Some(&slice[position].1)
    } else {
        None
    }
}

fn find_in_from_str_slice_map<'a, K: Copy + Ord + FromStr, V: Deref<Target = str>>(
    slice: &'a [(K, V)],
    value: &str,
) -> Option<&'a str> {
    if let Ok(position) = slice.binary_search_by_key(&K::from_str(value).ok()?, |t| t.0) {
        Some(&*slice[position].1)
    } else {
        None
    }
}
