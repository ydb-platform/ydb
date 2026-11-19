//! Global locale instances and system inspection.
//!
//! This is an auxiliary crate for i18n solutions that:
//!
//!  - Holds the appropriate default instances of locale.
//!  - Inspects the system for the initial values.
//!
//! You don't want to use it directly, but instead use an internationalisation crate like [locale].
//!
//! This crate is separate and intentionally minimal so that multiple i18n crates or multiple
//! versions of one that get into the application still share the current locale setting.
//!
//! [locale]: https://crates.io/crates/locale

#[macro_use]
extern crate lazy_static;

extern crate regex;

#[cfg(target_os = "macos")]
#[macro_use]
extern crate objc;

use regex::Regex;
use std::borrow::{Borrow,Cow};
use std::cell::RefCell;
use std::convert::AsRef;
use std::fmt;
use std::sync::Mutex;

// ------------------------------ LANGUAGE RANGE ---------------------------------

/// Language and culture identifier.
///
/// This object holds a [RFC4647] extended language range.
///
/// The internal data may be owned or shared from object with lifetime `'a`. The lifetime can be
/// extended using the `into_static()` method, which internally clones the data as needed.
///
/// # Syntax
///
/// The range is composed of `-`-separated alphanumeric subtags, possibly replaced by `*`s. It
/// might be empty.
///
/// In agreement with [RFC4647], this object only requires that the tag matches:
///
/// ```ebnf
/// language_tag = (alpha{1,8} | "*")
///                ("-" (alphanum{1,8} | "*"))*
/// ```
///
/// The exact interpretation is up to the downstream localization provider, but it expected that
/// it will be matched against a normalized [RFC5646] language tag, which has the structure:
///
/// ```ebnf
/// language_tag    = language
///                   ("-" script)?
///                   ("-" region)?
///                   ("-" variant)*
///                   ("-" extension)*
///                   ("-" private)?
///
/// language        = alpha{2,3} ("-" alpha{3}){0,3}
///
/// script          = aplha{4}
///
/// region          = alpha{2}
///                 | digit{3}
///
/// variant         = alphanum{5,8}
///                 | digit alphanum{3}
///
/// extension       = [0-9a-wyz] ("-" alphanum{2,8})+
///
/// private         = "x" ("-" alphanum{1,8})+
/// ```
///
///  * `language` is an [ISO639] 2-letter or, where not defined, 3-letter code. A code for
///     macro-language might be followed by code of specific dialect.
///  * `script` is an [ISO15924] 4-letter code.
///  * `region` is either an [ISO3166] 2-letter code or, for areas other than countries, [UN M.49]
///    3-digit numeric code.
///  * `variant` is a string indicating variant of the language.
///  * `extension` and `private` define additional options. The private part has same structure as
///    the Unicode [`-u-` extension][u_ext]. Available options are documented for the facets that
///    use them.
///
/// The values obtained by inspecting the system are normalized according to those rules.
///
/// The content will be case-normalized as recommended in [RFC5646] §2.1.1, namely:
///
///  * `language` is written in lowercase,
///  * `script` is written with first capital,
///  * `country` is written in uppercase and
///  * all other subtags are written in lowercase.
///
/// When detecting system configuration, additional options that may be generated under the
/// [`-u-` extension][u_ext] currently are:
///
/// * `cf` — Currency format (`account` for parenthesized negative values, `standard` for minus
///   sign).
/// * `fw` — First day of week (`mon` to `sun`).
/// * `hc` — Hour cycle (`h12` for 1–12, `h23` for 0–23).
/// * `ms` — Measurement system (`metric` or `ussystem`).
/// * `nu` — Numbering system—only decimal systems are currently used.
/// * `va` — Variant when locale is specified in Unix format and the tag after `@` does not
///   correspond to any variant defined in [Language subtag registry].
///
/// And under the `-x-` extension, following options are defined:
///
/// * `df` — Date format:
///
///     * `iso`: Short date should be in ISO format of `yyyy-MM-dd`.
///
///     For example `-df-iso`.
///
/// * `dm` — Decimal separator for monetary:
///
///     Followed by one or more Unicode codepoints in hexadecimal. For example `-dm-002d` means to
///     use comma.
///
/// * `ds` — Decimal separator for numbers:
///
///     Followed by one or more Unicode codepoints in hexadecimal. For example `-ds-002d` means to
///     use comma.
///
/// * `gm` — Group (thousand) separator for monetary:
///
///     Followed by one or more Unicode codepoints in hexadecimal. For example `-dm-00a0` means to
///     use non-breaking space.
///
/// * `gs` — Group (thousand) separator for numbers:
///
///     Followed by one or more Unicode codepoints in hexadecimal. For example `-ds-00a0` means to
///     use non-breaking space.
///
/// * `ls` — List separator:
///
///     Followed by one or more Unicode codepoints in hexadecimal. For example, `-ds-003b` means to
///     use a semicolon.
///
/// [RFC5646]: https://www.rfc-editor.org/rfc/rfc5646.txt
/// [RFC4647]: https://www.rfc-editor.org/rfc/rfc4647.txt
/// [ISO639]: https://en.wikipedia.org/wiki/ISO_639
/// [ISO15924]: https://en.wikipedia.org/wiki/ISO_15924
/// [ISO3166]: https://en.wikipedia.org/wiki/ISO_3166
/// [UN M.49]: https://en.wikipedia.org/wiki/UN_M.49
/// [u_ext]: http://www.unicode.org/reports/tr35/#u_Extension
/// [Language subtag registry]: https://www.iana.org/assignments/language-subtag-registry/language-subtag-registry
#[derive(Clone,Debug,Eq,Hash,PartialEq)]
pub struct LanguageRange<'a> {
    language: Cow<'a, str>
}

lazy_static! {
    static ref REGULAR_LANGUAGE_RANGE_REGEX: Regex = Regex::new(r"(?x) ^
        (?P<language> (?:
            [[:alpha:]]{2,3} (?: - [[:alpha:]]{3} ){0,3}
            | \* ))
        (?P<script> - (?: [[:alpha:]]{4} | \* ))?
        (?P<region> - (?: [[:alpha:]]{2} | [[:digit:]]{3} | \* ))?
        (?P<rest> (?: - (?: [[:alnum:]]{1,8} | \* ))*)
    $ ").unwrap();
    static ref LANGUAGE_RANGE_REGEX: Regex = Regex::new(r"(?x) ^
        (?: [[:alpha:]]{1,8} | \* )
        (?: - (?: [[:alnum:]]{1,8} | \* ))*
    $ ").unwrap();
    static ref UNIX_INVARIANT_REGEX: Regex = Regex::new(r"(?ix) ^
        (?: c | posix )
        (?: \. (?: [0-9a-zA-Z-]{1,20} ))?
    $ ").unwrap();
    static ref UNIX_TAG_REGEX: Regex = Regex::new(r"(?ix) ^
        (?P<language> [[:alpha:]]{2,3} )
        (?: _  (?P<region> [[:alpha:]]{2} | [[:digit:]]{3} ))?
        (?: \. (?P<encoding> [0-9a-zA-Z-]{1,20} ))?
        (?: @  (?P<variant> [[:alnum:]]{1,20} ))?
    $ ").unwrap();
}

fn is_owned<'a, T: ToOwned + ?Sized>(c: &Cow<'a, T>) -> bool {
    match *c {
        Cow::Owned(_) => true,
        Cow::Borrowed(_) => false,
    }
}

fn canon_lower<'a>(o: Option<&'a str>) -> Cow<'a, str> {
    match o {
        None => Cow::Borrowed(""),
        Some(s) =>
            if s.chars().any(char::is_uppercase) {
                Cow::Owned(s.to_ascii_lowercase())
            } else {
                Cow::Borrowed(s)
            },
    }
}

fn canon_script<'a>(o: Option<&'a str>) -> Cow<'a, str> {
    assert!(o.map_or(true, |s| s.len() >= 2 && &s[0..1] == "-"));
    match o {
        None => Cow::Borrowed(""),
        Some(s) =>
            if s[1..2].chars().next().unwrap().is_uppercase() &&
               s[2..].chars().all(char::is_lowercase) {
                Cow::Borrowed(s)
            } else {
                Cow::Owned(String::from("-") +
                           s[1..2].to_ascii_uppercase().as_ref() +
                           s[2..].to_ascii_lowercase().as_ref())
            },
    }
}

fn canon_upper<'a>(o: Option<&'a str>) -> Cow<'a, str> {
    assert!(o.map_or(true, |s| s.len() > 1 && &s[0..1] == "-"));
    match o {
        None => Cow::Borrowed(""),
        Some(s) =>
            if s.chars().any(char::is_lowercase) {
                Cow::Owned(s.to_ascii_uppercase())
            } else {
                Cow::Borrowed(s)
            },
    }
}

impl<'a> LanguageRange<'a> {
    /// Construct LanguageRange from string, with normalization.
    ///
    /// LanguageRange must follow the [RFC4647] syntax.
    /// It will be case-normalized as recommended in [RFC5646] §2.1.1, namely:
    ///
    ///  * `language`, if recognized, is written in lowercase,
    ///  * `script`, if recognized, is written with first capital,
    ///  * `country`, if recognized, is written in uppercase and
    ///  * all other subtags are written in lowercase.
    ///
    /// [RFC5646]: https://www.rfc-editor.org/rfc/rfc5646.txt
    /// [RFC4647]: https://www.rfc-editor.org/rfc/rfc4647.txt
    pub fn new(lt: &'a str) -> Result<LanguageRange> {
        if lt == "" {
            return Ok(LanguageRange {
                language: Cow::Borrowed(lt),
            });
        } else if let Some(caps) = REGULAR_LANGUAGE_RANGE_REGEX.captures(lt) {
            let language = canon_lower(caps.name("language").map(|m| m.as_str()));
            let script = canon_script(caps.name("script").map(|m| m.as_str()));
            let region = canon_upper(caps.name("region").map(|m| m.as_str()));
            let rest = canon_lower(caps.name("rest").map(|m| m.as_str()));
            if is_owned(&language) ||
                is_owned(&script) ||
                is_owned(&region) ||
                is_owned(&rest)
            {
                return Ok(LanguageRange {
                    language: Cow::Owned(
                        language.into_owned() +
                        script.borrow() +
                        region.borrow() +
                        rest.borrow()),
                });
            } else {
                return Ok(LanguageRange {
                    language: Cow::Borrowed(lt),
                });
            }
        } else if LANGUAGE_RANGE_REGEX.is_match(lt) {
            return Ok(LanguageRange {
                language: canon_lower(Some(lt)),
            });
        } else {
            return Err(Error::NotWellFormed);
        }
    }

    /// Return LanguageRange for the invariant locale.
    ///
    /// Invariant language is identified simply by empty string.
    pub fn invariant() -> LanguageRange<'static> {
        LanguageRange { language: Cow::Borrowed("") }
    }

    /// Clone the internal data to extend lifetime.
    pub fn into_static(self) -> LanguageRange<'static> {
        LanguageRange {
            language: Cow::Owned(self.language.into_owned())
        }
    }

    /// Create new instance sharing the internal data.
    pub fn to_shared(&'a self) -> Self {
        LanguageRange {
            language: Cow::Borrowed(self.language.borrow())
        }
    }

    /// Create language tag from Unix/Linux/GNU locale tag.
    ///
    /// Unix locale tags have the form
    ///
    /// > *language* [ `_` *region* ] [ `.` *encoding* ] [ `@` *variant* ]
    ///
    /// The *language* and *region* have the same format as RFC5646. *Encoding* is not relevant
    /// here, since Rust always uses Utf-8. That leaves *variant*, which is unfortunately rather
    /// free-form. So this function will translate known variants to corresponding RFC5646 subtags
    /// and represent anything else with Unicode POSIX variant (`-u-va-`) extension.
    ///
    /// Note: This function is public here for benefit of applications that may come across this
    /// kind of tags from other sources than system configuration.
    pub fn from_unix(s: &str) -> Result<LanguageRange<'static>> {
        if let Some(caps) = UNIX_TAG_REGEX.captures(s) {
            let src_variant = caps.name("variant").map(|m| m.as_str()).unwrap_or("").to_ascii_lowercase();
            let mut res = caps.name("language").map(|m| m.as_str()).unwrap().to_ascii_lowercase();
            let region = caps.name("region").map(|m| m.as_str()).unwrap_or("");
            let mut script = "";
            let mut variant = "";
            let mut uvariant = "";
            match src_variant.as_ref() {
            // Variants seen in the wild in GNU LibC (via http://lh.2xlibre.net/) or in Debian
            // GNU/Linux Stretch system. Treatment of things not found in RFC5646 subtag registry
            // (http://www.iana.org/assignments/language-subtag-registry/language-subtag-registry)
            // or CLDR according to notes at https://wiki.openoffice.org/wiki/LocaleMapping.
            // Dialects:
                // aa_ER@saaho - NOTE: Can't be found under that name in RFC5646 subtag registry,
                // but there is language Saho with code ssy, which is likely that thing.
                "saaho" if res == "aa" => res = String::from("ssy"),
            // Scripts:
                // @arabic
                "arabic" => script = "Arab",
                // @cyrillic
                "cyrl" => script = "Cyrl",
                "cyrillic" => script = "Cyrl",
                // @devanagari
                "devanagari" => script = "Deva",
                // @hebrew
                "hebrew" => script = "Hebr",
                // tt@iqtelif
                // Neither RFC5646 subtag registry nor CLDR knows anything about this, but as best
                // as I can tell it is Tatar name for Latin (default is Cyrillic).
                "iqtelif" => script = "Latn",
                // @Latn
                "latn" => script = "Latn",
                // @latin
                "latin" => script = "Latn",
                // en@shaw
                "shaw" => script = "Shaw",
            // Variants:
                // sr@ijekavianlatin
                "ijekavianlatin" => {
                    script = "Latn";
                    variant = "ijekavsk";
                },
                // sr@ije
                "ije" => variant = "ijekavsk",
                // sr@ijekavian
                "ijekavian" => variant = "ijekavsk",
                // ca@valencia
                "valencia" => variant = "valencia",
            // Currencies:
                // @euro - NOTE: We follow suite of Java and Openoffice and ignore it, because it
                // is default for all locales where it sometimes appears now, and because we use
                // explicit currency in monetary formatting anyway.
                "euro" => {},
            // Collation:
                // gez@abegede - NOTE: This is collation, but CLDR does not have any code for it,
                // so we for the moment leave it fall through as -u-va- instead of -u-co-.
            // Anything else:
                // en@boldquot, en@quot, en@piglatin - just randomish stuff
                // @cjknarrow - beware, it's gonna end up as -u-va-cjknarro due to lenght limit
                s if s.len() <= 8 => uvariant = &*s,
                s => uvariant = &s[0..8], // the subtags are limited to 8 chars, but some are longer
            };
            if script != "" {
                res.push('-');
                res.push_str(script);
            }
            if region != "" {
                res.push('-');
                res.push_str(&*region.to_ascii_uppercase());
            }
            if variant != "" {
                res.push('-');
                res.push_str(variant);
            }
            if uvariant != "" {
                res.push_str("-u-va-");
                res.push_str(uvariant);
            }
            return Ok(LanguageRange {
                language: Cow::Owned(res)
            });
        } else if UNIX_INVARIANT_REGEX.is_match(s) {
            return Ok(LanguageRange::invariant())
        } else {
            return Err(Error::NotWellFormed);
        }
    }
}

impl<'a> AsRef<str> for LanguageRange<'a> {
    fn as_ref(&self) -> &str {
        self.language.as_ref()
    }
}

impl<'a> fmt::Display for LanguageRange<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.language.fmt(f)
    }
}

// -------------------------------- LOCALE -------------------------------------

/// Locale configuration.
///
/// Users may accept several languages in some order of preference and may want to use rules from
/// different culture for some particular aspect of the program behaviour, and operating systems
/// allow them to specify this (to various extent).
///
/// The `Locale` objects represent the user configuration. They contain:
///
///  - The primary `LanguageRange`.
///  - Optional category-specific overrides.
///  - Optional fallbacks in case data (usually translations) for the primary language are not
///    available.
///
/// The set of categories is open-ended. The `locale` crate uses five well-known categories
/// `messages`, `numeric`, `time`, `collate` and `monetary`, but some systems define additional
/// ones (GNU Linux has additionally `paper`, `name`, `address`, `telephone` and `measurement`) and
/// these are provided in the user default `Locale` and other libraries can use them.
///
/// `Locale` is represented by a `,`-separated sequence of tags in `LanguageRange` syntax, where
/// all except the first one may be preceded by category name and `=` sign.
///
/// The first tag indicates the default locale, the tags prefixed by category names indicate
/// _overrides_ for those categories and the remaining tags indicate fallbacks.
///
/// Note that a syntactically valid value of HTTP `Accept-Language` header is a valid `Locale`. Not
/// the other way around though due to the presence of category selectors.
// TODO: Interning
#[derive(Clone,Debug,Eq,Hash,PartialEq)]
pub struct Locale {
    // TODO: Intern the string for performance reasons
    // XXX: Store pre-split to LanguageTags?
    inner: String,
}

lazy_static! {
    static ref LOCALE_ELEMENT_REGEX: Regex = Regex::new(r"(?ix) ^
        (?: (?P<category> [[:alpha:]]{1,20} ) = )?
        (?P<tag> (?: [[:alnum:]] | - | \* )+ )
    $ ").unwrap();
}

impl Locale {
    /// Obtain the user default locale.
    ///
    /// This is the locale indicated by operating environment.
    pub fn user_default() -> Locale {
        USER_LOCALE.clone()
    }

    /// Obtain the global default locale.
    ///
    /// The global default for `current()` locale. Defaults to `user_default()`.
    pub fn global_default() -> Locale {
        GLOBAL_LOCALE.lock().unwrap().clone()
    }

    /// Change the global default locale.
    ///
    /// Setting this overrides the default for new threads and threads that didn't do any
    /// locale-aware operation yet.
    pub fn set_global_default(lb: Locale) {
        *GLOBAL_LOCALE.lock().unwrap() = lb;
    }

    /// Obtain the current locale of current thread.
    ///
    /// Defaults to `global_default()` on first use in each thread.
    pub fn current() -> Locale {
        CURRENT_LOCALE.with(|l| l.borrow().clone())
    }

    /// Change the current locale of current thread.
    pub fn set_current(lb: Locale) {
        CURRENT_LOCALE.with(|l| *l.borrow_mut() = lb);
    }

    /// Construct locale from the string representation.
    ///
    /// `Locale` is represented by a `,`-separated sequence of tags in `LanguageRange` syntax, where
    /// all except the first one may be preceded by category name and `=` sign.
    ///
    /// The first tag indicates the default locale, the tags prefixed by category names indicate
    /// _overrides_ for those categories and the remaining tags indicate fallbacks.
    pub fn new(s: &str) -> Result<Locale> {
        let mut i = s.split(',');
        let mut res = Locale::from(
            try!(LanguageRange::new(
                    i.next().unwrap()))); // NOTE: split "" is (""), not ()
        for t in i {
            if let Some(caps) = LOCALE_ELEMENT_REGEX.captures(t) {
                let tag = try!(LanguageRange::new(
                        try!(caps.name("tag").map(|m| m.as_str()).ok_or(Error::NotWellFormed))));
                match caps.name("category").map(|m| m.as_str()) {
                    Some(cat) => res.add_category(cat.to_ascii_lowercase().as_ref(), &tag),
                    None => res.add(&tag),
                }
            } else {
                return Err(Error::NotWellFormed);
            }
        }
        return Ok(res);
    }

    /// Construct invariant locale.
    ///
    /// Invariant locale is represented simply with empty string.
    pub fn invariant() -> Locale {
        Locale::from(LanguageRange::invariant())
    }

    /// Append fallback language tag.
    ///
    /// Adds fallback to the end of the list.
    pub fn add(&mut self, tag: &LanguageRange) {
        for i in self.inner.split(',') {
            if i == tag.as_ref() {
                return; // don't add duplicates
            }
        }
        self.inner.push_str(",");
        self.inner.push_str(tag.as_ref());
    }

    /// Append category override.
    ///
    /// Appending new override for a category that already has one will not replace the existing
    /// override. This might change in future.
    pub fn add_category(&mut self, category: &str, tag: &LanguageRange) {
        if self.inner.split(',').next().unwrap() == tag.as_ref() {
            return; // don't add useless override equal to the primary tag
        }
        for i in self.inner.split(',') {
            if i.starts_with(category) &&
                    i[category.len()..].starts_with("=") &&
                    &i[category.len() + 1..] == tag.as_ref() {
                return; // don't add duplicates
            }
        }
        self.inner.push_str(",");
        self.inner.push_str(category);
        self.inner.push_str("=");
        self.inner.push_str(tag.as_ref());
    }

    /// Iterate over `LanguageRange`s in this `Locale`.
    ///
    /// Returns tuples of optional category (as string) and corresponding `LanguageRange`. All tags
    /// in the list are returned, in order of preference.
    ///
    /// The iterator is guaranteed to return at least one value.
    pub fn tags<'a>(&'a self) -> Tags<'a> {
        Tags { tags: self.inner.split(","), }
    }

    /// Iterate over `LanguageRange`s in this `Locale` applicable to given category.
    ///
    /// Returns `LanguageRange`s in the `Locale` that are applicable to provided category. The tags
    /// are returned in order of preference, which means the category-specific ones first and then
    /// the generic ones.
    ///
    /// The iterator is guaranteed to return at least one value.
    pub fn tags_for<'a, 'c>(&'a self, category: &'c str) -> TagsFor<'a, 'c> {
        let mut tags = self.inner.split(",");
        while let Some(s) = tags.clone().next() {
            if s.starts_with(category) && s[category.len()..].starts_with("=") {
                return TagsFor {
                    src: self.inner.as_ref(),
                    tags: tags,
                    category: Some(category),
                };
            }
            tags.next();
        }
        return TagsFor {
            src: self.inner.as_ref(),
            tags: self.inner.split(","),
            category: None,
        };
    }
}

/// Locale is specified by a string tag. This is the way to access it.
// FIXME: Do we want to provide the full string representation? We would have it as single string
// then.
impl AsRef<str> for Locale {
    fn as_ref(&self) -> &str {
        self.inner.as_ref()
    }
}

impl<'a> From<LanguageRange<'a>> for Locale {
    fn from(t: LanguageRange<'a>) -> Locale {
        Locale {
            inner: t.language.into_owned(),
        }
    }
}

impl fmt::Display for Locale {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.inner.fmt(f)
    }
}

/// Iterator over `LanguageRange`s for all categories in a `Locale`
///
/// Returns tuples of optional category (as string) and corresponding `LanguageRange`. All tags
/// in the list are returned, in order of preference.
///
/// The iterator is guaranteed to return at least one value.
pub struct Tags<'a> {
    tags: std::str::Split<'a, &'static str>,
}

impl<'a> Iterator for Tags<'a> {
    type Item = (Option<&'a str>, LanguageRange<'a>);
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(s) = self.tags.next() {
            if let Some(i) = s.find('=') {
                return Some((
                    Some(&s[..i]),
                    LanguageRange { language: Cow::Borrowed(&s[i+1..]), }));
            } else {
                return Some((
                    None,
                    LanguageRange { language: Cow::Borrowed(s), }));
            }
        } else {
            return None;
        }
    }
}

/// Iterator over `LanguageRange`s for specific category in a `Locale`
///
/// Returns `LanguageRange`s in the `Locale` that are applicable to provided category. The tags
/// are returned in order of preference, which means the category-specific ones first and then
/// the generic ones.
///
/// The iterator is guaranteed to return at least one value.
pub struct TagsFor<'a, 'c> {
    src: &'a str,
    tags: std::str::Split<'a, &'static str>,
    category: Option<&'c str>,
}

impl<'a, 'c> Iterator for TagsFor<'a, 'c> {
    type Item = LanguageRange<'a>;
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(cat) = self.category {
            while let Some(s) = self.tags.next() {
                if s.starts_with(cat) && s[cat.len()..].starts_with("=") {
                    return Some(
                        LanguageRange { language: Cow::Borrowed(&s[cat.len()+1..]) });
                }
            }
            self.category = None;
            self.tags = self.src.split(",");
        }
        while let Some(s) = self.tags.next() {
            if s.find('=').is_none() {
                return Some(
                    LanguageRange{ language: Cow::Borrowed(s) });
            }
        }
        return None;
    }
}

// ------------------------------- INSTANCES -----------------------------------

// TODO: We only need this until either std::sync::StaticMutex or std::sync::Mutex becomes usable
// with normal `static`.
// FIX-THE-TODO: Do we? A mutex might be usable, but we still need to initialize the value inside
// on first access!
lazy_static! {
    // TODO: Implement the constructor
    static ref USER_LOCALE: Locale = system_locale();
    static ref GLOBAL_LOCALE: Mutex<Locale> = Mutex::new(Locale::user_default());
}

thread_local!(
    static CURRENT_LOCALE: RefCell<Locale> = RefCell::new(Locale::global_default())
);

// NOTE: Cgi-style environment variable HTTP_ACCEPT_LANGUAGE is unlikely to be defined at any other
// time than when actually executing in CGI, so we can relatively safely always interpret it.
mod cgi;

// NOTE: Unix-style environment variables are actually inspected everywhere, because many users
// have them, because some software only uses those even on Windows and other systems.
mod unix;

// NOTE: Functions used exist from Vista on only
#[cfg(target_family = "windows")]
mod win32;

// Emscripten support
#[cfg(target_os = "emscripten")]
mod emscripten;

// macOS support
#[cfg(target_os = "macos")]
mod macos;

static INITIALISERS: &'static [fn() -> Option<Locale>] = &[
    cgi::system_locale,
    unix::system_locale,
    #[cfg(target_family = "windows")] win32::system_locale,
    #[cfg(target_os = "emscripten")] emscripten::system_locale,
	#[cfg(target_os = "macos")] macos::system_locale,
];

fn system_locale() -> Locale {
    for f in INITIALISERS {
        if let Some(l) = f() {
            return l;
        }
    }
    return Locale::invariant();
}

// --------------------------------- ERRORS ------------------------------------

/// Errors that may be returned by `locale_config`.
#[derive(Copy,Clone,Debug,PartialEq,Eq)]
pub enum Error {
    /// Provided definition was not well formed.
    ///
    /// This is returned when provided configuration string does not match even the rather loose
    /// definition for language range from [RFC4647] or the composition format used by `Locale`.
    ///
    /// [RFC4647]: https://www.rfc-editor.org/rfc/rfc4647.txt
    NotWellFormed,
    /// Placeholder for adding more errors in future. **Do not match!**.
    __NonExhaustive,
}

impl ::std::fmt::Display for Error {
    fn fmt(&self, out: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        use ::std::error::Error;
        out.write_str(self.description())
    }
}

impl ::std::error::Error for Error {
    fn description(&self) -> &str {
        match self {
            &Error::NotWellFormed => "Language tag is not well-formed.",
            // this is exception: here we do want exhaustive match so we don't publish version with
            // missing descriptions by mistake.
            &Error::__NonExhaustive => panic!("Placeholder error must not be instantiated!"),
        }
    }
}

/// Convenience Result alias.
type Result<T> = ::std::result::Result<T, Error>;

// --------------------------------- TESTS -------------------------------------

#[cfg(test)]
mod test {
    use super::LanguageRange;
    use super::Locale;
    use super::is_owned;
    use std::iter::FromIterator;

    #[test]
    fn simple_valid_lang_ranges() {
        assert_eq!("en-US", LanguageRange::new("en-US").unwrap().as_ref());
        assert_eq!("en-US", LanguageRange::new("EN-US").unwrap().as_ref());
        assert_eq!("en", LanguageRange::new("en").unwrap().as_ref());
        assert_eq!("eng-Latn-840", LanguageRange::new("eng-Latn-840").unwrap().as_ref());
        assert_eq!("english", LanguageRange::new("English").unwrap().as_ref());
    }

    #[test]
    fn wildcard_lang_ranges() {
        assert_eq!("*", LanguageRange::new("*").unwrap().as_ref());
        assert_eq!("zh-*", LanguageRange::new("zh-*").unwrap().as_ref());
        assert_eq!("zh-*-CN", LanguageRange::new("zh-*-cn").unwrap().as_ref());
        assert_eq!("en-*-simple-*", LanguageRange::new("En-*-Simple-*").unwrap().as_ref());
        assert_eq!("zh-Hans-*", LanguageRange::new("zh-hans-*").unwrap().as_ref());
    }

    #[test]
    fn complex_valid_lang_ranges() {
        assert_eq!("de-DE-u-email-co-phonebk-x-linux",
                   LanguageRange::new("de-DE-u-email-co-phonebk-x-linux").unwrap().as_ref());
        assert_eq!("vi-VN-u-fw-mon-hc-h24-ms-metric",
                   LanguageRange::new("vi-vn-u-fw-mon-hc-h24-ms-metric").unwrap().as_ref());
        assert_eq!("sl-Cyrl-YU-rozaj-solba-1994-b-1234-a-foobar-x-b-1234-a-foobar",
                   LanguageRange::new("sl-Cyrl-YU-rozaj-solba-1994-b-1234-a-Foobar-x-b-1234-a-Foobar").unwrap().as_ref());
    }

    #[test]
    fn invalid_lang_range_invalid_char() {
        assert!(LanguageRange::new("not a range").is_err());
    }

    #[test]
    fn invalid_lang_range_long_element() {
        assert!(LanguageRange::new("de-DE-u-email-co-phonebook-x-linux").is_err());
    }

    #[test]
    fn invalid_lang_range_leading_number() {
        assert!(LanguageRange::new("840").is_err());
    }

    #[test]
    fn invalid_lang_range_bad_asterisk() {
        assert!(LanguageRange::new("e*-US").is_err());
        assert!(LanguageRange::new("en-*s").is_err());
    }

    #[test]
    fn normal_lang_range() {
        // Check that the string is not copied if the tag is canonical
        assert!(!is_owned(&LanguageRange::new("en-US").unwrap().language));
        assert!(!is_owned(&LanguageRange::new("en").unwrap().language));
        assert!(!is_owned(&LanguageRange::new("zh-Hant-CN").unwrap().language));
        assert!(!is_owned(&LanguageRange::new("cs-CZ-x-ds-002e").unwrap().language));
        assert!(!is_owned(&LanguageRange::new("czech").unwrap().language));
    }

    #[test]
    fn locale_simple() {
        assert_eq!("en-US", Locale::new("en-US").unwrap().as_ref());
        assert_eq!("zh-Hant", Locale::new("zh-hant").unwrap().as_ref());
        assert_eq!("de-*", Locale::new("de-*").unwrap().as_ref());
        assert!(Locale::new("invalid!").is_err());
        assert!(Locale::new("hı-İN").is_err());
    }

    #[test]
    fn locale_list() {
        assert_eq!("cs-CZ,en-GB,en,*", Locale::new("cs-cz,en-gb,en,*").unwrap().as_ref());
        assert_eq!("cs-CZ,engrish", Locale::new("cs-cz,engrish").unwrap().as_ref());
        assert!(Locale::new("cs-cz,hı-İN").is_err());
    }

    #[test]
    fn locale_category() {
        assert_eq!("cs-CZ,messages=en-GB",
                   Locale::new("cs-CZ,messages=en-GB").unwrap().as_ref());
        assert_eq!("zh-Hant,time=ja-JP,measurement=en-US",
                   Locale::new("zh-hant,TIME=ja-jp,meaSURement=en-US").unwrap().as_ref());
        // the first item must be plain language tag
        assert!(Locale::new("messages=pl").is_err());
        // adding general alternate should not help
        assert!(Locale::new("numeric=de,fr-FR").is_err());
    }

    #[test]
    fn locale_dups() {
        assert_eq!("cs-CZ,en,de-AT", Locale::new("cs-CZ,en,de-AT,en").unwrap().as_ref());
        assert_eq!("en-US,en", Locale::new("en-us,en-US,EN,eN-Us,en").unwrap().as_ref());
    }

    #[test]
    fn locale_category_dups() {
        assert_eq!("cs-CZ",
                   Locale::new("cs-CZ,messages=cs-CZ,time=cs-cz,collate=CS-cz").unwrap().as_ref());
        assert_eq!("de-AT,en-AU",
                   Locale::new("de-AT,en-AU,messages=de-AT").unwrap().as_ref());
        // category overrides override, so don't drop if they are only equal to alternates
        assert_eq!("de-AT,en-AU,messages=en-AU",
                   Locale::new("de-AT,en-AU,messages=en-AU").unwrap().as_ref());
        assert_eq!("hi-IN,time=en-IN",
                   Locale::new("hi-IN,time=en-IN,TIME=EN-in,TiMe=En-iN").unwrap().as_ref());
    }

    #[test]
    fn unix_tags() {
        assert_eq!("cs-CZ", LanguageRange::from_unix("cs_CZ.UTF-8").unwrap().as_ref());
        assert_eq!("sr-RS-ijekavsk", LanguageRange::from_unix("sr_RS@ijekavian").unwrap().as_ref());
        assert_eq!("sr-Latn-ijekavsk", LanguageRange::from_unix("sr.UTF-8@ijekavianlatin").unwrap().as_ref());
        assert_eq!("en-Arab", LanguageRange::from_unix("en@arabic").unwrap().as_ref());
        assert_eq!("en-Arab", LanguageRange::from_unix("en.UTF-8@arabic").unwrap().as_ref());
        assert_eq!("de-DE", LanguageRange::from_unix("DE_de.UTF-8@euro").unwrap().as_ref());
        assert_eq!("ssy-ER", LanguageRange::from_unix("aa_ER@saaho").unwrap().as_ref());
        assert!(LanguageRange::from_unix("foo_BAR").is_err());
        assert!(LanguageRange::from_unix("en@arabic.UTF-8").is_err());
        assert_eq!("", LanguageRange::from_unix("C").unwrap().as_ref());
        assert_eq!("", LanguageRange::from_unix("C.UTF-8").unwrap().as_ref());
        assert_eq!("", LanguageRange::from_unix("C.ISO-8859-1").unwrap().as_ref());
        assert_eq!("", LanguageRange::from_unix("POSIX").unwrap().as_ref());
    }

    #[test]
    fn category_tag_list() {
        assert_eq!(
            Vec::from_iter(Locale::new("cs-CZ,messages=en-GB,time=de-DE,collate=en-US").unwrap().tags()),
            &[(None, LanguageRange::new("cs-CZ").unwrap()),
              (Some("messages"), LanguageRange::new("en-GB").unwrap()),
              (Some("time"), LanguageRange::new("de-DE").unwrap()),
              (Some("collate"), LanguageRange::new("en-US").unwrap()),
            ]);
    }

    #[test]
    fn tag_list_for() {
        let locale = Locale::new("cs-CZ,messages=en-GB,time=de-DE,sk-SK,pl-PL").unwrap();
        assert_eq!(
            Vec::from_iter(locale.tags_for("messages")),
            &[LanguageRange::new("en-GB").unwrap(),
              LanguageRange::new("cs-CZ").unwrap(),
              LanguageRange::new("sk-SK").unwrap(),
              LanguageRange::new("pl-PL").unwrap(),
            ]);
        assert_eq!(
            Vec::from_iter(locale.tags_for("time")),
            &[LanguageRange::new("de-DE").unwrap(),
              LanguageRange::new("cs-CZ").unwrap(),
              LanguageRange::new("sk-SK").unwrap(),
              LanguageRange::new("pl-PL").unwrap(),
            ]);
        assert_eq!(
            Vec::from_iter(locale.tags_for("measurement")),
            &[LanguageRange::new("cs-CZ").unwrap(),
              LanguageRange::new("sk-SK").unwrap(),
              LanguageRange::new("pl-PL").unwrap(),
            ]);
    }
}
