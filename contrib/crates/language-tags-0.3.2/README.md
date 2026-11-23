# rust-language-tags
[![Build Status](https://travis-ci.org/pyfisch/rust-language-tags.svg?branch=master)](https://travis-ci.org/pyfisch/rust-language-tags)
[![MIT licensed](https://img.shields.io/badge/license-MIT-blue.svg)](./LICENSE)
[![crates.io](http://meritbadge.herokuapp.com/language-tags)](https://crates.io/crates/language-tags)
[![Documentation](https://docs.rs/language-tags/badge.svg)](https://docs.rs/language-tags)

Language tags can be used identify human languages, scripts e.g. Latin script, countries and
other regions.

Language tags are defined in [BCP47](http://tools.ietf.org/html/bcp47), an introduction is
["Language tags in HTML and XML"](http://www.w3.org/International/articles/language-tags/) by
the W3C. They are commonly used in HTML and HTTP `Content-Language` and `Accept-Language`
header fields.

This package currently supports parsing (fully conformant parser), formatting and comparing
language tags.

## Examples
Create a simple language tag representing the French language as spoken
in Belgium and print it:

```rust
use language_tags::LanguageTag;
let langtag = LanguageTag::parse("fr-BE").unwrap();
assert_eq!(format!("{}", langtag), "fr-BE");
```

Parse a tag representing a special type of English specified by private agreement:

```rust
use language_tags::LanguageTag;
use std::iter::FromIterator;
let langtag: LanguageTag = "en-x-twain".parse().unwrap();
assert_eq!(langtag.primary_language(), "en");
assert_eq!(Vec::from_iter(langtag.private_use_subtags()), vec!["twain"]);
```

You can check for equality, but more often you should test if two tags match.
In this example we check if the resource in German language is suitable for
a user from Austria. While people speaking Austrian German normally understand
standard German the opposite is not always true. So the resource can be presented
to the user but if the resource was in `de-AT` and a user asked for a representation
in `de` the request should be rejected.


```rust
use language_tags::LanguageTag;
let mut langtag_server = LanguageTag::parse("de-AT").unwrap();
let mut langtag_user = LanguageTag::parse("de").unwrap();
assert!(langtag_user.matches(&langtag_server));
```

## Related crates

If you only want to validate and normalize the formatting of language tags or you are working with RDF consider using the [oxilangtag](https://github.com/oxigraph/oxilangtag) crate.
It is much more lightweight as it doesn't contain a language tag database and has a very similar interface to this crate.
