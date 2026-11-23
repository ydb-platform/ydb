# Native Rust build with dependencies shared through contrib/crates.

CRATE(icu_locale_core)

CRATE_VERSION(2.4.0)
CRATE_EDITION(2024)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/data.rs
    src/databake.rs
    src/extensions/mod.rs
    src/extensions/other/mod.rs
    src/extensions/private/mod.rs
    src/extensions/private/other.rs
    src/extensions/transform/fields.rs
    src/extensions/transform/key.rs
    src/extensions/transform/mod.rs
    src/extensions/transform/value.rs
    src/extensions/unicode/attribute.rs
    src/extensions/unicode/attributes.rs
    src/extensions/unicode/key.rs
    src/extensions/unicode/keywords.rs
    src/extensions/unicode/mod.rs
    src/extensions/unicode/subdivision.rs
    src/extensions/unicode/value.rs
    src/helpers.rs
    src/langid.rs
    src/locale.rs
    src/macros.rs
    src/parser/errors.rs
    src/parser/langid.rs
    src/parser/locale.rs
    src/parser/mod.rs
    src/preferences/extensions/mod.rs
    src/preferences/extensions/unicode/errors.rs
    src/preferences/extensions/unicode/keywords/calendar.rs
    src/preferences/extensions/unicode/keywords/collation.rs
    src/preferences/extensions/unicode/keywords/currency.rs
    src/preferences/extensions/unicode/keywords/currency_format.rs
    src/preferences/extensions/unicode/keywords/dictionary_break.rs
    src/preferences/extensions/unicode/keywords/emoji.rs
    src/preferences/extensions/unicode/keywords/first_day.rs
    src/preferences/extensions/unicode/keywords/hour_cycle.rs
    src/preferences/extensions/unicode/keywords/line_break.rs
    src/preferences/extensions/unicode/keywords/line_break_word.rs
    src/preferences/extensions/unicode/keywords/measurement_system.rs
    src/preferences/extensions/unicode/keywords/measurement_unit_override.rs
    src/preferences/extensions/unicode/keywords/mod.rs
    src/preferences/extensions/unicode/keywords/numbering_system.rs
    src/preferences/extensions/unicode/keywords/region_override.rs
    src/preferences/extensions/unicode/keywords/regional_subdivision.rs
    src/preferences/extensions/unicode/keywords/sentence_supression.rs
    src/preferences/extensions/unicode/keywords/timezone.rs
    src/preferences/extensions/unicode/keywords/variant.rs
    src/preferences/extensions/unicode/macros/enum_keyword.rs
    src/preferences/extensions/unicode/macros/mod.rs
    src/preferences/extensions/unicode/macros/struct_keyword.rs
    src/preferences/extensions/unicode/mod.rs
    src/preferences/locale.rs
    src/preferences/mod.rs
    src/serde.rs
    src/shortvec/litemap.rs
    src/shortvec/mod.rs
    src/subtags/language.rs
    src/subtags/mod.rs
    src/subtags/region.rs
    src/subtags/script.rs
    src/subtags/variant.rs
    src/subtags/variants.rs
    src/zerovec.rs
)

CRATE_INPUTS(
    Cargo.lock
    LICENSE
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    zerovec
)

PEERDIR(
    contrib/crates/litemap-0.8.3
    contrib/crates/tinystr-0.8.5
    contrib/crates/writeable-0.6.4
    contrib/crates/zerovec-0.11.9
)

LICENSE("Unicode-3.0")
VERSION(2.4.0)
ORIGINAL_SOURCE(https://static.crates.io/crates/icu_locale_core/icu_locale_core-2.4.0.crate)

CRATE_PROC_MACROS(
    contrib/crates/displaydoc-0.2.7
    contrib/crates/yoke-derive-0.8.4
    contrib/crates/zerofrom-derive-0.1.8
    contrib/crates/zerovec-derive-0.11.7
)

END()
