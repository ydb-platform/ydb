# Native Rust build with dependencies shared through contrib/crates.

CRATE(icu_properties)

CRATE_VERSION(2.4.0)
CRATE_EDITION(2024)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/bidi.rs
    src/code_point_map.rs
    src/code_point_set.rs
    src/emoji.rs
    src/enum_values.rs
    src/harfbuzz.rs
    src/names.rs
    src/props.rs
    src/provider.rs
    src/provider/names.rs
    src/runtime.rs
    src/script.rs
    src/trievalue.rs
    src/unicodeset_parse/mod.rs
    src/unicodeset_parse/parse.rs
)

CRATE_INPUTS(
    Cargo.lock
    LICENSE
    README.md
    data/PropertyValueAliases.txt
    data/prop_numbers.txt
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    compiled_data
)

PEERDIR(
    contrib/crates/icu_collections-2.4.0
    contrib/crates/icu_locale_core-2.4.0
    contrib/crates/icu_properties_data-2.4.0
    contrib/crates/icu_provider-2.4.0
    contrib/crates/zerotrie-0.2.5
    contrib/crates/zerovec-0.11.9
)

LICENSE("Unicode-3.0")
VERSION(2.4.0)
ORIGINAL_SOURCE(https://static.crates.io/crates/icu_properties/icu_properties-2.4.0.crate)

CRATE_PROC_MACROS(
    contrib/crates/displaydoc-0.2.7
    contrib/crates/yoke-derive-0.8.4
    contrib/crates/zerofrom-derive-0.1.8
    contrib/crates/zerovec-derive-0.11.7
)

END()
