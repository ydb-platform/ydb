# Native Rust build with dependencies shared through contrib/crates.

CRATE(icu_collections)

CRATE_VERSION(2.4.0)
CRATE_EDITION(2024)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/char16trie/mod.rs
    src/char16trie/trie.rs
    src/codepointinvlist/builder.rs
    src/codepointinvlist/conversions.rs
    src/codepointinvlist/cpinvlist.rs
    src/codepointinvlist/mod.rs
    src/codepointinvlist/utils.rs
    src/codepointinvliststringlist/mod.rs
    src/codepointtrie/builder.rs
    src/codepointtrie/cptrie.rs
    src/codepointtrie/error.rs
    src/codepointtrie/impl_const.rs
    src/codepointtrie/mod.rs
    src/codepointtrie/planes.rs
    src/codepointtrie/serde.rs
    src/codepointtrie/toml.rs
    src/iterator_utils.rs
)

CRATE_INPUTS(
    Cargo.lock
    LICENSE
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

PEERDIR(
    contrib/crates/potential_utf-0.1.6
    contrib/crates/utf8_iter-1.0.4
    contrib/crates/yoke-0.8.4
    contrib/crates/zerofrom-0.1.8
    contrib/crates/zerovec-0.11.9
)

LICENSE("Unicode-3.0")
VERSION(2.4.0)
ORIGINAL_SOURCE(https://static.crates.io/crates/icu_collections/icu_collections-2.4.0.crate)

CRATE_PROC_MACROS(
    contrib/crates/displaydoc-0.2.7
    contrib/crates/yoke-derive-0.8.4
    contrib/crates/zerofrom-derive-0.1.8
    contrib/crates/zerovec-derive-0.11.7
)

END()
