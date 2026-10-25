# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(icu_provider)

CRATE_VERSION(2.2.0)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/baked.rs
    src/baked/zerotrie.rs
    src/buf.rs
    src/buf/serde.rs
    src/constructors.rs
    src/data_provider.rs
    src/dynutil.rs
    src/error.rs
    src/export/mod.rs
    src/export/payload.rs
    src/fallback.rs
    src/hello_world.rs
    src/marker.rs
    src/request.rs
    src/response.rs
    src/serde_borrow_de_utils.rs
    src/varule_traits.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    Cargo.lock
    LICENSE
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    baked
)

PEERDIR(
    contrib/crates/icu_locale_core-2.2.0
    contrib/crates/writeable-0.6.3
    contrib/crates/yoke-0.8.3
    contrib/crates/zerofrom-0.1.8
    contrib/crates/zerotrie-0.2.4
    contrib/crates/zerovec-0.11.6
)

LICENSE("Unicode-3.0")
VERSION(2.2.0)
ORIGINAL_SOURCE(https://static.crates.io/crates/icu_provider/icu_provider-2.2.0.crate)

CRATE_PROC_MACROS(
    contrib/crates/displaydoc-0.2.6
    contrib/crates/yoke-derive-0.8.2
    contrib/crates/zerofrom-derive-0.1.7
    contrib/crates/zerovec-derive-0.11.3
)

END()
