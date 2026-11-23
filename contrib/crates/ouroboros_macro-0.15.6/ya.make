# Rust sources pinned by cryptography 41.0.6 Cargo.lock.

CRATE_PROC_MACRO(ouroboros_macro)

CRATE_VERSION(0.15.6)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/covariance_detection.rs
    src/generate/constructor.rs
    src/generate/derives.rs
    src/generate/into_heads.rs
    src/generate/mod.rs
    src/generate/struc.rs
    src/generate/summon_checker.rs
    src/generate/try_constructor.rs
    src/generate/type_asserts.rs
    src/generate/with_all.rs
    src/generate/with_each.rs
    src/info_structures.rs
    src/parse.rs
    src/utils.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    LICENSE_APACHE
    LICENSE_MIT
)

CRATE_FEATURES(
    std
)

CRATE_FLAGS(--cap-lints=allow)

PEERDIR(
    contrib/crates/Inflector-0.11.4
    contrib/crates/proc-macro-error-1.0.4
    contrib/crates/proc-macro2-1.0.107
    contrib/crates/quote-1.0.47
    contrib/crates/syn-1.0.109
)

LICENSE("MIT OR Apache-2.0")
VERSION(0.15.6)
ORIGINAL_SOURCE(https://static.crates.io/crates/ouroboros_macro/ouroboros_macro-0.15.6.crate)

CRATE_PROC_MACROS(
    contrib/crates/proc-macro-error-attr-1.0.4
)

END()
