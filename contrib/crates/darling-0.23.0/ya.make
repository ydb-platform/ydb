# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(darling)

CRATE_VERSION(0.23.0)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/macros_public.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    .gitignore
    CHANGELOG.md
    Cargo.lock
    LICENSE
    README.md
    clippy.toml
    compiletests.sh
    rustfmt.toml
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    default
    suggestions
)

PEERDIR(
    contrib/crates/darling_core-0.23.0
)

LICENSE("MIT")
VERSION(0.23.0)
ORIGINAL_SOURCE(https://static.crates.io/crates/darling/darling-0.23.0.crate)

CRATE_PROC_MACROS(
    contrib/crates/darling_macro-0.23.0
)

END()
