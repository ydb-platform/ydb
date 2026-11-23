# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(quote)

CRATE_VERSION(1.0.47)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    build.rs
    src/ext.rs
    src/format.rs
    src/ident_fragment.rs
    src/runtime.rs
    src/spanned.rs
    src/to_tokens.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    .gitignore
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    README.md
    rust-toolchain.toml
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    default
    proc-macro
)

PEERDIR(
    contrib/crates/proc-macro2-1.0.107
)

CRATE_BUILD_SCRIPT(contrib/crates/quote-1.0.47/build_script)

LICENSE("MIT OR Apache-2.0")
VERSION(1.0.47)
ORIGINAL_SOURCE(https://static.crates.io/crates/quote/quote-1.0.47.crate)

CRATE_SOURCES(
    src/runtime.rs
)

CRATE_SOURCES(
    src/ext.rs
    src/format.rs
    src/ident_fragment.rs
    src/lib.rs
    src/spanned.rs
    src/to_tokens.rs
)

END()

RECURSE(build_script)
