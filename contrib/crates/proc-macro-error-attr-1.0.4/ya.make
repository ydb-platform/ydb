# Rust sources pinned by cryptography 41.0.6 Cargo.lock.

CRATE_PROC_MACRO(proc_macro_error_attr)

CRATE_VERSION(1.0.4)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    build.rs
    src/parse.rs
    src/settings.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    .gitignore
    LICENSE-APACHE
    LICENSE-MIT
)

CRATE_FLAGS(--cap-lints=allow)

PEERDIR(
    contrib/crates/proc-macro2-1.0.107
    contrib/crates/quote-1.0.47
)

CRATE_BUILD_SCRIPT(contrib/crates/proc-macro-error-attr-1.0.4/build_script)

LICENSE("MIT OR Apache-2.0")
VERSION(1.0.4)
ORIGINAL_SOURCE(https://static.crates.io/crates/proc-macro-error-attr/proc-macro-error-attr-1.0.4.crate)

END()

RECURSE(build_script)
