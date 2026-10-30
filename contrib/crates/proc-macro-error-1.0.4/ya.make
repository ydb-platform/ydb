# Rust sources pinned by cryptography 41.0.6 Cargo.lock.

CRATE(proc_macro_error)

CRATE_VERSION(1.0.4)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    build.rs
    src/diagnostic.rs
    src/dummy.rs
    src/imp/delegate.rs
    src/imp/fallback.rs
    src/macros.rs
    src/sealed.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    .gitignore
    .gitlab-ci.yml
    .travis.yml
    CHANGELOG.md
    LICENSE-APACHE
    LICENSE-MIT
    README.md
)

CRATE_FEATURES(
    default
    syn
    syn-error
)

CRATE_FLAGS(--cap-lints=allow)

PEERDIR(
    contrib/crates/proc-macro2-1.0.107
    contrib/crates/quote-1.0.47
    contrib/crates/syn-1.0.109
)

CRATE_BUILD_SCRIPT(contrib/crates/proc-macro-error-1.0.4/build_script)

LICENSE("MIT OR Apache-2.0")
VERSION(1.0.4)
ORIGINAL_SOURCE(https://static.crates.io/crates/proc-macro-error/proc-macro-error-1.0.4.crate)

CRATE_PROC_MACROS(
    contrib/crates/proc-macro-error-attr-1.0.4
)

END()

RECURSE(build_script)
