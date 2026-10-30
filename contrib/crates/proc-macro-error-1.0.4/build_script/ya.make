# Rust sources pinned by cryptography 41.0.6 Cargo.lock.

RUST_BUILD_SCRIPT(build_script_build)

CRATE_VERSION(1.0.4)
CRATE_EDITION(2018)
CRATE_ROOT(../build.rs)
CRATE_MANIFEST(../Cargo.toml)
CRATE_MANIFEST_DIR(..)

CRATE_SOURCES(
    ../src/diagnostic.rs
    ../src/dummy.rs
    ../src/imp/delegate.rs
    ../src/imp/fallback.rs
    ../src/lib.rs
    ../src/macros.rs
    ../src/sealed.rs
)

CRATE_INPUTS(
    ../.cargo-checksum.json
    ../.cargo_vcs_info.json
    ../.gitignore
    ../.gitlab-ci.yml
    ../.travis.yml
    ../CHANGELOG.md
    ../LICENSE-APACHE
    ../LICENSE-MIT
    ../README.md
)

CRATE_FEATURES(
    default
    syn
    syn-error
)

CRATE_FLAGS(--cap-lints=allow)

PEERDIR(
    contrib/crates/version_check-0.9.5
)

LICENSE("MIT OR Apache-2.0")
VERSION(1.0.4)
ORIGINAL_SOURCE(https://static.crates.io/crates/proc-macro-error/proc-macro-error-1.0.4.crate)

END()
