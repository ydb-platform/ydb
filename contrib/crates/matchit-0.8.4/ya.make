# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(matchit)

CRATE_VERSION(0.8.4)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/error.rs
    src/escape.rs
    src/params.rs
    src/router.rs
    src/tree.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    .gitignore
    .rustfmt.toml
    Cargo.lock
    LICENSE
    LICENSE.httprouter
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    default
)

LICENSE("MIT AND BSD-3-Clause")
VERSION(0.8.4)
ORIGINAL_SOURCE(https://static.crates.io/crates/matchit/matchit-0.8.4.crate)

CRATE_INPUTS(
    README.md
)

CRATE_SOURCES(
    src/error.rs
    src/escape.rs
    src/lib.rs
    src/params.rs
    src/router.rs
    src/tree.rs
)

END()
