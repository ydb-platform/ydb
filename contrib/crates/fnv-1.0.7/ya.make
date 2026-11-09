# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(fnv)

CRATE_VERSION(1.0.7)
CRATE_EDITION(2015)
CRATE_ROOT(lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    .gitignore
    .travis.yml
    LICENSE-APACHE
    LICENSE-MIT
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    default
    std
)

LICENSE("Apache-2.0 OR MIT")
VERSION(1.0.7)
ORIGINAL_SOURCE(https://static.crates.io/crates/fnv/fnv-1.0.7.crate)

END()
