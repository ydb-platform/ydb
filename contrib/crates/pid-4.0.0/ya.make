# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(pid)

CRATE_VERSION(4.0.0)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
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

PEERDIR(
    contrib/crates/num-traits-0.2.19
)

LICENSE("MIT OR Apache-2.0")
VERSION(4.0.0)
ORIGINAL_SOURCE(https://static.crates.io/crates/pid/pid-4.0.0.crate)

CRATE_SOURCES(
    src/lib.rs
)

END()
