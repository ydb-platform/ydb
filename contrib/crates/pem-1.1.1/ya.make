# Rust sources pinned by cryptography 41.0.6 Cargo.lock.

CRATE(pem)

CRATE_VERSION(1.1.1)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/errors.rs
    src/parser.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    .gitignore
    .travis.yml
    CHANGELOG.md
    LICENSE.md
    README.md
    rustfmt.toml
)

CRATE_FLAGS(--cap-lints=allow)

PEERDIR(
    contrib/crates/base64-0.13.1
)

LICENSE("MIT")
VERSION(1.1.1)
ORIGINAL_SOURCE(https://static.crates.io/crates/pem/pem-1.1.1.crate)

END()
