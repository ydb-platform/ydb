# Native Rust build POC; dependencies and features follow Cargo.lock.

CRATE(crossbeam_deque)

CRATE_VERSION(0.8.6)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)
VERSION(0.8.6)
LICENSE("MIT OR Apache-2.0")

CRATE_SOURCES(src/deque.rs)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    CHANGELOG.md
    LICENSE-APACHE
    LICENSE-MIT
    README.md
)

CRATE_FLAGS(--cap-lints=allow)


CRATE_FEATURES(
    default
    std
)
PEERDIR(
    contrib/crates/crossbeam-epoch-0.9.18
    contrib/crates/crossbeam-utils-0.8.22
)

END()
