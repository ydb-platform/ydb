# Native Rust build POC; dependencies and features follow Cargo.lock.

CRATE(num_integer)

CRATE_VERSION(0.1.46)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)
VERSION(0.1.46)
LICENSE("MIT OR Apache-2.0")

CRATE_SOURCES(
    src/average.rs
    src/roots.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    LICENSE-APACHE
    LICENSE-MIT
    README.md
    RELEASES.md
)

CRATE_FLAGS(--cap-lints=allow)


CRATE_FEATURES(
    i128
    std
)
PEERDIR(contrib/crates/num-traits-0.2.19)

END()
