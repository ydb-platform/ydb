# Native Rust build POC; dependencies and features follow Cargo.lock.

CRATE(gif)

CRATE_VERSION(0.14.1)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)
VERSION(0.14.1)
LICENSE("MIT OR Apache-2.0")

CRATE_SOURCES(
    src/common.rs
    src/encoder.rs
    src/reader/converter.rs
    src/reader/decoder.rs
    src/reader/mod.rs
    src/traits.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    README.md
)

CRATE_FLAGS(--cap-lints=allow)


CRATE_FEATURES(
    color_quant
    default
    raii_no_panic
    std
)
PEERDIR(
    contrib/crates/color_quant-1.1.0
    contrib/crates/weezl-0.1.12
)

END()
