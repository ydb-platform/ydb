# Native Rust build POC; dependencies and features follow Cargo.lock.

CRATE(tendril)

CRATE_VERSION(0.4.3)
CRATE_EDITION(2015)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)
VERSION(0.4.3)
LICENSE("MIT OR Apache-2.0")

CRATE_SOURCES(
    examples/fuzz.rs
    src/bench.rs
    src/buf32.rs
    src/fmt.rs
    src/stream.rs
    src/tendril.rs
    src/utf8_decode.rs
    src/util.rs
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


PEERDIR(
    contrib/crates/futf-0.1.5
    contrib/crates/mac-0.1.1
    contrib/crates/utf-8-0.7.6
)

END()
