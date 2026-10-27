# Native Rust build POC; dependencies and features follow Cargo.lock.

CRATE(num_rational)

CRATE_VERSION(0.4.2)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)
VERSION(0.4.2)
LICENSE("MIT OR Apache-2.0")

CRATE_SOURCES(src/pow.rs)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    LICENSE-APACHE
    LICENSE-MIT
    README.md
    RELEASES.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(std)
PEERDIR(
    contrib/crates/num-integer-0.1.47
    contrib/crates/num-traits-0.2.19
)

CRATE_FEATURES(
    default
    num-bigint
    num-bigint-std
    serde
)

PEERDIR(
    contrib/crates/num-bigint-0.4.8
    contrib/crates/serde-1.0.229
)

CRATE_SOURCES(
    src/lib.rs
    src/pow.rs
)

CRATE_PROC_MACROS(
    contrib/crates/serde_derive-1.0.229
)

END()
