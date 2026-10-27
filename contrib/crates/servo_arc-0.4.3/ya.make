# Native Rust build POC; dependencies and features follow Cargo.lock.

CRATE(servo_arc)

CRATE_VERSION(0.4.3)
CRATE_EDITION(2021)
CRATE_ROOT(lib.rs)
CRATE_MANIFEST(Cargo.toml)
VERSION(0.4.3)
LICENSE("MIT OR Apache-2.0")


CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    LICENSE-APACHE
    LICENSE-MIT
    README.md
)

CRATE_FLAGS(--cap-lints=allow)


CRATE_FEATURES(
    default
    track_alloc_size
)
PEERDIR(
    contrib/crates/stable_deref_trait-1.2.1
)

END()
