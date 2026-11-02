# Native Rust build POC; dependencies and features follow Cargo.lock.

CRATE(phf_shared)

CRATE_VERSION(0.13.1)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)
VERSION(0.13.1)
LICENSE("MIT")


CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    CHANGELOG.md
    Cargo.lock
    LICENSE
    README.md
)

CRATE_FLAGS(--cap-lints=allow)


CRATE_FEATURES(
    default
    std
)
PEERDIR(
    contrib/crates/siphasher-1.0.4
)

END()
