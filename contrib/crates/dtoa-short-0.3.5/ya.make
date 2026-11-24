# Native Rust build POC; dependencies and features follow Cargo.lock.

CRATE(dtoa_short)

CRATE_VERSION(0.3.5)
CRATE_EDITION(2015)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)
VERSION(0.3.5)
LICENSE("MPL-2.0")


CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    LICENSE
    README.md
)

CRATE_FLAGS(--cap-lints=allow)


PEERDIR(
    contrib/crates/dtoa-1.0.11
)

END()
