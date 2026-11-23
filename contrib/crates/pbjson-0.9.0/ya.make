# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(pbjson)

CRATE_VERSION(0.9.0)
CRATE_EDITION(2024)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    Cargo.lock
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

PEERDIR(
    contrib/crates/base64-0.22.1
    contrib/crates/serde-1.0.229
)

LICENSE("MIT")
VERSION(0.9.0)
ORIGINAL_SOURCE(https://static.crates.io/crates/pbjson/pbjson-0.9.0.crate)

CRATE_PROC_MACROS(
    contrib/crates/serde_derive-1.0.229
)

END()
