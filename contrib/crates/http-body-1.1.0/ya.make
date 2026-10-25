# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(http_body)

CRATE_VERSION(1.1.0)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/frame.rs
    src/size_hint.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    CHANGELOG.md
    Cargo.lock
    LICENSE
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

PEERDIR(
    contrib/crates/bytes-1.12.1
    contrib/crates/http-1.5.0
)

LICENSE("MIT")
VERSION(1.1.0)
ORIGINAL_SOURCE(https://static.crates.io/crates/http-body/http-body-1.1.0.crate)

CRATE_SOURCES(
    src/frame.rs
    src/lib.rs
    src/size_hint.rs
)

CRATE_PROC_MACROS(
    contrib/crates/serde_derive-1.0.229
)

END()
