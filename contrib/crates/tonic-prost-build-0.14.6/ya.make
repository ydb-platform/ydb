# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(tonic_prost_build)

CRATE_VERSION(0.14.6)
CRATE_EDITION(2024)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/tests.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    CHANGELOG.md
    Cargo.lock
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    cleanup-markdown
    default
    transport
)

PEERDIR(
    contrib/crates/prettyplease-0.2.37
    contrib/crates/proc-macro2-1.0.107
    contrib/crates/prost-build-0.14.4
    contrib/crates/prost-types-0.14.4
    contrib/crates/quote-1.0.47
    contrib/crates/syn-2.0.119
    contrib/crates/tempfile-3.27.0
    contrib/crates/tonic-build-0.14.6
)

LICENSE("MIT")
VERSION(0.14.6)
ORIGINAL_SOURCE(https://static.crates.io/crates/tonic-prost-build/tonic-prost-build-0.14.6.crate)

CRATE_SOURCES(
    src/lib.rs
    src/tests.rs
)

CRATE_PROC_MACROS(
    contrib/crates/bytemuck_derive-1.12.1
    contrib/crates/prost-derive-0.14.4
    contrib/crates/serde_derive-1.0.229
)

END()
