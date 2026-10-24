# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(cpufeatures)

CRATE_VERSION(0.3.0)
CRATE_EDITION(2024)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/aarch64.rs
    src/loongarch64.rs
    src/miri.rs
    src/x86.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    CHANGELOG.md
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

IF (_RUST_TARGET == "aarch64-unknown-linux-gnu" OR _RUST_TARGET == "aarch64-unknown-linux-musl" OR _RUST_TARGET == "aarch64-apple-darwin")
    PEERDIR(contrib/crates/libc-0.2.189)
ENDIF()

LICENSE("MIT OR Apache-2.0")
VERSION(0.3.0)
ORIGINAL_SOURCE(https://static.crates.io/crates/cpufeatures/cpufeatures-0.3.0.crate)

END()
