# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(chacha20)

CRATE_VERSION(0.10.1)
CRATE_EDITION(2024)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/backends.rs
    src/backends/avx2.rs
    src/backends/avx512.rs
    src/backends/neon.rs
    src/backends/soft.rs
    src/backends/sse2.rs
    src/chacha.rs
    src/legacy.rs
    src/rng.rs
    src/variants.rs
    src/xchacha.rs
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

CRATE_FEATURES(
    rng
)

PEERDIR(
    contrib/crates/cfg-if-1.0.4
    contrib/crates/rand_core-0.10.1
)

IF (_RUST_TARGET == "aarch64-apple-darwin" OR _RUST_TARGET == "aarch64-unknown-linux-gnu" OR _RUST_TARGET == "aarch64-unknown-linux-musl")
ELSEIF (_RUST_TARGET == "x86_64-apple-darwin" OR _RUST_TARGET == "x86_64-pc-windows-msvc" OR _RUST_TARGET == "x86_64-unknown-linux-gnu" OR _RUST_TARGET == "x86_64-unknown-linux-musl")
    PEERDIR(
        contrib/crates/cpufeatures-0.3.0
    )

ENDIF()

LICENSE("MIT OR Apache-2.0")
VERSION(0.10.1)
ORIGINAL_SOURCE(https://static.crates.io/crates/chacha20/chacha20-0.10.1.crate)

END()
