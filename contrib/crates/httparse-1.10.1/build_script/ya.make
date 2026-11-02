# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

RUST_BUILD_SCRIPT(build_script_build)

CRATE_VERSION(1.10.1)
CRATE_EDITION(2018)
CRATE_ROOT(../build.rs)
CRATE_MANIFEST(../Cargo.toml)
CRATE_MANIFEST_DIR(..)

CRATE_SOURCES(
    ../src/iter.rs
    ../src/lib.rs
    ../src/macros.rs
    ../src/simd/avx2.rs
    ../src/simd/mod.rs
    ../src/simd/neon.rs
    ../src/simd/runtime.rs
    ../src/simd/sse42.rs
    ../src/simd/swar.rs
)

CRATE_INPUTS(
    ../.cargo-checksum.json
    ../.cargo_vcs_info.json
    ../.gitignore
    ../Cargo.lock
    ../LICENSE-APACHE
    ../LICENSE-MIT
    ../README.md
    ../clippy.toml
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    default
    std
)

LICENSE("MIT OR Apache-2.0")
VERSION(1.10.1)
ORIGINAL_SOURCE(https://static.crates.io/crates/httparse/httparse-1.10.1.crate)

CRATE_SOURCES(
    ../src/iter.rs
    ../src/lib.rs
    ../src/macros.rs
    ../src/simd/avx2.rs
    ../src/simd/mod.rs
    ../src/simd/neon.rs
    ../src/simd/runtime.rs
    ../src/simd/sse42.rs
    ../src/simd/swar.rs
)

END()
