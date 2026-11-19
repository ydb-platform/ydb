# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(simd_adler32)

CRATE_VERSION(0.3.10)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/hash.rs
    src/imp/avx2.rs
    src/imp/avx512.rs
    src/imp/mod.rs
    src/imp/neon.rs
    src/imp/scalar.rs
    src/imp/sse2.rs
    src/imp/ssse3.rs
    src/imp/wasm.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    .gitignore
    .rustfmt.toml
    CHANGELOG.md
    Cargo.lock
    LICENSE.md
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    std
)

LICENSE("MIT")
VERSION(0.3.10)
ORIGINAL_SOURCE(https://static.crates.io/crates/simd-adler32/simd-adler32-0.3.10.crate)

CRATE_FEATURES(
    const-generics
    default
)

CRATE_SOURCES(
    src/hash.rs
    src/imp/avx2.rs
    src/imp/avx512.rs
    src/imp/mod.rs
    src/imp/neon.rs
    src/imp/scalar.rs
    src/imp/sse2.rs
    src/imp/ssse3.rs
    src/imp/wasm.rs
    src/lib.rs
)

END()
