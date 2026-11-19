# Native Rust build with dependencies shared through contrib/crates.

CRATE(simdutf8)

CRATE_VERSION(0.1.5)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/basic.rs
    src/compat.rs
    src/implementation/aarch64/mod.rs
    src/implementation/aarch64/neon.rs
    src/implementation/algorithm.rs
    src/implementation/helpers.rs
    src/implementation/mod.rs
    src/implementation/wasm32/mod.rs
    src/implementation/wasm32/simd128.rs
    src/implementation/x86/avx2.rs
    src/implementation/x86/mod.rs
    src/implementation/x86/sse42.rs
)

CRATE_INPUTS(
    CHANGELOG.md
    Cargo.lock
    LICENSE-Apache
    LICENSE-MIT
    README.md
    release.toml
    rustfmt.toml
    wasm32-development.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    aarch64_neon
    default
    public_imp
    std
)

LICENSE("MIT OR Apache-2.0")
VERSION(0.1.5)
ORIGINAL_SOURCE(https://static.crates.io/crates/simdutf8/simdutf8-0.1.5.crate)

END()
