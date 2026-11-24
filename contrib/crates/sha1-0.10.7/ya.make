# Native Rust build with dependencies shared through contrib/crates.

CRATE(sha1)

CRATE_VERSION(0.10.7)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/compress.rs
    src/compress/loongarch64_asm.rs
    src/compress/soft.rs
    src/compress/x86.rs
)

CRATE_INPUTS(
    CHANGELOG.md
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    default
    std
)

PEERDIR(
    contrib/crates/cfg-if-1.0.5
    contrib/crates/cpufeatures-0.2.17
    contrib/crates/digest-0.10.7
)

LICENSE("MIT OR Apache-2.0")
VERSION(0.10.7)
ORIGINAL_SOURCE(https://static.crates.io/crates/sha1/sha1-0.10.7.crate)

END()
