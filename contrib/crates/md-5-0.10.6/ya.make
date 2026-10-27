# Native Rust build with dependencies shared through contrib/crates.

CRATE(md5)

CRATE_VERSION(0.10.6)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/compress.rs
    src/compress/loongarch64_asm.rs
    src/compress/soft.rs
    src/consts.rs
)

CRATE_INPUTS(
    CHANGELOG.md
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
    contrib/crates/digest-0.10.7
)

LICENSE("MIT OR Apache-2.0")
VERSION(0.10.6)
ORIGINAL_SOURCE(https://static.crates.io/crates/md-5/md-5-0.10.6.crate)

END()
