# Native Rust build with dependencies shared through contrib/crates.

CRATE(crc32fast)

CRATE_VERSION(1.5.3)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    build.rs
    src/baseline.rs
    src/combine.rs
    src/specialized/aarch64.rs
    src/specialized/mod.rs
    src/specialized/pclmulqdq.rs
    src/table.rs
)

CRATE_INPUTS(
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
)

CRATE_BUILD_SCRIPT(contrib/crates/crc32fast-1.5.3/build_script)

LICENSE("MIT OR Apache-2.0")
VERSION(1.5.3)
ORIGINAL_SOURCE(https://static.crates.io/crates/crc32fast/crc32fast-1.5.3.crate)

END()

RECURSE(build_script)
