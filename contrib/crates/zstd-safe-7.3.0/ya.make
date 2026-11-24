# Native Rust build with dependencies shared through contrib/crates.

CRATE(zstd_safe)

CRATE_VERSION(7.3.0)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    build.rs
    src/constants.rs
    src/constants_experimental.rs
    src/constants_seekable.rs
    src/seekable.rs
    src/tests.rs
)

CRATE_INPUTS(
    Cargo.lock
    LICENSE
    Readme.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    std
)

PEERDIR(
    contrib/crates/zstd-sys-2.1.2+zstd.1.5.7
)

CRATE_BUILD_SCRIPT(contrib/crates/zstd-safe-7.3.0/build_script)

LICENSE("BSD-3-Clause")
VERSION(7.3.0)
ORIGINAL_SOURCE(https://static.crates.io/crates/zstd-safe/zstd-safe-7.3.0.crate)

CRATE_FEATURES(
    arrays
    experimental
    legacy
    zdict_builder
    zstdmt
)

END()

RECURSE(build_script)
