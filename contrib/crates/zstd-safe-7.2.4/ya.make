# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(zstd_safe)

CRATE_VERSION(7.2.4)
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
    .cargo-checksum.json
    .cargo_vcs_info.json
    Cargo.lock
    LICENSE
    LICENSE.Apache-2.0
    LICENSE.Mit
    Readme.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    std
)

PEERDIR(
    contrib/crates/zstd-sys-2.0.16+zstd.1.5.7
)

CRATE_BUILD_SCRIPT(contrib/crates/zstd-safe-7.2.4/build_script)

LICENSE("MIT OR Apache-2.0")
VERSION(7.2.4)
ORIGINAL_SOURCE(https://static.crates.io/crates/zstd-safe/zstd-safe-7.2.4.crate)

END()

RECURSE(build_script)
