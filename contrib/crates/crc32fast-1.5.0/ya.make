# Native Rust build POC; dependencies and features follow Cargo.lock.

CRATE(crc32fast)

CRATE_VERSION(1.5.0)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)
VERSION(1.5.0)
LICENSE("MIT OR Apache-2.0")

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
    .cargo-checksum.json
    .cargo_vcs_info.json
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
CRATE_BUILD_SCRIPT(contrib/crates/crc32fast-1.5.0/build_script)
PEERDIR(contrib/crates/cfg-if-1.0.4)

END()

RECURSE(build_script)
