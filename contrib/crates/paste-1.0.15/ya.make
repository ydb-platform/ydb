# Native Rust build POC; dependencies and features follow Cargo.lock.

CRATE_PROC_MACRO(paste)

CRATE_VERSION(1.0.15)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)
VERSION(1.0.15)
LICENSE("MIT OR Apache-2.0")

CRATE_SOURCES(
    build.rs
    src/attr.rs
    src/error.rs
    src/segment.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    LICENSE-APACHE
    LICENSE-MIT
    README.md
)

CRATE_FLAGS(--cap-lints=allow)


CRATE_BUILD_SCRIPT(contrib/crates/paste-1.0.15/build_script)

CRATE_SOURCES(
    src/attr.rs
    src/error.rs
    src/lib.rs
    src/segment.rs
)

END()

RECURSE(build_script)
