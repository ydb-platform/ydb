# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(slotmap)

CRATE_VERSION(1.1.1)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    build.rs
    src/basic.rs
    src/dense.rs
    src/hop.rs
    src/secondary.rs
    src/sparse_secondary.rs
    src/util.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    .gitignore
    Cargo.lock
    LICENSE
    README.md
    RELEASES.md
    rustfmt.toml
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    default
    std
)

CRATE_BUILD_SCRIPT(contrib/crates/slotmap-1.1.1/build_script)

LICENSE("Zlib")
VERSION(1.1.1)
ORIGINAL_SOURCE(https://static.crates.io/crates/slotmap/slotmap-1.1.1.crate)

CRATE_SOURCES(
    src/basic.rs
    src/dense.rs
    src/hop.rs
    src/lib.rs
    src/secondary.rs
    src/sparse_secondary.rs
    src/util.rs
)

END()

RECURSE(build_script)
