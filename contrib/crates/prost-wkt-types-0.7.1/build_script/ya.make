# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

RUST_BUILD_SCRIPT(build_script_build)

CRATE_VERSION(0.7.1)
CRATE_EDITION(2021)
CRATE_ROOT(../build.rs)
CRATE_MANIFEST(../Cargo.toml)
CRATE_MANIFEST_DIR(..)

CRATE_SOURCES(
    ../src/lib.rs
    ../src/pbany.rs
    ../src/pbempty.rs
    ../src/pbmask.rs
    ../src/pbstruct.rs
    ../src/pbtime/datetime.rs
    ../src/pbtime/duration.rs
    ../src/pbtime/mod.rs
    ../src/pbtime/timestamp.rs
)

CRATE_INPUTS(
    ../.cargo-checksum.json
    ../.cargo_vcs_info.json
    ../Cargo.lock
    ../README.md
    ../proto/pbany.proto
    ../proto/pbempty.proto
    ../proto/pbmask.proto
    ../proto/pbstruct.proto
    ../proto/pbtime.proto
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    default
    std
)

PEERDIR(
    contrib/crates/prost-0.14.4
    contrib/crates/prost-build-0.14.4
    contrib/crates/prost-types-0.14.4
    contrib/crates/prost-wkt-build-0.7.1
    contrib/crates/regex-1.13.1
)

LICENSE("Apache-2.0")
VERSION(0.7.1)
ORIGINAL_SOURCE(https://static.crates.io/crates/prost-wkt-types/prost-wkt-types-0.7.1.crate)

CRATE_PROC_MACROS(
    contrib/crates/prost-derive-0.14.4
)

END()
