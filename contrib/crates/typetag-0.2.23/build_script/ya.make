# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

RUST_BUILD_SCRIPT(build_script_build)

CRATE_VERSION(0.2.23)
CRATE_EDITION(2021)
CRATE_ROOT(../build.rs)
CRATE_MANIFEST(../Cargo.toml)
CRATE_MANIFEST_DIR(..)

CRATE_SOURCES(
    ../src/adjacently.rs
    ../src/content.rs
    ../src/de.rs
    ../src/externally.rs
    ../src/internally.rs
    ../src/is_serialize_str.rs
    ../src/lib.rs
    ../src/private.rs
    ../src/ser.rs
)

CRATE_INPUTS(
    ../.cargo-checksum.json
    ../.cargo_vcs_info.json
    ../.gitignore
    ../Cargo.lock
    ../LICENSE-APACHE
    ../LICENSE-MIT
    ../README.md
)

CRATE_FLAGS(--cap-lints=allow)

LICENSE("MIT OR Apache-2.0")
VERSION(0.2.23)
ORIGINAL_SOURCE(https://static.crates.io/crates/typetag/typetag-0.2.23.crate)

CRATE_SOURCES(
    ../src/adjacently.rs
    ../src/content.rs
    ../src/de.rs
    ../src/externally.rs
    ../src/internally.rs
    ../src/is_serialize_str.rs
    ../src/lib.rs
    ../src/private.rs
    ../src/ser.rs
)

END()
