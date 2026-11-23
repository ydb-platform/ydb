# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(typetag)

CRATE_VERSION(0.2.23)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    build.rs
    src/adjacently.rs
    src/content.rs
    src/de.rs
    src/externally.rs
    src/internally.rs
    src/is_serialize_str.rs
    src/private.rs
    src/ser.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    .gitignore
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

PEERDIR(
    contrib/crates/erased-serde-0.4.10
    contrib/crates/inventory-0.3.25
    contrib/crates/once_cell-1.21.4
    contrib/crates/serde-1.0.229
)

CRATE_BUILD_SCRIPT(contrib/crates/typetag-0.2.23/build_script)

LICENSE("MIT OR Apache-2.0")
VERSION(0.2.23)
ORIGINAL_SOURCE(https://static.crates.io/crates/typetag/typetag-0.2.23.crate)

CRATE_SOURCES(
    src/adjacently.rs
    src/content.rs
    src/de.rs
    src/externally.rs
    src/internally.rs
    src/is_serialize_str.rs
    src/lib.rs
    src/private.rs
    src/ser.rs
)

CRATE_PROC_MACROS(
    contrib/crates/serde_derive-1.0.229
    contrib/crates/typetag-impl-0.2.23
)

END()

RECURSE(build_script)
