# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE_PROC_MACRO(mockall_derive)

CRATE_VERSION(0.14.0)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    build.rs
    src/automock.rs
    src/mock_function.rs
    src/mock_item.rs
    src/mock_item_struct.rs
    src/mock_trait.rs
    src/mockable_item.rs
    src/mockable_struct.rs
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

PEERDIR(
    contrib/crates/cfg-if-1.0.5
    contrib/crates/proc-macro2-1.0.107
    contrib/crates/quote-1.0.47
    contrib/crates/syn-2.0.119
)

CRATE_BUILD_SCRIPT(contrib/crates/mockall_derive-0.14.0/build_script)

LICENSE("MIT OR Apache-2.0")
VERSION(0.14.0)
ORIGINAL_SOURCE(https://static.crates.io/crates/mockall_derive/mockall_derive-0.14.0.crate)

END()

RECURSE(build_script)
