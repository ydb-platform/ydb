# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(mockall)

CRATE_VERSION(0.14.0)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    build.rs
    src/examples.rs
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
    contrib/crates/downcast-0.11.0
    contrib/crates/fragile-2.1.0
    contrib/crates/predicates-3.1.4
    contrib/crates/predicates-tree-1.0.13
)

CRATE_BUILD_SCRIPT(contrib/crates/mockall-0.14.0/build_script)

LICENSE("MIT OR Apache-2.0")
VERSION(0.14.0)
ORIGINAL_SOURCE(https://static.crates.io/crates/mockall/mockall-0.14.0.crate)

CRATE_PROC_MACROS(
    contrib/crates/mockall_derive-0.14.0
)

END()

RECURSE(build_script)
