# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(typeid)

CRATE_VERSION(1.0.3)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    build.rs
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

CRATE_BUILD_SCRIPT(contrib/crates/typeid-1.0.3/build_script)

LICENSE("MIT OR Apache-2.0")
VERSION(1.0.3)
ORIGINAL_SOURCE(https://static.crates.io/crates/typeid/typeid-1.0.3.crate)

CRATE_SOURCES(
    src/lib.rs
)

END()

RECURSE(build_script)
