# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(tower_layer)

CRATE_VERSION(0.3.3)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/identity.rs
    src/layer_fn.rs
    src/stack.rs
    src/tuple.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    CHANGELOG.md
    LICENSE
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

LICENSE("MIT")
VERSION(0.3.3)
ORIGINAL_SOURCE(https://static.crates.io/crates/tower-layer/tower-layer-0.3.3.crate)

CRATE_SOURCES(
    src/identity.rs
    src/layer_fn.rs
    src/lib.rs
    src/stack.rs
    src/tuple.rs
)

END()
