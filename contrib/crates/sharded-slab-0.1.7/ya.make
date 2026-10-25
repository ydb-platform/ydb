# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(sharded_slab)

CRATE_VERSION(0.1.7)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/cfg.rs
    src/clear.rs
    src/implementation.rs
    src/iter.rs
    src/macros.rs
    src/page/mod.rs
    src/page/slot.rs
    src/page/stack.rs
    src/pool.rs
    src/shard.rs
    src/sync.rs
    src/tests/custom_config.rs
    src/tests/loom_pool.rs
    src/tests/loom_slab.rs
    src/tests/mod.rs
    src/tests/properties.rs
    src/tid.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    .clog.toml
    .envrc
    .gitignore
    CHANGELOG.md
    IMPLEMENTATION.md
    LICENSE
    README.md
    bin/loom.sh
    flake.lock
    flake.nix
    rust-toolchain.toml
)

CRATE_FLAGS(--cap-lints=allow)

PEERDIR(
    contrib/crates/lazy_static-1.5.1
)

LICENSE("MIT")
VERSION(0.1.7)
ORIGINAL_SOURCE(https://static.crates.io/crates/sharded-slab/sharded-slab-0.1.7.crate)

CRATE_SOURCES(
    src/cfg.rs
    src/clear.rs
    src/implementation.rs
    src/iter.rs
    src/lib.rs
    src/macros.rs
    src/page/mod.rs
    src/page/slot.rs
    src/page/stack.rs
    src/pool.rs
    src/shard.rs
    src/sync.rs
    src/tests/custom_config.rs
    src/tests/loom_pool.rs
    src/tests/loom_slab.rs
    src/tests/mod.rs
    src/tests/properties.rs
    src/tid.rs
)

END()
