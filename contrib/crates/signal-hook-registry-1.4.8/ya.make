# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(signal_hook_registry)

CRATE_VERSION(1.4.8)
CRATE_EDITION(2015)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/half_lock.rs
    src/vec_map.rs
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
    contrib/crates/errno-0.3.14
    contrib/crates/libc-0.2.191
)

LICENSE("MIT OR Apache-2.0")
VERSION(1.4.8)
ORIGINAL_SOURCE(https://static.crates.io/crates/signal-hook-registry/signal-hook-registry-1.4.8.crate)

CRATE_SOURCES(
    src/half_lock.rs
    src/lib.rs
    src/vec_map.rs
)

END()
