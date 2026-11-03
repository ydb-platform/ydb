# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(windows_future)

CRATE_VERSION(0.3.2)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/async.rs
    src/async_ready.rs
    src/async_spawn.rs
    src/bindings.rs
    src/bindings_impl.rs
    src/future.rs
    src/join.rs
    src/waiter.rs
    src/when.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    Cargo.lock
    license-apache-2.0
    license-mit
    readme.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    std
)

PEERDIR(
    contrib/crates/windows-core-0.62.2
    contrib/crates/windows-link-0.2.1
    contrib/crates/windows-threading-0.2.1
)

LICENSE("MIT OR Apache-2.0")
VERSION(0.3.2)
ORIGINAL_SOURCE(https://static.crates.io/crates/windows-future/windows-future-0.3.2.crate)

CRATE_INPUTS(
    readme.md
)

CRATE_SOURCES(
    src/async.rs
    src/async_ready.rs
    src/async_spawn.rs
    src/bindings.rs
    src/bindings_impl.rs
    src/future.rs
    src/join.rs
    src/lib.rs
    src/waiter.rs
    src/when.rs
)

CRATE_PROC_MACROS(
    contrib/crates/windows-implement-0.60.2
    contrib/crates/windows-interface-0.59.3
)

END()
