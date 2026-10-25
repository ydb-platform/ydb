# Native Rust build with dependencies shared through contrib/crates.

CRATE(futures_task)

CRATE_VERSION(0.3.34)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/arc_wake.rs
    src/future_obj.rs
    src/noop_waker.rs
    src/spawn.rs
    src/waker.rs
    src/waker_ref.rs
)

CRATE_INPUTS(
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    alloc
    std
)

LICENSE("MIT OR Apache-2.0")
VERSION(0.3.34)
ORIGINAL_SOURCE(https://static.crates.io/crates/futures-task/futures-task-0.3.34.crate)

END()
