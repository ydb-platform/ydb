# Native Rust build with dependencies shared through contrib/crates.

CRATE(futures_channel)

CRATE_VERSION(0.3.34)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/lock.rs
    src/mpsc/mod.rs
    src/mpsc/queue.rs
    src/mpsc/sink_impl.rs
    src/oneshot.rs
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
    default
    futures-sink
    sink
    std
)

PEERDIR(
    contrib/crates/futures-core-0.3.34
    contrib/crates/futures-sink-0.3.34
)

LICENSE("MIT OR Apache-2.0")
VERSION(0.3.34)
ORIGINAL_SOURCE(https://static.crates.io/crates/futures-channel/futures-channel-0.3.34.crate)

END()
