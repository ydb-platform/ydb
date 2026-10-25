# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(futures_channel)

CRATE_VERSION(0.3.33)
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
    .cargo-checksum.json
    .cargo_vcs_info.json
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
    contrib/crates/futures-core-0.3.33
    contrib/crates/futures-sink-0.3.33
)

LICENSE("MIT OR Apache-2.0")
VERSION(0.3.33)
ORIGINAL_SOURCE(https://static.crates.io/crates/futures-channel/futures-channel-0.3.33.crate)

END()
