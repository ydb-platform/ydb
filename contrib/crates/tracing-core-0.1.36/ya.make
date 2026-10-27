# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(tracing_core)

CRATE_VERSION(0.1.36)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/callsite.rs
    src/dispatcher.rs
    src/event.rs
    src/field.rs
    src/lazy.rs
    src/metadata.rs
    src/parent.rs
    src/span.rs
    src/spin/mod.rs
    src/spin/mutex.rs
    src/spin/once.rs
    src/subscriber.rs
    src/sync.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    CHANGELOG.md
    Cargo.lock
    LICENSE
    README.md
    src/spin/LICENSE
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    default
    once_cell
    std
)

PEERDIR(
    contrib/crates/once_cell-1.21.4
)

LICENSE("MIT")
VERSION(0.1.36)
ORIGINAL_SOURCE(https://static.crates.io/crates/tracing-core/tracing-core-0.1.36.crate)

CRATE_FEATURES(
    valuable
)

CRATE_SOURCES(
    src/callsite.rs
    src/dispatcher.rs
    src/event.rs
    src/field.rs
    src/lazy.rs
    src/lib.rs
    src/metadata.rs
    src/parent.rs
    src/span.rs
    src/spin/mod.rs
    src/spin/mutex.rs
    src/spin/once.rs
    src/subscriber.rs
    src/sync.rs
)

CRATE_INPUTS(
    src/spin/LICENSE
)

END()
