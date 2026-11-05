# Native Rust build with dependencies shared through contrib/crates.

CRATE(tracing_log)

CRATE_VERSION(0.2.0)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/interest_cache.rs
    src/log_tracer.rs
)

CRATE_INPUTS(
    CHANGELOG.md
    LICENSE
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    default
    log-tracer
    std
)

PEERDIR(
    contrib/crates/log-0.4.34
    contrib/crates/once_cell-1.21.4
    contrib/crates/tracing-core-0.1.36
)

LICENSE("MIT")
VERSION(0.2.0)
ORIGINAL_SOURCE(https://static.crates.io/crates/tracing-log/tracing-log-0.2.0.crate)

END()
