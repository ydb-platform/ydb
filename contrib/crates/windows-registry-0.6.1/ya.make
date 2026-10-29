# Native Rust build with dependencies shared through contrib/crates.

CRATE(windows_registry)

CRATE_VERSION(0.6.1)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/bindings.rs
    src/data.rs
    src/key.rs
    src/key_iterator.rs
    src/open_options.rs
    src/pcwstr.rs
    src/transaction.rs
    src/type.rs
    src/value.rs
    src/value_iterator.rs
)

CRATE_INPUTS(
    Cargo.lock
    license-apache-2.0
    license-mit
    readme.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    default
    std
)

PEERDIR(
    contrib/crates/windows-link-0.2.1
    contrib/crates/windows-result-0.4.1
    contrib/crates/windows-strings-0.5.1
)

LICENSE("MIT OR Apache-2.0")
VERSION(0.6.1)
ORIGINAL_SOURCE(https://static.crates.io/crates/windows-registry/windows-registry-0.6.1.crate)

END()
