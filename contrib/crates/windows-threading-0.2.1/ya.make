# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(windows_threading)

CRATE_VERSION(0.2.1)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/bindings.rs
    src/pool.rs
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

PEERDIR(
    contrib/crates/windows-link-0.2.1
)

LICENSE("MIT OR Apache-2.0")
VERSION(0.2.1)
ORIGINAL_SOURCE(https://static.crates.io/crates/windows-threading/windows-threading-0.2.1.crate)

CRATE_INPUTS(
    readme.md
)

CRATE_SOURCES(
    src/bindings.rs
    src/lib.rs
    src/pool.rs
)

END()
