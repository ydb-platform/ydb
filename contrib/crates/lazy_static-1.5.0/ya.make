# Native Rust build POC; dependencies and features follow Cargo.lock.

CRATE(lazy_static)

CRATE_VERSION(1.5.0)
CRATE_EDITION(2015)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)
VERSION(1.5.0)
LICENSE("MIT OR Apache-2.0")

CRATE_SOURCES(
    examples/mutex_map.rs
    src/core_lazy.rs
    src/inline_lazy.rs
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




END()
