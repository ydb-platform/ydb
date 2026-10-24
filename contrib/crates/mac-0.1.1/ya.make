# Native Rust build POC; dependencies and features follow Cargo.lock.

CRATE(mac)

CRATE_VERSION(0.1.1)
CRATE_EDITION(2015)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)
VERSION(0.1.1)
LICENSE("MIT OR Apache-2.0")

CRATE_SOURCES(
    src/cfg.rs
    src/format.rs
    src/inspect.rs
    src/matches.rs
    src/mem.rs
    src/syntax_ext.rs
    src/test.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .travis.yml
    README.md
)

CRATE_FLAGS(--cap-lints=allow)




END()
