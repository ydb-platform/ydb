# Native Rust build POC; dependencies and features follow Cargo.lock.

CRATE(futf)

CRATE_VERSION(0.1.5)
CRATE_EDITION(2015)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)
VERSION(0.1.5)
LICENSE("MIT  OR  Apache-2.0")

CRATE_SOURCES(src/test.rs)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    .travis.yml
    LICENSE-APACHE
    LICENSE-MIT
    README.md
)

CRATE_FLAGS(--cap-lints=allow)


PEERDIR(
    contrib/crates/mac-0.1.1
    contrib/crates/new_debug_unreachable-1.0.6
)

END()
