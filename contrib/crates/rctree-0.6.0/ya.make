# Native Rust build POC; dependencies and features follow Cargo.lock.

CRATE(rctree)

CRATE_VERSION(0.6.0)
CRATE_EDITION(2015)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)
VERSION(0.6.0)
LICENSE("MIT")


CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    LICENSE
    README.md
)

CRATE_FLAGS(--cap-lints=allow)




END()
