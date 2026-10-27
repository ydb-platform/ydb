# Native Rust build POC; dependencies and features follow Cargo.lock.

CRATE(color_quant)

CRATE_VERSION(1.1.0)
CRATE_EDITION(2015)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)
VERSION(1.1.0)
LICENSE("MIT")

CRATE_SOURCES(src/math.rs)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    CHANGELOG.md
    LICENSE
    README.md
)

CRATE_FLAGS(--cap-lints=allow)




END()
