# Native Rust build POC; dependencies and features follow Cargo.lock.

CRATE(byteorder_lite)

CRATE_VERSION(0.1.0)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)
VERSION(0.1.0)
LICENSE("Unlicense OR MIT")

CRATE_SOURCES(src/io.rs)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    LICENSE-MIT
    README.md
    UNLICENSE
)

CRATE_FLAGS(--cap-lints=allow)


CRATE_FEATURES(
    default
    std
)

CRATE_SOURCES(
    src/io.rs
    src/lib.rs
)

END()
