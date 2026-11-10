# Native Rust build POC; dependencies and features follow Cargo.lock.

CRATE(byteorder)

CRATE_VERSION(1.5.0)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)
VERSION(1.5.0)
LICENSE("Unlicense OR MIT")

CRATE_SOURCES(src/io.rs)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    CHANGELOG.md
    COPYING
    LICENSE-MIT
    README.md
    UNLICENSE
    rustfmt.toml
)

CRATE_FLAGS(--cap-lints=allow)


CRATE_FEATURES(
    default
    std
)

CRATE_FEATURES(
    i128
)

CRATE_SOURCES(
    src/io.rs
    src/lib.rs
)

END()
