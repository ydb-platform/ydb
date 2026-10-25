# Native Rust build POC; dependencies and features follow Cargo.lock.

CRATE(float_cmp)

CRATE_VERSION(0.10.0)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)
VERSION(0.10.0)
LICENSE("MIT")

CRATE_SOURCES(
    src/eq.rs
    src/macros.rs
    src/ratio.rs
    src/ulps.rs
    src/ulps_eq.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    .travis.yml
    LICENSE
    README.md
)

CRATE_FLAGS(--cap-lints=allow)


CRATE_FEATURES(
    default
    num-traits
    ratio
)
PEERDIR(
    contrib/crates/num-traits-0.2.19
)

CRATE_SOURCES(
    src/eq.rs
    src/lib.rs
    src/macros.rs
    src/ratio.rs
    src/ulps.rs
    src/ulps_eq.rs
)

END()
