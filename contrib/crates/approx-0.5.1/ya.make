# Native Rust build POC; dependencies and features follow Cargo.lock.

CRATE(approx)

CRATE_VERSION(0.5.1)
CRATE_EDITION(2015)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)
VERSION(0.5.1)
LICENSE("Apache-2.0")

CRATE_SOURCES(
    src/abs_diff_eq.rs
    src/macros.rs
    src/relative_eq.rs
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


CRATE_FEATURES(std)
PEERDIR(
    contrib/crates/num-traits-0.2.19
)

END()
