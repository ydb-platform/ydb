# Native Rust build POC; dependencies and features follow Cargo.lock.

CRATE(bytemuck)

CRATE_VERSION(1.24.0)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)
VERSION(1.24.0)
LICENSE("Zlib OR Apache-2.0 OR MIT")

CRATE_SOURCES(
    src/allocation.rs
    src/anybitpattern.rs
    src/checked.rs
    src/contiguous.rs
    src/derive.rs
    src/internal.rs
    src/must.rs
    src/no_uninit.rs
    src/offset_of.rs
    src/pod.rs
    src/pod_in_option.rs
    src/transparent.rs
    src/zeroable.rs
    src/zeroable_in_option.rs
)

CRATE_INPUTS(
    .cargo/config.toml
    .cargo-checksum.json
    .cargo_vcs_info.json
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    LICENSE-ZLIB
    README.md
    changelog.md
    rustfmt.toml
)

CRATE_FLAGS(--cap-lints=allow)


CRATE_FEATURES(extern_crate_alloc)

END()
