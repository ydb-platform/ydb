# Native Rust build with dependencies shared through contrib/crates.

CRATE(bytemuck)

CRATE_VERSION(1.25.2)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

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
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    LICENSE-ZLIB
    README.md
    changelog.md
    rustfmt.toml
)

CRATE_FLAGS(--cap-lints=allow)

LICENSE("Zlib OR Apache-2.0 OR MIT")
VERSION(1.25.2)
ORIGINAL_SOURCE(https://static.crates.io/crates/bytemuck/bytemuck-1.25.2.crate)

CRATE_FEATURES(
    bytemuck_derive
    derive
    extern_crate_alloc
    must_cast
)

CRATE_PROC_MACROS(
    contrib/crates/bytemuck_derive-1.12.1
)

END()
