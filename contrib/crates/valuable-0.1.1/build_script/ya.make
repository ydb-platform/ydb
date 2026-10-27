# Native Rust build with dependencies shared through contrib/crates.

RUST_BUILD_SCRIPT(build_script_build)

CRATE_VERSION(0.1.1)
CRATE_EDITION(2021)
CRATE_ROOT(../build.rs)
CRATE_MANIFEST(../Cargo.toml)
CRATE_MANIFEST_DIR(..)

CRATE_SOURCES(
    ../no_atomic.rs
    ../src/enumerable.rs
    ../src/field.rs
    ../src/lib.rs
    ../src/listable.rs
    ../src/mappable.rs
    ../src/named_values.rs
    ../src/slice.rs
    ../src/structable.rs
    ../src/tuplable.rs
    ../src/valuable.rs
    ../src/value.rs
    ../src/visit.rs
)

CRATE_INPUTS(
    ../Cargo.lock
    ../README.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    alloc
    default
    derive
    std
    valuable-derive
)

LICENSE("MIT")
VERSION(0.1.1)
ORIGINAL_SOURCE(https://static.crates.io/crates/valuable/valuable-0.1.1.crate)

END()
