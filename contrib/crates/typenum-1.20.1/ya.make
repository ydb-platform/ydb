# Native Rust build with dependencies shared through contrib/crates.

CRATE(typenum)

CRATE_VERSION(1.20.1)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/array.rs
    src/bit.rs
    src/gen.rs
    src/gen/consts.rs
    src/gen/generic_const_mappings.rs
    src/gen/op.rs
    src/int.rs
    src/marker_traits.rs
    src/operator_aliases.rs
    src/private.rs
    src/tuple.rs
    src/type_operators.rs
    src/uint.rs
)

CRATE_INPUTS(
    CHANGELOG.md
    Cargo.lock
    LICENSE
    LICENSE-APACHE
    LICENSE-MIT
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

LICENSE("MIT OR Apache-2.0")
VERSION(1.20.1)
ORIGINAL_SOURCE(https://static.crates.io/crates/typenum/typenum-1.20.1.crate)


END()
