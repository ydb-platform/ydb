# Native Rust build POC; dependencies and features follow Cargo.lock.

CRATE(typenum)

CRATE_VERSION(1.19.0)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)
VERSION(1.19.0)
LICENSE("MIT OR Apache-2.0")

CRATE_SOURCES(
    build.rs
    src/array.rs
    src/bit.rs
    src/gen/consts.rs
    src/gen/generic_const_mappings.rs
    src/gen/op.rs
    src/gen.rs
    src/int.rs
    src/marker_traits.rs
    src/operator_aliases.rs
    src/private.rs
    src/type_operators.rs
    src/uint.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    CHANGELOG.md
    Cargo.lock
    LICENSE
    LICENSE-APACHE
    LICENSE-MIT
    README.md
)

CRATE_FLAGS(--cap-lints=allow)


CRATE_BUILD_SCRIPT(contrib/crates/typenum-1.19.0/build_script)

END()

RECURSE(build_script)
