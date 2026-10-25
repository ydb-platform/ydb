# Native Rust build POC; host build script.

RUST_BUILD_SCRIPT(build_script_build)

CRATE_VERSION(1.19.0)
CRATE_EDITION(2018)
CRATE_ROOT(../build.rs)
CRATE_MANIFEST(../Cargo.toml)
VERSION(1.19.0)
LICENSE("MIT OR Apache-2.0")
CRATE_MANIFEST_DIR(..)

CRATE_SOURCES(
    ../src/array.rs
    ../src/bit.rs
    ../src/gen/consts.rs
    ../src/gen/generic_const_mappings.rs
    ../src/gen/op.rs
    ../src/gen.rs
    ../src/int.rs
    ../src/lib.rs
    ../src/marker_traits.rs
    ../src/operator_aliases.rs
    ../src/private.rs
    ../src/type_operators.rs
    ../src/uint.rs
)

CRATE_INPUTS(
    ../.cargo-checksum.json
    ../.cargo_vcs_info.json
    ../CHANGELOG.md
    ../Cargo.lock
    ../LICENSE
    ../LICENSE-APACHE
    ../LICENSE-MIT
    ../README.md
)

CRATE_FLAGS(--cap-lints=allow)




END()
