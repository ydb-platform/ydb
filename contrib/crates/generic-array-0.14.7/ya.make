# Native Rust build with dependencies shared through contrib/crates.

CRATE(generic_array)

CRATE_VERSION(0.14.7)
CRATE_EDITION(2015)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    build.rs
    src/arr.rs
    src/functional.rs
    src/hex.rs
    src/impl_serde.rs
    src/impl_zeroize.rs
    src/impls.rs
    src/iter.rs
    src/sequence.rs
)

CRATE_INPUTS(
    CHANGELOG.md
    LICENSE
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    more_lengths
)

PEERDIR(
    contrib/crates/typenum-1.20.1
)

CRATE_BUILD_SCRIPT(contrib/crates/generic-array-0.14.7/build_script)

LICENSE("MIT")
VERSION(0.14.7)
ORIGINAL_SOURCE(https://static.crates.io/crates/generic-array/generic-array-0.14.7.crate)

END()

RECURSE(build_script)
