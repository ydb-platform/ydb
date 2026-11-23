# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(windows_numerics)

CRATE_VERSION(0.3.1)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/bindings.rs
    src/matrix3x2.rs
    src/matrix4x4.rs
    src/vector2.rs
    src/vector3.rs
    src/vector4.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    Cargo.lock
    license-apache-2.0
    license-mit
    readme.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    std
)

PEERDIR(
    contrib/crates/windows-core-0.62.2
    contrib/crates/windows-link-0.2.1
)

LICENSE("MIT OR Apache-2.0")
VERSION(0.3.1)
ORIGINAL_SOURCE(https://static.crates.io/crates/windows-numerics/windows-numerics-0.3.1.crate)

CRATE_INPUTS(
    readme.md
)

CRATE_SOURCES(
    src/bindings.rs
    src/lib.rs
    src/matrix3x2.rs
    src/matrix4x4.rs
    src/vector2.rs
    src/vector3.rs
    src/vector4.rs
)

CRATE_PROC_MACROS(
    contrib/crates/windows-implement-0.60.2
    contrib/crates/windows-interface-0.59.3
)

END()
