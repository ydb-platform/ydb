# Native Rust build POC; host build script.

RUST_BUILD_SCRIPT(build_script_build)

CRATE_VERSION(0.22.0)
CRATE_EDITION(2024)
CRATE_ROOT(../build.rs)
CRATE_MANIFEST(../Cargo.toml)
VERSION(0.22.0)
LICENSE("MIT")
CRATE_MANIFEST_DIR(..)

CRATE_SOURCES(../src/lib.rs)

CRATE_INPUTS(
    ../.cargo-checksum.json
    ../.cargo_vcs_info.json
    ../Cargo.lock
    ../Gir.toml
    ../LICENSE
    ../versions.txt
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_ENV(
    SYSTEM_DEPS_PANGOCAIRO_NO_PKG_CONFIG=1
    SYSTEM_DEPS_PANGOCAIRO_LIB=pangocairo
)

PEERDIR(
    contrib/crates/system-deps-7.0.7
)

CRATE_PROC_MACROS(
    contrib/crates/serde_derive-1.0.229
)

END()
