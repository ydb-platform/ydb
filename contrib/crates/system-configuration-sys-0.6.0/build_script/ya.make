# Native Rust build with dependencies shared through contrib/crates.

RUST_BUILD_SCRIPT(build_script_build)

CRATE_VERSION(0.6.0)
CRATE_EDITION(2021)
CRATE_ROOT(../build.rs)
CRATE_MANIFEST(../Cargo.toml)
CRATE_MANIFEST_DIR(..)

CRATE_SOURCES(
    ../src/dynamic_store.rs
    ../src/dynamic_store_copy_specific.rs
    ../src/lib.rs
    ../src/network_configuration.rs
    ../src/network_reachability.rs
    ../src/preferences.rs
    ../src/schema_definitions.rs
)

CRATE_INPUTS(
    ../LICENSE-APACHE
    ../LICENSE-MIT
)

CRATE_FLAGS(--cap-lints=allow)

LICENSE("MIT OR Apache-2.0")
VERSION(0.6.0)
ORIGINAL_SOURCE(https://static.crates.io/crates/system-configuration-sys/system-configuration-sys-0.6.0.crate)

END()
