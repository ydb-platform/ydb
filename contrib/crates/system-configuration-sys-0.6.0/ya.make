# Native Rust build with dependencies shared through contrib/crates.

CRATE(system_configuration_sys)

CRATE_VERSION(0.6.0)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    build.rs
    src/dynamic_store.rs
    src/dynamic_store_copy_specific.rs
    src/network_configuration.rs
    src/network_reachability.rs
    src/preferences.rs
    src/schema_definitions.rs
)

CRATE_INPUTS(
    LICENSE-APACHE
    LICENSE-MIT
)

CRATE_FLAGS(--cap-lints=allow)

PEERDIR(
    contrib/crates/core-foundation-sys-0.8.7
    contrib/crates/libc-0.2.191
)

CRATE_BUILD_SCRIPT(contrib/crates/system-configuration-sys-0.6.0/build_script)

LICENSE("MIT OR Apache-2.0")
VERSION(0.6.0)
ORIGINAL_SOURCE(https://static.crates.io/crates/system-configuration-sys/system-configuration-sys-0.6.0.crate)

END()

RECURSE(build_script)
