# Native Rust build with dependencies shared through contrib/crates.

CRATE(system_configuration)

CRATE_VERSION(0.7.0)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/dynamic_store.rs
    src/network_configuration.rs
    src/network_reachability.rs
    src/preferences.rs
)

CRATE_INPUTS(
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

PEERDIR(
    contrib/crates/bitflags-2.13.2
    contrib/crates/core-foundation-0.9.4
    contrib/crates/system-configuration-sys-0.6.0
)

LICENSE("MIT OR Apache-2.0")
VERSION(0.7.0)
ORIGINAL_SOURCE(https://static.crates.io/crates/system-configuration/system-configuration-0.7.0.crate)

CRATE_PROC_MACROS(
    contrib/crates/bytemuck_derive-1.12.1
)

END()
