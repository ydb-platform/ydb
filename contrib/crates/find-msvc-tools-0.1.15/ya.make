# Native Rust build with dependencies shared through contrib/crates.

CRATE(find_msvc_tools)

CRATE_VERSION(0.1.15)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/com.rs
    src/find_tools.rs
    src/registry.rs
    src/setup_config.rs
    src/tool.rs
    src/vs_instances.rs
    src/winapi.rs
    src/windows_link.rs
    src/windows_sys.rs
    src/windows_sys_bindings.rs
)

CRATE_INPUTS(
    CHANGELOG.md
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

LICENSE("MIT OR Apache-2.0")
VERSION(0.1.15)
ORIGINAL_SOURCE(https://static.crates.io/crates/find-msvc-tools/find-msvc-tools-0.1.15.crate)

END()
