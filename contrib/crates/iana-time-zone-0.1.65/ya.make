# Native Rust build with dependencies shared through contrib/crates.

CRATE(iana_time_zone)

CRATE_VERSION(0.1.65)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/ffi_utils.rs
    src/platform.rs
    src/tz_aix.rs
    src/tz_android.rs
    src/tz_darwin.rs
    src/tz_freebsd.rs
    src/tz_haiku.rs
    src/tz_illumos.rs
    src/tz_linux.rs
    src/tz_netbsd.rs
    src/tz_ohos.rs
    src/tz_wasm32_emscripten.rs
    src/tz_wasm32_unknown.rs
    src/tz_wasm32_wasi.rs
    src/tz_windows.rs
    src/windows_bindings.rs
)

CRATE_INPUTS(
    CHANGELOG.md
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    README.md
    bindings.txt
    deny.toml
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    fallback
)

IF (OS_DARWIN AND ARCH_ARM64 OR OS_DARWIN AND ARCH_X86_64)
    PEERDIR(
    contrib/crates/core-foundation-sys-0.8.7
)

ELSEIF (OS_LINUX AND ARCH_AARCH64 AND _RUST_MODE == "host" OR OS_LINUX AND ARCH_AARCH64 AND NOT MUSL OR OS_LINUX AND ARCH_AARCH64 AND _RUST_MODE != "host" AND MUSL OR OS_LINUX AND ARCH_X86_64 AND _RUST_MODE == "host" OR OS_LINUX AND ARCH_X86_64 AND NOT MUSL OR OS_LINUX AND ARCH_X86_64 AND _RUST_MODE != "host" AND MUSL)
ENDIF()

LICENSE("MIT OR Apache-2.0")
VERSION(0.1.65)
ORIGINAL_SOURCE(https://static.crates.io/crates/iana-time-zone/iana-time-zone-0.1.65.crate)

END()
