# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(getrandom)

CRATE_VERSION(0.3.4)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    build.rs
    src/backends.rs
    src/backends/apple_other.rs
    src/backends/custom.rs
    src/backends/efi_rng.rs
    src/backends/esp_idf.rs
    src/backends/fuchsia.rs
    src/backends/getentropy.rs
    src/backends/getrandom.rs
    src/backends/hermit.rs
    src/backends/linux_android_with_fallback.rs
    src/backends/linux_raw.rs
    src/backends/netbsd.rs
    src/backends/rdrand.rs
    src/backends/rndr.rs
    src/backends/sanitizer.rs
    src/backends/solaris.rs
    src/backends/solid.rs
    src/backends/unsupported.rs
    src/backends/use_file.rs
    src/backends/vxworks.rs
    src/backends/wasi_p1.rs
    src/backends/wasi_p2.rs
    src/backends/wasm_js.rs
    src/backends/windows.rs
    src/backends/windows_legacy.rs
    src/error.rs
    src/error_std_impls.rs
    src/lazy.rs
    src/util.rs
    src/util_libc.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    CHANGELOG.md
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    README.md
    SECURITY.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    std
)

PEERDIR(
    contrib/crates/cfg-if-1.0.5
)

IF (OS_DARWIN AND ARCH_ARM64 OR OS_LINUX AND ARCH_AARCH64 AND _RUST_MODE == "host" OR OS_LINUX AND ARCH_AARCH64 AND NOT MUSL OR OS_LINUX AND ARCH_AARCH64 AND _RUST_MODE != "host" AND MUSL OR OS_DARWIN AND ARCH_X86_64 OR OS_LINUX AND ARCH_X86_64 AND _RUST_MODE == "host" OR OS_LINUX AND ARCH_X86_64 AND NOT MUSL OR OS_LINUX AND ARCH_X86_64 AND _RUST_MODE != "host" AND MUSL)
    PEERDIR(
    contrib/crates/libc-0.2.191
)

ELSEIF (OS_WINDOWS AND ARCH_X86_64)
ENDIF()

CRATE_BUILD_SCRIPT(contrib/crates/getrandom-0.3.4/build_script)

LICENSE("MIT OR Apache-2.0")
VERSION(0.3.4)
ORIGINAL_SOURCE(https://static.crates.io/crates/getrandom/getrandom-0.3.4.crate)

CRATE_FEATURES(
    wasm_js
)

CRATE_SOURCES(
    src/lazy.rs
    src/util_libc.rs
)

CRATE_INPUTS(
    README.md
)

CRATE_SOURCES(
    src/backends.rs
    src/backends/apple_other.rs
    src/backends/custom.rs
    src/backends/efi_rng.rs
    src/backends/esp_idf.rs
    src/backends/fuchsia.rs
    src/backends/getentropy.rs
    src/backends/getrandom.rs
    src/backends/hermit.rs
    src/backends/linux_android_with_fallback.rs
    src/backends/linux_raw.rs
    src/backends/netbsd.rs
    src/backends/rdrand.rs
    src/backends/rndr.rs
    src/backends/sanitizer.rs
    src/backends/solaris.rs
    src/backends/solid.rs
    src/backends/unsupported.rs
    src/backends/use_file.rs
    src/backends/vxworks.rs
    src/backends/wasi_p1.rs
    src/backends/wasi_p2.rs
    src/backends/wasm_js.rs
    src/backends/windows.rs
    src/backends/windows_legacy.rs
    src/error.rs
    src/error_std_impls.rs
    src/lib.rs
    src/util.rs
)


END()

RECURSE(build_script)
