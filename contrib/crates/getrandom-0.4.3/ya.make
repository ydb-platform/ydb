# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(getrandom)

CRATE_VERSION(0.4.3)
CRATE_EDITION(2024)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    build.rs
    src/backends.rs
    src/backends/apple_other.rs
    src/backends/custom.rs
    src/backends/efi_rng.rs
    src/backends/esp_idf.rs
    src/backends/extern_impl.rs
    src/backends/fuchsia.rs
    src/backends/getentropy.rs
    src/backends/getrandom.rs
    src/backends/hermit.rs
    src/backends/linux_android_with_fallback.rs
    src/backends/linux_raw.rs
    src/backends/netbsd.rs
    src/backends/rdrand.rs
    src/backends/rndr.rs
    src/backends/solaris.rs
    src/backends/solid.rs
    src/backends/unsupported.rs
    src/backends/use_file.rs
    src/backends/vxworks.rs
    src/backends/wasi_p1.rs
    src/backends/wasi_p2_3.rs
    src/backends/wasm_js.rs
    src/backends/windows.rs
    src/backends/windows_legacy.rs
    src/error.rs
    src/error_std_impls.rs
    src/sys_rng.rs
    src/util.rs
    src/utils/get_errno.rs
    src/utils/lazy_bool.rs
    src/utils/lazy_ptr.rs
    src/utils/sanitizer.rs
    src/utils/sys_fill_exact.rs
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
    sys_rng
)

PEERDIR(
    contrib/crates/cfg-if-1.0.5
    contrib/crates/libc-0.2.191
    contrib/crates/rand_core-0.10.1
)

CRATE_BUILD_SCRIPT(contrib/crates/getrandom-0.4.3/build_script)

LICENSE("MIT OR Apache-2.0")
VERSION(0.4.3)
ORIGINAL_SOURCE(https://static.crates.io/crates/getrandom/getrandom-0.4.3.crate)

CRATE_SOURCES(
    src/utils/get_errno.rs
    src/utils/lazy_bool.rs
    src/utils/lazy_ptr.rs
    src/utils/sanitizer.rs
    src/utils/sys_fill_exact.rs
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
    src/backends/extern_impl.rs
    src/backends/fuchsia.rs
    src/backends/getentropy.rs
    src/backends/getrandom.rs
    src/backends/hermit.rs
    src/backends/linux_android_with_fallback.rs
    src/backends/linux_raw.rs
    src/backends/netbsd.rs
    src/backends/rdrand.rs
    src/backends/rndr.rs
    src/backends/solaris.rs
    src/backends/solid.rs
    src/backends/unsupported.rs
    src/backends/use_file.rs
    src/backends/vxworks.rs
    src/backends/wasi_p1.rs
    src/backends/wasi_p2_3.rs
    src/backends/wasm_js.rs
    src/backends/windows.rs
    src/backends/windows_legacy.rs
    src/error.rs
    src/error_std_impls.rs
    src/lib.rs
    src/sys_rng.rs
    src/util.rs
)


END()

RECURSE(build_script)
