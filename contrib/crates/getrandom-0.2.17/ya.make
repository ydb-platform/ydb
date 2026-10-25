# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(getrandom)

CRATE_VERSION(0.2.17)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/apple-other.rs
    src/custom.rs
    src/error.rs
    src/error_impls.rs
    src/espidf.rs
    src/fuchsia.rs
    src/getentropy.rs
    src/getrandom.rs
    src/hermit.rs
    src/js.rs
    src/lazy.rs
    src/linux_android.rs
    src/linux_android_with_fallback.rs
    src/netbsd.rs
    src/rdrand.rs
    src/solaris.rs
    src/solid.rs
    src/use_file.rs
    src/util.rs
    src/util_libc.rs
    src/vxworks.rs
    src/wasi.rs
    src/windows.rs
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

PEERDIR(
    contrib/crates/cfg-if-1.0.4
)

IF (_RUST_TARGET == "aarch64-apple-darwin" OR _RUST_TARGET == "aarch64-unknown-linux-gnu" OR _RUST_TARGET == "aarch64-unknown-linux-musl" OR _RUST_TARGET == "x86_64-apple-darwin" OR _RUST_TARGET == "x86_64-unknown-linux-gnu" OR _RUST_TARGET == "x86_64-unknown-linux-musl")
    PEERDIR(
        contrib/crates/libc-0.2.189
    )

ELSEIF (_RUST_TARGET == "x86_64-pc-windows-msvc")
ENDIF()

LICENSE("MIT OR Apache-2.0")
VERSION(0.2.17)
ORIGINAL_SOURCE(https://static.crates.io/crates/getrandom/getrandom-0.2.17.crate)

END()
