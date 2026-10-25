# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(filetime)

CRATE_VERSION(0.2.29)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/redox.rs
    src/unix/android.rs
    src/unix/linux.rs
    src/unix/macos.rs
    src/unix/mod.rs
    src/unix/utimensat.rs
    src/unix/utimes.rs
    src/wasm.rs
    src/windows.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    .gitignore
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    README.md
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
VERSION(0.2.29)
ORIGINAL_SOURCE(https://static.crates.io/crates/filetime/filetime-0.2.29.crate)

END()
