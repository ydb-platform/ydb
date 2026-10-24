# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(jobserver)

CRATE_VERSION(0.1.35)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/error.rs
    src/unix.rs
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
    contrib/crates/libc-0.2.189
)

LICENSE("MIT OR Apache-2.0")
VERSION(0.1.35)
ORIGINAL_SOURCE(https://static.crates.io/crates/jobserver/jobserver-0.1.35.crate)

IF (_RUST_TARGET == "x86_64-pc-windows-msvc")
    PEERDIR(contrib/crates/getrandom-0.4.3)
ENDIF()

END()
