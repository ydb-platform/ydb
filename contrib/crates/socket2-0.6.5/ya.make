# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(socket2)

CRATE_VERSION(0.6.5)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/sockaddr.rs
    src/socket.rs
    src/sockref.rs
    src/sys/unix.rs
    src/sys/windows.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    all
)

IF (_RUST_TARGET == "aarch64-apple-darwin" OR _RUST_TARGET == "aarch64-unknown-linux-gnu" OR _RUST_TARGET == "aarch64-unknown-linux-musl" OR _RUST_TARGET == "x86_64-apple-darwin" OR _RUST_TARGET == "x86_64-unknown-linux-gnu" OR _RUST_TARGET == "x86_64-unknown-linux-musl")
    PEERDIR(
        contrib/crates/libc-0.2.189
    )

ELSEIF (_RUST_TARGET == "x86_64-pc-windows-msvc")
    PEERDIR(
        contrib/crates/windows-sys-0.61.2
    )

ENDIF()

LICENSE("MIT OR Apache-2.0")
VERSION(0.6.5)
ORIGINAL_SOURCE(https://static.crates.io/crates/socket2/socket2-0.6.5.crate)

END()
