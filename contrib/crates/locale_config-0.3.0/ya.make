# Native Rust build POC; dependencies and features follow Cargo.lock.

CRATE(locale_config)

CRATE_VERSION(0.3.0)
CRATE_EDITION(2015)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)
VERSION(0.3.0)
LICENSE("MIT")

CRATE_SOURCES(
    examples/show-user-locale.rs
    src/cgi.rs
    src/emscripten.rs
    src/macos.rs
    src/unix.rs
    src/win32.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    LICENSE
    README.md
)

CRATE_FLAGS(--cap-lints=allow)


IF (_RUST_TARGET == "x86_64-unknown-linux-gnu" OR _RUST_TARGET == "x86_64-unknown-linux-musl" OR _RUST_TARGET == "aarch64-unknown-linux-gnu" OR _RUST_TARGET == "aarch64-unknown-linux-musl")
    PEERDIR(
        contrib/crates/lazy_static-1.5.0
        contrib/crates/regex-1.13.1
    )
ELSEIF (_RUST_TARGET == "x86_64-apple-darwin" OR _RUST_TARGET == "aarch64-apple-darwin")
    PEERDIR(
        contrib/crates/lazy_static-1.5.0
        contrib/crates/objc-0.2.7
        contrib/crates/objc-foundation-0.1.1
        contrib/crates/regex-1.13.1
    )
ELSEIF (_RUST_TARGET == "x86_64-pc-windows-msvc")
    PEERDIR(
        contrib/crates/lazy_static-1.5.0
        contrib/crates/regex-1.13.1
        contrib/crates/winapi-0.3.9
    )
ENDIF()

END()
