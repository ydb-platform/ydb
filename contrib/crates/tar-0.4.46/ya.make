# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(tar)

CRATE_VERSION(0.4.46)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/archive.rs
    src/builder.rs
    src/entry.rs
    src/entry_type.rs
    src/error.rs
    src/header.rs
    src/pax.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo/config.toml
    .cargo_vcs_info.json
    .gitignore
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    default
    xattr
)

PEERDIR(
    contrib/crates/filetime-0.2.29
)

IF (_RUST_TARGET == "aarch64-apple-darwin" OR _RUST_TARGET == "aarch64-unknown-linux-gnu" OR _RUST_TARGET == "aarch64-unknown-linux-musl" OR _RUST_TARGET == "x86_64-apple-darwin" OR _RUST_TARGET == "x86_64-unknown-linux-gnu" OR _RUST_TARGET == "x86_64-unknown-linux-musl")
    PEERDIR(
        contrib/crates/libc-0.2.189
        contrib/crates/xattr-1.6.1
    )

ELSEIF (_RUST_TARGET == "x86_64-pc-windows-msvc")
ENDIF()

LICENSE("MIT OR Apache-2.0")
VERSION(0.4.46)
ORIGINAL_SOURCE(https://static.crates.io/crates/tar/tar-0.4.46.crate)

END()
