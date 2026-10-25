# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(dirs_sys)

CRATE_VERSION(0.5.0)
CRATE_EDITION(2015)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/xdg_user_dirs.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    .gitignore
    CONTRIBUTING.md
    LICENSE-APACHE
    LICENSE-MIT
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

PEERDIR(
    contrib/crates/option-ext-0.2.0
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
VERSION(0.5.0)
ORIGINAL_SOURCE(https://static.crates.io/crates/dirs-sys/dirs-sys-0.5.0.crate)

END()
