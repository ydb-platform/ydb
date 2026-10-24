# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(gethostname)

CRATE_VERSION(1.1.0)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    .gitignore
    CHANGELOG.md
    Cargo.lock
    LICENSE
    README.md
    deny.toml
    justfile
)

CRATE_FLAGS(--cap-lints=allow)

IF (_RUST_TARGET == "aarch64-apple-darwin" OR _RUST_TARGET == "aarch64-unknown-linux-gnu" OR _RUST_TARGET == "aarch64-unknown-linux-musl" OR _RUST_TARGET == "x86_64-apple-darwin" OR _RUST_TARGET == "x86_64-unknown-linux-gnu" OR _RUST_TARGET == "x86_64-unknown-linux-musl")
    PEERDIR(
        contrib/crates/rustix-1.1.4
    )

ELSEIF (_RUST_TARGET == "x86_64-pc-windows-msvc")
    PEERDIR(
        contrib/crates/windows-link-0.2.1
    )

ENDIF()

LICENSE("Apache-2.0")
VERSION(1.1.0)
ORIGINAL_SOURCE(https://static.crates.io/crates/gethostname/gethostname-1.1.0.crate)

END()
