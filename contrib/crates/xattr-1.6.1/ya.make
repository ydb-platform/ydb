# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(xattr)

CRATE_VERSION(1.6.1)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/error.rs
    src/sys/bsd.rs
    src/sys/linux_macos.rs
    src/sys/mod.rs
    src/sys/unsupported.rs
    src/util.rs
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

CRATE_FEATURES(
    default
    unsupported
)

PEERDIR(
    contrib/crates/rustix-1.1.5
)

LICENSE("MIT OR Apache-2.0")
VERSION(1.6.1)
ORIGINAL_SOURCE(https://static.crates.io/crates/xattr/xattr-1.6.1.crate)

CRATE_SOURCES(
    src/error.rs
    src/lib.rs
    src/sys/bsd.rs
    src/sys/linux_macos.rs
    src/sys/mod.rs
    src/sys/unsupported.rs
    src/util.rs
)

CRATE_PROC_MACROS(
    contrib/crates/bytemuck_derive-1.12.1
)

END()
