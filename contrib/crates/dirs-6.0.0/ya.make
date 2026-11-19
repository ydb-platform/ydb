# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(dirs)

CRATE_VERSION(6.0.0)
CRATE_EDITION(2015)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/lin.rs
    src/mac.rs
    src/wasm.rs
    src/win.rs
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
    contrib/crates/dirs-sys-0.5.0
)

LICENSE("MIT OR Apache-2.0")
VERSION(6.0.0)
ORIGINAL_SOURCE(https://static.crates.io/crates/dirs/dirs-6.0.0.crate)

CRATE_SOURCES(
    src/lib.rs
    src/lin.rs
    src/mac.rs
    src/wasm.rs
    src/win.rs
)

END()
