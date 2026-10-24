# Native Rust build POC; dependencies and features follow Cargo.lock.

CRATE(parking_lot_core)

CRATE_VERSION(0.9.12)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)
VERSION(0.9.12)
LICENSE("MIT OR Apache-2.0")

CRATE_SOURCES(
    build.rs
    src/parking_lot.rs
    src/spinwait.rs
    src/thread_parker/generic.rs
    src/thread_parker/linux.rs
    src/thread_parker/mod.rs
    src/thread_parker/redox.rs
    src/thread_parker/sgx.rs
    src/thread_parker/unix.rs
    src/thread_parker/wasm.rs
    src/thread_parker/wasm_atomic.rs
    src/thread_parker/windows/bindings.rs
    src/thread_parker/windows/keyed_event.rs
    src/thread_parker/windows/mod.rs
    src/thread_parker/windows/waitaddress.rs
    src/util.rs
    src/word_lock.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
)

CRATE_FLAGS(--cap-lints=allow)


IF (_RUST_TARGET == "x86_64-unknown-linux-gnu" OR _RUST_TARGET == "x86_64-unknown-linux-musl" OR _RUST_TARGET == "aarch64-unknown-linux-gnu" OR _RUST_TARGET == "aarch64-unknown-linux-musl" OR _RUST_TARGET == "x86_64-apple-darwin" OR _RUST_TARGET == "aarch64-apple-darwin")
    CRATE_BUILD_SCRIPT(contrib/crates/parking_lot_core-0.9.12/build_script)
    PEERDIR(
        contrib/crates/cfg-if-1.0.4
        contrib/crates/libc-0.2.189
        contrib/crates/smallvec-1.15.2
    )
ELSEIF (_RUST_TARGET == "x86_64-pc-windows-msvc")
    CRATE_BUILD_SCRIPT(contrib/crates/parking_lot_core-0.9.12/build_script)
    PEERDIR(
        contrib/crates/cfg-if-1.0.4
        contrib/crates/smallvec-1.15.2
        contrib/crates/windows-link-0.2.1
    )
ENDIF()

END()

RECURSE(build_script)
