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

IF (OS_LINUX AND ARCH_X86_64 AND _RUST_MODE == "host" OR OS_LINUX AND ARCH_X86_64 AND NOT MUSL OR OS_LINUX AND ARCH_X86_64 AND _RUST_MODE != "host" AND MUSL OR OS_LINUX AND ARCH_AARCH64 AND _RUST_MODE == "host" OR OS_LINUX AND ARCH_AARCH64 AND NOT MUSL OR OS_LINUX AND ARCH_AARCH64 AND _RUST_MODE != "host" AND MUSL OR OS_DARWIN AND ARCH_X86_64 OR OS_DARWIN AND ARCH_ARM64)
    CRATE_BUILD_SCRIPT(contrib/crates/parking_lot_core-0.9.12/build_script)
    PEERDIR(
    contrib/crates/cfg-if-1.0.5
    contrib/crates/libc-0.2.191
    contrib/crates/smallvec-1.16.4
)
ELSEIF (OS_WINDOWS AND ARCH_X86_64)
    CRATE_BUILD_SCRIPT(contrib/crates/parking_lot_core-0.9.12/build_script)
    PEERDIR(
    contrib/crates/cfg-if-1.0.5
    contrib/crates/smallvec-1.16.4
    contrib/crates/windows-link-0.2.1
)
ENDIF()

CRATE_FEATURES(
    backtrace
    petgraph
)

PEERDIR(
)

CRATE_SOURCES(
    src/thread_parker/generic.rs
    src/thread_parker/linux.rs
    src/thread_parker/redox.rs
    src/thread_parker/sgx.rs
    src/thread_parker/unix.rs
    src/thread_parker/wasm.rs
    src/thread_parker/wasm_atomic.rs
    src/thread_parker/windows/mod.rs
)

CRATE_SOURCES(
    src/lib.rs
    src/parking_lot.rs
    src/spinwait.rs
    src/thread_parker/mod.rs
    src/thread_parker/windows/bindings.rs
    src/thread_parker/windows/keyed_event.rs
    src/thread_parker/windows/waitaddress.rs
    src/util.rs
    src/word_lock.rs
)

CRATE_PROC_MACROS(
    contrib/crates/serde_derive-1.0.229
)

END()

RECURSE(build_script)
