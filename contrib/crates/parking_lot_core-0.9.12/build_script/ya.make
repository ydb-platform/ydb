# Native Rust build POC; host build script.

RUST_BUILD_SCRIPT(build_script_build)

CRATE_VERSION(0.9.12)
CRATE_EDITION(2021)
CRATE_ROOT(../build.rs)
CRATE_MANIFEST(../Cargo.toml)
VERSION(0.9.12)
LICENSE("MIT OR Apache-2.0")
CRATE_MANIFEST_DIR(..)

CRATE_SOURCES(
    ../src/lib.rs
    ../src/parking_lot.rs
    ../src/spinwait.rs
    ../src/thread_parker/generic.rs
    ../src/thread_parker/linux.rs
    ../src/thread_parker/mod.rs
    ../src/thread_parker/redox.rs
    ../src/thread_parker/sgx.rs
    ../src/thread_parker/unix.rs
    ../src/thread_parker/wasm.rs
    ../src/thread_parker/wasm_atomic.rs
    ../src/thread_parker/windows/bindings.rs
    ../src/thread_parker/windows/keyed_event.rs
    ../src/thread_parker/windows/mod.rs
    ../src/thread_parker/windows/waitaddress.rs
    ../src/util.rs
    ../src/word_lock.rs
)

CRATE_INPUTS(
    ../.cargo-checksum.json
    ../.cargo_vcs_info.json
    ../Cargo.lock
    ../LICENSE-APACHE
    ../LICENSE-MIT
)

CRATE_FLAGS(--cap-lints=allow)




CRATE_FEATURES(
    backtrace
    deadlock_detection
    petgraph
)

CRATE_SOURCES(
    ../src/thread_parker/generic.rs
    ../src/thread_parker/linux.rs
    ../src/thread_parker/redox.rs
    ../src/thread_parker/sgx.rs
    ../src/thread_parker/unix.rs
    ../src/thread_parker/wasm.rs
    ../src/thread_parker/wasm_atomic.rs
    ../src/thread_parker/windows/mod.rs
)

CRATE_SOURCES(
    ../src/lib.rs
    ../src/parking_lot.rs
    ../src/spinwait.rs
    ../src/thread_parker/mod.rs
    ../src/thread_parker/windows/bindings.rs
    ../src/thread_parker/windows/keyed_event.rs
    ../src/thread_parker/windows/waitaddress.rs
    ../src/util.rs
    ../src/word_lock.rs
)

END()
