# Native Rust build POC; host build script.

RUST_BUILD_SCRIPT(build_script_build)

CRATE_VERSION(1.13.0)
CRATE_EDITION(2021)
CRATE_ROOT(../build.rs)
CRATE_MANIFEST(../Cargo.toml)
VERSION(1.13.0)
LICENSE("MIT OR Apache-2.0")
CRATE_MANIFEST_DIR(..)

CRATE_SOURCES(
    ../src/broadcast/mod.rs
    ../src/broadcast/test.rs
    ../src/compile_fail/mod.rs
    ../src/compile_fail/quicksort_race1.rs
    ../src/compile_fail/quicksort_race2.rs
    ../src/compile_fail/quicksort_race3.rs
    ../src/compile_fail/rc_return.rs
    ../src/compile_fail/rc_upvar.rs
    ../src/compile_fail/scope_join_bad.rs
    ../src/job.rs
    ../src/join/mod.rs
    ../src/join/test.rs
    ../src/latch.rs
    ../src/lib.rs
    ../src/private.rs
    ../src/registry.rs
    ../src/scope/mod.rs
    ../src/scope/test.rs
    ../src/sleep/counters.rs
    ../src/sleep/mod.rs
    ../src/spawn/mod.rs
    ../src/spawn/test.rs
    ../src/test.rs
    ../src/thread_pool/mod.rs
    ../src/thread_pool/test.rs
    ../src/unwind.rs
)

CRATE_INPUTS(
    ../.cargo-checksum.json
    ../.cargo_vcs_info.json
    ../Cargo.lock
    ../LICENSE-APACHE
    ../LICENSE-MIT
    ../README.md
    ../src/sleep/README.md
)

CRATE_FLAGS(--cap-lints=allow)




CRATE_SOURCES(
    ../src/broadcast/mod.rs
    ../src/broadcast/test.rs
    ../src/compile_fail/mod.rs
    ../src/compile_fail/quicksort_race1.rs
    ../src/compile_fail/quicksort_race2.rs
    ../src/compile_fail/quicksort_race3.rs
    ../src/compile_fail/rc_return.rs
    ../src/compile_fail/rc_upvar.rs
    ../src/compile_fail/scope_join_bad.rs
    ../src/job.rs
    ../src/join/mod.rs
    ../src/join/test.rs
    ../src/latch.rs
    ../src/lib.rs
    ../src/private.rs
    ../src/registry.rs
    ../src/scope/mod.rs
    ../src/scope/test.rs
    ../src/sleep/counters.rs
    ../src/sleep/mod.rs
    ../src/spawn/mod.rs
    ../src/spawn/test.rs
    ../src/test.rs
    ../src/thread_pool/mod.rs
    ../src/thread_pool/test.rs
    ../src/unwind.rs
)

CRATE_INPUTS(
    ../src/sleep/README.md
)

END()
