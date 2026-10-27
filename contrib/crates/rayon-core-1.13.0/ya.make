# Native Rust build POC; dependencies and features follow Cargo.lock.

CRATE(rayon_core)

CRATE_VERSION(1.13.0)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)
VERSION(1.13.0)
LICENSE("MIT OR Apache-2.0")

CRATE_SOURCES(
    build.rs
    src/broadcast/mod.rs
    src/broadcast/test.rs
    src/compile_fail/mod.rs
    src/compile_fail/quicksort_race1.rs
    src/compile_fail/quicksort_race2.rs
    src/compile_fail/quicksort_race3.rs
    src/compile_fail/rc_return.rs
    src/compile_fail/rc_upvar.rs
    src/compile_fail/scope_join_bad.rs
    src/job.rs
    src/join/mod.rs
    src/join/test.rs
    src/latch.rs
    src/private.rs
    src/registry.rs
    src/scope/mod.rs
    src/scope/test.rs
    src/sleep/counters.rs
    src/sleep/mod.rs
    src/spawn/mod.rs
    src/spawn/test.rs
    src/test.rs
    src/thread_pool/mod.rs
    src/thread_pool/test.rs
    src/unwind.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    README.md
    src/sleep/README.md
)

CRATE_FLAGS(--cap-lints=allow)


CRATE_BUILD_SCRIPT(contrib/crates/rayon-core-1.13.0/build_script)
PEERDIR(
    contrib/crates/crossbeam-deque-0.8.8
    contrib/crates/crossbeam-utils-0.8.23
)

CRATE_SOURCES(
    src/broadcast/mod.rs
    src/broadcast/test.rs
    src/compile_fail/mod.rs
    src/compile_fail/quicksort_race1.rs
    src/compile_fail/quicksort_race2.rs
    src/compile_fail/quicksort_race3.rs
    src/compile_fail/rc_return.rs
    src/compile_fail/rc_upvar.rs
    src/compile_fail/scope_join_bad.rs
    src/job.rs
    src/join/mod.rs
    src/join/test.rs
    src/latch.rs
    src/lib.rs
    src/private.rs
    src/registry.rs
    src/scope/mod.rs
    src/scope/test.rs
    src/sleep/counters.rs
    src/sleep/mod.rs
    src/spawn/mod.rs
    src/spawn/test.rs
    src/test.rs
    src/thread_pool/mod.rs
    src/thread_pool/test.rs
    src/unwind.rs
)

CRATE_INPUTS(
    src/sleep/README.md
)

END()

RECURSE(build_script)
