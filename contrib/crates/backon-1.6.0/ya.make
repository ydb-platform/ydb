# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(backon)

CRATE_VERSION(1.6.0)
CRATE_EDITION(2024)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/backoff/api.rs
    src/backoff/constant.rs
    src/backoff/exponential.rs
    src/backoff/fibonacci.rs
    src/backoff/mod.rs
    src/blocking_retry.rs
    src/blocking_retry_with_context.rs
    src/blocking_sleep.rs
    src/docs/examples/mod.rs
    src/docs/mod.rs
    src/embassy_timer_sleep.rs
    src/retry.rs
    src/retry_with_context.rs
    src/sleep.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    Cargo.lock
    LICENSE
    README.md
    src/docs/examples/basic.md
    src/docs/examples/closure.md
    src/docs/examples/custom_sleeper.md
    src/docs/examples/inside_mut_self.md
    src/docs/examples/retry_after.md
    src/docs/examples/sqlx.md
    src/docs/examples/wasm32_basic.md
    src/docs/examples/with_args.md
    src/docs/examples/with_mut_self.md
    src/docs/examples/with_self.md
    src/docs/examples/with_specific_error.md
)

CRATE_FLAGS(--cap-lints=allow)

PEERDIR(
    contrib/crates/fastrand-2.5.0
)

LICENSE("Apache-2.0")
VERSION(1.6.0)
ORIGINAL_SOURCE(https://static.crates.io/crates/backon/backon-1.6.0.crate)

END()
