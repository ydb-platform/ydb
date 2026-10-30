# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(pyo3_async_runtimes)

CRATE_VERSION(0.29.0)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    pytests/common/mod.rs
    pytests/test_async_std_asyncio.rs
    pytests/test_async_std_run_forever.rs
    pytests/test_async_std_uvloop.rs
    pytests/test_race_condition_regression.rs
    pytests/test_tokio_current_thread_asyncio.rs
    pytests/test_tokio_current_thread_run_forever.rs
    pytests/test_tokio_current_thread_uvloop.rs
    pytests/test_tokio_multi_thread_asyncio.rs
    pytests/test_tokio_multi_thread_run_forever.rs
    pytests/test_tokio_multi_thread_uvloop.rs
    pytests/tokio_asyncio/mod.rs
    pytests/tokio_run_forever/mod.rs
    src/async_std.rs
    src/err.rs
    src/generic.rs
    src/testing.rs
    src/tokio.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    .devcontainer/Dockerfile
    .devcontainer/devcontainer.json
    .githooks/pre-commit
    .githooks/pre-push
    CHANGELOG.md
    Cargo.lock
    Code-of-Conduct.md
    Contributing.md
    LICENSE
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    default
    tokio
    tokio-runtime
)

PEERDIR(
    contrib/crates/futures-channel-0.3.34
    contrib/crates/futures-util-0.3.34
    contrib/crates/once_cell-1.21.4
    contrib/crates/pin-project-lite-0.2.17
    contrib/crates/pyo3-0.29.2
    contrib/crates/tokio-1.54.1
)

LICENSE("Apache-2.0")
VERSION(0.29.0)
ORIGINAL_SOURCE(https://static.crates.io/crates/pyo3-async-runtimes/pyo3-async-runtimes-0.29.0.crate)

CRATE_PROC_MACROS(
    contrib/crates/futures-macro-0.3.34
    contrib/crates/pyo3-macros-0.29.2
    contrib/crates/serde_derive-1.0.229
    contrib/crates/tokio-macros-2.7.2
)

END()
