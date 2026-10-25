# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(cc)

CRATE_VERSION(1.4.0)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/command_helpers.rs
    src/flags.rs
    src/parallel/async_executor.rs
    src/parallel/command_runner.rs
    src/parallel/job_token.rs
    src/parallel/mod.rs
    src/parallel/stderr.rs
    src/target.rs
    src/target/apple.rs
    src/target/generated.rs
    src/target/llvm.rs
    src/target/parser.rs
    src/tempfile.rs
    src/tool.rs
    src/utilities.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    .gitignore
    CHANGELOG.md
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    README.md
    clippy.toml
    src/detect_compiler_family.c
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    parallel
)

PEERDIR(
    contrib/crates/find-msvc-tools-0.1.9
    contrib/crates/jobserver-0.1.35
    contrib/crates/libc-0.2.189
    contrib/crates/shlex-2.0.1
)

LICENSE("MIT OR Apache-2.0")
VERSION(1.4.0)
ORIGINAL_SOURCE(https://static.crates.io/crates/cc/cc-1.4.0.crate)

END()
