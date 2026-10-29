# Native Rust build with dependencies shared through contrib/crates.

CRATE(cc)

CRATE_VERSION(1.10.0)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/build_env.rs
    src/command_helpers.rs
    src/compile_commands.rs
    src/flags.rs
    src/logger.rs
    src/parallel/async_executor.rs
    src/parallel/command_runner.rs
    src/parallel/job_token.rs
    src/parallel/mod.rs
    src/parallel/reactor/mod.rs
    src/parallel/reactor/unix.rs
    src/parallel/reactor/unsupported.rs
    src/parallel/reactor/windows.rs
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
    CHANGELOG.md
    CONTRIBUTING.md
    Cargo.lock
    DEVELOPMENT.md
    LICENSE-APACHE
    LICENSE-MIT
    README.md
    release-plz.toml
    src/detect_compiler_family.c
    src/detect_cpp_stdlib.cpp
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    parallel
)

PEERDIR(
    contrib/crates/find-msvc-tools-0.1.15
    contrib/crates/jobserver-0.1.35
    contrib/crates/libc-0.2.191
    contrib/crates/shlex-2.0.1
)

LICENSE("MIT OR Apache-2.0")
VERSION(1.10.0)
ORIGINAL_SOURCE(https://static.crates.io/crates/cc/cc-1.10.0.crate)

END()
