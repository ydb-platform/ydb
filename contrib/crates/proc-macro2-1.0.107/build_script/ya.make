# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

RUST_BUILD_SCRIPT(build_script_build)

CRATE_VERSION(1.0.107)
CRATE_EDITION(2021)
CRATE_ROOT(../build.rs)
CRATE_MANIFEST(../Cargo.toml)
CRATE_MANIFEST_DIR(..)

CRATE_SOURCES(
    ../src/detection.rs
    ../src/extra.rs
    ../src/fallback.rs
    ../src/lib.rs
    ../src/location.rs
    ../src/marker.rs
    ../src/num.rs
    ../src/parse.rs
    ../src/probe.rs
    ../src/probe/proc_macro_span.rs
    ../src/probe/proc_macro_span_file.rs
    ../src/probe/proc_macro_span_location.rs
    ../src/rcvec.rs
    ../src/rustc_literal_escaper.rs
    ../src/wrapper.rs
)

CRATE_INPUTS(
    ../.cargo-checksum.json
    ../.cargo_vcs_info.json
    ../.gitignore
    ../Cargo.lock
    ../LICENSE-APACHE
    ../LICENSE-MIT
    ../README.md
    ../rust-toolchain.toml
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    default
    proc-macro
)

LICENSE("MIT OR Apache-2.0")
VERSION(1.0.107)
ORIGINAL_SOURCE(https://static.crates.io/crates/proc-macro2/proc-macro2-1.0.107.crate)

CRATE_FEATURES(
    span-locations
)

CRATE_SOURCES(
    ../src/wrapper.rs
)

CRATE_SOURCES(
    ../src/detection.rs
    ../src/extra.rs
    ../src/fallback.rs
    ../src/lib.rs
    ../src/location.rs
    ../src/marker.rs
    ../src/num.rs
    ../src/parse.rs
    ../src/probe.rs
    ../src/probe/proc_macro_span.rs
    ../src/probe/proc_macro_span_file.rs
    ../src/probe/proc_macro_span_location.rs
    ../src/rcvec.rs
    ../src/rustc_literal_escaper.rs
)

END()
