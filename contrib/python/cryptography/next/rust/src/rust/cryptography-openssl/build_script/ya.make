# Rust sources pinned by cryptography 41.0.6 Cargo.lock.

RUST_BUILD_SCRIPT(build_script_build)

CRATE_VERSION(0.1.0)
CRATE_EDITION(2021)
CRATE_ROOT(../build.rs)
CRATE_MANIFEST(../Cargo.toml)
CRATE_MANIFEST_DIR(..)

CRATE_SOURCES(
    ../src/fips.rs
    ../src/hmac.rs
    ../src/lib.rs
)

CRATE_FLAGS(--cap-lints=allow)

LICENSE(Apache-2.0 OR BSD-3-Clause)
VERSION(0.1.0)

END()
