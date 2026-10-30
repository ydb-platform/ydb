# Native Rust build POC; dependencies and features follow Cargo.lock.

CRATE_PROC_MACRO(nalgebra_macros)

CRATE_VERSION(0.3.0)
CRATE_EDITION(2024)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)
VERSION(0.3.0)
LICENSE("Apache-2.0")

CRATE_SOURCES(
    src/matrix_vector_impl.rs
    src/stack_impl.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    Cargo.lock
    LICENSE
    README.md
)

CRATE_FLAGS(--cap-lints=allow)


PEERDIR(
    contrib/crates/proc-macro2-1.0.107
    contrib/crates/quote-1.0.47
    contrib/crates/syn-2.0.119
)

END()
