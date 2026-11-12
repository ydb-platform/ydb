# Native Rust build with dependencies shared through contrib/crates.

CRATE_PROC_MACRO(multiversion_no_op)

CRATE_VERSION(1.0.0)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_INPUTS(
    COPYRIGHT
    LICENSE-APACHE
    LICENSE-MIT
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

LICENSE("Apache-2.0 OR MIT")
VERSION(1.0.0)
ORIGINAL_SOURCE(https://static.crates.io/crates/multiversion_no_op/multiversion_no_op-1.0.0.crate)

END()
