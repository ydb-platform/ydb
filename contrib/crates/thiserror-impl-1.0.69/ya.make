# Native Rust build with dependencies shared through contrib/crates.

CRATE_PROC_MACRO(thiserror_impl)

CRATE_VERSION(1.0.69)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/ast.rs
    src/attr.rs
    src/expand.rs
    src/fmt.rs
    src/generics.rs
    src/prop.rs
    src/scan_expr.rs
    src/span.rs
    src/valid.rs
)

CRATE_INPUTS(
    LICENSE-APACHE
    LICENSE-MIT
)

CRATE_FLAGS(--cap-lints=allow)

PEERDIR(
    contrib/crates/proc-macro2-1.0.107
    contrib/crates/quote-1.0.47
    contrib/crates/syn-2.0.119
)

LICENSE("MIT OR Apache-2.0")
VERSION(1.0.69)
ORIGINAL_SOURCE(https://static.crates.io/crates/thiserror-impl/thiserror-impl-1.0.69.crate)

END()
