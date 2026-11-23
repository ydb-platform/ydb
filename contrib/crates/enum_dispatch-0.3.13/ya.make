# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE_PROC_MACRO(enum_dispatch)

CRATE_VERSION(0.3.13)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/attributed_parser.rs
    src/cache.rs
    src/enum_dispatch_arg_list.rs
    src/enum_dispatch_item.rs
    src/enum_dispatch_variant.rs
    src/expansion.rs
    src/filter_attrs.rs
    src/supported_generics.rs
    src/syn_utils.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    .gitignore
    CHANGELOG.md
    LICENSE
    README.md
    enum_dispatch.svg
)

CRATE_FLAGS(--cap-lints=allow)

PEERDIR(
    contrib/crates/once_cell-1.21.4
    contrib/crates/proc-macro2-1.0.107
    contrib/crates/quote-1.0.47
    contrib/crates/syn-2.0.119
)

LICENSE("MIT OR Apache-2.0")
VERSION(0.3.13)
ORIGINAL_SOURCE(https://static.crates.io/crates/enum_dispatch/enum_dispatch-0.3.13.crate)

CRATE_SOURCES(
    src/attributed_parser.rs
    src/cache.rs
    src/enum_dispatch_arg_list.rs
    src/enum_dispatch_item.rs
    src/enum_dispatch_variant.rs
    src/expansion.rs
    src/filter_attrs.rs
    src/lib.rs
    src/supported_generics.rs
    src/syn_utils.rs
)

END()
