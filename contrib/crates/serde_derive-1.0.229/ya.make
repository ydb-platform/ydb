# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE_PROC_MACRO(serde_derive)

CRATE_VERSION(1.0.229)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/bound.rs
    src/de.rs
    src/de/enum_.rs
    src/de/enum_adjacently.rs
    src/de/enum_externally.rs
    src/de/enum_internally.rs
    src/de/enum_untagged.rs
    src/de/identifier.rs
    src/de/struct_.rs
    src/de/tuple.rs
    src/de/unit.rs
    src/deprecated.rs
    src/dummy.rs
    src/fragment.rs
    src/internals/ast.rs
    src/internals/attr.rs
    src/internals/case.rs
    src/internals/check.rs
    src/internals/ctxt.rs
    src/internals/mod.rs
    src/internals/name.rs
    src/internals/receiver.rs
    src/internals/respan.rs
    src/internals/symbol.rs
    src/pretend.rs
    src/ser.rs
    src/this.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    README.md
    crates-io.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    default
)

PEERDIR(
    contrib/crates/proc-macro2-1.0.107
    contrib/crates/quote-1.0.47
    contrib/crates/syn-3.0.9
)

LICENSE("MIT OR Apache-2.0")
VERSION(1.0.229)
ORIGINAL_SOURCE(https://static.crates.io/crates/serde_derive/serde_derive-1.0.229.crate)

CRATE_SOURCES(
    src/bound.rs
    src/de.rs
    src/de/enum_.rs
    src/de/enum_adjacently.rs
    src/de/enum_externally.rs
    src/de/enum_internally.rs
    src/de/enum_untagged.rs
    src/de/identifier.rs
    src/de/struct_.rs
    src/de/tuple.rs
    src/de/unit.rs
    src/deprecated.rs
    src/dummy.rs
    src/fragment.rs
    src/internals/ast.rs
    src/internals/attr.rs
    src/internals/case.rs
    src/internals/check.rs
    src/internals/ctxt.rs
    src/internals/mod.rs
    src/internals/name.rs
    src/internals/receiver.rs
    src/internals/respan.rs
    src/internals/symbol.rs
    src/lib.rs
    src/pretend.rs
    src/ser.rs
    src/this.rs
)

END()
