# Native Rust build POC; dependencies and features follow Cargo.lock.

CRATE_PROC_MACRO(glib_macros)

CRATE_VERSION(0.22.0)
CRATE_EDITION(2024)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)
VERSION(0.22.0)
LICENSE("MIT")

CRATE_SOURCES(
    src/async_test.rs
    src/boxed_derive.rs
    src/clone.rs
    src/closure.rs
    src/derived_properties_attribute.rs
    src/downgrade_derive/enums.rs
    src/downgrade_derive/fields.rs
    src/downgrade_derive/mod.rs
    src/downgrade_derive/structs.rs
    src/enum_derive.rs
    src/error_domain_derive.rs
    src/flags_attribute.rs
    src/object_impl_attributes/interface.rs
    src/object_impl_attributes/subclass.rs
    src/object_impl_attributes.rs
    src/properties.rs
    src/shared_boxed_derive.rs
    src/utils.rs
    src/value_delegate_derive.rs
    src/variant_derive.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    COPYRIGHT
    Cargo.lock
    LICENSE
)

CRATE_FLAGS(--cap-lints=allow)


PEERDIR(
    contrib/crates/heck-0.5.0
    contrib/crates/proc-macro2-1.0.107
    contrib/crates/quote-1.0.47
    contrib/crates/syn-2.0.119
)

END()
