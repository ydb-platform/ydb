# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(windows_core)

CRATE_VERSION(0.62.2)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/agile_reference.rs
    src/array.rs
    src/as_impl.rs
    src/com_object.rs
    src/event.rs
    src/guid.rs
    src/handles.rs
    src/imp/array_proxy.rs
    src/imp/bindings.rs
    src/imp/can_into.rs
    src/imp/com_bindings.rs
    src/imp/factory_cache.rs
    src/imp/generic_factory.rs
    src/imp/marshaler.rs
    src/imp/mod.rs
    src/imp/ref_count.rs
    src/imp/sha1.rs
    src/imp/weak_ref_count.rs
    src/imp/windows.rs
    src/inspectable.rs
    src/interface.rs
    src/out_param.rs
    src/out_ref.rs
    src/param.rs
    src/param_value.rs
    src/ref.rs
    src/runtime_name.rs
    src/runtime_type.rs
    src/scoped_interface.rs
    src/type.rs
    src/unknown.rs
    src/weak.rs
    src/windows.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    Cargo.lock
    license-apache-2.0
    license-mit
    readme.md
    windows-core.natvis
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    std
)

PEERDIR(
    contrib/crates/windows-link-0.2.1
    contrib/crates/windows-result-0.4.1
    contrib/crates/windows-strings-0.5.1
)

LICENSE("MIT OR Apache-2.0")
VERSION(0.62.2)
ORIGINAL_SOURCE(https://static.crates.io/crates/windows-core/windows-core-0.62.2.crate)

CRATE_SOURCES(
    src/imp/windows.rs
    src/windows.rs
)

CRATE_INPUTS(
    readme.md
)

CRATE_SOURCES(
    src/agile_reference.rs
    src/array.rs
    src/as_impl.rs
    src/com_object.rs
    src/event.rs
    src/guid.rs
    src/handles.rs
    src/imp/array_proxy.rs
    src/imp/bindings.rs
    src/imp/can_into.rs
    src/imp/com_bindings.rs
    src/imp/factory_cache.rs
    src/imp/generic_factory.rs
    src/imp/marshaler.rs
    src/imp/mod.rs
    src/imp/ref_count.rs
    src/imp/sha1.rs
    src/imp/weak_ref_count.rs
    src/inspectable.rs
    src/interface.rs
    src/lib.rs
    src/out_param.rs
    src/out_ref.rs
    src/param.rs
    src/param_value.rs
    src/ref.rs
    src/runtime_name.rs
    src/runtime_type.rs
    src/scoped_interface.rs
    src/type.rs
    src/unknown.rs
    src/weak.rs
)

CRATE_PROC_MACROS(
    contrib/crates/windows-implement-0.60.2
    contrib/crates/windows-interface-0.59.3
)

END()
