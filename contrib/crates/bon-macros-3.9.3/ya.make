# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE_PROC_MACRO(bon_macros)

CRATE_VERSION(3.9.3)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/bon.rs
    src/builder/builder_gen/builder_decl.rs
    src/builder/builder_gen/builder_derives/clone.rs
    src/builder/builder_gen/builder_derives/debug.rs
    src/builder/builder_gen/builder_derives/into.rs
    src/builder/builder_gen/builder_derives/into_future.rs
    src/builder/builder_gen/builder_derives/mod.rs
    src/builder/builder_gen/finish_fn.rs
    src/builder/builder_gen/generic_setters.rs
    src/builder/builder_gen/getters.rs
    src/builder/builder_gen/input_fn/mod.rs
    src/builder/builder_gen/input_fn/validation.rs
    src/builder/builder_gen/input_struct.rs
    src/builder/builder_gen/member/config/blanket.rs
    src/builder/builder_gen/member/config/getter.rs
    src/builder/builder_gen/member/config/mod.rs
    src/builder/builder_gen/member/config/setters.rs
    src/builder/builder_gen/member/config/with/closure.rs
    src/builder/builder_gen/member/config/with/mod.rs
    src/builder/builder_gen/member/into_conversion.rs
    src/builder/builder_gen/member/mod.rs
    src/builder/builder_gen/member/named.rs
    src/builder/builder_gen/mod.rs
    src/builder/builder_gen/models.rs
    src/builder/builder_gen/setters/mod.rs
    src/builder/builder_gen/start_fn.rs
    src/builder/builder_gen/state_mod.rs
    src/builder/builder_gen/top_level_config/generics.rs
    src/builder/builder_gen/top_level_config/mod.rs
    src/builder/builder_gen/top_level_config/on.rs
    src/builder/item_fn.rs
    src/builder/item_impl.rs
    src/builder/item_struct.rs
    src/builder/mod.rs
    src/collections/map.rs
    src/collections/mod.rs
    src/collections/set.rs
    src/error/mod.rs
    src/error/panic_context.rs
    src/normalization/cfg/mod.rs
    src/normalization/cfg/parse.rs
    src/normalization/cfg/visit.rs
    src/normalization/generics_namespace.rs
    src/normalization/impl_traits.rs
    src/normalization/lifetimes.rs
    src/normalization/mod.rs
    src/normalization/self_ty.rs
    src/normalization/syntax_variant.rs
    src/parsing/bon_crate_path.rs
    src/parsing/const_.rs
    src/parsing/docs.rs
    src/parsing/item_sig.rs
    src/parsing/mod.rs
    src/parsing/simple_closure.rs
    src/parsing/spanned_key.rs
    src/privatize.rs
    src/tests/attr_setters.rs
    src/tests/mod.rs
    src/tests/syntax_errors.rs
    src/util/attrs.rs
    src/util/expr.rs
    src/util/fn_arg.rs
    src/util/generic_param.rs
    src/util/ide.rs
    src/util/ident.rs
    src/util/item.rs
    src/util/iterator.rs
    src/util/meta_list.rs
    src/util/mod.rs
    src/util/path.rs
    src/util/punctuated.rs
    src/util/ty/match_types.rs
    src/util/ty/mod.rs
    src/util/vec.rs
    src/util/visibility.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    alloc
    default
    implied-bounds
)

PEERDIR(
    contrib/crates/darling-0.23.0
    contrib/crates/ident_case-1.0.1
    contrib/crates/prettyplease-0.2.37
    contrib/crates/proc-macro2-1.0.107
    contrib/crates/quote-1.0.47
    contrib/crates/syn-2.0.119
)

LICENSE("MIT OR Apache-2.0")
VERSION(3.9.3)
ORIGINAL_SOURCE(https://static.crates.io/crates/bon-macros/bon-macros-3.9.3.crate)

CRATE_FEATURES(
    std
)

CRATE_PROC_MACROS(
    contrib/crates/darling_macro-0.23.0
    contrib/crates/rustversion-1.0.23
)

END()
