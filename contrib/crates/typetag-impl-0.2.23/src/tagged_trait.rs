use crate::{private, Mode, TraitArgs};
use proc_macro2::TokenStream;
use quote::quote;
use syn::{parse_quote, Error, ItemTrait, LitStr, TypeParamBound};

pub(crate) fn expand(args: TraitArgs, mut input: ItemTrait, mode: Mode) -> TokenStream {
    if mode.de && !input.generics.params.is_empty() {
        let msg = "deserialization of generic traits is not supported yet; \
                   use #[typetag::serialize] to generate serialization only";
        return Error::new_spanned(input.generics, msg).to_compile_error();
    }

    augment_trait(&mut input, mode);

    let (serialize_impl, deserialize_impl) = match args {
        TraitArgs::External => externally_tagged(&input),
        TraitArgs::Internal {
            tag,
            default_variant,
        } => internally_tagged(tag, default_variant, &input),
        TraitArgs::Adjacent {
            tag,
            content,
            default_variant,
            deny_unknown_fields,
        } => adjacently_tagged(tag, content, default_variant, deny_unknown_fields, &input),
    };

    let object = &input.ident;

    let mut expanded = TokenStream::new();

    if mode.ser {
        let mut impl_generics = input.generics.clone();
        impl_generics.params.push(parse_quote!('typetag));
        let (impl_generics, _, _) = impl_generics.split_for_impl();
        let (_, ty_generics, where_clause) = input.generics.split_for_impl();

        expanded.extend(quote! {
            impl #impl_generics typetag::#private::serde::Serialize
            for dyn #object #ty_generics + 'typetag #where_clause {
                fn serialize<S>(&self, serializer: S) -> typetag::#private::Result<S::Ok, S::Error>
                where
                    S: typetag::#private::serde::Serializer,
                {
                    #serialize_impl
                }
            }
        });

        for marker_traits in &[quote!(Send), quote!(Sync), quote!(Send + Sync)] {
            expanded.extend(quote! {
                impl #impl_generics typetag::#private::serde::Serialize
                for dyn #object #ty_generics + #marker_traits + 'typetag #where_clause {
                    fn serialize<S>(&self, serializer: S) -> typetag::#private::Result<S::Ok, S::Error>
                    where
                        S: typetag::#private::serde::Serializer,
                    {
                        typetag::#private::serde::Serialize::serialize(self as &dyn #object #ty_generics, serializer)
                    }
                }
            });
        }
    }

    if mode.de {
        let registry = build_registry(&input);

        let is_send = has_supertrait(&input, "Send");
        let is_sync = has_supertrait(&input, "Sync");
        let (strictest, others) = match (is_send, is_sync) {
            (false, false) => (quote!(), vec![]),
            (true, false) => (quote!(Send), vec![quote!()]),
            (false, true) => (quote!(Sync), vec![quote!()]),
            (true, true) => (
                quote!(Send + Sync),
                vec![quote!(), quote!(Send), quote!(Sync)],
            ),
        };

        expanded.extend(quote! {
            #registry

            impl typetag::#private::Strictest for dyn #object {
                type Object = dyn #object + #strictest;
            }

            #[allow(unknown_lints, non_local_definitions)] // false positive: https://github.com/rust-lang/rust/issues/121621
            impl<'de> typetag::#private::serde::Deserialize<'de> for typetag::#private::Box<dyn #object + #strictest> {
                fn deserialize<D>(deserializer: D) -> typetag::#private::Result<Self, D::Error>
                where
                    D: typetag::#private::serde::Deserializer<'de>,
                {
                    #deserialize_impl
                }
            }
        });

        for marker_traits in others {
            expanded.extend(quote! {
                #[allow(unknown_lints, non_local_definitions)] // false positive: https://github.com/rust-lang/rust/issues/121621
                impl<'de> typetag::#private::serde::Deserialize<'de> for typetag::#private::Box<dyn #object + #marker_traits> {
                    fn deserialize<D>(deserializer: D) -> typetag::#private::Result<Self, D::Error>
                    where
                        D: typetag::#private::serde::Deserializer<'de>,
                    {
                        typetag::#private::Result::Ok(
                            <typetag::#private::Box<dyn #object + #strictest>
                                as typetag::#private::serde::Deserialize<'de>>::deserialize(deserializer)?
                        )
                    }
                }
            });
        }
    }

    quote! {
        #input

        #[allow(non_upper_case_globals)]
        const _: () = {
            #expanded
        };
    }
}

fn augment_trait(input: &mut ItemTrait, mode: Mode) {
    if mode.ser {
        input.supertraits.push(parse_quote!(typetag::Serialize));

        input.items.push(parse_quote! {
            #[doc(hidden)]
            fn typetag_name(&self) -> &'static str;
        });
    }

    if mode.de {
        input.supertraits.push(parse_quote!(typetag::Deserialize));

        // Only to catch missing typetag attribute on impl blocks. Not called.
        input.items.push(parse_quote! {
            #[doc(hidden)]
            fn typetag_deserialize(&self);
        });
    }
}

fn build_registry(input: &ItemTrait) -> TokenStream {
    let vis = &input.vis;
    let object = &input.ident;

    quote! {
        type TypetagStrictest = <dyn #object as typetag::#private::Strictest>::Object;
        type TypetagFn = typetag::#private::DeserializeFn<TypetagStrictest>;

        #vis struct TypetagRegistration<T> {
            name: &'static str,
            deserializer: T,
        }

        typetag::#private::inventory::collect!(TypetagRegistration<TypetagFn>);

        impl dyn #object {
            #[doc(hidden)]
            #vis const fn typetag_register<T>(name: &'static str, deserializer: T) -> TypetagRegistration<T> {
                TypetagRegistration { name, deserializer }
            }
        }
    }
}

fn static_registry() -> TokenStream {
    quote! {
        static TYPETAG: typetag::#private::once_cell::race::OnceBox<typetag::#private::Registry<TypetagStrictest>> = typetag::#private::once_cell::race::OnceBox::new();
        let registry = TYPETAG.get_or_init(|| {
            let mut map = typetag::#private::BTreeMap::new();
            let mut names = typetag::#private::Vec::new();
            for registered in typetag::#private::inventory::iter::<TypetagRegistration<TypetagFn>> {
                match map.entry(registered.name) {
                    typetag::#private::btree_map::Entry::Vacant(entry) => {
                        entry.insert(typetag::#private::Option::Some(registered.deserializer));
                    }
                    typetag::#private::btree_map::Entry::Occupied(mut entry) => {
                        entry.insert(typetag::#private::Option::None);
                    }
                }
                names.push(registered.name);
            }
            names.sort_unstable();
            typetag::#private::Box::new(typetag::#private::Registry { map, names })
        });
    }
}

fn externally_tagged(input: &ItemTrait) -> (TokenStream, TokenStream) {
    let object = &input.ident;
    let object_name = object.to_string();
    let (_, ty_generics, _) = input.generics.split_for_impl();
    let static_registry = static_registry();

    let serialize_impl = quote! {
        let name = <Self as #object #ty_generics>::typetag_name(self);
        typetag::#private::externally::serialize(serializer, name, self)
    };

    let deserialize_impl = quote! {
        #static_registry
        typetag::#private::externally::deserialize(deserializer, #object_name, registry)
    };

    (serialize_impl, deserialize_impl)
}

fn internally_tagged(
    tag: LitStr,
    default_variant: Option<LitStr>,
    input: &ItemTrait,
) -> (TokenStream, TokenStream) {
    let object = &input.ident;
    let object_name = object.to_string();
    let (_, ty_generics, _) = input.generics.split_for_impl();
    let static_registry = static_registry();
    let default_variant_literal = match default_variant {
        Some(variant) => quote!(typetag::#private::Option::Some(#variant)),
        None => quote!(typetag::#private::Option::None),
    };

    let serialize_impl = quote! {
        let name = <Self as #object #ty_generics>::typetag_name(self);
        typetag::#private::internally::serialize(serializer, #tag, name, self)
    };

    let deserialize_impl = quote! {
        #static_registry
        typetag::#private::internally::deserialize(deserializer, #object_name, #tag, #default_variant_literal, registry)
    };

    (serialize_impl, deserialize_impl)
}

fn adjacently_tagged(
    tag: LitStr,
    content: LitStr,
    default_variant: Option<LitStr>,
    deny_unknown_fields: bool,
    input: &ItemTrait,
) -> (TokenStream, TokenStream) {
    let object = &input.ident;
    let object_name = object.to_string();
    let (_, ty_generics, _) = input.generics.split_for_impl();
    let static_registry = static_registry();
    let default_variant_literal = match default_variant {
        Some(variant) => quote!(typetag::#private::Option::Some(#variant)),
        None => quote!(typetag::#private::Option::None),
    };

    let serialize_impl = quote! {
        let name = <Self as #object #ty_generics>::typetag_name(self);
        typetag::#private::adjacently::serialize(serializer, #object_name, #tag, name, #content, self)
    };

    let deserialize_impl = quote! {
        #static_registry
        typetag::#private::adjacently::deserialize(
            deserializer,
            #object_name,
            &[#tag, #content],
            #default_variant_literal,
            registry,
            #deny_unknown_fields,
        )
    };

    (serialize_impl, deserialize_impl)
}

fn has_supertrait(input: &ItemTrait, find: &str) -> bool {
    for supertrait in &input.supertraits {
        if let TypeParamBound::Trait(trait_bound) = supertrait {
            if trait_bound.maybe.is_none() && trait_bound.path.is_ident(find) {
                return true;
            }
        }
    }
    false
}
