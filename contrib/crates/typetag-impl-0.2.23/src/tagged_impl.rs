use crate::{private, ImplArgs, Mode};
use proc_macro2::TokenStream;
use quote::quote;
use syn::{parse_quote, Error, ItemImpl, Type, TypePath};

pub(crate) fn expand(args: ImplArgs, mut input: ItemImpl, mode: Mode) -> TokenStream {
    if mode.de && !input.generics.params.is_empty() {
        let msg = "deserialization of generic impls is not supported yet; \
                   use #[typetag::serialize] to generate serialization only";
        return Error::new_spanned(input.generics, msg).to_compile_error();
    }

    let name = match args.name {
        Some(name) => quote!(#name),
        None => match type_name(&input.self_ty) {
            Some(name) => quote!(#name),
            None => {
                let msg = "use #[typetag::serde(name = \"...\")] to specify a unique name";
                return Error::new_spanned(&input.self_ty, msg).to_compile_error();
            }
        },
    };

    augment_impl(&mut input, &name, mode);

    let (object, _for_token) = &input.trait_.as_ref().unwrap();
    let this = &input.self_ty;

    let mut expanded = quote! {
        #input
    };

    if mode.de {
        expanded.extend(quote! {
            typetag::#private::inventory::submit! {
                <dyn #object>::typetag_register(
                    #name,
                    (|deserializer| typetag::#private::Result::Ok(
                        typetag::#private::Box::new(
                            typetag::#private::erased_serde::deserialize::<#this>(deserializer)?
                        ),
                    )) as typetag::#private::DeserializeFn<<dyn #object as typetag::#private::Strictest>::Object>,
                )
            }
        });
    }

    expanded
}

fn augment_impl(input: &mut ItemImpl, name: &TokenStream, mode: Mode) {
    if mode.ser {
        input.items.push(parse_quote! {
            #[doc(hidden)]
            fn typetag_name(&self) -> &'static str {
                #name
            }
        });
    }

    if mode.de {
        input.items.push(parse_quote! {
            #[doc(hidden)]
            fn typetag_deserialize(&self) {}
        });
    }
}

fn type_name(mut ty: &Type) -> Option<String> {
    loop {
        match ty {
            Type::Path(TypePath {
                attrs: _,
                qself: None,
                path,
            }) => {
                return Some(path.segments.last().unwrap().ident.to_string());
            }
            Type::Group(group) => {
                ty = &group.elem;
            }
            _ => return None,
        }
    }
}
