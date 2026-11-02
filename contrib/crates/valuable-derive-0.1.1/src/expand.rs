use proc_macro2::{Span, TokenStream};
use quote::{format_ident, quote, ToTokens};
use syn::{Error, Ident, Result};

use crate::attr::{parse_attrs, Attrs, Context, Position};

pub(crate) fn derive_valuable(input: &mut syn::DeriveInput) -> TokenStream {
    let cx = Context::default();
    match &input.data {
        syn::Data::Struct(data) => derive_struct(cx, input, data),
        syn::Data::Enum(data) => derive_enum(cx, input, data),
        syn::Data::Union(data) => {
            // It's impossible to derive union because we cannot safely reference the field.
            Err(Error::new(
                data.union_token.span,
                "#[derive(Valuable)] may only be used on structs and enums",
            ))
        }
    }
    .unwrap_or_else(Error::into_compile_error)
}

fn derive_struct(
    cx: Context,
    input: &syn::DeriveInput,
    data: &syn::DataStruct,
) -> Result<TokenStream> {
    let struct_attrs = parse_attrs(&cx, &input.attrs, Position::Struct);
    let field_attrs: Vec<_> = data
        .fields
        .iter()
        .map(|f| parse_attrs(&cx, &f.attrs, Position::from(&data.fields)))
        .collect();
    if struct_attrs.transparent() && data.fields.len() != 1 {
        cx.error(Error::new_spanned(
            input,
            format!(
                "#[valuable(transparent)] struct needs exactly one field, but has {}",
                data.fields.len()
            ),
        ))
    }
    cx.check()?;

    let name = &input.ident;
    let name_literal = struct_attrs.rename(name);

    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();
    let allowed_lints = allowed_lints();

    if struct_attrs.transparent() {
        let field = data.fields.iter().next().unwrap();
        let access = field.ident.as_ref().map_or_else(
            || syn::Index::from(0).to_token_stream(),
            ToTokens::to_token_stream,
        );
        let access = respan(quote! { &self.#access }, &field.ty);
        let valuable_impl = quote! {
            #[automatically_derived]
            impl #impl_generics ::valuable::Valuable for #name #ty_generics #where_clause {
                fn as_value(&self) -> ::valuable::Value<'_> {
                    ::valuable::Valuable::as_value(#access)
                }

                fn visit(&self, visitor: &mut dyn ::valuable::Visit) {
                    ::valuable::Valuable::visit(#access, visitor);
                }
            }
        };

        return Ok(quote! {
            #allowed_lints
            const _: () = {
                #valuable_impl
            };
        });
    }

    let visit_fields;
    let struct_def;
    let mut named_fields_statics = None;

    match &data.fields {
        syn::Fields::Named(_) => {
            // <struct>_FIELDS
            let named_fields_static_name = format_ident!("{}_FIELDS", input.ident);
            named_fields_statics = Some(named_fields_static(
                &named_fields_static_name,
                &data.fields,
                &field_attrs,
            ));

            struct_def = quote! {
                ::valuable::StructDef::new_static(
                    #name_literal,
                    ::valuable::Fields::Named(#named_fields_static_name),
                )
            };

            let fields = data
                .fields
                .iter()
                .enumerate()
                .filter(|(i, _)| !field_attrs[*i].skip())
                .map(|(_, field)| {
                    let f = field.ident.as_ref();
                    let tokens = quote! {
                        &self.#f
                    };
                    respan(tokens, &field.ty)
                });
            visit_fields = quote! {
                visitor.visit_named_fields(&::valuable::NamedValues::new(
                    #named_fields_static_name,
                    &[
                        #(::valuable::Valuable::as_value(#fields),)*
                    ],
                ));
            }
        }
        syn::Fields::Unnamed(_) | syn::Fields::Unit => {
            let indices: Vec<_> = data
                .fields
                .iter()
                .enumerate()
                .filter(|(i, _)| !field_attrs[*i].skip())
                .map(|(i, field)| {
                    let index = syn::Index::from(i);
                    let tokens = quote! {
                        &self.#index
                    };
                    respan(tokens, &field.ty)
                })
                .collect();

            let len = indices.len();
            struct_def = quote! {
                ::valuable::StructDef::new_static(
                    #name_literal,
                    ::valuable::Fields::Unnamed(#len),
                )
            };

            visit_fields = quote! {
                visitor.visit_unnamed_fields(
                    &[
                        #(::valuable::Valuable::as_value(#indices),)*
                    ],
                );
            };
        }
    }

    let structable_impl = quote! {
        #[automatically_derived]
        impl #impl_generics ::valuable::Structable for #name #ty_generics #where_clause {
            fn definition(&self) -> ::valuable::StructDef<'_> {
                #struct_def
            }
        }
    };

    let valuable_impl = quote! {
        #[automatically_derived]
        impl #impl_generics ::valuable::Valuable for #name #ty_generics #where_clause {
            fn as_value(&self) -> ::valuable::Value<'_> {
                ::valuable::Value::Structable(self)
            }

            fn visit(&self, visitor: &mut dyn ::valuable::Visit) {
                #visit_fields
            }
        }
    };

    Ok(quote! {
        #allowed_lints
        const _: () = {
            #named_fields_statics
            #structable_impl
            #valuable_impl
        };
    })
}

fn derive_enum(cx: Context, input: &syn::DeriveInput, data: &syn::DataEnum) -> Result<TokenStream> {
    let enum_attrs = parse_attrs(&cx, &input.attrs, Position::Enum);
    let variant_attrs: Vec<_> = data
        .variants
        .iter()
        .map(|v| parse_attrs(&cx, &v.attrs, Position::Variant))
        .collect();
    let field_attrs: Vec<Vec<_>> = data
        .variants
        .iter()
        .map(|v| {
            v.fields
                .iter()
                .map(|f| parse_attrs(&cx, &f.attrs, Position::from(&v.fields)))
                .collect()
        })
        .collect();
    cx.check()?;

    let name = &input.ident;
    let name_literal = enum_attrs.rename(name);

    // <enum>_VARIANTS
    let variants_static_name = format_ident!("{}_VARIANTS", input.ident);
    // `static FIELDS: &[NamedField<'static>]` for variant with named fields
    let mut named_fields_statics = vec![];
    let mut variant_defs = vec![];
    let mut variant_fn = vec![];
    let mut visit_variants = vec![];

    for (variant_index, variant) in data.variants.iter().enumerate() {
        let variant_name = &variant.ident;
        let variant_name_literal = variant_attrs[variant_index].rename(variant_name);

        match &variant.fields {
            syn::Fields::Named(_) => {
                // <enum>_<variant>_FIELDS
                let named_fields_static_name =
                    format_ident!("{}_{}_FIELDS", input.ident, variant.ident);
                named_fields_statics.push(named_fields_static(
                    &named_fields_static_name,
                    &variant.fields,
                    &field_attrs[variant_index],
                ));

                variant_defs.push(quote! {
                    ::valuable::VariantDef::new(
                        #variant_name_literal,
                        ::valuable::Fields::Named(#named_fields_static_name),
                    ),
                });

                variant_fn.push(quote! {
                    Self::#variant_name { .. } => {
                        ::valuable::Variant::Static(&#variants_static_name[#variant_index])
                    }
                });

                let mut fields = Vec::with_capacity(variant.fields.len());
                let mut as_value = Vec::with_capacity(variant.fields.len());
                for (_, field) in variant
                    .fields
                    .iter()
                    .enumerate()
                    .filter(|(i, _)| !field_attrs[variant_index][*i].skip())
                {
                    let f = field.ident.as_ref();
                    fields.push(f);
                    let tokens = quote! {
                        // HACK(taiki-e): This `&` is not actually needed to calling as_value,
                        // but is needed to emulate multi-token span on stable Rust.
                        &#f
                    };
                    as_value.push(respan(tokens, &field.ty));
                }
                let skipped = if fields.len() == variant.fields.len() {
                    quote! {}
                } else {
                    quote!(..)
                };
                visit_variants.push(quote! {
                    Self::#variant_name { #(#fields,)* #skipped } => {
                        visitor.visit_named_fields(
                            &::valuable::NamedValues::new(
                                #named_fields_static_name,
                                &[
                                    #(::valuable::Valuable::as_value(#as_value),)*
                                ],
                            ),
                        );
                    }
                });
            }
            syn::Fields::Unnamed(_) => {
                variant_fn.push(quote! {
                    Self::#variant_name(..) => {
                        ::valuable::Variant::Static(&#variants_static_name[#variant_index])
                    }
                });

                let bindings: Vec<_> = (0..variant.fields.len())
                    .map(|i| format_ident!("__binding_{}", i))
                    .collect();
                let as_value: Vec<_> = bindings
                    .iter()
                    .zip(&variant.fields)
                    .enumerate()
                    .filter(|(i, _)| !field_attrs[variant_index][*i].skip())
                    .map(|(_, (binding, field))| {
                        let tokens = quote! {
                            // HACK(taiki-e): This `&` is not actually needed to calling as_value,
                            // but is needed to emulate multi-token span on stable Rust.
                            &#binding
                        };
                        respan(tokens, &field.ty)
                    })
                    .collect();

                let len = as_value.len();
                variant_defs.push(quote! {
                    ::valuable::VariantDef::new(
                        #variant_name_literal,
                        ::valuable::Fields::Unnamed(#len),
                    ),
                });

                visit_variants.push(quote! {
                    Self::#variant_name(#(#bindings),*) => {
                        visitor.visit_unnamed_fields(
                            &[
                                #(::valuable::Valuable::as_value(#as_value),)*
                            ],
                        );
                    }
                });
            }
            syn::Fields::Unit => {
                variant_defs.push(quote! {
                    ::valuable::VariantDef::new(
                        #variant_name_literal,
                        ::valuable::Fields::Unnamed(0),
                    ),
                });

                variant_fn.push(quote! {
                    Self::#variant_name => {
                        ::valuable::Variant::Static(&#variants_static_name[#variant_index])
                    }
                });

                visit_variants.push(quote! {
                    Self::#variant_name => {
                        visitor.visit_unnamed_fields(
                            &[],
                        );
                    }
                });
            }
        }
    }

    let variants_static = quote! {
        static #variants_static_name: &[::valuable::VariantDef<'static>] = &[
            #(#variant_defs)*
        ];
    };

    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();
    let enumerable_impl = quote! {
        #[automatically_derived]
        impl #impl_generics ::valuable::Enumerable for #name #ty_generics #where_clause {
            fn definition(&self) -> ::valuable::EnumDef<'_> {
                ::valuable::EnumDef::new_static(
                    #name_literal,
                    #variants_static_name,
                )
            }

            fn variant(&self) -> ::valuable::Variant<'_> {
                match self {
                    #(#variant_fn)*
                }
            }
        }
    };

    let valuable_impl = quote! {
        #[automatically_derived]
        impl #impl_generics ::valuable::Valuable for #name #ty_generics #where_clause {
            fn as_value(&self) -> ::valuable::Value<'_> {
                ::valuable::Value::Enumerable(self)
            }

            fn visit(&self, visitor: &mut dyn ::valuable::Visit) {
                match self {
                    #(#visit_variants)*
                }
            }
        }
    };

    let allowed_lints = allowed_lints();
    Ok(quote! {
        #allowed_lints
        const _: () = {
            #(#named_fields_statics)*
            #variants_static
            #enumerable_impl
            #valuable_impl
        };
    })
}

// `static <name>: &[NamedField<'static>] = &[ ... ];`
fn named_fields_static(name: &Ident, fields: &syn::Fields, field_attrs: &[Attrs]) -> TokenStream {
    debug_assert!(matches!(fields, syn::Fields::Named(..)));
    let named_fields = fields
        .iter()
        .enumerate()
        .filter(|(i, _)| !field_attrs[*i].skip())
        .map(|(i, field)| {
            let field_name_literal = field_attrs[i].rename(field.ident.as_ref().unwrap());
            quote! {
                ::valuable::NamedField::new(#field_name_literal),
            }
        });
    quote! {
        static #name: &[::valuable::NamedField<'static>] = &[
            #(#named_fields)*
        ];
    }
}

// Returns attributes that should be applied to generated code.
fn allowed_lints() -> TokenStream {
    quote! {
        #[allow(non_upper_case_globals)]
        #[allow(clippy::unknown_clippy_lints)]
        #[allow(clippy::used_underscore_binding)]
        #[allow(clippy::indexing_slicing)]
    }
}

fn respan(tokens: TokenStream, span: &impl ToTokens) -> TokenStream {
    let mut iter = span.to_token_stream().into_iter();
    // `Span` on stable Rust has a limitation that only points to the first
    // token, not the whole tokens. We can work around this limitation by
    // using the first/last span of the tokens like `syn::Error::new_spanned` does.
    let start_span = iter.next().map_or_else(Span::call_site, |t| t.span());
    let end_span = iter.last().map_or(start_span, |t| t.span());

    let mut tokens = tokens.into_iter().collect::<Vec<_>>();
    if let Some(tt) = tokens.first_mut() {
        tt.set_span(start_span);
    }
    for tt in tokens.iter_mut().skip(1) {
        tt.set_span(end_span);
    }
    tokens.into_iter().collect()
}
