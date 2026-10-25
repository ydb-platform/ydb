//! Provides an implementation of a `syn`- and `quote`-compatible syntax item describing the
//! shortened enum form used by `enum_dispatch`.
//!
//! The syntax is *mostly* identical to that of standard enums. The only difference is the
//! specification of enum variants -- in the custom `EnumDispatchItem` type, each variant must be
//! specified as a `syn::Type` rather than a `syn::Variant`. In the case of basic unit fields named
//! after existing scoped types, a normal Rust enum can be parsed as an EnumDispatchItem without
//! issue.
use quote::TokenStreamExt;

use crate::enum_dispatch_variant::EnumDispatchVariant;
use crate::filter_attrs::FilterAttrs;

/// A structure that can be used to store syntax information about an `enum_dispatch` enum.
///
/// Mostly identical to `syn::ItemEnum`.
#[derive(Clone)]
pub struct EnumDispatchItem {
    pub attrs: Vec<syn::Attribute>,
    pub vis: syn::Visibility,
    enum_token: syn::token::Enum,
    pub ident: syn::Ident,
    pub generics: syn::Generics,
    brace_token: syn::token::Brace,
    pub variants: syn::punctuated::Punctuated<EnumDispatchVariant, syn::token::Comma>,
}

/// Allows `EnumDispatchItem`s to be parsed from `String`s or `TokenStream`s.
impl syn::parse::Parse for EnumDispatchItem {
    fn parse(input: syn::parse::ParseStream) -> syn::parse::Result<Self> {
        let attrs = input.call(syn::Attribute::parse_outer)?;
        let vis: syn::Visibility = input.parse()?;
        let enum_token = input.parse::<syn::Token![enum]>()?;
        let ident: syn::Ident = input.parse()?;
        let generics: syn::Generics = input.parse()?;
        let where_clause = input.parse()?;
        let content;
        let brace_token = syn::braced!(content in input);
        let variants = content.parse_terminated(EnumDispatchVariant::parse, syn::Token![,])?;
        Ok(Self {
            attrs,
            vis,
            enum_token,
            ident,
            generics: syn::Generics {
                where_clause,
                ..generics
            },
            brace_token,
            variants,
        })
    }
}

/// Allows `EnumDispatchItem`s to be converted into `TokenStream`s.
impl quote::ToTokens for EnumDispatchItem {
    fn to_tokens(&self, tokens: &mut proc_macro2::TokenStream) {
        tokens.append_all(self.attrs.outer());
        self.vis.to_tokens(tokens);
        self.enum_token.to_tokens(tokens);
        self.ident.to_tokens(tokens);
        self.generics.to_tokens(tokens);
        self.generics.where_clause.to_tokens(tokens);
        self.brace_token.surround(tokens, |tokens| {
            self.variants.to_tokens(tokens);
        });
    }
}

/// Custom conversion implementation that expands the shorthand `enum_dispatch` enum syntax into a
/// standard Rust enum syntax.
impl ::std::convert::From<EnumDispatchItem> for syn::ItemEnum {
    fn from(item: EnumDispatchItem) -> syn::ItemEnum {
        use ::std::iter::FromIterator;
        let variants: Vec<syn::Variant> = item
            .variants
            .iter()
            .map(|variant: &EnumDispatchVariant| syn::Variant {
                attrs: variant.attrs.to_owned(),
                ident: variant.ident.to_owned(),
                fields: syn::Fields::Unnamed(syn::FieldsUnnamed {
                    paren_token: Default::default(),
                    unnamed: {
                        let mut punctuated = syn::punctuated::Punctuated::new();
                        punctuated.push(syn::Field {
                            attrs: variant.field_attrs.to_owned(),
                            vis: syn::Visibility::Inherited,
                            ident: None,
                            colon_token: Default::default(),
                            ty: variant.ty.to_owned(),
                            mutability: syn::FieldMutability::None,
                        });
                        punctuated
                    },
                }),
                discriminant: None,
            })
            .collect();
        syn::ItemEnum {
            attrs: item.attrs,
            vis: item.vis,
            enum_token: item.enum_token,
            ident: item.ident,
            generics: syn::Generics {
                where_clause: item.generics.where_clause,
                ..item.generics
            },
            brace_token: item.brace_token,
            variants: syn::punctuated::Punctuated::from_iter(variants),
        }
    }
}
