//! Provides an implementation of a `syn`- and `quote`-compatible syntax item describing a single
//! variant for the shortened enum form used by `enum_dispatch`.
//!
//! Each variant can be either just a type, or a name with a single associated tuple type
//! parameter. In the first form, the name is simply the same as the type. In the second, the name
//! is explicitly specified.

use std::iter::FromIterator;

use quote::TokenStreamExt;

use crate::filter_attrs::FilterAttrs;

/// A structure that can be used to store syntax information about an `enum_dispatch` enum variant.
#[derive(Clone)]
pub struct EnumDispatchVariant {
    pub attrs: Vec<syn::Attribute>,
    pub ident: syn::Ident,
    pub field_attrs: Vec<syn::Attribute>,
    pub ty: syn::Type,
}

/// Allows `EnumDispatchItem`s to be parsed from `String`s or `TokenStream`s.
impl syn::parse::Parse for EnumDispatchVariant {
    fn parse(input: syn::parse::ParseStream) -> syn::parse::Result<Self> {
        let attrs = input.call(syn::Attribute::parse_outer)?;
        let ident: syn::Ident = input.parse()?;
        let (field_attrs, ty) = if input.peek(syn::token::Brace) {
            unimplemented!("enum_dispatch variants cannot have braces for arguments");
        } else if input.peek(syn::token::Paren) {
            let input: syn::FieldsUnnamed = input.parse()?;
            let mut fields = input.unnamed.iter();
            let field_1 = fields
                .next()
                .expect("Named enum_dispatch variants must have one unnamed field");
            if fields.next().is_some() {
                panic!("Named enum_dispatch variants can only have one unnamed field");
            }
            (field_1.attrs.clone(), field_1.ty.clone())
        } else {
            (vec![], into_type(ident.clone()))
        };
        Ok(EnumDispatchVariant {
            attrs,
            ident,
            field_attrs,
            ty,
        })
    }
}

/// Allows `EnumDispatchVariant`s to be converted into `TokenStream`s.
impl quote::ToTokens for EnumDispatchVariant {
    fn to_tokens(&self, tokens: &mut proc_macro2::TokenStream) {
        tokens.append_all(self.attrs.outer());
        self.ident.to_tokens(tokens);
        syn::token::Paren::default().surround(tokens, |tokens| {
            tokens.append_all(self.field_attrs.iter());
            self.ty.to_tokens(tokens);
        });
    }
}

/// When expanding shorthand `enum_dispatch` enum syntax, each specified, unnamed type variant must
/// acquire an associated identifier to use for the name of the standard Rust enum variant.
///
/// Note that `proc_macro_attribute`s cannot provide custom syntax parsing. Unless using a
/// function-style procedural macro, each type must already be parseable as a unit enum variant.
/// This rules out, for example, generic types with lifetime or type parameters. For these, an
/// explicitly named variant must be used.
fn into_type(ident: syn::Ident) -> syn::Type {
    syn::Type::Path(syn::TypePath {
        path: syn::Path {
            leading_colon: None,
            segments: syn::punctuated::Punctuated::from_iter(vec![syn::PathSegment {
                arguments: syn::PathArguments::None,
                ident,
            }]),
        },
        qself: None,
    })
}
