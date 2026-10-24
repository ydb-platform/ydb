#![recursion_limit = "256"]
#![allow(
    clippy::expl_impl_clone_on_copy, // https://github.com/rust-lang/rust-clippy/issues/15842
    clippy::needless_pass_by_value,
    clippy::single_match_else,
    clippy::too_many_lines
)]

mod parse;
mod tagged_impl;
mod tagged_trait;

use crate::parse::{ImplArgs, Input, TraitArgs};
use proc_macro::TokenStream;
use proc_macro2::{Ident, Span};
use quote::{ToTokens, TokenStreamExt as _};
use syn::parse_macro_input;

#[derive(Copy, Clone)]
pub(crate) struct Mode {
    ser: bool,
    de: bool,
}

#[proc_macro_attribute]
pub fn serde(args: TokenStream, input: TokenStream) -> TokenStream {
    let ser = true;
    let de = true;
    expand(args, input, Mode { ser, de })
}

#[proc_macro_attribute]
pub fn serialize(args: TokenStream, input: TokenStream) -> TokenStream {
    let ser = true;
    let de = false;
    expand(args, input, Mode { ser, de })
}

#[proc_macro_attribute]
pub fn deserialize(args: TokenStream, input: TokenStream) -> TokenStream {
    let ser = false;
    let de = true;
    expand(args, input, Mode { ser, de })
}

fn expand(args: TokenStream, input: TokenStream, mode: Mode) -> TokenStream {
    let input = parse_macro_input!(input as Input);

    TokenStream::from(match input {
        Input::Trait(input) => {
            let args = parse_macro_input!(args as TraitArgs);
            tagged_trait::expand(args, input, mode)
        }
        Input::Impl(input) => {
            let args = parse_macro_input!(args as ImplArgs);
            tagged_impl::expand(args, input, mode)
        }
    })
}

#[allow(non_camel_case_types)]
struct private;

impl ToTokens for private {
    fn to_tokens(&self, tokens: &mut proc_macro2::TokenStream) {
        tokens.append(Ident::new(
            concat!("__private", env!("CARGO_PKG_VERSION_PATCH")),
            Span::call_site(),
        ));
    }
}
