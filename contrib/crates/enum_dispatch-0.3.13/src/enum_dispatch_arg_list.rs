//! Provides an implementation of a `syn`- and `quote`-compatible syntax item describing the
//! list of arguments that can be passed to an `#[enum_dispatch(...)]` attribute.

pub struct EnumDispatchArgList {
    pub arg_list: syn::punctuated::Punctuated<syn::Path, syn::token::Comma>,
}

impl syn::parse::Parse for EnumDispatchArgList {
    fn parse(input: &syn::parse::ParseBuffer) -> Result<Self, syn::Error> {
        let arg_list = syn::punctuated::Punctuated::parse_terminated(input)?;
        Ok(Self { arg_list })
    }
}
