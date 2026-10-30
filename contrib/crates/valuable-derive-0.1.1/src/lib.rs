#![warn(rust_2018_idioms, unreachable_pub)]

#[macro_use]
mod error;

mod attr;
mod expand;

use proc_macro::TokenStream;
use syn::parse_macro_input;

/// Derive a `Valuable` implementation for a struct or enum.
///
/// # Attributes
///
/// ## `#[valuable(rename = "...")]`
///
/// Use the given name instead of its Rust name.
///
/// ## `#[valuable(transparent)]`
///
/// Delegate the trait implementation to the field.
///
/// This attribute can only be used on a struct that has a single field.
///
/// ## `#[valuable(skip)]`
///
/// Skip the field.
///
/// # Examples
///
/// ```
/// use valuable::Valuable;
///
/// #[derive(Valuable)]
/// struct HelloWorld {
///     message: Message,
/// }
///
/// #[derive(Valuable)]
/// enum Message {
///     HelloWorld,
///     Custom(String),
/// }
/// ```
#[proc_macro_derive(Valuable, attributes(valuable))]
pub fn derive_valuable(input: TokenStream) -> TokenStream {
    let mut input = parse_macro_input!(input as syn::DeriveInput);
    expand::derive_valuable(&mut input).into()
}
