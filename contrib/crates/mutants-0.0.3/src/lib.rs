// Copyright 2021 Martin Pool

//! Attribute macros to control how [cargo-mutants](https://crates.io/crates/cargo-mutants) mutates code.
//!
//! For example, a function that is difficult to test, or has disruptive effects when mutated, can
//! be marked with [macro@skip].
//!
//! # Changelog
//!
//! ## 0.0.3
//!
//! * Reset edition to 2018 for broader compatibility.

use proc_macro::TokenStream;

/// `cargo mutants` should not mutate functions marked with this attribute.
///
/// This can currently only be applied to functions, not modules or other syntactic constructs.
///
/// ```
/// #[mutants::skip]
/// pub fn some_difficult_function() {
///     // ...
/// }
/// ```
///
/// This is a no-op during compilation, but is seen by cargo-mutants as it processes the source.
#[proc_macro_attribute]
pub fn skip(_attr: TokenStream, item: TokenStream) -> TokenStream {
    item
}
