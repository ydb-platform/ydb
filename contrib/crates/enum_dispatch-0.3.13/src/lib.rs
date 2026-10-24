//! `enum_dispatch` provides a set of macros that can be used to easily refactor dynamically
//! dispatched trait accesses to improve their performance by up to 10x.
//!
//! Accessing structures through dynamic dispatch is known to have a high runtime cost. Dynamic
//! dispatch is traditionally used to hide unnecessary type information, improving encapsulation
//! and making it trivial to add new implementations. However, this hiding of information means
//! that each time a structure is dynamically accessed, the program must perform a lookup of the
//! type's information in a virtual table. The extra round-trips to the vtable quickly add up.
//!
//! In Rust, dynamic dispatch is done using traits. Rust 2018 adds the `impl` and `dyn` keywords to
//! make it easier to keep track of instances of dynamic dispatch, but it's not always easy to
//! avoid it entirely.
//!
//! # Feature documentation
//!
//! For full documentation of features like generic support, custom variant names, and more, please
//! check the repository's
//! [README](https://gitlab.com/antonok/enum_dispatch/-/blob/master/README.md).
//!
//! # How it works
//!
//! Observe the following example of code describing a user interface with knobs. Each knob can
//! hold a value between 0.0 and 1.0. Some knobs provide a *linear* range, whereas other knobs
//! provide a *logarithmic* range.
//!
//! ```
//! trait KnobControl {
//!     fn set_position(&mut self, value: f64);
//!     fn get_value(&self) -> f64;
//! }
//!
//! struct LinearKnob {
//!     position: f64,
//! }
//!
//! struct LogarithmicKnob {
//!     position: f64,
//! }
//!
//! impl KnobControl for LinearKnob {
//!     fn set_position(&mut self, value: f64) {
//!         self.position = value;
//!     }
//!
//!     fn get_value(&self) -> f64 {
//!         self.position
//!     }
//! }
//!
//! impl KnobControl for LogarithmicKnob {
//!     fn set_position(&mut self, value: f64) {
//!         self.position = value;
//!     }
//!
//!     fn get_value(&self) -> f64 {
//!         (self.position + 1.).log2()
//!     }
//! }
//!
//! fn main() {
//!     let v: Vec<Box<dyn KnobControl>> = vec![
//!         //set the knobs
//!     ];
//!
//!     //use the knobs
//! }
//! ```
//!
//! There are other ways to keep an arbitrarily ordered list of different knob types, but none of
//! them are quite as simple or easy to maintain. Unfortunately, this implementation uses both heap
//! allocated `Box`es and dynamic dispatch, which will have performance implications.
//!
//! One alternative is to use introduce a new enum type that can hold either a `LinearKnob` or a
//! `LogarithmicKnob` as a variant, and also implements `KnobControl` by matching on itself and
//! delegating calls to its variants. This would look like the following:
//!
//! ```
//! # trait KnobControl {
//! #     fn set_position(&mut self, value: f64);
//! #     fn get_value(&self) -> f64;
//! # }
//! #
//! # struct LinearKnob {
//! #     position: f64,
//! # }
//! #
//! # struct LogarithmicKnob {
//! #     position: f64,
//! # }
//! #
//! # impl KnobControl for LinearKnob {
//! #     fn set_position(&mut self, value: f64) {
//! #         self.position = value;
//! #     }
//! #
//! #     fn get_value(&self) -> f64 {
//! #         self.position
//! #     }
//! # }
//! #
//! # impl KnobControl for LogarithmicKnob {
//! #     fn set_position(&mut self, value: f64) {
//! #         self.position = value;
//! #     }
//! #
//! #     fn get_value(&self) -> f64 {
//! #         (self.position + 1.).log2()
//! #     }
//! # }
//! #
//! enum Knob {
//!     Linear(LinearKnob),
//!     Logarithmic(LogarithmicKnob),
//! }
//!
//! impl KnobControl for Knob {
//!     fn set_position(&mut self, value: f64) {
//!         match self {
//!             Knob::Linear(inner_knob) => inner_knob.set_position(value),
//!             Knob::Logarithmic(inner_knob) => inner_knob.set_position(value),
//!         }
//!     }
//!
//!     fn get_value(&self) -> f64 {
//!         match self {
//!             Knob::Linear(inner_knob) => inner_knob.get_value(),
//!             Knob::Logarithmic(inner_knob) => inner_knob.get_value(),
//!         }
//!     }
//! }
//! ```
//!
//! Performance with this implementation is significantly improved, since all the information the
//! program could possibly need to know about each knob can be deduced at compile time. Besides
//! avoiding heap allocations and vtable lookups, this allows the compiler to squeeze out even more
//! optimization through function inlining.
//!
//! However, it's easy to see that the cost of maintaining the source code for this extra structure
//! is quite high. What happens when we add more knob types? What happens when we add more trait
//! methods? Even worse, what happens when we do both!
//!
//! The resulting code is very repetitive, but that makes it a great target for automatic
//! generation. The `enum_dispatch` macro can do the automatic generation for you. Examine the code
//! to generate the same implementation when using `enum_dispatch`.
//!
//! ```
//! # use enum_dispatch::enum_dispatch;
//! #
//! #[enum_dispatch]
//! trait KnobControl {
//!     //...
//! #     fn set_position(&mut self, value: f64);
//! #     fn get_value(&self) -> f64;
//! }
//! #
//! # struct LinearKnob {
//! #     position: f64,
//! # }
//! #
//! # struct LogarithmicKnob {
//! #     position: f64,
//! # }
//! #
//! # impl KnobControl for LinearKnob {
//! #     fn set_position(&mut self, value: f64) {
//! #         self.position = value;
//! #     }
//! #
//! #     fn get_value(&self) -> f64 {
//! #         self.position
//! #     }
//! # }
//! #
//! # impl KnobControl for LogarithmicKnob {
//! #     fn set_position(&mut self, value: f64) {
//! #         self.position = value;
//! #     }
//! #
//! #     fn get_value(&self) -> f64 {
//! #         (self.position + 1.).log2()
//! #     }
//! # }
//!
//! #[enum_dispatch(KnobControl)]
//! enum Knob {
//!     LinearKnob,
//!     LogarithmicKnob,
//! }
//! ```
//!
//! That's it. `enum_dispatch` will also automatically generate implementations of
//! `std::convert::From` for each enum variant, so that new `Knob`s can be created without concern
//! for the names of each enum variant.
//!
//! ```
//! # use enum_dispatch::enum_dispatch;
//! #
//! # #[enum_dispatch]
//! # trait KnobControl {
//! #     fn set_position(&mut self, value: f64);
//! #     fn get_value(&self) -> f64;
//! # }
//! #
//! # struct LinearKnob {
//! #     position: f64,
//! # }
//! #
//! # struct LogarithmicKnob {
//! #     position: f64,
//! # }
//! #
//! # impl KnobControl for LinearKnob {
//! #     fn set_position(&mut self, value: f64) {
//! #         self.position = value;
//! #     }
//! #
//! #     fn get_value(&self) -> f64 {
//! #         self.position
//! #     }
//! # }
//! #
//! # impl KnobControl for LogarithmicKnob {
//! #     fn set_position(&mut self, value: f64) {
//! #         self.position = value;
//! #     }
//! #
//! #     fn get_value(&self) -> f64 {
//! #         (self.position + 1.).log2()
//! #     }
//! # }
//! #
//! # #[enum_dispatch(KnobControl)]
//! # enum Knob {
//! #     LinearKnob,
//! #     LogarithmicKnob,
//! # }
//! #
//! # fn some_existing_knobs() -> (LinearKnob, LogarithmicKnob) {
//! #     (LinearKnob { position: 0.5 }, LogarithmicKnob { position: 0.5 })
//! # }
//! #
//! let (a_linear_knob, a_logarithmic_knob) = some_existing_knobs();
//!
//! let knob = Knob::from(a_linear_knob);
//! let knob = Knob::from(a_logarithmic_knob);
//! ```
//!
//! # Performance
//!
//! The `benches` directory contains three benchmarks of different natures, each comparing four
//! different methods of accessing a traited struct of an arbitrary type. The four methods are as
//! follows:
//!
//! | test name    | explanation                                                                             |
//! |--------------|-----------------------------------------------------------------------------------------|
//! | boxdyn       | The easiest way to access a struct, using a heap allocation and dynamic dispatch.       |
//! | refdyn       | Accesses the struct by reference, but still using dynamic dispatch. No heap allocation. |
//! | customderive | Uses a similar macro approach from the external [`enum_derive`](https://github.com/DanielKeep/rust-custom-derive) crate, which implements a method that returns an inner type as a dynamic trait object. |
//! | enumdispatch | Implemented using this crate.                                                           |
//!
//! ## The benchmarks
//!
//! The following benchmark results were measured on a Ryzen 7 2700x CPU.
//!
//! ### compiler_optimized
//!
//! The first set of benchmarks creates trait objects and measures the speed of accessing a method
//! on them.
//!
//! ```text
//! test benches::boxdyn_compiler_optimized       ... bench:   2,135,418 ns/iter (+/- 12,575)
//! test benches::customderive_compiler_optimized ... bench:   2,611,860 ns/iter (+/- 18,644)
//! test benches::enumdispatch_compiler_optimized ... bench:           0 ns/iter (+/- 0)
//! test benches::refdyn_compiler_optimized       ... bench:   2,132,591 ns/iter (+/- 22,114)
//! ```
//!
//! It's easy to see that `enum_dispatch` is the clear winner here!
//!
//! Ok, fine. This wasn't a fair test. The compiler is able to "look through" the trait method call
//! in the enum_dispatch case, notices that the result is unused, and removes it as an
//! optimization. However, this still highlights an important property of `enum_dispatch`ed types:
//! the compiler is able to infer much better optimizations when possible.
//!
//! ### blackbox
//!
//! The next set of benchmarks uses the `test::black_box` method to hide the fact that the result
//! of the method is unused.
//!
//! ```text
//! test benches::boxdyn_blackbox       ... bench:   2,131,736 ns/iter (+/- 24,937)
//! test benches::customderive_blackbox ... bench:   2,611,721 ns/iter (+/- 23,502)
//! test benches::enumdispatch_blackbox ... bench:     471,740 ns/iter (+/- 1,439)
//! test benches::refdyn_blackbox       ... bench:   2,131,978 ns/iter (+/- 21,547)
//! ```
//!
//! The competitors faced virtually no impact, whereas `enum_dispatch` takes the full force of the
//! `black_box` call. This test shows the power that avoiding dynamic dispatch gives to the
//! compiler in the context of the previous test, but also demonstrates how much faster
//! `enum_dispatch` is in real code: almost 5 times faster than the closest alternative.
//!
//! ### homogenous_vec
//!
//! The final set of benchmarks puts 1024 traited structs of arbitrary types at random into a `Vec`
//! and measures the time it takes to successively iterate over the entire `Vec`, calling
//! `black_box`ed methods on each element.
//!
//! ```text
//! test benches::boxdyn_homogeneous_vec       ... bench:   5,900,191 ns/iter (+/- 95,169)
//! test benches::customderive_homogeneous_vec ... bench:   4,831,808 ns/iter (+/- 140,437)
//! test benches::enumdispatch_homogeneous_vec ... bench:     479,630 ns/iter (+/- 3,531)
//! test benches::refdyn_homogeneous_vec       ... bench:   5,658,461 ns/iter (+/- 137,128)
//! ```
//!
//! This might be one of the most likely use cases for traited structs of arbitrary types, and it's
//! where `enum_dispatch` really shines. Since a `Vec` of `enum_dispatch` objects is actually a
//! `Vec` of enums rather than addresses, accessing an element takes half the indirection of the
//! other techniques. Add that to the lack of vtable accesses, and we have a result that is 10
//! times faster than the closest alternative, and almost 12 times faster than the best technique
//! from the standard library.

#![doc(
    html_logo_url = "https://gitlab.com/antonok/enum_dispatch/raw/master/enum_dispatch.svg",
    html_favicon_url = "https://gitlab.com/antonok/enum_dispatch/raw/master/enum_dispatch.svg"
)]

extern crate proc_macro;

use proc_macro2::TokenStream;
use quote::{ToTokens, TokenStreamExt};

/// Used for converting a macro input into an ItemTrait or an EnumDispatchItem.
mod attributed_parser;
/// Provides local storage for enum and trait definitions so that they can be accessed later.
mod cache;
/// Provides a custom syntax specification for the arguments to an `#[enum_dispatch(...)]` attribute.
mod enum_dispatch_arg_list;
/// Provides a custom syntax specification for enum dispatch syntax blocks.
mod enum_dispatch_item;
/// Provides a custom syntax specification for the variants of enum dispatch syntax blocks.
mod enum_dispatch_variant;
/// Provides utilities for building enum dispatch implementations.
mod expansion;
/// Convenience trait for token parsing.
mod filter_attrs;
/// Codifies the kinds of generic arguments supported in an `#[enum_dispatch(T<...>)]` attribute.
mod supported_generics;
/// Convenience methods for constructing `syn` types.
mod syn_utils;

use crate::expansion::add_enum_impls;
use crate::supported_generics::{convert_to_supported_generic, num_supported_generics};

/// Annotating a trait or enum definition with an `#[enum_dispatch]` attribute will register it
/// with the enum_dispatch library, allowing it to be used to generate impl blocks elsewhere.
///
/// Annotating a trait or enum definition with an `#[enum_dispatch(BlockName)]` attribute will
/// generate an enum dispatch implementation for the specified trait/enum pair, as well as adding
/// an impl of std::convert::From for each variant. When annotating a trait, BlockName should be the
/// name of a registered enum. When annotating an enum, BlockName should be the name of a registered
/// trait.
///
/// An annotated enum should have variants that are simply the names of types imported to the
/// current scope. To force individual variants to use a custom name when expanded, each variant
/// can also take the form of a normal tuple-style enum variant with a single field.
#[proc_macro_attribute]
pub fn enum_dispatch(attr: proc_macro::TokenStream, item: proc_macro::TokenStream) -> proc_macro::TokenStream {
    enum_dispatch2(attr.into(), item.into()).into()
}

/// `proc_macro2::TokenStream` compatible version of the `enum_dispatch` function.
///
/// Using only `proc_macro2::TokenStream` inside the entire crate makes methods unit-testable and
/// removes the need for conversions everywhere.
fn enum_dispatch2(attr: TokenStream, item: TokenStream) -> TokenStream {
    let new_block = attributed_parser::parse_attributed(item.clone()).unwrap();
    let mut expanded = match &new_block {
        attributed_parser::ParsedItem::Trait(traitdef) => {
            cache::cache_trait(traitdef.to_owned());
            item
        }
        attributed_parser::ParsedItem::EnumDispatch(enumdef) => {
            cache::cache_enum_dispatch(enumdef.clone());
            syn::ItemEnum::from(enumdef.to_owned())
                .into_token_stream()
        }
    };
    // If the attributes are non-empty, the new block should be "linked" to the listed definitions.
    // Those definitions may or may not have been cached yet.
    // If one is not cached yet, the link will be pushed into the cache, and impl generation will
    // be deferred until the missing definition is encountered.
    // For now, we assume it is already cached.
    if !attr.is_empty() {
        let attr_parse_result = syn::parse2::<enum_dispatch_arg_list::EnumDispatchArgList>(attr)
            .expect("Could not parse arguments to `#[enum_dispatch(...)]`.")
            .arg_list
            .into_iter()
            .try_for_each(|p| {
                if p.leading_colon.is_some() || p.segments.len() != 1 {
                    panic!("Paths in `#[enum_dispatch(...)]` are not supported.");
                }
                let syn::PathSegment {
                    ident: attr_name,
                    arguments: attr_generics
                } = p.segments.last().unwrap();
                let attr_generics = match attr_generics.clone() {
                    syn::PathArguments::None => vec![],
                    syn::PathArguments::AngleBracketed(args) => {
                        assert!(args.colon2_token.is_none());
                        match args.args.iter().map(convert_to_supported_generic).collect::<Result<Vec<_>, _>>() {
                            Ok(v) => v,
                            Err((unsupported, span)) => {
                                let error_string = unsupported.to_string();
                                return Err(quote::quote_spanned! {span=>
                                    compile_error!(#error_string)
                                });
                            }
                        }
                    }
                    syn::PathArguments::Parenthesized(_) => panic!("Expected angle bracketed generic arguments, found parenthesized arguments"),
                };
                match &new_block {
                    attributed_parser::ParsedItem::Trait(traitdef) => {
                        let supported_generics = num_supported_generics(&traitdef.generics);
                        cache::defer_link((attr_name, attr_generics.len()), (&traitdef.ident, supported_generics))
                    }
                    attributed_parser::ParsedItem::EnumDispatch(enumdef) => {
                        let supported_generics = num_supported_generics(&enumdef.generics);
                        cache::defer_link((attr_name, attr_generics.len()), (&enumdef.ident, supported_generics))
                    }
                }
                Ok(())
            });
        if let Err(e) = attr_parse_result {
            return e;
        }
    };
    // It would be much simpler to just always retrieve all definitions from the cache. However,
    // span information is not stored in the cache. Saving the newly retrieved definition prevents
    // *all* of the span information from being lost.
    match new_block {
        attributed_parser::ParsedItem::Trait(traitdef) => {
            let supported_generics = num_supported_generics(&traitdef.generics);
            let additional_enums =
                cache::fulfilled_by_trait(&traitdef.ident, supported_generics);
            for enumdef in additional_enums {
                expanded.append_all(add_enum_impls(enumdef, traitdef.clone()));
            }
        }
        attributed_parser::ParsedItem::EnumDispatch(enumdef) => {
            let supported_generics = num_supported_generics(&enumdef.generics);
            let additional_traits =
                cache::fulfilled_by_enum(&enumdef.ident, supported_generics);
            for traitdef in additional_traits {
                expanded.append_all(add_enum_impls(enumdef.clone(), traitdef));
            }
        }
    }
    expanded
}
