//! Shared utilities for macro code generation.

use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote_spanned};
use syn::{Attribute, Type, Visibility, spanned::Spanned};

pub(crate) fn type_to_snake_case(ty: &Type) -> String {
    if let Type::Path(type_path) = ty
        && let Some(segment) = type_path.path.segments.last()
    {
        return ident_to_snake_case(&segment.ident.to_string());
    }
    "unknown".to_string()
}

pub(crate) fn ident_to_snake_case(s: &str) -> String {
    let mut result = String::new();
    for (i, ch) in s.chars().enumerate() {
        if ch.is_uppercase() {
            if i > 0 {
                result.push('_');
            }
            result.push(ch.to_lowercase().next().unwrap());
        } else {
            result.push(ch);
        }
    }
    result
}

pub(crate) fn method_name_to_pascal_case(ident: &syn::Ident) -> String {
    let s = ident.to_string();
    let mut result = String::new();
    let mut capitalize_next = true;

    for ch in s.chars() {
        if ch == '_' {
            capitalize_next = true;
        } else if capitalize_next {
            result.push(ch.to_uppercase().next().unwrap());
            capitalize_next = false;
        } else {
            result.push(ch);
        }
    }
    result
}

pub(crate) fn type_name_string(ty: &Type) -> String {
    if let Type::Path(type_path) = ty
        && let Some(segment) = type_path.path.segments.last()
    {
        return segment.ident.to_string();
    }
    panic!("Cannot extract type name from impl block - expected a simple type path");
}

/// Adjust visibility for marker structs generated in a submodule.
/// For `Inherited` (private) visibility, returns `pub(super)` so the struct
/// is accessible from the parent module.
pub(crate) fn marker_struct_visibility(vis: &Visibility) -> Visibility {
    match vis {
        Visibility::Inherited => syn::parse_quote!(pub(super)),
        other => other.clone(),
    }
}

pub(crate) fn extract_allow_attrs(attrs: &[Attribute]) -> Vec<&Attribute> {
    attrs
        .iter()
        .filter(|attr| attr.path().is_ident("allow"))
        .collect()
}

/// Generates a marker struct definition with appropriate visibility.
/// Converts the method name to PascalCase for the struct name.
pub(crate) fn generate_marker_struct(method: &syn::ImplItemFn) -> TokenStream2 {
    let struct_name = method_name_to_pascal_case(&method.sig.ident);
    let struct_ident = format_ident!("{}", struct_name);
    let struct_visibility = marker_struct_visibility(&method.vis);
    let span = method.span();

    quote_spanned! { span=>
        #struct_visibility struct #struct_ident;
    }
}

/// Generates a const definition that references a marker struct.
/// The const keeps the original method name, while referencing the PascalCase struct.
pub(crate) fn generate_const_definition(
    method: &syn::ImplItemFn,
    module_ident: &syn::Ident,
) -> TokenStream2 {
    let struct_name = method_name_to_pascal_case(&method.sig.ident);
    let struct_ident = format_ident!("{}", struct_name);
    let method_ident = &method.sig.ident;
    let visibility = &method.vis;
    let span = method.span();
    let allow_attrs = extract_allow_attrs(&method.attrs);

    quote_spanned! { span=>
        #[allow(non_upper_case_globals, dead_code)]
        #(#allow_attrs)*
        #visibility const #method_ident: #module_ident::#struct_ident = #module_ident::#struct_ident;
    }
}
