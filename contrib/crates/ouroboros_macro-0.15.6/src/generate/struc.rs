use crate::{
    info_structures::StructInfo,
    utils::{self, replace_this_with_lifetime},
};
use proc_macro2::TokenStream;
use quote::quote;
use syn::Error;

/// Creates the struct that will actually store the data. This involves properly organizing the
/// fields, collecting metadata about them, reversing the order everything is stored in, and
/// converting any uses of 'this to 'static.
pub fn create_actual_struct_def(info: &StructInfo) -> Result<TokenStream, Error> {
    let vis = utils::submodule_contents_visiblity(&info.vis);
    let ident = &info.ident;
    let generics = &info.generics;

    let field_defs: Vec<_> = info
        .fields
        .iter()
        // Reverse the order of all fields. We ensure that items in the struct are only dependent
        // on references to items above them. Rust drops items in a struct in forward declaration order.
        // This would cause parents being dropped before children, necessitating the reversal.
        .rev()
        .map(|field| {
            let name = &field.name;
            let ty = field.stored_type();
            quote! {
                #[doc(hidden)]
                #name: #ty
            }
        })
        .collect();

    // Create the new struct definition.
    let mut where_clause = quote! {};
    if let Some(clause) = &generics.where_clause {
        where_clause = quote! { #clause };
    }
    let def = quote! {
        #vis struct #ident #generics #where_clause {
            #(#field_defs),*
        }
    };

    // Finally, replace the fake 'this lifetime with the one we found.
    let fake_lifetime = info.fake_lifetime();
    let def = replace_this_with_lifetime(quote! { #def }, fake_lifetime.clone());

    Ok(def)
}
