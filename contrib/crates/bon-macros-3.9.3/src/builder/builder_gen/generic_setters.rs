use super::models::BuilderGenCtx;
use crate::parsing::ItemSigConfig;
use crate::util::prelude::*;
use std::collections::BTreeSet;
use syn::punctuated::Punctuated;
use syn::token::Where;
use syn::visit::Visit;

pub(super) struct GenericSettersCtx<'a> {
    base: &'a BuilderGenCtx,
    config: &'a ItemSigConfig<String>,
}

impl<'a> GenericSettersCtx<'a> {
    pub(super) fn new(base: &'a BuilderGenCtx, config: &'a ItemSigConfig<String>) -> Self {
        Self { base, config }
    }

    pub(super) fn generic_setter_methods(&self) -> Result<TokenStream> {
        let generics = &self.base.generics.decl_without_defaults;

        let type_param_idents: Vec<&syn::Ident> = generics
            .iter()
            .filter_map(|param| match param {
                syn::GenericParam::Type(type_param) => Some(&type_param.ident),
                _ => None,
            })
            .collect();

        // Check for interdependent type parameters in generic bounds
        for param in generics {
            if let syn::GenericParam::Type(type_param) = param {
                let mut params = TypeParamFinder::new(&type_param_idents);

                for bound in &type_param.bounds {
                    params.visit_type_param_bound(bound);
                }

                // Self-referential type params are fine
                params.found.remove(&type_param.ident);

                if let Some(first_param) = params.found.iter().next() {
                    let params_str = params
                        .found
                        .iter()
                        .map(|p| format!("`{p}`"))
                        .collect::<Vec<_>>()
                        .join(", ");
                    bail!(
                        first_param,
                        "generic conversion methods cannot be generated for interdependent type parameters; \
                         the bounds on generic parameter `{}` reference other type parameters: {}\n\
                         \n\
                         Consider removing `generics(setters(...))` or restructuring your types to avoid interdependencies",
                        type_param.ident,
                        params_str
                    );
                }
            }
        }

        // Check for interdependent type parameters in where clauses
        if let Some(where_clause) = &self.base.generics.where_clause {
            for predicate in &where_clause.predicates {
                let mut params = TypeParamFinder::new(&type_param_idents);
                params.visit_where_predicate(predicate);
                if params.found.len() > 1 {
                    let params_str = params
                        .found
                        .iter()
                        .map(|p| format!("`{p}`"))
                        .collect::<Vec<_>>()
                        .join(", ");
                    bail!(
                        predicate,
                        "generic conversion methods cannot be generated for interdependent type parameters; \
                         the where clause predicate references multiple type parameters: {}\n\
                         \n\
                         Consider removing `generics(setters(...))` or restructuring your types to avoid interdependencies",
                        params_str
                    );
                }
            }
        }

        let mut methods = Vec::with_capacity(generics.len());

        for (index, param) in generics.iter().enumerate() {
            match param {
                syn::GenericParam::Type(type_param) => {
                    methods.push(self.generic_setter_method(index, type_param));
                }
                syn::GenericParam::Const(const_param) => {
                    bail!(
                        &const_param.ident,
                        "const generic parameters are not yet supported with `generics(setters(...))`; \
                         only type parameters can be overridden, feel free to open an issue if you need \
                         this feature"
                    );
                }
                syn::GenericParam::Lifetime(_) => {
                    // Skip lifetimes, they don't get setters
                }
            }
        }

        Ok(quote! {
            #(#methods)*
        })
    }

    fn generic_setter_method(
        &self,
        param_index: usize,
        type_param: &syn::TypeParam,
    ) -> TokenStream {
        let builder_ident = &self.base.builder_type.ident;
        let state_var = &self.base.state_var;
        let where_clause = &self.base.generics.where_clause;

        let param_ident = &type_param.ident;
        let method_name = self.method_name(param_ident);

        let vis = self
            .config
            .vis
            .as_ref()
            .map(|v| &v.value)
            .unwrap_or(&self.base.builder_type.vis);

        let docs = self.method_docs(param_ident);

        // Build the generic arguments for the output type, where the current parameter
        // is replaced with a new type variable. Even though the `GenericsNamespace`
        let new_type_var = self
            .base
            .namespace
            // Add `New` prefix to make the type variable more readable in the docs and IDE hints
            .unique_ident(format!("New{param_ident}"));

        // Copy the bounds from the original type parameter to the new one
        let bounds = &type_param.bounds;
        let new_type_param = if bounds.is_empty() {
            quote!(#new_type_var)
        } else {
            quote!(#new_type_var: #bounds)
        };

        let output_generic_args = self
            .base
            .generics
            .args
            .iter()
            .enumerate()
            .map(|(i, arg)| {
                if i == param_index {
                    quote!(#new_type_var)
                } else {
                    quote!(#arg)
                }
            })
            .collect::<Vec<_>>();

        // Check which named members use this generic parameter
        let mut runtime_asserts = Vec::new();
        let mut type_state_bounds = Vec::new();
        let named_member_conversions = self
            .base
            .named_members()
            .enumerate()
            .map(|(idx, member)| {
                let uses_param = member_uses_generic_param(member, param_ident);
                let index = syn::Index::from(idx);
                if uses_param {
                    // Add compile-time type state constraint
                    let state_mod = &self.base.state_mod.ident;
                    let field_pascal = &member.name.pascal;
                    type_state_bounds.push(quote! {
                        #state_var::#field_pascal: #state_mod::IsUnset
                    });

                    // Add runtime assert that this field is None
                    let field_ident = &member.name.orig;
                    let message = format!(
                        "BUG: field `{field_ident}` should be None \
                        when converting generic parameter `{param_ident}`"
                    );
                    runtime_asserts.push(quote! {
                        ::core::assert!(named.#index.is_none(), #message);
                    });
                    // Field uses the generic parameter, so create a new None
                    quote!(::core::option::Option::None)
                } else {
                    // Field doesn't use the generic parameter, so move it from the tuple
                    quote!(named.#index)
                }
            })
            .collect::<Vec<_>>();

        let receiver_field = self.base.receiver().map(|receiver| {
            let ident = &receiver.field_ident;
            quote!(#ident: self.#ident,)
        });

        let start_fn_fields = self.base.start_fn_args().map(|member| {
            let ident = &member.ident;
            quote!(#ident: self.#ident,)
        });

        let custom_fields = self.base.custom_fields().map(|field| {
            let ident = &field.ident;
            quote!(#ident: self.#ident,)
        });

        // Extend where clause with type state bounds and update type parameter references
        let extended_where_clause = {
            let mut clause = where_clause.clone().unwrap_or_else(|| syn::WhereClause {
                where_token: Where::default(),
                predicates: Punctuated::default(),
            });

            for predicate in &mut clause.predicates {
                replace_type_param_in_predicate(predicate, param_ident, &new_type_var);
            }

            for bound in type_state_bounds {
                clause.predicates.push(syn::parse_quote!(#bound));
            }

            (!clause.predicates.is_empty()).then(|| clause)
        };

        quote! {
            #(#docs)*
            #[inline(always)]
            #vis fn #method_name<#new_type_param>(
                self
            ) -> #builder_ident<#(#output_generic_args,)* #state_var>
            #extended_where_clause
            {
                let named = self.__unsafe_private_named;

                // Runtime safety asserts to ensure fields using the converted
                // generic parameter are None
                #(#runtime_asserts)*

                #builder_ident {
                    __unsafe_private_phantom: ::core::marker::PhantomData,
                    #receiver_field
                    #(#start_fn_fields)*
                    #(#custom_fields)*
                    __unsafe_private_named: (
                        #(#named_member_conversions,)*
                    ),
                }
            }
        }
    }

    fn method_name(&self, param_ident: &syn::Ident) -> syn::Ident {
        let param_name_snake = param_ident.pascal_to_snake_case();

        // Name is guaranteed to be present due to validation in parse_setters_config
        let name_pattern = &self
            .config
            .name
            .as_ref()
            .expect("name should be validated")
            .value;

        let method_name = name_pattern.replace("{}", &param_name_snake.to_string());

        syn::Ident::new(&method_name, param_ident.span())
    }

    fn method_docs(&self, param_ident: &syn::Ident) -> Vec<syn::Attribute> {
        // If custom docs are provided, use them
        if let Some(ref docs) = self.config.docs {
            return docs.value.clone();
        }

        // Otherwise, generate default documentation
        let doc = format!(
            "Convert the `{param_ident}` generic parameter to a different type.\n\
            \n\
            This method allows changing the type of the `{param_ident}` parameter on the builder, \
            which is useful when you need to build up values with different types at \
            different stages of construction."
        );

        vec![syn::parse_quote!(#[doc = #doc])]
    }
}

struct TypeParamFinder<'ty, 'ast> {
    type_params: &'ty [&'ty syn::Ident],

    // Use a `BTreeSet` for deterministic ordering
    found: BTreeSet<&'ast syn::Ident>,
}

impl<'ty> TypeParamFinder<'ty, '_> {
    fn new(type_params: &'ty [&'ty syn::Ident]) -> Self {
        Self {
            type_params,
            found: BTreeSet::new(),
        }
    }
}

impl<'ast> Visit<'ast> for TypeParamFinder<'_, 'ast> {
    fn visit_path(&mut self, path: &'ast syn::Path) {
        // Check if this path is one of our type parameters
        if let Some(param) = path.get_ident() {
            if self.type_params.contains(&param) {
                self.found.insert(param);
            }
        }

        // Continue visiting nested paths
        syn::visit::visit_path(self, path);
    }
}

fn replace_type_param_in_predicate(
    predicate: &mut syn::WherePredicate,
    old_param: &syn::Ident,
    new_param: &syn::Ident,
) {
    use syn::visit_mut::VisitMut;

    struct TypeParamReplacer<'a> {
        old_param: &'a syn::Ident,
        new_param: &'a syn::Ident,
    }

    impl VisitMut for TypeParamReplacer<'_> {
        fn visit_path_mut(&mut self, path: &mut syn::Path) {
            // Replace simple paths like `T`
            if path.is_ident(self.old_param) {
                if let Some(segment) = path.segments.first_mut() {
                    segment.ident = self.new_param.clone();
                }
            }
            // Continue visiting nested paths
            syn::visit_mut::visit_path_mut(self, path);
        }

        fn visit_type_path_mut(&mut self, type_path: &mut syn::TypePath) {
            // Handle qualified paths like T::Assoc
            if let Some(qself) = &mut type_path.qself {
                self.visit_type_mut(&mut qself.ty);
            }
            self.visit_path_mut(&mut type_path.path);
        }
    }

    let mut replacer = TypeParamReplacer {
        old_param,
        new_param,
    };
    replacer.visit_where_predicate_mut(predicate);
}

/// Check if a member's type uses a specific generic parameter
fn member_uses_generic_param(member: &super::NamedMember, param_ident: &syn::Ident) -> bool {
    let member_ty = member.underlying_norm_ty();
    type_uses_generic_param(member_ty, param_ident)
}

/// Recursively check if a type uses a specific generic parameter
fn type_uses_generic_param(ty: &syn::Type, param_ident: &syn::Ident) -> bool {
    struct GenericParamVisitor<'a> {
        param_ident: &'a syn::Ident,
        found: bool,
    }

    impl<'ast> Visit<'ast> for GenericParamVisitor<'_> {
        fn visit_type_path(&mut self, type_path: &'ast syn::TypePath) {
            // Early return if already found to avoid unnecessary recursion
            if self.found {
                return;
            }

            // Check if the path is the generic parameter we're looking for
            if type_path.path.is_ident(self.param_ident) {
                self.found = true;
                return;
            }

            // For qualified paths like T::Assoc or <T as Trait>::Assoc,
            // check if the first segment (or qself) uses the generic parameter

            if let Some(qself) = &type_path.qself {
                // For <T as Trait>::Assoc syntax
                self.visit_type(&qself.ty);
            } else if let Some(segment) = type_path.path.segments.first() {
                // For T::Assoc syntax
                if segment.ident == *self.param_ident {
                    self.found = true;
                    return;
                }
            }

            // Continue visiting the rest of the type path
            syn::visit::visit_type_path(self, type_path);
        }
    }

    let mut visitor = GenericParamVisitor {
        param_ident,
        found: false,
    };
    visitor.visit_type(ty);
    visitor.found
}
