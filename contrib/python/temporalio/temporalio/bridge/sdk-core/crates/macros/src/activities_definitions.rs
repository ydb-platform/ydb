use crate::macro_utils::{
    extract_allow_attrs, generate_marker_struct, method_name_to_pascal_case, type_name_string,
    type_to_snake_case,
};
use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote, quote_spanned};
use syn::{
    Attribute, Block, Expr, FnArg, ImplItem, ItemImpl, ReturnType, Stmt, Type, TypePath,
    parse::ParseStream, spanned::Spanned,
};

#[derive(Copy, Clone, Eq, PartialEq)]
pub(crate) enum ParseMode {
    /// `#[activities]` — a real implementation. Methods must take an `ActivityContext`.
    Activities,
    /// `#[activity_definitions]` — declarations only. Methods must not take an `ActivityContext`,
    /// and their bodies must be `unimplemented!()`.
    Definitions,
}

pub(crate) struct ActivitiesDefinition {
    impl_block: ItemImpl,
    activities: Vec<ActivityMethod>,
    mode: ParseMode,
}

pub(crate) fn parse_activities(input: ParseStream) -> syn::Result<ActivitiesDefinition> {
    ActivitiesDefinition::parse_with_mode(input, ParseMode::Activities)
}

pub(crate) fn parse_definitions(input: ParseStream) -> syn::Result<ActivitiesDefinition> {
    ActivitiesDefinition::parse_with_mode(input, ParseMode::Definitions)
}

#[derive(Default)]
struct ActivityAttributes {
    name_override: Option<syn::Expr>,
    definition_path: Option<syn::Path>,
}

struct ActivityMethod {
    method: syn::ImplItemFn,
    attributes: ActivityAttributes,
    is_async: bool,
    is_static: bool,
    input_types: Vec<Type>,
    output_type: Option<Type>,
}

impl ActivitiesDefinition {
    fn parse_with_mode(input: ParseStream, mode: ParseMode) -> syn::Result<Self> {
        let impl_block: ItemImpl = input.parse()?;
        let mut activities = Vec::new();

        for item in &impl_block.items {
            if let ImplItem::Fn(method) = item {
                let has_activity_attr = method
                    .attrs
                    .iter()
                    .any(|attr| attr.path().is_ident("activity"));

                if has_activity_attr {
                    let activity = parse_activity_method(method, mode)?;
                    activities.push(activity);
                }
            }
        }

        Ok(ActivitiesDefinition {
            impl_block,
            activities,
            mode,
        })
    }
}

fn parse_activity_method(method: &syn::ImplItemFn, mode: ParseMode) -> syn::Result<ActivityMethod> {
    let attributes = extract_activity_attributes(method.attrs.as_slice())?;
    let is_async = method.sig.asyncness.is_some();

    if mode == ParseMode::Definitions
        && let Some(definition_attr) = method
            .attrs
            .iter()
            .find(|a| a.path().is_ident("activity") && a.meta.require_list().is_ok())
        && attributes.definition_path.is_some()
    {
        return Err(syn::Error::new_spanned(
            definition_attr,
            "`definition = ...` is not allowed inside `#[activity_definitions]`; this block \
             *is* the definition",
        ));
    }

    // Determine if static (no self receiver) or instance (Arc<Self>)
    let is_static = match method.sig.inputs.first() {
        Some(FnArg::Receiver(receiver)) => {
            if mode == ParseMode::Definitions {
                return Err(syn::Error::new_spanned(
                    receiver,
                    "Activity definitions must not take self; declare only the input/output \
                     contract",
                ));
            }
            if receiver.colon_token.is_some() {
                validate_arc_self_type(&receiver.ty)?;
                false
            } else {
                return Err(syn::Error::new_spanned(
                    receiver,
                    "Activity methods with instance state must use `self: Arc<Self>` as the \
                     receiver, not `self`, `&self`, or `&mut self`",
                ));
            }
        }
        Some(FnArg::Typed(_)) | None => true,
    };

    if mode == ParseMode::Definitions {
        validate_unimplemented_body(&method.block)?;
    }

    let input_types = extract_input_types(&method.sig, mode)?;
    let output_type = extract_output_type(&method.sig);

    Ok(ActivityMethod {
        method: method.clone(),
        attributes,
        is_async,
        is_static,
        input_types,
        output_type,
    })
}

fn validate_unimplemented_body(block: &Block) -> syn::Result<()> {
    let err = || {
        syn::Error::new_spanned(
            block,
            "Activity definition bodies must be exactly `unimplemented!()`",
        )
    };
    if block.stmts.len() != 1 {
        return Err(err());
    }
    let mac = match &block.stmts[0] {
        Stmt::Macro(s) => &s.mac,
        Stmt::Expr(Expr::Macro(e), _) => &e.mac,
        _ => return Err(err()),
    };
    if !mac.path.is_ident("unimplemented") || !mac.tokens.is_empty() {
        return Err(err());
    }
    Ok(())
}

fn extract_activity_attributes(attrs: &[Attribute]) -> syn::Result<ActivityAttributes> {
    let mut activity_attributes = ActivityAttributes::default();

    for attr in attrs {
        if attr.path().is_ident("activity") && attr.meta.require_list().is_ok() {
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("name") {
                    let value = meta.value()?;
                    let expr: syn::Expr = value.parse()?;
                    activity_attributes.name_override = Some(expr);
                    Ok(())
                } else if meta.path.is_ident("definition") {
                    let value = meta.value()?;
                    let path: syn::Path = value.parse()?;
                    activity_attributes.definition_path = Some(path);
                    Ok(())
                } else {
                    Err(meta.error("unsupported activity attribute"))
                }
            })?;
        }
    }

    Ok(activity_attributes)
}

fn validate_arc_self_type(ty: &Type) -> syn::Result<()> {
    let expected: Type = syn::parse_quote!(Arc<Self>);

    if let (Type::Path(actual_path), Type::Path(expected_path)) = (ty, &expected)
        && let (Some(actual_seg), Some(expected_seg)) = (
            actual_path.path.segments.last(),
            expected_path.path.segments.last(),
        )
        && actual_seg == expected_seg
    {
        return Ok(());
    }

    Err(syn::Error::new_spanned(
        ty,
        "Instance activity methods must use `self: Arc<Self>` as the receiver type",
    ))
}

fn extract_input_types(sig: &syn::Signature, mode: ParseMode) -> syn::Result<Vec<Type>> {
    let mut found_ctx = false;
    let mut types = Vec::new();
    for arg in &sig.inputs {
        if let FnArg::Typed(pat_type) = arg {
            let is_ctx = matches!(&*pat_type.ty, Type::Path(type_path)
                if type_path
                    .path
                    .segments
                    .last()
                    .map(|s| s.ident == "ActivityContext")
                    .unwrap_or(false));
            if is_ctx {
                if mode == ParseMode::Definitions {
                    return Err(syn::Error::new_spanned(
                        pat_type,
                        "`#[activity_definitions]` methods must not take an `ActivityContext`; \
                         declare only the input/output contract",
                    ));
                }
                found_ctx = true;
            } else if found_ctx || mode == ParseMode::Definitions {
                types.push((*pat_type.ty).clone());
            }
        }
    }
    if mode == ParseMode::Activities && !found_ctx {
        return Err(syn::Error::new(
            sig.inputs.span(),
            "Activity functions must have an ActivityContext parameter as either the first \
             parameter, or the second after `self: Arc<Self>`.",
        ));
    }
    if types.len() > 6 {
        return Err(syn::Error::new(
            sig.inputs.span(),
            "Activity functions support at most 6 input parameters (after ActivityContext).",
        ));
    }
    Ok(types)
}

fn extract_output_type(sig: &syn::Signature) -> Option<Type> {
    match &sig.output {
        ReturnType::Type(_, ty) => {
            // Extract T from Result<T, ActivityError>
            if let Type::Path(TypePath { path, .. }) = &**ty
                && let Some(segment) = path.segments.last()
                && segment.ident == "Result"
                && let syn::PathArguments::AngleBracketed(args) = &segment.arguments
                && let Some(syn::GenericArgument::Type(output_ty)) = args.args.first()
            {
                return Some(output_ty.clone());
            }
            // If it's not a Result type, assume it returns the type directly
            Some((**ty).clone())
        }
        ReturnType::Default => None,
    }
}

/// Returns the token stream for the `Input` associated type given the list of input types.
fn multi_args_input_type(types: &[Type]) -> TokenStream2 {
    match types.len() {
        0 => quote! { () },
        1 => {
            let t = &types[0];
            quote! { #t }
        }
        n => {
            let multi_args = format_ident!("MultiArgs{}", n);
            let types = types.iter();
            quote! { ::temporalio_common::data_converters::#multi_args<#(#types),*> }
        }
    }
}

/// Returns the destructuring statement for multi-arg input, or nothing for 0/1 args.
fn multi_args_destructure(types: &[Type]) -> TokenStream2 {
    let n = types.len();
    if n <= 1 {
        return quote! {};
    }
    let multi_args = format_ident!("MultiArgs{}", n);
    let idents = multi_args_idents(n);
    quote! {
        let ::temporalio_common::data_converters::#multi_args(#(#idents),*) = input;
    }
}

/// Returns identifiers `__arg0`, `__arg1`, ... for N arguments.
/// For single-arg, returns `input` to preserve backward compatibility.
fn multi_args_idents(n: usize) -> Vec<syn::Ident> {
    if n == 1 {
        vec![format_ident!("input")]
    } else {
        (0..n).map(|i| format_ident!("__arg{}", i)).collect()
    }
}

impl ActivitiesDefinition {
    pub(crate) fn codegen(&self) -> TokenStream {
        let impl_type = &self.impl_block.self_ty;
        let impl_type_name = type_name_string(impl_type);
        let module_name = type_to_snake_case(impl_type);
        let module_ident = format_ident!("{}", module_name);
        let is_definitions = self.mode == ParseMode::Definitions;

        // Re-emit the impl block with `#[activity]` attrs stripped and methods renamed with a `__`
        // prefix so the marker-struct const can keep the original name. Re-emitting also ensures
        // any types referenced in the user's signatures (e.g. `ActivityError`) keep their imports
        // live; in Definitions mode the bodies are `unimplemented!()` and never called, so the
        // rewritten methods are tagged `#[allow(dead_code)]`.
        let cleaned_impl = {
            let mut cleaned = self.impl_block.clone();
            for item in &mut cleaned.items {
                if let ImplItem::Fn(method) = item {
                    let is_activity = method
                        .attrs
                        .iter()
                        .any(|attr| attr.path().is_ident("activity"));

                    method
                        .attrs
                        .retain(|attr| !attr.path().is_ident("activity"));

                    if is_activity {
                        let new_name = format_ident!("__{}", method.sig.ident);
                        method.sig.ident = new_name;
                        if is_definitions {
                            method
                                .attrs
                                .push(syn::parse_quote!(#[allow(dead_code, unused_variables)]));
                        }
                    }
                }
            }
            quote! { #cleaned }
        };

        // Generate marker structs (inside module, no external references)
        let activity_structs: Vec<_> = self
            .activities
            .iter()
            .map(|act| generate_marker_struct(&act.method))
            .collect();

        // Generate consts in impl block pointing to marker structs
        let activity_consts: Vec<_> = self
            .activities
            .iter()
            .map(|act| {
                let visibility = &act.method.vis;
                let method_ident = &act.method.sig.ident;
                let struct_name = method_name_to_pascal_case(&act.method.sig.ident);
                let struct_ident = format_ident!("{}", struct_name);
                let span = act.method.span();
                let allow_attrs = extract_allow_attrs(&act.method.attrs);
                quote_spanned! { span=>
                    #[allow(non_upper_case_globals)]
                    #(#allow_attrs)*
                    #visibility const #method_ident: #module_ident::#struct_ident = #module_ident::#struct_ident;
                }
            })
            .collect();

        // Run methods and `ExecutableActivity`/`ActivityImplementer`/`HasOnlyStaticMethods`
        // impls only make sense for real activities; definitions skip them entirely.
        let run_impls: Vec<_> = if is_definitions {
            Vec::new()
        } else {
            self.activities
                .iter()
                .map(|act| self.generate_run_impl(act, impl_type, &module_ident))
                .collect()
        };

        let activity_impls: Vec<_> = self
            .activities
            .iter()
            .map(|act| {
                self.generate_activity_definition_impl(
                    act,
                    impl_type,
                    &impl_type_name,
                    &module_ident,
                )
            })
            .collect();

        let implementer_impl = if is_definitions {
            quote! {}
        } else {
            self.generate_activity_implementer_impl(impl_type, &module_ident)
        };

        let has_only_static = if !is_definitions && self.activities.iter().all(|a| a.is_static) {
            quote! {
                impl ::temporalio_sdk::activities::HasOnlyStaticMethods for #impl_type {}
            }
        } else {
            quote! {}
        };

        // Generate impl block with consts
        let const_impl = quote! {
            impl #impl_type {
                #(#activity_consts)*
            }
        };

        let output = quote! {
            #cleaned_impl

            #const_impl

            // Module contains only the marker structs (no use super::*)
            mod #module_ident {
                #(#activity_structs)*
            }

            // Run methods, trait impls are outside the module
            #(#run_impls)*

            #(#activity_impls)*

            #implementer_impl

            #has_only_static
        };

        output.into()
    }

    fn generate_run_impl(
        &self,
        activity: &ActivityMethod,
        impl_type: &Type,
        module_ident: &syn::Ident,
    ) -> TokenStream2 {
        let struct_name = method_name_to_pascal_case(&activity.method.sig.ident);
        let struct_ident = format_ident!("{}", struct_name);
        let prefixed_method = format_ident!("__{}", activity.method.sig.ident);

        let input_type = multi_args_input_type(&activity.input_types);
        let output_type = activity
            .output_type
            .as_ref()
            .map(|t| quote! { #t })
            .unwrap_or(quote! { () });

        let has_input = !activity.input_types.is_empty();

        // Build the parameters and call based on static vs instance and input
        let (params, method_call) = if activity.is_static {
            let params = if has_input {
                quote! { self, ctx: ::temporalio_sdk::activities::ActivityContext, input: #input_type }
            } else {
                quote! { self, ctx: ::temporalio_sdk::activities::ActivityContext }
            };
            let call = if has_input {
                let destructure = multi_args_destructure(&activity.input_types);
                let arg_idents = multi_args_idents(activity.input_types.len());
                quote! {
                    #destructure
                    #impl_type::#prefixed_method(ctx, #(#arg_idents),*)
                }
            } else {
                quote! { #impl_type::#prefixed_method(ctx) }
            };
            (params, call)
        } else {
            let params = if has_input {
                quote! { self, instance: ::std::sync::Arc<#impl_type>, ctx: ::temporalio_sdk::activities::ActivityContext, input: #input_type }
            } else {
                quote! { self, instance: ::std::sync::Arc<#impl_type>, ctx: ::temporalio_sdk::activities::ActivityContext }
            };
            let call = if has_input {
                let destructure = multi_args_destructure(&activity.input_types);
                let arg_idents = multi_args_idents(activity.input_types.len());
                quote! {
                    #destructure
                    #impl_type::#prefixed_method(instance, ctx, #(#arg_idents),*)
                }
            } else {
                quote! { #impl_type::#prefixed_method(instance, ctx) }
            };
            (params, call)
        };

        let return_type =
            quote! { Result<#output_type, ::temporalio_sdk::activities::ActivityError> };

        // If the method returns void (no return type), wrap with Ok(())
        let result_wrapper = if activity.output_type.is_none() {
            quote! { ; Ok(()) }
        } else {
            quote! {}
        };

        // Common methods for all marker structs
        let common_methods = quote! {
            /// Returns the activity name (delegates to ActivityDefinition::name())
            pub fn name(&self) -> &str {
                <Self as ::temporalio_common::ActivityDefinition>::name(self)
            }
        };

        if activity.is_async {
            quote! {
                impl #module_ident::#struct_ident {
                    #common_methods

                    pub async fn run(#params) -> #return_type {
                        #method_call.await #result_wrapper
                    }
                }
            }
        } else {
            quote! {
                impl #module_ident::#struct_ident {
                    #common_methods

                    pub fn run(#params) -> #return_type {
                        #method_call #result_wrapper
                    }
                }
            }
        }
    }

    fn generate_activity_definition_impl(
        &self,
        activity: &ActivityMethod,
        impl_type: &Type,
        impl_type_name: &str,
        module_ident: &syn::Ident,
    ) -> TokenStream2 {
        let struct_name = method_name_to_pascal_case(&activity.method.sig.ident);
        let struct_ident = format_ident!("{}", struct_name);

        let input_type = multi_args_input_type(&activity.input_types);
        let output_type = &activity
            .output_type
            .as_ref()
            .map(|t| quote! { #t })
            .unwrap_or(quote! { () });

        let activity_name = if let Some(ref definition_path) = activity.attributes.definition_path {
            quote! {
                <#definition_path as ::temporalio_common::ActivityDefinition>::name(&#definition_path)
            }
        } else if let Some(ref name_expr) = activity.attributes.name_override {
            quote! { #name_expr }
        } else {
            let default_name = format!("{}::{}", impl_type_name, activity.method.sig.ident);
            quote! { #default_name }
        };
        let definition_assertions = activity
            .attributes
            .definition_path
            .as_ref()
            .map(|definition_path| {
                quote! {
                    const _: () = {
                        trait ActivityImplMustMatchDefinition<T> {}
                        impl<T> ActivityImplMustMatchDefinition<T> for T {}
                        fn assert_input_matches_definition<Impl, Def>()
                        where
                            Impl: ActivityImplMustMatchDefinition<Def>,
                        {}
                        fn assert_output_matches_definition<Impl, Def>()
                        where
                            Impl: ActivityImplMustMatchDefinition<Def>,
                        {}
                        let _ = assert_input_matches_definition::<
                            <#module_ident::#struct_ident as ::temporalio_common::ActivityDefinition>::Input,
                            <#definition_path as ::temporalio_common::ActivityDefinition>::Input,
                        >;
                        let _ = assert_output_matches_definition::<
                            <#module_ident::#struct_ident as ::temporalio_common::ActivityDefinition>::Output,
                            <#definition_path as ::temporalio_common::ActivityDefinition>::Output,
                        >;
                    };
                }
            })
            .unwrap_or_default();

        let activity_definition_impl = quote! {
            impl ::temporalio_common::ActivityDefinition for #module_ident::#struct_ident {
                type Input = #input_type;
                type Output = #output_type;

                fn name(&self) -> &str {
                    #activity_name
                }
            }
        };

        if self.mode == ParseMode::Definitions {
            return quote! {
                #activity_definition_impl
                #definition_assertions
            };
        }

        let prefixed_method = format_ident!("__{}", activity.method.sig.ident);
        let has_input = !activity.input_types.is_empty();

        let receiver_pattern = if activity.is_static {
            quote! { _receiver }
        } else {
            quote! { receiver }
        };

        let method_call = if has_input {
            let destructure = multi_args_destructure(&activity.input_types);
            let arg_idents = multi_args_idents(activity.input_types.len());
            if activity.is_static {
                quote! {
                    #destructure
                    #impl_type::#prefixed_method(ctx, #(#arg_idents),*)
                }
            } else {
                quote! {
                    #destructure
                    #impl_type::#prefixed_method(receiver.unwrap(), ctx, #(#arg_idents),*)
                }
            }
        } else if activity.is_static {
            quote! { #impl_type::#prefixed_method(ctx) }
        } else {
            quote! { #impl_type::#prefixed_method(receiver.unwrap(), ctx) }
        };

        let input_param = if has_input {
            quote! { input: Self::Input, }
        } else {
            quote! { _input: Self::Input, }
        };

        let result_returner = if activity.output_type.is_none() {
            quote! {; Ok(()) }
        } else {
            quote! {}
        };
        let execute_body = if activity.is_async {
            quote! {
                async move { #method_call.await #result_returner }.boxed()
            }
        } else {
            quote! {
                tokio::task::spawn_blocking(move || { #method_call #result_returner })
                    .map(|jh| match jh {
                        Err(err) => Err(::temporalio_sdk::activities::ActivityError::from(err)),
                        Ok(v) => v,
                    })
                    .boxed()
            }
        };

        quote! {
            #activity_definition_impl

            impl ::temporalio_sdk::activities::ExecutableActivity for #module_ident::#struct_ident {
                type Implementer = #impl_type;

                fn definition() -> Self {
                    #module_ident::#struct_ident
                }

                fn execute(
                    #receiver_pattern: Option<::std::sync::Arc<Self::Implementer>>,
                    ctx: ::temporalio_sdk::activities::ActivityContext,
                    #input_param
                ) -> ::futures::future::BoxFuture<'static,
                    Result<Self::Output, ::temporalio_sdk::activities::ActivityError>>
                {
                    use ::futures::FutureExt;

                    #execute_body
                }
            }

            #definition_assertions
        }
    }

    fn generate_activity_implementer_impl(
        &self,
        impl_type: &Type,
        module_ident: &syn::Ident,
    ) -> TokenStream2 {
        let instance_activities: Vec<_> = self
            .activities
            .iter()
            .map(|a| {
                let struct_name = method_name_to_pascal_case(&a.method.sig.ident);
                let struct_ident = format_ident!("{}", struct_name);
                quote! {
                    defs.register_activity::<#module_ident::#struct_ident>(self.clone());
                }
            })
            .collect();

        let register_instance_body = if instance_activities.is_empty() {
            quote! {}
        } else {
            quote! { #(#instance_activities)* }
        };

        quote! {
            impl ::temporalio_sdk::activities::ActivityImplementer for #impl_type {
                fn register_all(
                    self: ::std::sync::Arc<Self>,
                    defs: &mut ::temporalio_sdk::activities::ActivityDefinitions,
                ) {
                    #register_instance_body
                }
            }
        }
    }
}
