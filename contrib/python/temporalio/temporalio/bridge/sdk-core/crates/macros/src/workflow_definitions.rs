use crate::macro_utils::{
    generate_const_definition, generate_marker_struct, method_name_to_pascal_case,
    type_name_string, type_to_snake_case,
};
use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote};
use syn::{
    Attribute, FnArg, ImplItem, ItemImpl, ReturnType, Type, TypePath,
    parse::{Parse, ParseStream},
};

/// Convert an optional type to a quoted token stream, defaulting to `()` if None.
fn type_to_tokens(ty: Option<&Type>) -> TokenStream2 {
    ty.map(|t| quote! { #t }).unwrap_or(quote! { () })
}

/// Common data extracted from handler methods (signal, query, update)
struct HandlerCodegenInfo {
    struct_ident: syn::Ident,
    prefixed_method: syn::Ident,
    input_type_tokens: TokenStream2,
    handler_name: TokenStream2,
    marker_struct: TokenStream2,
    const_def: TokenStream2,
}

impl HandlerCodegenInfo {
    fn new(
        method: &syn::ImplItemFn,
        attributes: &MethodAttributes,
        input_type: Option<&Type>,
        module_ident: &syn::Ident,
    ) -> Self {
        let struct_name = method_name_to_pascal_case(&method.sig.ident);
        let struct_ident = format_ident!("{}", struct_name);
        let prefixed_method = format_ident!("__{}", method.sig.ident);
        let input_type_tokens = type_to_tokens(input_type);
        let handler_name = {
            let method_ident: &syn::Ident = &method.sig.ident;
            if let Some(ref name_expr) = attributes.name_override {
                quote! { #name_expr }
            } else {
                let name = method_ident.to_string();
                quote! { #name }
            }
        };
        let marker_struct = generate_marker_struct(method);
        let const_def = generate_const_definition(method, module_ident);

        Self {
            struct_ident,
            prefixed_method,
            input_type_tokens,
            handler_name,
            marker_struct,
            const_def,
        }
    }
}

/// Generate a method call with or without input parameter (for sync methods with &mut self)
fn generate_method_call(prefixed_method: &syn::Ident, has_input: bool) -> TokenStream2 {
    if has_input {
        quote! { self.#prefixed_method(ctx, input) }
    } else {
        quote! { self.#prefixed_method(ctx) }
    }
}

fn generate_decode_arm(
    handler_name: &TokenStream2,
    input_type: &TokenStream2,
    payloads: TokenStream2,
) -> TokenStream2 {
    quote! {
        #handler_name => {
            let ctx = ::temporalio_workflow::common::data_converters::SerializationContext {
                data: &::temporalio_workflow::common::data_converters::SerializationContextData::Workflow,
                converter,
            };
            let input: #input_type = <::temporalio_workflow::common::data_converters::PayloadConverter as ::temporalio_workflow::common::data_converters::GenericPayloadConverter>::from_payloads(
                converter,
                &ctx,
                #payloads,
            )?;
            Ok(::std::option::Option::Some(
                ::std::boxed::Box::new(input) as ::std::boxed::Box<dyn ::std::any::Any>
            ))
        },
    }
}

/// Parsed representation of a `#[workflow_methods]` impl block
pub(crate) struct WorkflowMethodsDefinition {
    impl_block: ItemImpl,
    init_method: Option<InitMethod>,
    run_method: RunMethod,
    signals: Vec<SignalMethod>,
    queries: Vec<QueryMethod>,
    updates: Vec<UpdateMethod>,
}

#[derive(Default)]
struct MethodAttributes {
    name_override: Option<syn::Expr>,
}

struct InitMethod {
    method: syn::ImplItemFn,
    input_type: Option<Type>,
}

struct RunMethod {
    method: syn::ImplItemFn,
    attributes: MethodAttributes,
    input_type: Option<Type>,
    output_type: Option<Type>,
}

struct SignalMethod {
    method: syn::ImplItemFn,
    attributes: MethodAttributes,
    is_async: bool,
    input_type: Option<Type>,
}

struct QueryMethod {
    method: syn::ImplItemFn,
    attributes: MethodAttributes,
    input_type: Option<Type>,
    output_type: Option<Type>,
    is_fallible: bool,
}

struct UpdateMethod {
    method: syn::ImplItemFn,
    attributes: MethodAttributes,
    is_async: bool,
    input_type: Option<Type>,
    /// The Ok type if fallible, or the direct return type if infallible
    output_type: Option<Type>,
    /// True if the handler returns Result<T, E>, false if it returns T directly
    is_fallible: bool,
    validator: Option<syn::Ident>,
}

struct ValidatorMethod {
    method: syn::ImplItemFn,
    update_name: syn::Ident,
}

impl Parse for WorkflowMethodsDefinition {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let impl_block: ItemImpl = input.parse()?;
        let mut init_method = None;
        let mut run_method = None;
        let mut signals = Vec::new();
        let mut queries = Vec::new();
        let mut updates = Vec::new();
        let mut validators = Vec::new();

        for item in &impl_block.items {
            if let ImplItem::Fn(method) = item {
                let has_init = has_attr(&method.attrs, "init");
                let has_run = has_attr(&method.attrs, "run");
                let has_signal = has_attr(&method.attrs, "signal");
                let has_query = has_attr(&method.attrs, "query");
                let has_update = has_attr(&method.attrs, "update");
                let has_update_validator = has_attr(&method.attrs, "update_validator");

                // Count how many workflow attributes are present
                let attr_count = [
                    has_init,
                    has_run,
                    has_signal,
                    has_query,
                    has_update,
                    has_update_validator,
                ]
                .iter()
                .filter(|&&b| b)
                .count();

                if attr_count > 1 {
                    return Err(syn::Error::new_spanned(
                        method,
                        "A method can only have one of #[init], #[run], #[signal], #[query], #[update], or #[update_validator]",
                    ));
                }

                if has_init {
                    if init_method.is_some() {
                        return Err(syn::Error::new_spanned(
                            method,
                            "Only one #[init] method is allowed per workflow",
                        ));
                    }
                    init_method = Some(parse_init_method(method)?);
                } else if has_run {
                    if run_method.is_some() {
                        return Err(syn::Error::new_spanned(
                            method,
                            "Only one #[run] method is allowed per workflow",
                        ));
                    }
                    run_method = Some(parse_run_method(method)?);
                } else if has_signal {
                    signals.push(parse_signal_method(method)?);
                } else if has_query {
                    queries.push(parse_query_method(method)?);
                } else if has_update {
                    updates.push(parse_update_method(method)?);
                } else if has_update_validator {
                    validators.push(parse_validator_method(method)?);
                }
            }
        }

        let run_method = run_method.ok_or_else(|| {
            syn::Error::new_spanned(
                &impl_block,
                "#[workflow_methods] requires exactly one #[run] method",
            )
        })?;

        // Validate that all validator targets exist and link them to updates
        for validator in &validators {
            let target_exists = updates
                .iter()
                .any(|u| u.method.sig.ident == validator.update_name);
            if !target_exists {
                return Err(syn::Error::new_spanned(
                    &validator.method,
                    format!(
                        "No #[update] method '{}' found for validator",
                        validator.update_name
                    ),
                ));
            }
        }

        // Link validators to their updates
        for update in &mut updates {
            if let Some(validator) = validators
                .iter()
                .find(|v| v.update_name == update.method.sig.ident)
            {
                update.validator = Some(validator.method.sig.ident.clone());
            }
        }

        Ok(WorkflowMethodsDefinition {
            impl_block,
            init_method,
            run_method,
            signals,
            queries,
            updates,
        })
    }
}

fn has_attr(attrs: &[Attribute], name: &str) -> bool {
    attrs.iter().any(|attr| attr.path().is_ident(name))
}

fn extract_method_attributes(
    attrs: &[Attribute],
    attr_name: &str,
) -> syn::Result<MethodAttributes> {
    let mut method_attrs = MethodAttributes::default();

    for attr in attrs {
        if attr.path().is_ident(attr_name) && attr.meta.require_list().is_ok() {
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("name") {
                    let value = meta.value()?;
                    let expr: syn::Expr = value.parse()?;
                    method_attrs.name_override = Some(expr);
                    Ok(())
                } else {
                    Err(meta.error(format!("unsupported {attr_name} attribute")))
                }
            })?;
        }
    }

    Ok(method_attrs)
}

fn parse_init_method(method: &syn::ImplItemFn) -> syn::Result<InitMethod> {
    // Validate: must not be async
    if method.sig.asyncness.is_some() {
        return Err(syn::Error::new_spanned(
            &method.sig,
            "#[init] methods must not be async",
        ));
    }

    // Validate: must return Self
    let error_msg = "#[init] methods must return Self";
    match &method.sig.output {
        ReturnType::Type(_, ty) => {
            if let Type::Path(TypePath { path, .. }) = &**ty {
                if !path.is_ident("Self") {
                    return Err(syn::Error::new_spanned(ty, error_msg));
                }
            } else {
                return Err(syn::Error::new_spanned(ty, error_msg));
            }
        }
        ReturnType::Default => {
            return Err(syn::Error::new_spanned(&method.sig, error_msg));
        }
    }
    let input_type = extract_input_type(&method.sig)?;

    Ok(InitMethod {
        method: method.clone(),
        input_type,
    })
}

fn parse_run_method(method: &syn::ImplItemFn) -> syn::Result<RunMethod> {
    let attributes = extract_method_attributes(&method.attrs, "run")?;

    if method.sig.asyncness.is_none() {
        return Err(syn::Error::new_spanned(
            &method.sig,
            "#[run] methods must be async",
        ));
    }

    // Async run methods must not have self to prevent unsound references across awaits
    validate_no_self_receiver_for_async(method, "run")?;

    let input_type = extract_input_type(&method.sig)?;
    let output_type = extract_workflow_result_type(&method.sig)?;

    Ok(RunMethod {
        method: method.clone(),
        attributes,
        input_type,
        output_type,
    })
}

fn parse_signal_method(method: &syn::ImplItemFn) -> syn::Result<SignalMethod> {
    let attributes = extract_method_attributes(&method.attrs, "signal")?;
    let is_async = method.sig.asyncness.is_some();

    // Async signals must not have self, sync signals use &mut self
    if is_async {
        validate_no_self_receiver_for_async(method, "signal")?;
    } else {
        validate_mut_self_receiver(method)?;
    }

    let input_type = extract_input_type(&method.sig)?;

    Ok(SignalMethod {
        method: method.clone(),
        attributes,
        is_async,
        input_type,
    })
}

fn parse_query_method(method: &syn::ImplItemFn) -> syn::Result<QueryMethod> {
    let attributes = extract_method_attributes(&method.attrs, "query")?;

    if method.sig.asyncness.is_some() {
        return Err(syn::Error::new_spanned(
            &method.sig,
            "#[query] methods must not be async (queries are synchronous)",
        ));
    }

    validate_immut_self_receiver(method)?;

    let input_type = extract_input_type(&method.sig)?;
    let (output_type, is_fallible) = extract_maybe_result_output_type(&method.sig);

    Ok(QueryMethod {
        method: method.clone(),
        attributes,
        input_type,
        output_type,
        is_fallible,
    })
}

fn parse_update_method(method: &syn::ImplItemFn) -> syn::Result<UpdateMethod> {
    let attributes = extract_method_attributes(&method.attrs, "update")?;
    let is_async = method.sig.asyncness.is_some();

    // Async updates must not have self, sync updates use &mut self
    if is_async {
        validate_no_self_receiver_for_async(method, "update")?;
    } else {
        validate_mut_self_receiver(method)?;
    }

    let input_type = extract_input_type(&method.sig)?;
    let (output_type, is_fallible) = extract_maybe_result_output_type(&method.sig);

    Ok(UpdateMethod {
        method: method.clone(),
        attributes,
        is_async,
        input_type,
        output_type,
        is_fallible,
        validator: None,
    })
}

fn parse_validator_method(method: &syn::ImplItemFn) -> syn::Result<ValidatorMethod> {
    if method.sig.asyncness.is_some() {
        return Err(syn::Error::new_spanned(
            &method.sig,
            "#[update_validator] methods must not be async",
        ));
    }

    validate_immut_self_receiver(method)?;

    let update_name = extract_validator_target(&method.attrs)?;

    Ok(ValidatorMethod {
        method: method.clone(),
        update_name,
    })
}

fn extract_validator_target(attrs: &[Attribute]) -> syn::Result<syn::Ident> {
    for attr in attrs {
        if attr.path().is_ident("update_validator") {
            return attr.parse_args();
        }
    }
    unreachable!("extract_validator_target called without update_validator attribute")
}

fn validate_mut_self_receiver(method: &syn::ImplItemFn) -> syn::Result<()> {
    match method.sig.inputs.first() {
        Some(FnArg::Receiver(receiver)) => {
            if receiver.reference.is_none() || receiver.mutability.is_none() {
                return Err(syn::Error::new_spanned(
                    receiver,
                    "This method must use `&mut self` as the receiver",
                ));
            }
            Ok(())
        }
        _ => Err(syn::Error::new_spanned(
            &method.sig,
            "This method must have `&mut self` as the first parameter",
        )),
    }
}

fn validate_immut_self_receiver(method: &syn::ImplItemFn) -> syn::Result<()> {
    match method.sig.inputs.first() {
        Some(FnArg::Receiver(receiver)) => {
            if receiver.reference.is_none() {
                return Err(syn::Error::new_spanned(
                    receiver,
                    "#[query] methods must use `&self` as the receiver (not `self`)",
                ));
            }
            if receiver.mutability.is_some() {
                return Err(syn::Error::new_spanned(
                    receiver,
                    "#[query] methods must use `&self` as the receiver (not `&mut self`) - queries are read-only",
                ));
            }
            Ok(())
        }
        _ => Err(syn::Error::new_spanned(
            &method.sig,
            "#[query] methods must have `&self` as the first parameter",
        )),
    }
}

fn validate_no_self_receiver_for_async(
    method: &syn::ImplItemFn,
    attr_name: &str,
) -> syn::Result<()> {
    // Async workflow methods must NOT have a self receiver to prevent unsound references
    // across await points (signals/updates can mutate state during awaits).
    if let Some(FnArg::Receiver(receiver)) = method.sig.inputs.first() {
        return Err(syn::Error::new_spanned(
            receiver,
            format!(
                "Async #[{attr_name}] methods must not have a self parameter. \
                 Use WorkflowContext to access state safely."
            ),
        ));
    }
    Ok(())
}

fn extract_input_type(sig: &syn::Signature) -> syn::Result<Option<Type>> {
    // Skip self (if present), then find parameter after context
    let mut found_ctx = false;
    for arg in &sig.inputs {
        match arg {
            FnArg::Receiver(_) => continue,
            FnArg::Typed(pat_type) => {
                if found_ctx {
                    return Ok(Some((*pat_type.ty).clone()));
                }
                if is_wf_context_type(&pat_type.ty) {
                    found_ctx = true;
                }
            }
        }
    }
    Ok(None)
}

fn is_wf_context_type(ty: &Type) -> bool {
    // Check for WorkflowContext, SyncWorkflowContext, or WorkflowContextView
    let inner_type = match ty {
        Type::Reference(r) => &*r.elem,
        other => other,
    };

    if let Type::Path(TypePath { path, .. }) = inner_type
        && let Some(segment) = path.segments.last()
    {
        return segment.ident == "WorkflowContext"
            || segment.ident == "SyncWorkflowContext"
            || segment.ident == "WorkflowContextView";
    }
    false
}

fn extract_workflow_result_type(sig: &syn::Signature) -> syn::Result<Option<Type>> {
    match &sig.output {
        ReturnType::Type(_, ty) => {
            if let Type::Path(TypePath { path, .. }) = &**ty
                && let Some(segment) = path.segments.last()
                && segment.ident == "WorkflowResult"
                && let syn::PathArguments::AngleBracketed(args) = &segment.arguments
                && let Some(syn::GenericArgument::Type(output_ty)) = args.args.first()
            {
                return Ok(Some(output_ty.clone()));
            }
            Ok(None)
        }
        ReturnType::Default => Ok(None),
    }
}

/// Check if a return type looks like a Result and extract the Ok type if so.
/// Returns (ok_type, is_result) where:
/// - If the type looks like `Result<T, E>`, returns (T, true)
/// - Otherwise returns (original_type, false)
fn extract_maybe_result_output_type(sig: &syn::Signature) -> (Option<Type>, bool) {
    match &sig.output {
        ReturnType::Default => (None, false),
        ReturnType::Type(_, ty) => {
            if let Type::Path(TypePath { path, .. }) = &**ty
                && let Some(segment) = path.segments.last()
                && segment.ident == "Result"
                && let syn::PathArguments::AngleBracketed(args) = &segment.arguments
                && let Some(syn::GenericArgument::Type(ok_ty)) = args.args.first()
            {
                (Some(ok_ty.clone()), true)
            } else {
                (Some((**ty).clone()), false)
            }
        }
    }
}

impl WorkflowMethodsDefinition {
    fn run_struct_ident(&self) -> syn::Ident {
        let struct_name = method_name_to_pascal_case(&self.run_method.method.sig.ident);
        format_ident!("{}", struct_name)
    }

    pub(crate) fn codegen_with_options(&self, factory_only: bool) -> TokenStream {
        let impl_type = &self.impl_block.self_ty;
        let impl_type_name = type_name_string(impl_type);
        let module_name = type_to_snake_case(impl_type);
        let module_ident = format_ident!("{}", module_name);

        // Generate the cleaned impl block with:
        // - workflow attributes stripped
        // - Annotated methods renamed with __ prefix
        let mut cleaned_impl = self.impl_block.clone();
        for item in &mut cleaned_impl.items {
            if let ImplItem::Fn(method) = item {
                let is_workflow_method = has_attr(&method.attrs, "init")
                    || has_attr(&method.attrs, "run")
                    || has_attr(&method.attrs, "signal")
                    || has_attr(&method.attrs, "query")
                    || has_attr(&method.attrs, "update")
                    || has_attr(&method.attrs, "update_validator");

                // Strip workflow-related attributes
                method.attrs.retain(|attr| {
                    !attr.path().is_ident("init")
                        && !attr.path().is_ident("run")
                        && !attr.path().is_ident("signal")
                        && !attr.path().is_ident("query")
                        && !attr.path().is_ident("update")
                        && !attr.path().is_ident("update_validator")
                });

                // Rename workflow methods with __ prefix
                if is_workflow_method {
                    let new_name = format_ident!("__{}", method.sig.ident);
                    method.sig.ident = new_name;
                }
            }
        }

        let mut marker_structs = Vec::new();
        let mut const_definitions = Vec::new();
        let mut trait_impls = Vec::new();

        // Run method marker struct and impls
        let run_method = &self.run_method;
        let struct_ident = self.run_struct_ident();

        let input_type = type_to_tokens(
            run_method.input_type.as_ref().or(self
                .init_method
                .as_ref()
                .and_then(|m| m.input_type.as_ref())),
        );

        let output_type = type_to_tokens(run_method.output_type.as_ref());

        let workflow_name = if let Some(ref name_expr) = run_method.attributes.name_override {
            quote! { #name_expr }
        } else {
            quote! { #impl_type_name }
        };

        marker_structs.push(generate_marker_struct(&run_method.method));
        const_definitions.push(generate_const_definition(&run_method.method, &module_ident));

        trait_impls.push(quote! {
            impl ::temporalio_workflow::common::WorkflowDefinition for #module_ident::#struct_ident {
                type Input = #input_type;
                type Output = #output_type;

                fn name(&self) -> &str {
                    #workflow_name
                }
            }

            impl ::temporalio_workflow::common::HasWorkflowDefinition for #module_ident::#struct_ident {
                type Run = Self;
            }

            impl ::temporalio_workflow::common::WorkflowDefinition for #impl_type {
                type Input = #input_type;
                type Output = #output_type;

                fn name(&self) -> &str {
                    #workflow_name
                }
            }

            impl ::temporalio_workflow::common::HasWorkflowDefinition for #impl_type {
                type Run = #module_ident::#struct_ident;
            }
        });

        for signal in &self.signals {
            let (structs, consts, impls) =
                self.generate_signal_code(signal, impl_type, &module_ident);
            marker_structs.push(structs);
            const_definitions.push(consts);
            trait_impls.push(impls);
        }

        for query in &self.queries {
            let (structs, consts, impls) =
                self.generate_query_code(query, impl_type, &module_ident);
            marker_structs.push(structs);
            const_definitions.push(consts);
            trait_impls.push(impls);
        }

        for update in &self.updates {
            let (structs, consts, impls) =
                self.generate_update_code(update, impl_type, &module_ident);
            marker_structs.push(structs);
            const_definitions.push(consts);
            trait_impls.push(impls);
        }

        let workflow_impl =
            self.generate_workflow_implementation(impl_type, &module_ident, factory_only);

        let const_impl = quote! {
            impl #impl_type {
                /// Returns the workflow type name
                pub const fn name() -> &'static str {
                    #workflow_name
                }

                #(#const_definitions)*
            }
        };

        let output = quote! {
            #cleaned_impl

            #const_impl

            mod #module_ident {
                #(#marker_structs)*
            }

            #(#trait_impls)*

            #workflow_impl
        };

        output.into()
    }

    fn generate_signal_code(
        &self,
        signal: &SignalMethod,
        impl_type: &Type,
        module_ident: &syn::Ident,
    ) -> (TokenStream2, TokenStream2, TokenStream2) {
        let info = HandlerCodegenInfo::new(
            &signal.method,
            &signal.attributes,
            signal.input_type.as_ref(),
            module_ident,
        );
        let struct_ident = &info.struct_ident;
        let input_type = &info.input_type_tokens;
        let signal_name = &info.handler_name;

        let run_struct_ident = self.run_struct_ident();
        let definition_impl = quote! {
            impl ::temporalio_workflow::common::SignalDefinition for #module_ident::#struct_ident {
                type Workflow = #module_ident::#run_struct_ident;
                type Input = #input_type;

                fn name(&self) -> &str {
                    #signal_name
                }
            }
        };

        let has_input = signal.input_type.is_some();
        let executable_impl = if signal.is_async {
            let prefixed_method = &info.prefixed_method;
            let method_call = if has_input {
                quote! { #impl_type::#prefixed_method(&mut ctx, input) }
            } else {
                quote! { #impl_type::#prefixed_method(&mut ctx) }
            };
            quote! {
                impl ::temporalio_workflow::workflows::ExecutableAsyncSignal<#module_ident::#struct_ident> for #impl_type {
                    fn handle(
                        mut ctx: ::temporalio_workflow::WorkflowContext<Self>,
                        input: <#module_ident::#struct_ident as ::temporalio_workflow::common::SignalDefinition>::Input,
                    ) -> ::temporalio_workflow::__private::futures_util::future::LocalBoxFuture<'static, ()> {
                        ::temporalio_workflow::__private::futures_util::FutureExt::boxed_local(
                            async move { #method_call.await }
                        )
                    }
                }
            }
        } else {
            let method_call = generate_method_call(&info.prefixed_method, has_input);
            quote! {
                impl ::temporalio_workflow::workflows::ExecutableSyncSignal<#module_ident::#struct_ident> for #impl_type {
                    fn handle(
                        &mut self,
                        ctx: &mut ::temporalio_workflow::SyncWorkflowContext<Self>,
                        input: <#module_ident::#struct_ident as ::temporalio_workflow::common::SignalDefinition>::Input,
                    ) {
                        #method_call
                    }
                }
            }
        };

        let trait_impl = quote! {
            #definition_impl
            #executable_impl
        };

        (info.marker_struct, info.const_def, trait_impl)
    }

    fn generate_query_code(
        &self,
        query: &QueryMethod,
        impl_type: &Type,
        module_ident: &syn::Ident,
    ) -> (TokenStream2, TokenStream2, TokenStream2) {
        let info = HandlerCodegenInfo::new(
            &query.method,
            &query.attributes,
            query.input_type.as_ref(),
            module_ident,
        );
        let struct_ident = &info.struct_ident;
        let prefixed_method = &info.prefixed_method;
        let input_type = &info.input_type_tokens;
        let output_type = type_to_tokens(query.output_type.as_ref());
        let query_name = &info.handler_name;

        let method_call = generate_method_call(prefixed_method, query.input_type.is_some());

        let body = if query.is_fallible {
            quote! { #method_call }
        } else {
            quote! { Ok(#method_call) }
        };

        let run_struct_ident = self.run_struct_ident();
        let trait_impl = quote! {
            impl ::temporalio_workflow::common::QueryDefinition for #module_ident::#struct_ident {
                type Workflow = #module_ident::#run_struct_ident;
                type Input = #input_type;
                type Output = #output_type;

                fn name(&self) -> &str {
                    #query_name
                }
            }

            impl ::temporalio_workflow::workflows::ExecutableQuery<#module_ident::#struct_ident> for #impl_type {
                fn handle(
                    &self,
                    ctx: &::temporalio_workflow::WorkflowContextView,
                    input: <#module_ident::#struct_ident as ::temporalio_workflow::common::QueryDefinition>::Input,
                ) -> Result<<#module_ident::#struct_ident as ::temporalio_workflow::common::QueryDefinition>::Output, Box<dyn ::std::error::Error + Send + Sync>> {
                    #body
                }
            }
        };

        (info.marker_struct, info.const_def, trait_impl)
    }

    fn generate_update_code(
        &self,
        update: &UpdateMethod,
        impl_type: &Type,
        module_ident: &syn::Ident,
    ) -> (TokenStream2, TokenStream2, TokenStream2) {
        let info = HandlerCodegenInfo::new(
            &update.method,
            &update.attributes,
            update.input_type.as_ref(),
            module_ident,
        );
        let struct_ident = &info.struct_ident;
        let input_type = &info.input_type_tokens;
        let output_type = type_to_tokens(update.output_type.as_ref());
        let update_name = &info.handler_name;

        let run_struct_ident = self.run_struct_ident();
        let definition_impl = quote! {
            impl ::temporalio_workflow::common::UpdateDefinition for #module_ident::#struct_ident {
                type Workflow = #module_ident::#run_struct_ident;
                type Input = #input_type;
                type Output = #output_type;

                fn name(&self) -> &str {
                    #update_name
                }
            }
        };

        let has_input = update.input_type.is_some();
        let validate_impl = if let Some(ref validator_name) = update.validator {
            let prefixed_validator = format_ident!("__{}", validator_name);
            quote! {
                fn validate(&self, ctx: &::temporalio_workflow::WorkflowContextView, input: &<#module_ident::#struct_ident as ::temporalio_workflow::common::UpdateDefinition>::Input) -> Result<(), Box<dyn ::std::error::Error + Send + Sync>> {
                    self.#prefixed_validator(ctx, input)
                }
            }
        } else {
            quote! {}
        };

        let executable_impl = if update.is_async {
            let prefixed_method = &info.prefixed_method;
            let method_call = if has_input {
                quote! { #impl_type::#prefixed_method(&mut ctx, input) }
            } else {
                quote! { #impl_type::#prefixed_method(&mut ctx) }
            };
            let handle_body = if update.is_fallible {
                quote! {
                    ::temporalio_workflow::__private::futures_util::FutureExt::boxed_local(
                        async move { #method_call.await }
                    )
                }
            } else {
                quote! {
                    ::temporalio_workflow::__private::futures_util::FutureExt::boxed_local(
                        ::temporalio_workflow::__private::futures_util::FutureExt::map(
                            async move { #method_call.await },
                            Ok,
                        )
                    )
                }
            };
            quote! {
                impl ::temporalio_workflow::workflows::ExecutableAsyncUpdate<#module_ident::#struct_ident> for #impl_type {
                    fn handle(
                        mut ctx: ::temporalio_workflow::WorkflowContext<Self>,
                        input: <#module_ident::#struct_ident as ::temporalio_workflow::common::UpdateDefinition>::Input,
                    ) -> ::temporalio_workflow::__private::futures_util::future::LocalBoxFuture<'static, Result<<#module_ident::#struct_ident as ::temporalio_workflow::common::UpdateDefinition>::Output, Box<dyn ::std::error::Error + Send + Sync>>> {
                        #handle_body
                    }

                    #validate_impl
                }
            }
        } else {
            let method_call = generate_method_call(&info.prefixed_method, has_input);
            let body = if update.is_fallible {
                quote! { #method_call }
            } else {
                quote! { Ok(#method_call) }
            };
            quote! {
                impl ::temporalio_workflow::workflows::ExecutableSyncUpdate<#module_ident::#struct_ident> for #impl_type {
                    fn handle(
                        &mut self,
                        ctx: &mut ::temporalio_workflow::SyncWorkflowContext<Self>,
                        input: <#module_ident::#struct_ident as ::temporalio_workflow::common::UpdateDefinition>::Input,
                    ) -> Result<<#module_ident::#struct_ident as ::temporalio_workflow::common::UpdateDefinition>::Output, Box<dyn ::std::error::Error + Send + Sync>> {
                        #body
                    }

                    #validate_impl
                }
            }
        };

        let trait_impl = quote! {
            #definition_impl
            #executable_impl
        };

        (info.marker_struct, info.const_def, trait_impl)
    }

    fn generate_workflow_implementation(
        &self,
        impl_type: &Type,
        module_ident: &syn::Ident,
        factory_only: bool,
    ) -> TokenStream2 {
        let run_method = &self.run_method;

        let run_struct_name = method_name_to_pascal_case(&run_method.method.sig.ident);
        let run_struct_ident = format_ident!("{}", run_struct_name);
        let prefixed_run = format_ident!("__{}", run_method.method.sig.ident);

        // Determine if init exists and takes input, or if run takes input
        let has_init = self.init_method.is_some();
        let init_has_input = self
            .init_method
            .as_ref()
            .map(|m| m.input_type.is_some())
            .unwrap_or(false);
        let run_has_input = run_method.input_type.is_some();

        let init_body = if let Some(ref init_method) = self.init_method {
            let prefixed_init = format_ident!("__{}", init_method.method.sig.ident);
            if init_has_input {
                quote! {
                    #impl_type::#prefixed_init(&ctx, input.expect("init takes input"))
                }
            } else {
                quote! {
                    #impl_type::#prefixed_init(&ctx)
                }
            }
        } else if factory_only {
            // For factory-only workflows, init() should never be called
            quote! {
                panic!(
                    "This workflow must be registered with register_workflow_with_factory"
                )
            }
        } else {
            quote! {
                Self::default()
            }
        };

        let run_call = if run_has_input {
            quote! { Self::#prefixed_run(&mut ctx, input.expect("run takes input")).await }
        } else {
            quote! { Self::#prefixed_run(&mut ctx).await }
        };

        let run_impl_body = quote! {
            ::temporalio_workflow::__private::futures_util::FutureExt::boxed_local(async move {
                let result = #run_call;
                match result {
                    Ok(value) => Ok(
                        ::std::boxed::Box::new(value)
                            as ::std::boxed::Box<dyn ::temporalio_workflow::workflow_interceptors::WorkflowOutputValue>
                    ),
                    Err(e) => Err(e),
                }
            })
        };

        // Generate signal dispatch match arms
        let signal_names: Vec<TokenStream2> = self
            .signals
            .iter()
            .map(|s| {
                let info = HandlerCodegenInfo::new(
                    &s.method,
                    &s.attributes,
                    s.input_type.as_ref(),
                    module_ident,
                );
                let handler_name = &info.handler_name;
                quote! { (#handler_name).to_string() }
            })
            .collect();
        let decode_signal_arms: Vec<TokenStream2> = self
            .signals
            .iter()
            .map(|s| {
                let info = HandlerCodegenInfo::new(
                    &s.method,
                    &s.attributes,
                    s.input_type.as_ref(),
                    module_ident,
                );
                let input_type = &info.input_type_tokens;
                let handler_name = &info.handler_name;

                generate_decode_arm(handler_name, input_type, quote! { payloads.payloads })
            })
            .collect();
        let dispatch_signal_arms: Vec<TokenStream2> = self
            .signals
            .iter()
            .map(|s| {
                let info = HandlerCodegenInfo::new(
                    &s.method,
                    &s.attributes,
                    s.input_type.as_ref(),
                    module_ident,
                );
                let struct_ident = &info.struct_ident;
                let handler_name = &info.handler_name;

                if s.is_async {
                    quote! {
                        #handler_name => <Self as ::temporalio_workflow::workflows::ExecutableAsyncSignal<#module_ident::#struct_ident>>::dispatch(ctx.clone(), input),
                    }
                } else {
                    quote! {
                        #handler_name => <Self as ::temporalio_workflow::workflows::ExecutableSyncSignal<#module_ident::#struct_ident>>::dispatch(ctx, input),
                    }
                }
            })
            .collect();

        let decode_signal_impl = if self.signals.is_empty() {
            quote! {}
        } else {
            quote! {
                fn decode_signal_input(
                    name: &str,
                    payloads: ::temporalio_workflow::common::protos::temporal::api::common::v1::Payloads,
                    converter: &::temporalio_workflow::common::data_converters::PayloadConverter,
                ) -> Result<::std::option::Option<::std::boxed::Box<dyn ::std::any::Any>>, ::temporalio_workflow::workflows::WorkflowError> {
                    match name {
                        #(#decode_signal_arms)*
                        _ => Ok(::std::option::Option::None),
                    }
                }
            }
        };

        let dispatch_signal_impl = if self.signals.is_empty() {
            quote! {
                fn dispatch_signal(
                    _ctx: ::temporalio_workflow::WorkflowContext<Self>,
                    name: &str,
                    _input: ::std::boxed::Box<dyn ::std::any::Any>,
                ) -> ::temporalio_workflow::__private::futures_util::future::LocalBoxFuture<'static, Result<(), ::temporalio_workflow::workflows::WorkflowError>> {
                    unreachable!("typed signal dispatch called for unknown signal handler '{name}'")
                }
            }
        } else {
            quote! {
                fn dispatch_signal(
                    ctx: ::temporalio_workflow::WorkflowContext<Self>,
                    name: &str,
                    input: ::std::boxed::Box<dyn ::std::any::Any>,
                ) -> ::temporalio_workflow::__private::futures_util::future::LocalBoxFuture<'static, Result<(), ::temporalio_workflow::workflows::WorkflowError>> {
                    match name {
                        #(#dispatch_signal_arms)*
                        _ => unreachable!("typed signal dispatch called for unknown signal handler '{name}'"),
                    }
                }
            }
        };

        // Generate update dispatch match arms
        let decode_update_arms: Vec<TokenStream2> = self
            .updates
            .iter()
            .map(|u| {
                let info = HandlerCodegenInfo::new(
                    &u.method,
                    &u.attributes,
                    u.input_type.as_ref(),
                    module_ident,
                );
                let input_type = &info.input_type_tokens;
                let handler_name = &info.handler_name;

                generate_decode_arm(handler_name, input_type, quote! { payloads.payloads })
            })
            .collect();
        let dispatch_update_arms: Vec<TokenStream2> = self
            .updates
            .iter()
            .map(|u| {
                let info = HandlerCodegenInfo::new(
                    &u.method,
                    &u.attributes,
                    u.input_type.as_ref(),
                    module_ident,
                );
                let struct_ident = &info.struct_ident;
                let handler_name = &info.handler_name;

                if u.is_async {
                    quote! {
                        #handler_name => <Self as ::temporalio_workflow::workflows::ExecutableAsyncUpdate<#module_ident::#struct_ident>>::dispatch(ctx.clone(), input),
                    }
                } else {
                    quote! {
                        #handler_name => <Self as ::temporalio_workflow::workflows::ExecutableSyncUpdate<#module_ident::#struct_ident>>::dispatch(ctx, input),
                    }
                }
            })
            .collect();

        let update_definitions: Vec<TokenStream2> = self
            .updates
            .iter()
            .map(|u| {
                let info = HandlerCodegenInfo::new(
                    &u.method,
                    &u.attributes,
                    u.input_type.as_ref(),
                    module_ident,
                );
                let handler_name = &info.handler_name;
                let has_validator = u.validator.is_some();
                quote! {
                    ::temporalio_workflow::runtime::types::UpdateDefinitionDescriptor {
                        name: (#handler_name).to_string(),
                        has_validator: #has_validator,
                    }
                }
            })
            .collect();

        // Generate validate_update match arms
        let validate_update_arms: Vec<TokenStream2> = self
            .updates
            .iter()
            .filter(|u| u.validator.is_some())
            .map(|u| {
                let info = HandlerCodegenInfo::new(
                    &u.method,
                    &u.attributes,
                    u.input_type.as_ref(),
                    module_ident,
                );
                let struct_ident = &info.struct_ident;
                let handler_name = &info.handler_name;

                let update_trait = if u.is_async {
                    quote! { ::temporalio_workflow::workflows::ExecutableAsyncUpdate<#module_ident::#struct_ident> }
                } else {
                    quote! { ::temporalio_workflow::workflows::ExecutableSyncUpdate<#module_ident::#struct_ident> }
                };

                quote! {
                    #handler_name => <Self as #update_trait>::dispatch_validate(self, &ctx, input),
                }
            })
            .collect();

        let decode_update_impl = if self.updates.is_empty() {
            quote! {}
        } else {
            quote! {
                fn decode_update_input(
                    name: &str,
                    payloads: ::temporalio_workflow::common::protos::temporal::api::common::v1::Payloads,
                    converter: &::temporalio_workflow::common::data_converters::PayloadConverter,
                ) -> Result<::std::option::Option<::std::boxed::Box<dyn ::std::any::Any>>, ::temporalio_workflow::workflows::WorkflowError> {
                    match name {
                        #(#decode_update_arms)*
                        _ => Ok(::std::option::Option::None),
                    }
                }
            }
        };

        let dispatch_update_impl = if self.updates.is_empty() {
            quote! {
                fn dispatch_update(
                    _ctx: ::temporalio_workflow::WorkflowContext<Self>,
                    name: &str,
                    _input: ::std::boxed::Box<dyn ::std::any::Any>,
                ) -> ::temporalio_workflow::__private::futures_util::future::LocalBoxFuture<'static, Result<::std::boxed::Box<dyn ::temporalio_workflow::workflow_interceptors::WorkflowOutputValue>, ::temporalio_workflow::workflows::WorkflowError>> {
                    unreachable!("typed update dispatch called for unknown update handler '{name}'")
                }

                fn validate_update(
                    &self,
                    _ctx: ::temporalio_workflow::WorkflowContextView,
                    name: &str,
                    _input: ::std::boxed::Box<dyn ::std::any::Any>,
                ) -> Result<(), ::temporalio_workflow::workflows::WorkflowError> {
                    unreachable!("typed update validation called for unknown update handler '{name}'")
                }
            }
        } else {
            quote! {
                fn dispatch_update(
                    ctx: ::temporalio_workflow::WorkflowContext<Self>,
                    name: &str,
                    input: ::std::boxed::Box<dyn ::std::any::Any>,
                ) -> ::temporalio_workflow::__private::futures_util::future::LocalBoxFuture<'static, Result<::std::boxed::Box<dyn ::temporalio_workflow::workflow_interceptors::WorkflowOutputValue>, ::temporalio_workflow::workflows::WorkflowError>> {
                    match name {
                        #(#dispatch_update_arms)*
                        _ => unreachable!("typed update dispatch called for unknown update handler '{name}'"),
                    }
                }

                fn validate_update(
                    &self,
                    ctx: ::temporalio_workflow::WorkflowContextView,
                    name: &str,
                    input: ::std::boxed::Box<dyn ::std::any::Any>,
                ) -> Result<(), ::temporalio_workflow::workflows::WorkflowError> {
                    match name {
                        #(#validate_update_arms)*
                        _ => unreachable!("typed update validation called for unknown update handler '{name}'"),
                    }
                }
            }
        };

        // Generate dispatch_query match arms
        let query_names: Vec<TokenStream2> = self
            .queries
            .iter()
            .map(|q| {
                let info = HandlerCodegenInfo::new(
                    &q.method,
                    &q.attributes,
                    q.input_type.as_ref(),
                    module_ident,
                );
                let handler_name = &info.handler_name;
                quote! { (#handler_name).to_string() }
            })
            .collect();
        let decode_query_arms: Vec<TokenStream2> = self
            .queries
            .iter()
            .map(|q| {
                let info = HandlerCodegenInfo::new(
                    &q.method,
                    &q.attributes,
                    q.input_type.as_ref(),
                    module_ident,
                );
                let input_type = &info.input_type_tokens;
                let handler_name = &info.handler_name;

                generate_decode_arm(
                    handler_name,
                    input_type,
                    quote! { payloads.payloads.clone() },
                )
            })
            .collect();
        let dispatch_query_arms: Vec<TokenStream2> = self
            .queries
            .iter()
            .map(|q| {
                let info = HandlerCodegenInfo::new(
                    &q.method,
                    &q.attributes,
                    q.input_type.as_ref(),
                    module_ident,
                );
                let struct_ident = &info.struct_ident;
                let handler_name = &info.handler_name;

                quote! {
                    #handler_name => <Self as ::temporalio_workflow::workflows::ExecutableQuery<#module_ident::#struct_ident>>::dispatch(self, &ctx, input),
                }
            })
            .collect();

        let decode_query_impl = if self.queries.is_empty() {
            quote! {}
        } else {
            quote! {
                fn decode_query_input(
                    name: &str,
                    payloads: &::temporalio_workflow::common::protos::temporal::api::common::v1::Payloads,
                    converter: &::temporalio_workflow::common::data_converters::PayloadConverter,
                ) -> Result<::std::option::Option<::std::boxed::Box<dyn ::std::any::Any>>, ::temporalio_workflow::workflows::WorkflowError> {
                    match name {
                        #(#decode_query_arms)*
                        _ => Ok(::std::option::Option::None),
                    }
                }
            }
        };

        let dispatch_query_impl = if self.queries.is_empty() {
            quote! {
                fn dispatch_query(
                    &self,
                    _ctx: ::temporalio_workflow::WorkflowContextView,
                    name: &str,
                    _input: ::std::boxed::Box<dyn ::std::any::Any>,
                ) -> Result<::std::boxed::Box<dyn ::temporalio_workflow::workflow_interceptors::WorkflowOutputValue>, ::temporalio_workflow::workflows::WorkflowError> {
                    unreachable!("typed query dispatch called for unknown query handler '{name}'")
                }
            }
        } else {
            quote! {
                fn dispatch_query(
                    &self,
                    ctx: ::temporalio_workflow::WorkflowContextView,
                    name: &str,
                    input: ::std::boxed::Box<dyn ::std::any::Any>,
                ) -> Result<::std::boxed::Box<dyn ::temporalio_workflow::workflow_interceptors::WorkflowOutputValue>, ::temporalio_workflow::workflows::WorkflowError> {
                    match name {
                        #(#dispatch_query_arms)*
                        _ => unreachable!("typed query dispatch called for unknown query handler '{name}'"),
                    }
                }
            }
        };

        quote! {
            impl ::temporalio_workflow::runtime::entry::WorkflowImplementation for #impl_type {
                type Run = #module_ident::#run_struct_ident;

                const HAS_INIT: bool = #has_init;
                const INIT_TAKES_INPUT: bool = #init_has_input;

                fn name() -> &'static str {
                    <#impl_type>::name()
                }

                fn definition() -> ::temporalio_workflow::runtime::types::WorkflowDefinitionDescriptor {
                    ::temporalio_workflow::runtime::types::WorkflowDefinitionDescriptor {
                        workflow_type: Self::name().to_string(),
                        has_init: #has_init,
                        init_takes_input: #init_has_input,
                        signals: vec![#(#signal_names),*],
                        queries: vec![#(#query_names),*],
                        updates: vec![#(#update_definitions),*],
                    }
                }

                fn init(
                    ctx: ::temporalio_workflow::WorkflowContextView,
                    input: ::std::option::Option<<Self::Run as ::temporalio_workflow::common::WorkflowDefinition>::Input>,
                ) -> Self {
                    #init_body
                }

                fn run(
                    mut ctx: ::temporalio_workflow::WorkflowContext<Self>,
                    input: ::std::option::Option<<Self::Run as ::temporalio_workflow::common::WorkflowDefinition>::Input>,
                ) -> ::temporalio_workflow::__private::futures_util::future::LocalBoxFuture<'static, Result<::std::boxed::Box<dyn ::temporalio_workflow::workflow_interceptors::WorkflowOutputValue>, ::temporalio_workflow::WorkflowTermination>> {
                    #run_impl_body
                }

                #dispatch_signal_impl
                #decode_signal_impl
                #dispatch_update_impl
                #decode_update_impl
                #dispatch_query_impl
                #decode_query_impl
            }
        }
    }
}
