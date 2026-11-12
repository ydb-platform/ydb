// vim: tw=80
use proc_macro2::{Span, TokenStream};
use quote::{format_ident, quote};
use syn::{
    *,
    parse::{Parse, ParseStream},
    spanned::Spanned
};

use crate::{
    Attrs,
    compile_error,
    deanonymize,
    deimplify,
    demutify,
    deselfify,
    deselfify_args,
    dewhereselfify,
    find_ident_from_path,
    gen_mock_ident,
    AttrFormatter,
};

/// Make any implicit lifetime parameters explicit
fn add_lifetime_parameters(sig: &mut Signature) {
    fn add_to_trait_object(generics: &mut Generics, var: &Pat, to: &mut TypeTraitObject) {
        let mut has_lifetime = false;
        for bound in to.bounds.iter() {
            if let TypeParamBound::Lifetime(_) = bound {
                has_lifetime = true;
            }
        }
        if ! has_lifetime {
            let arg_ident = match *var {
                Pat::Wild(_) => {
                    compile_error(var.span(),
                        "Mocked methods must have named arguments");
                    format_ident!("dont_care")
                },
                Pat::Ident(ref pat_ident) => {
                    if let Some(r) = &pat_ident.by_ref {
                        compile_error(r.span(),
                            "Mockall does not support by-reference argument bindings");
                    }
                    if let Some((_at, subpat)) = &pat_ident.subpat {
                        compile_error(subpat.span(),
                            "Mockall does not support subpattern bindings");
                    }
                    pat_ident.ident.clone()
                },
                _ => {
                    compile_error(var.span(),
                        "Unsupported argument type");
                    format_ident!("dont_care")
                }
            };
            let s = format!("'__mockall_{arg_ident}");
            let span = Span::call_site();
            let lt = Lifetime::new(&s, span);
            to.bounds.push(TypeParamBound::Lifetime(lt.clone()));
            generics.lt_token.get_or_insert(Token![<](span));
            generics.gt_token.get_or_insert(Token![>](span));
            let gpl = GenericParam::Lifetime(LifetimeParam::new(lt));
            generics.params.push(gpl);
        }
    }

    fn add_to_type(generics: &mut Generics, var: &Pat, ty: &mut Type) {
        match ty {
            Type::Array(ta) => add_to_type(generics, var, ta.elem.as_mut()),
            Type::BareFn(_) => (),
            Type::ImplTrait(_) => (),
            Type::Path(_) => (),
            Type::Ptr(_) => (),
            Type::Reference(tr) => {
                match tr.elem.as_mut() {
                    Type::Paren(tp) => {
                        if let Type::TraitObject(to) = tp.elem.as_mut() {
                            add_to_trait_object(generics, var, to);
                        } else {
                            add_to_type(generics, var, tr.elem.as_mut());
                        }
                    },
                    Type::TraitObject(to) => {
                        add_to_trait_object(generics, var, to);
                        // We need to wrap it in a Paren.  Otherwise it won't be
                        // syntactically valid after we add a lifetime bound,
                        // due to a "ambiguous `+` in a type" error
                        *tr.elem = Type::Paren(TypeParen {
                            paren_token: token::Paren::default(),
                            elem: Box::new(Type::TraitObject(to.clone()))
                        });
                    },
                    _ => add_to_type(generics, var, tr.elem.as_mut()),
                }
            },
            Type::Slice(ts) => add_to_type(generics, var, ts.elem.as_mut()),
            Type::Tuple(tt) => {
                for ty in tt.elems.iter_mut() {
                    add_to_type(generics, var, ty)
                }
            },
            _ => compile_error(ty.span(), "unsupported type in this position")
        }
    }

    for arg in sig.inputs.iter_mut() {
        if let FnArg::Typed(pt) = arg {
            add_to_type(&mut sig.generics, &pt.pat, &mut pt.ty)
        }
    }
}

/// Generate a #[derive(Debug)] Attribute
fn derive_debug() -> Attribute {
    let ml = parse2(quote!(derive(Debug))).unwrap();
    Attribute {
        pound_token: <Token![#]>::default(),
        style: AttrStyle::Outer,
        bracket_token: token::Bracket::default(),
        meta: Meta::List(ml)
    }
}

/// Add "Mock" to the front of the named type
fn mock_ident_in_type(ty: &mut Type) {
    match ty {
        Type::Path(type_path) => {
            if type_path.path.segments.len() != 1 {
                compile_error(type_path.path.span(),
                    "mockall_derive only supports structs defined in the current module");
                return;
            }
            let ident = &mut type_path.path.segments.last_mut().unwrap().ident;
            *ident = gen_mock_ident(ident)
        },
        x => {
            compile_error(x.span(),
                "mockall_derive only supports mocking traits and structs");
        }
    };
}

/// Performs transformations on the ItemImpl to make it mockable
fn mockable_item_impl(mut impl_: ItemImpl, name: &Ident, generics: &Generics)
    -> ItemImpl
{
    mock_ident_in_type(&mut impl_.self_ty);
    for item in impl_.items.iter_mut() {
        if let ImplItem::Fn(ref mut iim) = item {
            mockable_method(iim, name, generics);
        }
    }
    impl_
}

/// Performs transformations on the method to make it mockable
fn mockable_method(meth: &mut ImplItemFn, name: &Ident, generics: &Generics)
{
    demutify(&mut meth.sig.inputs);
    deselfify_args(&mut meth.sig.inputs, name, generics);
    add_lifetime_parameters(&mut meth.sig);
    deimplify(&mut meth.sig.output);
    dewhereselfify(&mut meth.sig.generics);
    if let ReturnType::Type(_, ty) = &mut meth.sig.output {
        deselfify(ty, name, generics);
        deanonymize(ty);
    }
    sanity_check_sig(&meth.sig);
}

/// Performs transformations on the method to make it mockable
fn mockable_trait_method(
    meth: &mut TraitItemFn,
    name: &Ident,
    generics: &Generics)
{
    demutify(&mut meth.sig.inputs);
    deselfify_args(&mut meth.sig.inputs, name, generics);
    add_lifetime_parameters(&mut meth.sig);
    deimplify(&mut meth.sig.output);
    dewhereselfify(&mut meth.sig.generics);
    if let ReturnType::Type(_, ty) = &mut meth.sig.output {
        deselfify(ty, name, generics);
        deanonymize(ty);
    }
    sanity_check_sig(&meth.sig);
}

/// Generates a mockable item impl from a trait method definition
fn mockable_trait(trait_: ItemTrait, name: &Ident, generics: &Generics)
    -> ItemImpl
{
    let items = trait_.items.into_iter()
    .map(|ti| {
        match ti {
            TraitItem::Fn(mut tif) => {
                mockable_trait_method(&mut tif, name, generics);
                ImplItem::Fn(tif2iif(tif, &Visibility::Inherited))
            },
            TraitItem::Const(tic) => {
                ImplItem::Const(tic2iic(tic, &Visibility::Inherited))
            },
            TraitItem::Type(tit) => {
                ImplItem::Type(tit2iit(tit, &Visibility::Inherited))
            },
            _ => {
                compile_error(ti.span(), "Unsupported in this context");
                ImplItem::Verbatim(TokenStream::new())
            }
        }
    }).collect::<Vec<_>>();
    let mut trait_path = Path::from(trait_.ident);
    let mut struct_path = Path::from(name.clone());
    let (_, stg, _) = generics.split_for_impl();
    let (_, ttg, _) = trait_.generics.split_for_impl();
    if let Ok(abga) = parse2::<AngleBracketedGenericArguments>(quote!(#stg)) {
        struct_path.segments.last_mut().unwrap().arguments =
            PathArguments::AngleBracketed(abga);
    }
    if let Ok(abga) = parse2::<AngleBracketedGenericArguments>(quote!(#ttg)) {
        trait_path.segments.last_mut().unwrap().arguments =
            PathArguments::AngleBracketed(abga);
    }
    let self_ty = Box::new(Type::Path(TypePath{
        qself: None,
        path: struct_path,
    }));
    ItemImpl {
        attrs: trait_.attrs,
        defaultness: None,
        unsafety: trait_.unsafety,
        impl_token: <Token![impl]>::default(),
        generics: generics.clone(),
        trait_: Some((None, trait_path, <Token![for]>::default())),
        self_ty,
        brace_token: trait_.brace_token,
        items
    }
}

fn sanity_check_sig(sig: &Signature) {
    for arg in sig.inputs.iter() {
        if let FnArg::Typed(pt) = arg {
            if let Type::ImplTrait(it) = pt.ty.as_ref() {
                let bounds = &it.bounds;
                let s = format!(
                    "Mockall does not support \"impl trait\" in argument position.  Use \"T: {}\" instead",
                    quote!(#bounds)
                );
                compile_error(it.span(), &s);
            }
        }
    }
}

/// Converts a TraitItemConst into an ImplItemConst
fn tic2iic(tic: TraitItemConst, vis: &syn::Visibility) -> ImplItemConst {
    let span = tic.span();
    let (eq_token, expr) = tic.default.unwrap_or_else(|| {
        compile_error(span,
            "Mocked associated consts must have a default implementation");
        (<Token![=]>::default(), Expr::Verbatim(TokenStream::new()))
    });
    ImplItemConst {
        attrs: tic.attrs,
        vis: vis.clone(),
        defaultness: None,
        const_token: tic.const_token,
        generics: tic.generics,
        ident: tic.ident,
        colon_token: tic.colon_token,
        ty: tic.ty,
        eq_token,
        expr,
        semi_token: tic.semi_token
    }
}

/// Converts a TraitItemFn into an ImplItemFn
fn tif2iif(m: syn::TraitItemFn, vis: &syn::Visibility)
    -> syn::ImplItemFn
{
    let empty_block = Block {
        brace_token: token::Brace::default(),
        stmts: Vec::new()
    };
    syn::ImplItemFn{
        attrs: m.attrs,
        vis: vis.clone(),
        defaultness: None,
        sig: m.sig,
        block: empty_block
    }
}

/// Converts a TraitItemType into an ImplItemType
fn tit2iit(tit: TraitItemType, vis: &Visibility) -> ImplItemType {
    let span = tit.span();
    let (eq_token, ty) = tit.default.unwrap_or_else(|| {
        compile_error(span,
            "associated types in mock! must be fully specified");
        (token::Eq::default(), Type::Verbatim(TokenStream::new()))
    });
    ImplItemType {
        attrs: tit.attrs,
        vis: vis.clone(),
        defaultness: None,
        type_token: tit.type_token,
        ident: tit.ident,
        generics: tit.generics,
        eq_token,
        ty,
        semi_token: tit.semi_token,
    }
}

/// Like a TraitItemFn, but with a visibility
struct TraitItemVFn {
    pub vis: Visibility,
    pub tif: TraitItemFn
}

impl Parse for TraitItemVFn {
    fn parse(input: ParseStream) -> syn::parse::Result<Self> {
        let attrs = input.call(Attribute::parse_outer)?;
        let vis: syn::Visibility = input.parse()?;
        let mut tif: TraitItemFn = input.parse()?;
        tif.attrs = attrs;
        Ok(Self{vis, tif})
    }
}

pub(crate) struct MockableStruct {
    pub attrs: Vec<Attribute>,
    pub consts: Vec<ImplItemConst>,
    pub generics: Generics,
    /// Inherent methods of the mockable struct
    pub methods: Vec<ImplItemFn>,
    pub name: Ident,
    pub vis: Visibility,
    pub impls: Vec<ItemImpl>,
}

impl MockableStruct {
    /// Does this struct derive Debug?
    pub fn derives_debug(&self) -> bool {
        self.attrs.iter()
        .any(|attr|{
            let mut derive_debug = false;
            if attr.path().is_ident("derive") {
                attr.parse_nested_meta(|meta| {
                    if meta.path.is_ident("Debug") {
                        derive_debug = true;
                    }
                    Ok(())
                }).unwrap();
            }
            derive_debug
        })
    }
}

impl From<(Attrs, ItemTrait)> for MockableStruct {
    fn from((attrs, item_trait): (Attrs, ItemTrait)) -> MockableStruct {
        let trait_ = attrs.substitute_trait(&item_trait);
        // Strip "must_use" from a trait definition.  For traits, the "must_use"
        // should apply only when the trait is used like "impl Trait" or "dyn
        // Trait".  So it shouldn't necessarily affect the mock struct that
        // implements the trait.
        let mut attrs = AttrFormatter::new(&trait_.attrs)
            .doc(true)
            .async_trait(true)
            .trait_variant(true)
            .must_use(false)
            .format();
        attrs.push(derive_debug());
        let vis = trait_.vis.clone();
        let name = gen_mock_ident(&trait_.ident);
        let generics = trait_.generics.clone();
        let impls = vec![mockable_trait(trait_, &name, &generics)];
        MockableStruct {
            attrs,
            consts: Vec::new(),
            vis,
            name,
            generics,
            methods: Vec::new(),
            impls
        }
    }
}

impl From<ItemImpl> for MockableStruct {
    fn from(mut item_impl: ItemImpl) -> MockableStruct {
        let name = match &*item_impl.self_ty {
            Type::Path(type_path) => {
                let n = find_ident_from_path(&type_path.path).0;
                let self_generics = &type_path.path.segments.last().unwrap().arguments;
                if let PathArguments::AngleBracketed(abga) = &self_generics {
                    if item_impl.generics.params.len() != abga.args.len() {
                        // If a struct's impl block has elided lifetimes, then
                        // they won't show up in the impl block's generics
                        // list.  automock can't currently handle that.
                        // https://github.com/asomers/mockall/issues/610
                        compile_error(item_impl.span(),
                            "automock does not currently support structs with elided lifetimes");
                    }
                }
                gen_mock_ident(&n)
            },
            x => {
                compile_error(x.span(),
                    "mockall_derive only supports mocking traits and structs");
                Ident::new("", Span::call_site())
            }
        };
        let mut attrs = item_impl.attrs.clone();
        attrs.push(derive_debug());
        let mut consts = Vec::new();
        let generics = item_impl.generics.clone();
        let mut methods = Vec::new();
        let vis = Visibility::Public(Token![pub](Span::call_site()));
        let mut impls = Vec::new();
        if let Some((bang, _path, _)) = &item_impl.trait_ {
            if bang.is_some() {
                compile_error(bang.span(), "Unsupported by automock");
            }

            // Substitute any associated types in this ItemImpl.
            // NB: this would not be necessary if the user always fully
            // qualified them, e.g. `<Self as MyTrait>::MyType`
            let mut attrs = Attrs::default();
            for item in item_impl.items.iter() {
                match item {
                    ImplItem::Const(_iic) =>
                        (),
                    ImplItem::Fn(_meth) =>
                        (),
                    ImplItem::Type(ty) => {
                        attrs.attrs.insert(ty.ident.clone(), ty.ty.clone());
                    },
                    x => compile_error(x.span(), "Unsupported by automock")
                }
            }
            attrs.substitute_item_impl(&mut item_impl);
            impls.push(mockable_item_impl(item_impl, &name, &generics));
        } else {
            for item in item_impl.items.into_iter() {
                match item {
                    ImplItem::Fn(mut meth) => {
                        mockable_method(&mut meth, &name, &item_impl.generics);
                        methods.push(meth)
                    },
                    ImplItem::Const(iic) => consts.push(iic),
                    // Rust doesn't allow types in an inherent impl
                    x => compile_error(x.span(),
                        "Unsupported by Mockall in this context"),
                }
            }
        };
        MockableStruct {
            attrs,
            consts,
            generics,
            methods,
            name,
            vis,
            impls,
        }
    }
}

impl Parse for MockableStruct {
    fn parse(input: ParseStream) -> syn::parse::Result<Self> {
        let attrs = input.call(syn::Attribute::parse_outer)?;
        let vis: syn::Visibility = input.parse()?;
        let original_name: syn::Ident = input.parse()?;
        let mut generics: syn::Generics = input.parse()?;
        let wc: Option<syn::WhereClause> = input.parse()?;
        generics.where_clause = wc;
        let name = gen_mock_ident(&original_name);
        let impl_content;
        let _brace_token = braced!(impl_content in input);
        let mut consts = Vec::new();
        let mut methods = Vec::new();
        while !impl_content.is_empty() {
            let item: ImplItem = impl_content.parse()?;
            match item {
                ImplItem::Verbatim(ts) => {
                    let tivf: TraitItemVFn = parse2(ts)?;
                    let mut iim = tif2iif(tivf.tif, &tivf.vis);
                    mockable_method(&mut iim, &name, &generics);
                    methods.push(iim);
                }
                ImplItem::Const(iic) => consts.push(iic),
                _ => {
                    return Err(input.error("Unsupported in this context"));
                }
            }
        }

        let mut impls = Vec::new();
        while !input.is_empty() {
            let item: Item = input.parse()?;
            match item {
                Item::Impl(mut ii) => {
                    for item in ii.items.iter_mut() {
                        // Convert any methods that syn couldn't parse as
                        // ImplItemFn.
                        if let ImplItem::Verbatim(ts) = item {
                            let tif: TraitItemFn = parse2(ts.clone()).unwrap();
                            let iim = tif2iif(tif, &Visibility::Inherited);
                            *item = ImplItem::Fn(iim);
                        }
                    }
                    impls.push(mockable_item_impl(ii, &name, &generics));
                }
                _ => return Err(input.error("Unsupported in this context")),
            }
        }

        Ok(
            MockableStruct {
                attrs,
                consts,
                generics,
                methods,
                name,
                vis,
                impls
            }
        )
    }
}

#[cfg(test)]
mod t {
    use super::*;

mod add_lifetime_parameters {
    use super::*;

    #[test]
    fn array() {
        let mut meth: TraitItemFn = parse2(quote!(
            fn foo(&self, x: [&dyn T; 1]);
        )).unwrap();
        add_lifetime_parameters(&mut meth.sig);
        assert_eq!(
            quote!(fn foo<'__mockall_x>(&self, x: [&(dyn T + '__mockall_x); 1]);)
                .to_string(),
            quote!(#meth).to_string()
        );
    }

    #[test]
    fn bare_fn_with_named_args() {
        let mut meth: TraitItemFn = parse2(quote!(
            fn foo(&self, x: fn(&dyn T));
        )).unwrap();
        add_lifetime_parameters(&mut meth.sig);
        assert_eq!(
            quote!(fn foo(&self, x: fn(&dyn T));).to_string(),
            quote!(#meth).to_string()
        );
    }

    #[test]
    fn plain() {
        let mut meth: TraitItemFn = parse2(quote!(
            fn foo(&self, x: &dyn T);
        )).unwrap();
        add_lifetime_parameters(&mut meth.sig);
        assert_eq!(
            quote!(fn foo<'__mockall_x>(&self, x: &(dyn T + '__mockall_x));)
                .to_string(),
            quote!(#meth).to_string()
        );
    }

    #[test]
    fn slice() {
        let mut meth: TraitItemFn = parse2(quote!(
            fn foo(&self, x: &[&dyn T]);
        )).unwrap();
        add_lifetime_parameters(&mut meth.sig);
        assert_eq!(
            quote!(fn foo<'__mockall_x>(&self, x: &[&(dyn T + '__mockall_x)]);)
                .to_string(),
            quote!(#meth).to_string()
        );
    }

    #[test]
    fn tuple() {
        let mut meth: TraitItemFn = parse2(quote!(
            fn foo(&self, x: (&dyn T, u32));
        )).unwrap();
        add_lifetime_parameters(&mut meth.sig);
        assert_eq!(
            quote!(fn foo<'__mockall_x>(&self, x: (&(dyn T + '__mockall_x), u32));)
                .to_string(),
            quote!(#meth).to_string()
        );
    }

    #[test]
    fn with_anonymous_lifetime() {
        let mut meth: TraitItemFn = parse2(quote!(
            fn foo(&self, x: &(dyn T + '_));
        )).unwrap();
        add_lifetime_parameters(&mut meth.sig);
        assert_eq!(
            quote!(fn foo(&self, x: &(dyn T + '_));).to_string(),
            quote!(#meth).to_string()
        );
    }

    #[test]
    fn with_parens() {
        let mut meth: TraitItemFn = parse2(quote!(
            fn foo(&self, x: &(dyn T));
        )).unwrap();
        add_lifetime_parameters(&mut meth.sig);
        assert_eq!(
            quote!(fn foo<'__mockall_x>(&self, x: &(dyn T + '__mockall_x));)
                .to_string(),
            quote!(#meth).to_string()
        );
    }

    #[test]
    fn with_lifetime_parameter() {
        let mut meth: TraitItemFn = parse2(quote!(
            fn foo<'a>(&self, x: &(dyn T + 'a));
        )).unwrap();
        add_lifetime_parameters(&mut meth.sig);
        assert_eq!(
            quote!(fn foo<'a>(&self, x: &(dyn T + 'a));).to_string(),
            quote!(#meth).to_string()
        );
    }

    #[test]
    fn with_static_lifetime() {
        let mut meth: TraitItemFn = parse2(quote!(
            fn foo(&self, x: &(dyn T + 'static));
        )).unwrap();
        add_lifetime_parameters(&mut meth.sig);
        assert_eq!(
            quote!(fn foo(&self, x: &(dyn T + 'static));).to_string(),
            quote!(#meth).to_string()
        );
    }

}

mod sanity_check_sig {
    use super::*;

    #[test]
    #[should_panic(expected = "Mockall does not support \"impl trait\" in argument position.  Use \"T: SomeTrait\" instead.")]
    fn impl_trait() {
        let meth: ImplItemFn = parse2(quote!(
            fn foo(&self, x: impl SomeTrait) {}
        )).unwrap();
        sanity_check_sig(&meth.sig);
    }
}
}
