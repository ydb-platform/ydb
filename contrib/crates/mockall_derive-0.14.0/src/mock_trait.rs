// vim: tw=80
use proc_macro2::Span;
use quote::{ToTokens, format_ident, quote};
use std::{
   collections::hash_map::DefaultHasher,
   fmt::Write,
   hash::{Hash, Hasher}
};
use syn::{
    *,
    spanned::Spanned
};

use crate::{
    AttrFormatter,
    mock_function::{self, MockFunction},
    compile_error
};

pub(crate) struct MockTrait {
    pub attrs: Vec<Attribute>,
    pub consts: Vec<ImplItemConst>,
    pub generics: Generics,
    pub methods: Vec<MockFunction>,
    /// Internally-used name of the trait used.
    pub ss_name: Ident,
    /// Fully-qualified name of the trait
    pub trait_path: Path,
    /// Path on which the trait is implemented.  Usually will be the same as
    /// structname, but might include concrete generic parameters.
    self_path: PathSegment,
    pub types: Vec<ImplItemType>,
    pub unsafety: Option<Token![unsafe]>
}

impl MockTrait {
    fn ss_name_priv(trait_path: &Path) -> Ident {
        let id = join_path_segments(trait_path, "__");
        // Skip the hashing step for easy debugging of generated code
        if let Some(hash) = hash_path_arguments(trait_path) {
            // Hash the path args to permit mocking structs that implement
            // multiple traits distinguished only by their path args
            format_ident!("{id}_{hash}")
        } else {
            format_ident!("{id}")
        }
    }

    pub fn ss_name(&self) -> &Ident {
        &self.ss_name
    }

    /// Create a new MockTrait
    ///
    /// # Arguments
    /// * `structname` - name of the struct that implements this trait
    /// * `struct_generics` - Generics of the parent structure
    /// * `impl_`  -    Mockable ItemImpl for a trait
    /// * `vis`     -   Visibility of the struct
    pub fn new(structname: &Ident,
               struct_generics: &Generics,
               impl_: ItemImpl,
               vis: &Visibility) -> Self
    {
        let mut consts = Vec::new();
        let mut methods = Vec::new();
        let mut types = Vec::new();
        let trait_path = if let Some((_, path, _)) = impl_.trait_ {
            path
        } else {
            compile_error(impl_.span(), "impl block must implement a trait");
            Path::from(format_ident!("__mockall_invalid"))
        };
        let ss_name = MockTrait::ss_name_priv(&trait_path);
        let self_path = match *impl_.self_ty {
            Type::Path(mut type_path) =>
                type_path.path.segments.pop().unwrap().into_value(),
            x => {
                compile_error(x.span(),
                    "mockall_derive only supports mocking traits and structs");
                PathSegment::from(Ident::new("", Span::call_site()))
            }
        };

        for ii in impl_.items.into_iter() {
            match ii {
                ImplItem::Const(iic) => {
                    consts.push(iic);
                },
                ImplItem::Fn(iif) => {
                    let mf = mock_function::Builder::new(&iif.sig, vis)
                        .attrs(&iif.attrs)
                        .levels(2)
                        .call_levels(0)
                        .struct_(structname)
                        .struct_generics(struct_generics)
                        .trait_(&ss_name)
                        .build();
                    methods.push(mf);
                },
                ImplItem::Type(iit) => {
                    types.push(iit);
                },
                _ => {
                    compile_error(ii.span(),
                    "This impl item is not yet supported by MockAll");
                }
            }
        }
        MockTrait {
            attrs: impl_.attrs,
            consts,
            generics: impl_.generics,
            methods,
            ss_name,
            trait_path,
            self_path,
            types,
            unsafety: impl_.unsafety
        }
    }

    /// Generate code for the trait implementation on the mock struct
    ///
    /// # Arguments
    ///
    /// * `modname`:    Name of the parent struct's private module
    // Supplying modname is an unfortunately hack.  Ideally MockTrait
    // wouldn't need to know that.
    pub fn trait_impl(&self, modname: &Ident) -> impl ToTokens {
        let trait_impl_attrs = AttrFormatter::new(&self.attrs)
            .must_use(false)
            .format();
        let impl_attrs = AttrFormatter::new(&self.attrs)
            .async_trait(false)
            .doc(false)
            .format();
        let (ig, _tg, wc) = self.generics.split_for_impl();
        let consts = &self.consts;
        let path_args = &self.self_path.arguments;
        let calls = self.methods.iter()
                .map(|meth| meth.call(Some(modname)))
                .collect::<Vec<_>>();
        let contexts = self.methods.iter()
            .filter(|meth| meth.is_static())
            .map(|meth| meth.context_fn(Some(modname)))
            .collect::<Vec<_>>();
        let expects = self.methods.iter()
            .filter(|meth| !meth.is_static())
            .map(|meth| {
                if meth.is_method_generic() {
                    // Specific impls with generic methods are TODO.
                    meth.expect(modname, None)
                } else {
                    meth.expect(modname, Some(path_args))
                }
            }).collect::<Vec<_>>();
        let trait_path = &self.trait_path;
        let self_path = &self.self_path;
        let types = &self.types;
        let unsafety = &self.unsafety;
        quote!(
            #(#trait_impl_attrs)*
            #unsafety impl #ig #trait_path for #self_path #wc {
                #(#consts)*
                #(#types)*
                #(#calls)*
            }
            #(#impl_attrs)*
            impl #ig #self_path #wc {
                #(#expects)*
                #(#contexts)*
            }
        )
    }
}

fn join_path_segments(path: &Path, sep: &str) -> String {
    let mut output = String::new();
    for segment in &path.segments {
        if write!(
            output,
            "{}{}",
            if output.is_empty() { "" } else { sep },
            segment.ident
        )
        .is_err()
        {
            break;
        };
    }
    output
}

fn hash_path_arguments(path: &Path) -> Option<u64> {
    let mut hasher = DefaultHasher::new();
    let mut is_some = false;
    for arguments in path
        .segments
        .iter()
        .map(|segment| &segment.arguments)
        .filter(|arguments| !arguments.is_empty())
    {
        arguments.hash(&mut hasher);
        is_some = true;
    }
    is_some.then(|| hasher.finish())
}
