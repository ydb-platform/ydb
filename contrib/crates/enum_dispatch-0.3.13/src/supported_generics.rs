//! Utilities for dealing with generic arguments and parameters.

/// Represents any single generic argument from e.g. `#[enum_dispatch(Ty<...>)]` that can be
/// supported by `enum_dispatch`.
pub enum SupportedGenericArg {
    /// A `_` type.
    Inferred,
    /// A named generic argument, e.g. `T`.
    Identifier(proc_macro2::Ident),
    /// A const generic char, e.g. `'a'`.
    ConstChar(syn::LitChar),
    /// A const generic byte, e.g. `b'a'`.
    ConstByte(syn::LitByte),
    /// A const generic integer, e.g. `9`.
    ConstInt(syn::LitInt),
    /// A const generic integer, e.g. `true`.
    ConstBool(syn::LitBool),
}

/// Represents any single generic argument from `#[enum_dispatch(Ty<...>)]` that can _not_ be
/// supported by `enum_dispatch`.
pub enum UnsupportedGenericArg {
    NonIdentifierType,
    NonIntegralConstGenericType,
    Lifetime,
    Constraint,
    AssocType,
    AssocConst,
    Unknown,
}

impl std::fmt::Display for UnsupportedGenericArg {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NonIdentifierType => write!(f, "Generic types in #[enum_dispatch(...)] must be identifiers"),
            Self::NonIntegralConstGenericType => write!(f, "Non-integral const generic types in #[enum_dispatch(...)] are not supported"),
            Self::Lifetime => write!(f, "Lifetime generics in #[enum_dispatch(...)] are not supported"),
            Self::AssocType => write!(f, "Generic associated types in #[enum_dispatch(...)] are not supported"),
            Self::AssocConst => write!(f, "Generic associated constants in #[enum_dispatch(...)] are not supported"),
            Self::Constraint => write!(f, "Generic trait constraints in #[enum_dispatch(...)] are not supported"),
            Self::Unknown => write!(f, "Unsupported generic argument syntax in #[enum_dispatch(...)]"),
        }
    }
}

/// Strings corresponding to types that are supported as const generics.
const SUPPORTED_CONST_GENERIC_TYPES: &[&str] = &[
    "u8",
    "i8",
    "u16",
    "i16",
    "u32",
    "i32",
    "u64",
    "i64",
    "u128",
    "i128",
    "usize",
    "isize",
    "char",
    "bool",
];

/// Counts the number of supported generic parameters from an enum or trait definition.
pub fn num_supported_generics(g: &syn::Generics) -> usize {
    let type_generics = g.type_params().count();
    let const_generics = g.const_params().filter(|p| {
        if let syn::Type::Path(syn::TypePath { qself: None, path }) = &p.ty {
            for supported_type in SUPPORTED_CONST_GENERIC_TYPES {
                if path.is_ident(supported_type) {
                    return true;
                }
            }
        }
        false
    }).count();

    type_generics + const_generics
}

/// Converts a `syn::GenericArgument` to a `SupportedGenericArg`, or an `UnsupportedGenericArg` if
/// it is not supported.
pub fn convert_to_supported_generic(generic_arg: &syn::GenericArgument) -> Result<SupportedGenericArg, (UnsupportedGenericArg, proc_macro2::Span)> {
    use syn::spanned::Spanned as _;
    let span = generic_arg.span();

    match generic_arg {
        syn::GenericArgument::Type(syn::Type::Path(t)) if t.qself.is_none() => {
            if let Some(ident) = t.path.get_ident() {
                Ok(SupportedGenericArg::Identifier(ident.clone()))
            } else {
                Err((UnsupportedGenericArg::NonIdentifierType, span))
            }
        }
        syn::GenericArgument::Type(syn::Type::Infer(_)) => Ok(SupportedGenericArg::Inferred),
        syn::GenericArgument::Type(_) => Err((UnsupportedGenericArg::NonIdentifierType, span)),
        syn::GenericArgument::Const(syn::Expr::Lit(syn::ExprLit { attrs: _, lit })) => {
            match lit {
                syn::Lit::Byte(b) => Ok(SupportedGenericArg::ConstByte(b.clone())),
                syn::Lit::Char(c) => Ok(SupportedGenericArg::ConstChar(c.clone())),
                syn::Lit::Int(i) => Ok(SupportedGenericArg::ConstInt(i.clone())),
                syn::Lit::Bool(b) => Ok(SupportedGenericArg::ConstBool(b.clone())),
                _ => Err((UnsupportedGenericArg::NonIntegralConstGenericType, span)),
            }
        }
        syn::GenericArgument::Const(_) => Err((UnsupportedGenericArg::NonIntegralConstGenericType, span)),
        syn::GenericArgument::Lifetime(_) => Err((UnsupportedGenericArg::Lifetime, span)),
        syn::GenericArgument::Constraint(_) => Err((UnsupportedGenericArg::Constraint, span)),
        syn::GenericArgument::AssocType(_) => Err((UnsupportedGenericArg::AssocType, span)),
        syn::GenericArgument::AssocConst(_) => Err((UnsupportedGenericArg::AssocConst, span)),
        _ => Err((UnsupportedGenericArg::Unknown, span)),
    }
}
