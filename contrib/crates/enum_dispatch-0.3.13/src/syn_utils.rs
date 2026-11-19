//! Contains common convenience methods for building `syn` syntactical constructs.

use syn::{punctuated::Punctuated, Expr, ExprPath, Ident, Path, PathArguments, PathSegment};

use std::iter::FromIterator;

/// Builds an expression from an identifier, i.e. just a named variable
pub fn plain_identifier_expr(ident: Ident) -> Expr {
    Expr::Path(ExprPath {
        attrs: vec![],
        qself: None,
        path: Path {
            leading_colon: None,
            segments: Punctuated::from_iter(vec![PathSegment {
                ident,
                arguments: PathArguments::None,
            }]),
        },
    })
}
