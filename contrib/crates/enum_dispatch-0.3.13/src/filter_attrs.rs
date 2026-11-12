//! Convenience traits for splitting inner and outer attributes. These were originally private to
//! the syn crate.

/// Private trait copied from syn::attr.rs for convenience when implementing ToTokens
pub trait FilterAttrs<'a> {
    type Ret: Iterator<Item = &'a syn::Attribute>;

    fn outer(self) -> Self::Ret;
}

/// Private trait impl copied from syn::attr.rs for convenience when implementing ToTokens
impl<'a, T> FilterAttrs<'a> for T
where
    T: IntoIterator<Item = &'a syn::Attribute>,
{
    type Ret = ::std::iter::Filter<T::IntoIter, fn(&&syn::Attribute) -> bool>;

    fn outer(self) -> Self::Ret {
        fn is_outer(attr: &&syn::Attribute) -> bool {
            matches!(attr.style, syn::AttrStyle::Outer)
        }
        self.into_iter().filter(is_outer)
    }
}
