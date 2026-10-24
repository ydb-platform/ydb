use super::SpannedKey;
use crate::util::prelude::*;
use darling::FromMeta;

/// "Item signature" is a set of parameters that configures some aspects of
/// an item like a function, struct, struct field, module, trait. All of them
/// have configurable properties that are specified here.
///
/// The generic parameter `N` specifies the type used for the name field:
/// - `syn::Ident` (default): For regular identifiers
/// - `String`: For pattern strings (e.g., "conv_{}")
#[derive(Debug, Clone)]
pub(crate) struct ItemSigConfig<N = syn::Ident> {
    pub(crate) name: Option<SpannedKey<N>>,
    pub(crate) vis: Option<SpannedKey<syn::Visibility>>,
    pub(crate) docs: Option<SpannedKey<Vec<syn::Attribute>>>,
}

impl<N> Default for ItemSigConfig<N> {
    fn default() -> Self {
        Self {
            name: None,
            vis: None,
            docs: None,
        }
    }
}

impl ItemSigConfig<syn::Ident> {
    pub(crate) fn name(&self) -> Option<&syn::Ident> {
        self.name.as_ref().map(|name| &name.value)
    }
}

impl<N> ItemSigConfig<N> {
    pub(crate) fn vis(&self) -> Option<&syn::Visibility> {
        self.vis.as_ref().map(|vis| &vis.value)
    }

    pub(crate) fn docs(&self) -> Option<&[syn::Attribute]> {
        self.docs.as_ref().map(|docs| docs.value.as_slice())
    }
}

pub(crate) struct ItemSigConfigParsing<'a> {
    pub(crate) meta: &'a syn::Meta,
    pub(crate) reject_self_mentions: Option<&'static str>,
}

impl<'a> ItemSigConfigParsing<'a> {
    pub(crate) fn new(meta: &'a syn::Meta, reject_self_mentions: Option<&'static str>) -> Self {
        ItemSigConfigParsing {
            meta,
            reject_self_mentions,
        }
    }

    pub(crate) fn parse<N>(self) -> Result<ItemSigConfig<N>>
    where
        N: FromMeta,
    {
        let meta = self.meta;

        if let syn::Meta::NameValue(_) = meta {
            let name = SpannedKey::from_meta(meta)?;
            return Ok(ItemSigConfig {
                name: Some(name),
                vis: None,
                docs: None,
            });
        }

        #[derive(Debug, FromMeta)]
        struct Full<N> {
            name: Option<SpannedKey<N>>,
            vis: Option<SpannedKey<syn::Visibility>>,

            #[darling(default, with = super::parse_docs, map = Some)]
            doc: Option<SpannedKey<Vec<syn::Attribute>>>,
        }

        let full: Full<N> = crate::parsing::parse_non_empty_paren_meta_list(meta)?;

        if let Some(context) = self.reject_self_mentions {
            if let Some(docs) = &full.doc {
                crate::parsing::reject_self_mentions_in_docs(context, docs)?;
            }
        }

        let config = ItemSigConfig {
            name: full.name,
            vis: full.vis,
            docs: full.doc,
        };

        Ok(config)
    }
}
