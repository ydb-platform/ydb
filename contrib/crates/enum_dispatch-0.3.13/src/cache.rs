//! Procedural macros don't offer a good way to store information between macro invocations.  In
//! addition, all syntax-related structures implement `!Send` and `!Sync`, making it impossible to
//! keep them in any sort of static storage. This module uses some workarounds to add that
//! functionality.
//!
//! Fortunately, `TokenStream`s can be converted to and from `String`s, which can be stored
//! statically. Unfortunately, doing so strips any related `Span` information, preventing error
//! messages from being as informative as they could be. For now, it seems this is the best option
//! available.
use quote::ToTokens;

use once_cell::sync::Lazy;

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

use crate::enum_dispatch_item;

/// Uniquely identifies a trait or an enum. This is based on its name and number of arguments.
#[derive(PartialEq, Eq, Hash, Clone)]
struct UniqueItemId {
    item_name: String,
    num_generics: usize,
}

impl UniqueItemId {
    /// Convenience constructor for UniqueItemId.
    pub fn new(item_name: String, num_generics: usize) -> Self {
        Self {
            item_name,
            num_generics,
        }
    }
}

// Magical storage for trait definitions so that they can be used when parsing other syntax
// structures.
static TRAIT_DEFS: Lazy<Mutex<HashMap<UniqueItemId, String>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));
static ENUM_DEFS: Lazy<Mutex<HashMap<UniqueItemId, String>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));
static DEFERRED_LINKS: Lazy<Mutex<HashMap<UniqueItemId, Vec<UniqueItemId>>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));
static ENUM_CONVERSION_IMPLS_DEFS: Lazy<Mutex<HashSet<UniqueItemId>>> =
    Lazy::new(|| Mutex::new(HashSet::new()));

/// Store a trait definition for future reference.
pub fn cache_trait(item: syn::ItemTrait) {
    let num_generics = crate::supported_generics::num_supported_generics(&item.generics);
    let uid = UniqueItemId::new(item.ident.to_string(), num_generics);
    TRAIT_DEFS
        .lock()
        .unwrap()
        .insert(uid, item.into_token_stream().to_string());
}

/// Store an enum definition for future reference.
pub fn cache_enum_dispatch(item: enum_dispatch_item::EnumDispatchItem) {
    let num_generics = crate::supported_generics::num_supported_generics(&item.generics);
    let uid = UniqueItemId::new(item.ident.to_string(), num_generics);
    ENUM_DEFS
        .lock()
        .unwrap()
        .insert(uid, item.into_token_stream().to_string());
}

/// Store whether a From/TryInto definition has been defined once for an enum.
pub fn cache_enum_conversion_impls_defined(item: syn::Ident, num_generics: usize) {
    let uid = UniqueItemId::new(item.to_string(), num_generics);
    ENUM_CONVERSION_IMPLS_DEFS.lock().unwrap().insert(uid);
}

/// Cache a "link" to be fulfilled once the needed definition is also cached.
///
/// The number of generic arguments is also cached and must be equal in order to fulfill a link,
/// however the actual generic arguments themselves may have different names.
pub fn defer_link(
    (needed, needed_num_generics): (&::proc_macro2::Ident, usize),
    (cached, cached_num_generics): (&::proc_macro2::Ident, usize),
) {
    use std::collections::hash_map::Entry;

    let (needed, cached) = (
        UniqueItemId::new(needed.to_string(), needed_num_generics),
        UniqueItemId::new(cached.to_string(), cached_num_generics),
    );
    let mut deferred_links = DEFERRED_LINKS.lock().unwrap();
    if deferred_links.contains_key(&needed) {
        deferred_links
            .get_mut(&needed)
            .unwrap()
            .push(cached.to_owned());
    } else {
        deferred_links.insert(needed.to_owned(), vec![cached.to_owned()]);
    }
    if let Entry::Vacant(e) = deferred_links.entry(cached.clone()) {
        e.insert(vec![needed]);
    } else {
        deferred_links.get_mut(&cached).unwrap().push(needed);
    }
}

/// Returns a list of all of the trait definitions that were previously linked to the supplied enum
/// name.
pub fn fulfilled_by_enum(
    defname: &::proc_macro2::Ident,
    num_generic_args: usize,
) -> Vec<syn::ItemTrait> {
    let idents = match DEFERRED_LINKS
        .lock()
        .unwrap()
        .remove_entry(&UniqueItemId::new(defname.to_string(), num_generic_args))
    {
        Some((_, links)) => links,
        None => vec![],
    };
    idents
        .iter()
        .filter_map(
            |ident_string| TRAIT_DEFS.lock().unwrap().get(ident_string).map(|entry| syn::parse(entry.parse().unwrap()).unwrap())
        )
        .collect()
}

/// Returns a list of all of the enum definitions that were previously linked to the supplied trait
/// name.
pub fn fulfilled_by_trait(
    defname: &::proc_macro2::Ident,
    num_generic_args: usize,
) -> Vec<enum_dispatch_item::EnumDispatchItem> {
    let idents = match DEFERRED_LINKS
        .lock()
        .unwrap()
        .remove_entry(&UniqueItemId::new(defname.to_string(), num_generic_args))
    {
        Some((_, links)) => links,
        None => vec![],
    };
    idents
        .iter()
        .filter_map(
            |ident_string| ENUM_DEFS.lock().unwrap().get(ident_string).map(|entry| syn::parse(entry.parse().unwrap()).unwrap())
        )
        .collect()
}

/// Returns true if From/TryInto was already defined for this enum
pub fn conversion_impls_def_by_enum(item: &syn::Ident, num_generics: usize) -> bool {
    ENUM_CONVERSION_IMPLS_DEFS
        .lock()
        .unwrap()
        .contains(&UniqueItemId::new(item.to_string(), num_generics))
}
