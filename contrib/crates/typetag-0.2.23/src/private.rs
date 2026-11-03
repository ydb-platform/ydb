#[doc(hidden)]
pub extern crate erased_serde;
#[doc(hidden)]
pub extern crate inventory;
#[doc(hidden)]
pub extern crate once_cell;
#[doc(hidden)]
pub extern crate serde;

#[doc(hidden)]
pub mod externally {
    #[doc(hidden)]
    pub use crate::externally::*;
}

#[doc(hidden)]
pub mod internally {
    #[doc(hidden)]
    pub use crate::internally::*;
}

#[doc(hidden)]
pub mod adjacently {
    #[doc(hidden)]
    pub use crate::adjacently::*;
}

#[doc(hidden)]
pub use alloc::collections::btree_map;
#[doc(hidden)]
pub use core::option::Option;
#[doc(hidden)]
pub use core::result::Result;

#[doc(hidden)]
pub type Box<T> = alloc::boxed::Box<T>;
#[doc(hidden)]
pub type BTreeMap<K, V> = alloc::collections::BTreeMap<K, V>;
#[doc(hidden)]
pub type BTreeMapEntry<'a, K, V> = alloc::collections::btree_map::Entry<'a, K, V>;
#[doc(hidden)]
pub type Vec<T> = alloc::vec::Vec<T>;

#[doc(hidden)]
pub type DeserializeFn<T> = fn(&mut dyn erased_serde::Deserializer) -> erased_serde::Result<Box<T>>;

#[doc(hidden)]
pub struct Registry<T: ?Sized> {
    #[doc(hidden)]
    pub map: BTreeMap<&'static str, Option<DeserializeFn<T>>>,
    #[doc(hidden)]
    pub names: Vec<&'static str>,
}

#[doc(hidden)]
pub trait Strictest {
    type Object: ?Sized;
}
