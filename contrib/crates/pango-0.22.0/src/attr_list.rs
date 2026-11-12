// Take a look at the license at the top of the repository in the LICENSE file.

use std::mem;

use glib::translate::*;

use crate::{AttrIterator, AttrList, Attribute, ffi};

impl AttrList {
    #[doc(alias = "pango_attr_list_change")]
    pub fn change(&self, attr: impl Into<Attribute>) {
        unsafe {
            let attr = attr.into();
            ffi::pango_attr_list_change(self.to_glib_none().0, attr.to_glib_none().0);
            mem::forget(attr); //As attr transferred fully
        }
    }

    #[cfg(feature = "v1_46")]
    #[cfg_attr(docsrs, doc(cfg(feature = "v1_46")))]
    #[doc(alias = "pango_attr_list_equal")]
    fn equal(&self, other_list: &AttrList) -> bool {
        unsafe {
            from_glib(ffi::pango_attr_list_equal(
                self.to_glib_none().0,
                other_list.to_glib_none().0,
            ))
        }
    }

    #[doc(alias = "pango_attr_list_insert")]
    pub fn insert(&self, attr: impl Into<Attribute>) {
        unsafe {
            let attr = attr.into();
            ffi::pango_attr_list_insert(self.to_glib_none().0, attr.to_glib_none().0);
            mem::forget(attr); //As attr transferred fully
        }
    }

    #[doc(alias = "pango_attr_list_insert_before")]
    pub fn insert_before(&self, attr: impl Into<Attribute>) {
        unsafe {
            let attr = attr.into();
            ffi::pango_attr_list_insert_before(self.to_glib_none().0, attr.to_glib_none().0);
            mem::forget(attr); //As attr transferred fully
        }
    }

    #[doc(alias = "pango_attr_list_get_iterator")]
    #[doc(alias = "get_iterator")]
    pub fn iterator(&self) -> AttrIterator<'_> {
        unsafe { from_glib_full(ffi::pango_attr_list_get_iterator(self.to_glib_none().0)) }
    }
}

#[cfg(feature = "v1_46")]
#[cfg_attr(docsrs, doc(cfg(feature = "v1_46")))]
impl PartialEq for AttrList {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.equal(other)
    }
}

#[cfg(feature = "v1_46")]
#[cfg_attr(docsrs, doc(cfg(feature = "v1_46")))]
impl Eq for AttrList {}

#[cfg(feature = "v1_50")]
#[cfg_attr(docsrs, doc(cfg(feature = "v1_50")))]
impl std::str::FromStr for AttrList {
    type Err = glib::BoolError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::from_string(s)
    }
}
