// Take a look at the license at the top of the repository in the LICENSE file.

use std::{marker::PhantomData, mem, ptr};

use glib::{SList, translate::*};

use crate::{AttrType, Attribute, FontDescription, Language, ffi};

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AttrIterator<'list> {
    ptr: ptr::NonNull<ffi::PangoAttrIterator>,
    list: PhantomData<&'list crate::AttrList>,
}

impl Clone for AttrIterator<'_> {
    #[inline]
    fn clone(&self) -> Self {
        let ptr = unsafe {
            ptr::NonNull::new_unchecked(ffi::pango_attr_iterator_copy(self.ptr.as_ptr()))
        };
        Self {
            ptr,
            list: PhantomData,
        }
    }
}

impl Drop for AttrIterator<'_> {
    #[inline]
    fn drop(&mut self) {
        unsafe {
            ffi::pango_attr_iterator_destroy(self.ptr.as_ptr());
        }
    }
}

#[cfg(feature = "v1_44")]
#[cfg_attr(docsrs, doc(cfg(feature = "v1_44")))]
impl glib::prelude::StaticType for AttrIterator<'_> {
    #[inline]
    fn static_type() -> glib::Type {
        unsafe { from_glib(ffi::pango_attr_iterator_get_type()) }
    }
}

impl AttrIterator<'_> {
    #[doc(alias = "pango_attr_iterator_get")]
    pub fn get(&self, type_: AttrType) -> Option<Attribute> {
        unsafe {
            from_glib_none(ffi::pango_attr_iterator_get(
                mut_override(self.to_glib_none().0),
                type_.into_glib(),
            ))
        }
    }

    #[doc(alias = "pango_attr_iterator_get_attrs")]
    #[doc(alias = "get_attrs")]
    pub fn attrs(&self) -> SList<Attribute> {
        unsafe {
            FromGlibPtrContainer::from_glib_full(ffi::pango_attr_iterator_get_attrs(mut_override(
                self.to_glib_none().0,
            )))
        }
    }

    #[doc(alias = "pango_attr_iterator_next")]
    pub fn next_style_change(&mut self) -> bool {
        unsafe { from_glib(ffi::pango_attr_iterator_next(self.to_glib_none_mut().0)) }
    }

    #[doc(alias = "pango_attr_iterator_range")]
    pub fn range(&self) -> (i32, i32) {
        unsafe {
            let mut start = mem::MaybeUninit::uninit();
            let mut end = mem::MaybeUninit::uninit();
            ffi::pango_attr_iterator_range(
                mut_override(self.to_glib_none().0),
                start.as_mut_ptr(),
                end.as_mut_ptr(),
            );
            let start = start.assume_init();
            let end = end.assume_init();
            (start, end)
        }
    }
    #[doc(alias = "pango_attr_iterator_get_font")]
    #[doc(alias = "get_font")]
    pub fn font(&self) -> (FontDescription, Option<Language>, SList<Attribute>) {
        unsafe {
            let desc = FontDescription::new();
            let mut language = mem::MaybeUninit::uninit();
            let mut extra_attrs = mem::MaybeUninit::uninit();

            ffi::pango_attr_iterator_get_font(
                mut_override(self.to_glib_none().0),
                mut_override(desc.to_glib_none().0),
                language.as_mut_ptr(),
                extra_attrs.as_mut_ptr(),
            );

            (
                desc,
                from_glib_full(language.assume_init()),
                FromGlibPtrContainer::from_glib_full(extra_attrs.assume_init()),
            )
        }
    }
}

impl<'list> IntoIterator for AttrIterator<'list> {
    type Item = SList<Attribute>;
    type IntoIter = AttrIntoIter<'list>;
    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        AttrIntoIter(Some(self))
    }
}

#[derive(Clone, Debug)]
#[repr(transparent)]
pub struct AttrIntoIter<'list>(Option<AttrIterator<'list>>);

impl Iterator for AttrIntoIter<'_> {
    type Item = SList<Attribute>;
    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(iter) = &mut self.0 {
            let attrs = iter.attrs();
            if !iter.next_style_change() {
                self.0 = None;
            }
            Some(attrs)
        } else {
            None
        }
    }
}

impl std::iter::FusedIterator for AttrIntoIter<'_> {}

#[doc(hidden)]
impl<'a, 'list> ToGlibPtr<'a, *const ffi::PangoAttrIterator> for AttrIterator<'list>
where
    'list: 'a,
{
    type Storage = PhantomData<&'a Self>;
    #[inline]
    fn to_glib_none(&'a self) -> Stash<'a, *const ffi::PangoAttrIterator, Self> {
        Stash(self.ptr.as_ptr() as *const _, PhantomData)
    }
}

#[doc(hidden)]
impl<'a, 'list> ToGlibPtrMut<'a, *mut ffi::PangoAttrIterator> for AttrIterator<'list>
where
    'list: 'a,
{
    type Storage = PhantomData<&'a mut Self>;
    #[inline]
    fn to_glib_none_mut(&'a mut self) -> StashMut<'a, *mut ffi::PangoAttrIterator, Self> {
        StashMut(self.ptr.as_ptr(), PhantomData)
    }
}

#[doc(hidden)]
impl FromGlibPtrFull<*mut ffi::PangoAttrIterator> for AttrIterator<'_> {
    #[inline]
    unsafe fn from_glib_full(ptr: *mut ffi::PangoAttrIterator) -> Self {
        unsafe {
            Self {
                ptr: ptr::NonNull::new_unchecked(ptr),
                list: PhantomData,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn attr_iterator() {
        let default_lang = crate::Language::default();
        let attributes = crate::AttrList::new();
        attributes.insert(crate::AttrColor::new_foreground(0x2000, 0x2000, 0x2000));
        attributes.insert(crate::AttrSize::new(10 * crate::SCALE));
        attributes.insert(crate::AttrLanguage::new(&default_lang));
        let iter = attributes.iterator();
        {
            let mut iter = iter.clone();
            loop {
                let (desc, lang, attrs) = iter.font();
                if !attrs.is_empty() {
                    assert_eq!(desc.size(), 10 * crate::SCALE);
                    assert_eq!(lang.map(|l| l.to_string()), Some(default_lang.to_string()));
                }
                for attr in attrs {
                    attr.downcast_ref::<crate::AttrColor>().unwrap();
                }
                if !iter.next_style_change() {
                    break;
                }
            }
        }
        let mut max = 0;
        for (i, mut attrs) in iter.into_iter().enumerate() {
            if i == 0 {
                attrs
                    .pop_front()
                    .unwrap()
                    .downcast_ref::<crate::AttrColor>()
                    .unwrap();
                attrs
                    .pop_front()
                    .unwrap()
                    .downcast_ref::<crate::AttrSize>()
                    .unwrap();
                attrs
                    .pop_front()
                    .unwrap()
                    .downcast_ref::<crate::AttrLanguage>()
                    .unwrap();
                assert!(attrs.is_empty());
            }
            max = i + 1;
        }
        // ensure the list was iterated
        assert!(max > 0);
    }
}
