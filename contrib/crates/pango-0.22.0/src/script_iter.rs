// Take a look at the license at the top of the repository in the LICENSE file.

use std::{marker::PhantomData, mem, ptr};

use glib::{GStr, translate::*};

use crate::{Script, ffi};

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ScriptIter<'text> {
    ptr: ptr::NonNull<ffi::PangoScriptIter>,
    text: PhantomData<&'text GStr>,
}

#[cfg(feature = "v1_44")]
#[cfg_attr(docsrs, doc(cfg(feature = "v1_44")))]
impl Clone for ScriptIter<'_> {
    #[inline]
    fn clone(&self) -> Self {
        let ptr = unsafe {
            ptr::NonNull::new_unchecked(glib::gobject_ffi::g_boxed_copy(
                ffi::pango_script_iter_get_type(),
                self.ptr.as_ptr() as *mut _,
            ) as *mut _)
        };
        Self {
            ptr,
            text: PhantomData,
        }
    }
}

impl Drop for ScriptIter<'_> {
    #[inline]
    fn drop(&mut self) {
        unsafe {
            ffi::pango_script_iter_free(self.ptr.as_ptr());
        }
    }
}

#[cfg(feature = "v1_44")]
#[cfg_attr(docsrs, doc(cfg(feature = "v1_44")))]
impl glib::prelude::StaticType for ScriptIter<'_> {
    #[inline]
    fn static_type() -> glib::Type {
        unsafe { from_glib(ffi::pango_script_iter_get_type()) }
    }
}

impl<'text> ScriptIter<'text> {
    #[doc(alias = "pango_script_iter_new")]
    pub fn new(text: impl AsRef<GStr> + 'text) -> Self {
        let text = text.as_ref();
        let length = text.len() as i32;
        unsafe { from_glib_full(ffi::pango_script_iter_new(text.as_ptr(), length)) }
    }

    #[doc(alias = "pango_script_iter_get_range")]
    #[doc(alias = "get_range")]
    pub fn range(&mut self) -> (&'text GStr, &'text GStr, Script) {
        unsafe {
            let mut start = ptr::null();
            let mut end = ptr::null();
            let mut script = mem::MaybeUninit::uninit();
            ffi::pango_script_iter_get_range(
                self.to_glib_none_mut().0,
                &mut start,
                &mut end,
                script.as_mut_ptr(),
            );
            (
                GStr::from_ptr(start),
                GStr::from_ptr(end),
                from_glib(script.assume_init()),
            )
        }
    }

    #[doc(alias = "pango_script_iter_next")]
    #[doc(alias = "next")]
    pub fn next_range(&mut self) -> bool {
        unsafe { from_glib(ffi::pango_script_iter_next(self.to_glib_none_mut().0)) }
    }
}

impl<'text> IntoIterator for ScriptIter<'text> {
    type Item = (&'text GStr, &'text GStr, Script);
    type IntoIter = ScriptIntoIter<'text>;

    fn into_iter(self) -> Self::IntoIter {
        ScriptIntoIter(Some(self))
    }
}

#[cfg_attr(feature = "v1_44", derive(Clone))]
#[derive(Debug)]
#[repr(transparent)]
pub struct ScriptIntoIter<'text>(Option<ScriptIter<'text>>);

impl<'text> Iterator for ScriptIntoIter<'text> {
    type Item = (&'text GStr, &'text GStr, Script);

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(iter) = &mut self.0 {
            let attrs = iter.range();
            if !iter.next_range() {
                self.0 = None;
            }
            Some(attrs)
        } else {
            None
        }
    }
}

impl std::iter::FusedIterator for ScriptIntoIter<'_> {}

#[doc(hidden)]
impl<'a, 'text> ToGlibPtr<'a, *const ffi::PangoScriptIter> for ScriptIter<'text>
where
    'text: 'a,
{
    type Storage = PhantomData<&'a Self>;
    #[inline]
    fn to_glib_none(&'a self) -> Stash<'a, *const ffi::PangoScriptIter, Self> {
        Stash(self.ptr.as_ptr() as *const _, PhantomData)
    }
}

#[doc(hidden)]
impl<'a, 'text> ToGlibPtrMut<'a, *mut ffi::PangoScriptIter> for ScriptIter<'text>
where
    'text: 'a,
{
    type Storage = PhantomData<&'a mut Self>;
    #[inline]
    fn to_glib_none_mut(&'a mut self) -> StashMut<'a, *mut ffi::PangoScriptIter, Self> {
        StashMut(self.ptr.as_ptr(), PhantomData)
    }
}

#[doc(hidden)]
impl FromGlibPtrFull<*mut ffi::PangoScriptIter> for ScriptIter<'_> {
    #[inline]
    unsafe fn from_glib_full(ptr: *mut ffi::PangoScriptIter) -> Self {
        unsafe {
            Self {
                ptr: ptr::NonNull::new_unchecked(ptr),
                text: PhantomData,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    const SCRIPTS: &glib::GStr = glib::gstr!(
        "\u{0020}\u{0946}\u{0939}\u{093F}\u{0928}\u{094D}\u{0926}\u{0940}\u{0020}\
         \u{0627}\u{0644}\u{0639}\u{0631}\u{0628}\u{064A}\u{0629}\u{0020}"
    );

    #[test]
    fn script_iter() {
        let iter = super::ScriptIter::new(SCRIPTS);
        let scripts = iter.into_iter().collect::<Vec<_>>();
        assert_eq!(scripts.len(), 2);
        assert_eq!(scripts[0].0, SCRIPTS);
        assert_eq!(scripts[0].1, &SCRIPTS[23..]);
        assert_eq!(scripts[0].2, crate::Script::Devanagari);
        assert_eq!(scripts[1].0, &SCRIPTS[23..]);
        assert_eq!(scripts[1].1, &SCRIPTS[38..]);
        assert_eq!(scripts[1].2, crate::Script::Arabic);
    }
}
