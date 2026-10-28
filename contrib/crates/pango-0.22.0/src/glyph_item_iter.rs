// Take a look at the license at the top of the repository in the LICENSE file.

use std::{marker::PhantomData, mem};

use glib::{GStr, GString, prelude::*, translate::*};

use crate::{GlyphItem, ffi};

#[derive(Clone, Debug)]
pub struct GlyphItemIter<'item> {
    inner: ffi::PangoGlyphItemIter,
    text: GString,
    item: PhantomData<&'item GlyphItem>,
}

impl StaticType for GlyphItemIter<'_> {
    #[inline]
    fn static_type() -> glib::Type {
        unsafe { from_glib(ffi::pango_glyph_item_iter_get_type()) }
    }
}

impl<'item> GlyphItemIter<'item> {
    #[doc(alias = "pango_glyph_item_iter_init_start")]
    pub fn new_start(glyph_item: &'item GlyphItem, text: &str) -> Result<Self, glib::BoolError> {
        unsafe {
            let mut iter = mem::MaybeUninit::zeroed();
            let text = GString::from(text);
            let res: bool = from_glib(ffi::pango_glyph_item_iter_init_start(
                iter.as_mut_ptr(),
                mut_override(glyph_item.to_glib_none().0),
                text.as_ptr(),
            ));

            if res {
                Ok(Self {
                    inner: iter.assume_init(),
                    text,
                    item: PhantomData,
                })
            } else {
                Err(glib::bool_error!("Failed to create glyph item iter"))
            }
        }
    }

    #[doc(alias = "pango_glyph_item_iter_init_end")]
    pub fn new_end(glyph_item: &'item GlyphItem, text: &str) -> Result<Self, glib::BoolError> {
        unsafe {
            let mut iter = mem::MaybeUninit::zeroed();
            let text = GString::from(text);
            let res: bool = from_glib(ffi::pango_glyph_item_iter_init_end(
                iter.as_mut_ptr(),
                mut_override(glyph_item.to_glib_none().0),
                text.as_ptr(),
            ));

            if res {
                Ok(Self {
                    inner: iter.assume_init(),
                    text,
                    item: PhantomData,
                })
            } else {
                Err(glib::bool_error!("Failed to create glyph item iter"))
            }
        }
    }

    #[doc(alias = "pango_glyph_item_iter_next_cluster")]
    pub fn next_cluster(&mut self) -> bool {
        unsafe {
            from_glib(ffi::pango_glyph_item_iter_next_cluster(
                self.to_glib_none_mut().0,
            ))
        }
    }

    #[doc(alias = "pango_glyph_item_iter_prev_cluster")]
    pub fn prev_cluster(&mut self) -> bool {
        unsafe {
            from_glib(ffi::pango_glyph_item_iter_prev_cluster(
                self.to_glib_none_mut().0,
            ))
        }
    }

    #[inline]
    pub fn glyph_item(&self) -> &'item GlyphItem {
        unsafe { &*(&self.inner.glyph_item as *const _ as *const GlyphItem) }
    }
    #[inline]
    pub fn text(&self) -> &GStr {
        self.text.as_gstr()
    }
    #[inline]
    pub fn start_glyph(&self) -> i32 {
        self.inner.start_glyph
    }
    #[inline]
    pub fn start_index(&self) -> i32 {
        self.inner.start_index
    }
    #[inline]
    pub fn start_char(&self) -> i32 {
        self.inner.start_char
    }
    #[inline]
    pub fn end_glyph(&self) -> i32 {
        self.inner.end_glyph
    }
    #[inline]
    pub fn end_index(&self) -> i32 {
        self.inner.end_index
    }
    #[inline]
    pub fn end_char(&self) -> i32 {
        self.inner.end_char
    }
}

impl<'item> IntoIterator for GlyphItemIter<'item> {
    type Item = (i32, i32, i32, i32, i32, i32);
    type IntoIter = GlyphItemIntoIter<'item>;
    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        GlyphItemIntoIter(Some(self))
    }
}

#[derive(Clone, Debug)]
#[repr(transparent)]
pub struct GlyphItemIntoIter<'item>(Option<GlyphItemIter<'item>>);

impl Iterator for GlyphItemIntoIter<'_> {
    type Item = (i32, i32, i32, i32, i32, i32);
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(iter) = &mut self.0 {
            let values = (
                iter.start_glyph(),
                iter.start_index(),
                iter.start_char(),
                iter.end_glyph(),
                iter.end_index(),
                iter.end_char(),
            );
            if !iter.next_cluster() {
                self.0 = None;
            }
            Some(values)
        } else {
            None
        }
    }
}

impl std::iter::FusedIterator for GlyphItemIntoIter<'_> {}

#[doc(hidden)]
impl<'a, 'item> ToGlibPtr<'a, *const ffi::PangoGlyphItemIter> for GlyphItemIter<'item>
where
    'item: 'a,
{
    type Storage = PhantomData<&'a Self>;
    #[inline]
    fn to_glib_none(&'a self) -> Stash<'a, *const ffi::PangoGlyphItemIter, Self> {
        Stash(&self.inner, PhantomData)
    }
}

#[doc(hidden)]
impl<'a, 'item> ToGlibPtrMut<'a, *mut ffi::PangoGlyphItemIter> for GlyphItemIter<'item>
where
    'item: 'a,
{
    type Storage = PhantomData<&'a mut Self>;
    #[inline]
    fn to_glib_none_mut(&'a mut self) -> StashMut<'a, *mut ffi::PangoGlyphItemIter, Self> {
        StashMut(&mut self.inner, PhantomData)
    }
}
