// Take a look at the license at the top of the repository in the LICENSE file.

use std::fmt;

use glib::translate::*;

use crate::{Attribute, Font, Gravity, Language, Script};

glib::wrapper! {
    #[doc(alias = "PangoAnalysis")]
    pub struct Analysis(BoxedInline<crate::ffi::PangoAnalysis>);
}

impl Analysis {
    #[inline]
    pub fn font(&self) -> Font {
        unsafe { from_glib_none(self.inner.font) }
    }

    #[inline]
    pub fn level(&self) -> u8 {
        self.inner.level
    }

    #[inline]
    pub fn gravity(&self) -> Gravity {
        unsafe { from_glib(self.inner.gravity as i32) }
    }

    #[inline]
    pub fn flags(&self) -> u8 {
        self.inner.flags
    }

    #[inline]
    pub fn script(&self) -> Script {
        unsafe { from_glib(self.inner.script as i32) }
    }

    #[inline]
    pub fn language(&self) -> Language {
        unsafe { from_glib_none(self.inner.language) }
    }

    pub fn extra_attrs(&self) -> Vec<Attribute> {
        unsafe { FromGlibPtrContainer::from_glib_none(self.inner.extra_attrs) }
    }
}

impl fmt::Debug for Analysis {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Analysis")
            .field("font", &self.font())
            .field("level", &self.level())
            .field("gravity", &self.gravity())
            .field("flags", &self.flags())
            .field("script", &self.script())
            .field("extra_attrs", &self.extra_attrs())
            .finish()
    }
}
