// Take a look at the license at the top of the repository in the LICENSE file.

use glib::translate::*;

use crate::{AttrType, Language, ffi};

define_attribute_struct!(AttrLanguage, ffi::PangoAttrLanguage, &[AttrType::Language]);

impl AttrLanguage {
    #[doc(alias = "pango_attr_language_new")]
    pub fn new(language: &Language) -> Self {
        unsafe {
            from_glib_full(ffi::pango_attr_language_new(mut_override(
                language.to_glib_none().0,
            )))
        }
    }

    pub fn value(&self) -> Language {
        unsafe { from_glib_none(self.inner.value) }
    }
}
