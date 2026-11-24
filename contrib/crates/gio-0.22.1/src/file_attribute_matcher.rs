// Take a look at the license at the top of the repository in the LICENSE file.

use std::iter::FusedIterator;

use crate::ffi;
use glib::{GString, translate::*};

pub struct FileAttributematcherIter(crate::FileAttributeMatcher);

impl Iterator for FileAttributematcherIter {
    type Item = GString;

    #[doc(alias = "g_file_attribute_matcher_enumerate_next")]
    fn next(&mut self) -> Option<GString> {
        unsafe {
            from_glib_none(ffi::g_file_attribute_matcher_enumerate_next(
                self.0.to_glib_none().0,
            ))
        }
    }
}

impl FusedIterator for FileAttributematcherIter {}

impl IntoIterator for crate::FileAttributeMatcher {
    type Item = GString;
    type IntoIter = FileAttributematcherIter;

    fn into_iter(self) -> Self::IntoIter {
        FileAttributematcherIter(self)
    }
}
