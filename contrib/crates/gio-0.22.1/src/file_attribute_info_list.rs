// Take a look at the license at the top of the repository in the LICENSE file.

use glib::translate::*;

use crate::{FileAttributeInfo, FileAttributeInfoList, ffi};

impl FileAttributeInfoList {
    #[doc(alias = "g_file_attribute_info_list_lookup")]
    pub fn lookup(&self, name: &str) -> Option<FileAttributeInfo> {
        unsafe {
            let res = ffi::g_file_attribute_info_list_lookup(
                self.to_glib_none().0,
                name.to_glib_none().0,
            );
            if res.is_null() {
                None
            } else {
                Some(from_glib_none(res))
            }
        }
    }

    pub fn attributes(&self) -> Vec<FileAttributeInfo> {
        unsafe {
            let ptr: *const _ = self.to_glib_none().0;
            FromGlibContainer::from_glib_none_num((*ptr).infos, (*ptr).n_infos as usize)
        }
    }
}
