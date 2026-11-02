// Take a look at the license at the top of the repository in the LICENSE file.

use std::{
    mem,
    time::{Duration, SystemTime},
};

use glib::{StrV, translate::*};

use crate::{FileAttributeValue, FileInfo, ffi};

impl FileInfo {
    #[cfg_attr(feature = "v2_62", deprecated)]
    #[doc(alias = "g_file_info_get_modification_time")]
    #[doc(alias = "get_modification_time")]
    pub fn modification_time(&self) -> SystemTime {
        unsafe {
            let mut result = mem::MaybeUninit::uninit();
            ffi::g_file_info_get_modification_time(self.to_glib_none().0, result.as_mut_ptr());
            let result = result.assume_init();

            if result.tv_sec > 0 {
                let duration = Duration::from_secs(result.tv_sec as u64)
                    + Duration::from_millis(result.tv_usec as u64);
                SystemTime::UNIX_EPOCH + duration
            } else {
                let duration = Duration::from_secs((-result.tv_sec) as u64)
                    + Duration::from_millis(result.tv_usec as u64);
                SystemTime::UNIX_EPOCH - duration
            }
        }
    }

    #[cfg_attr(feature = "v2_62", deprecated)]
    #[doc(alias = "g_file_info_set_modification_time")]
    pub fn set_modification_time(&self, mtime: SystemTime) {
        let diff = mtime
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("failed to convert time");
        unsafe {
            ffi::g_file_info_set_modification_time(
                self.to_glib_none().0,
                mut_override(&glib::ffi::GTimeVal {
                    tv_sec: diff.as_secs() as _,
                    tv_usec: diff.subsec_micros() as _,
                }),
            );
        }
    }

    #[doc(alias = "g_file_info_get_attribute_stringv")]
    #[doc(alias = "get_attribute_stringv")]
    pub fn attribute_stringv(&self, attribute: &str) -> StrV {
        unsafe {
            FromGlibPtrContainer::from_glib_none(ffi::g_file_info_get_attribute_stringv(
                self.to_glib_none().0,
                attribute.to_glib_none().0,
            ))
        }
    }

    #[doc(alias = "g_file_info_set_attribute_stringv")]
    pub fn set_attribute_stringv(&self, attribute: &str, attr_value: impl IntoStrV) {
        unsafe {
            attr_value.run_with_strv(|attr_value| {
                ffi::g_file_info_set_attribute_stringv(
                    self.to_glib_none().0,
                    attribute.to_glib_none().0,
                    attr_value.as_ptr() as *mut _,
                );
            });
        }
    }

    #[doc(alias = "g_file_info_set_attribute")]
    pub fn set_attribute<'a>(&self, attribute: &str, value: impl Into<FileAttributeValue<'a>>) {
        unsafe {
            let value: FileAttributeValue<'a> = value.into();
            ffi::g_file_info_set_attribute(
                self.to_glib_none().0,
                attribute.to_glib_none().0,
                value.type_().into_glib(),
                value.as_ptr(),
            );
        }
    }
}
