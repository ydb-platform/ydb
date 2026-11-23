// Take a look at the license at the top of the repository in the LICENSE file.

use glib::translate::*;

use crate::{Credentials, CredentialsType, ffi};

impl Credentials {
    #[doc(alias = "g_credentials_get_native")]
    #[doc(alias = "get_native")]
    pub fn native(&self, native_type: CredentialsType) -> glib::ffi::gpointer {
        unsafe { ffi::g_credentials_get_native(self.to_glib_none().0, native_type.into_glib()) }
    }

    #[cfg(unix)]
    #[cfg_attr(docsrs, doc(cfg(unix)))]
    #[doc(alias = "g_credentials_get_unix_pid")]
    #[doc(alias = "get_unix_pid")]
    pub fn unix_pid(&self) -> Result<libc::pid_t, glib::Error> {
        unsafe {
            let mut error = std::ptr::null_mut();
            let ret = ffi::g_credentials_get_unix_pid(self.to_glib_none().0, &mut error);
            if error.is_null() {
                Ok(ret)
            } else {
                Err(from_glib_full(error))
            }
        }
    }

    #[cfg(unix)]
    #[cfg_attr(docsrs, doc(cfg(unix)))]
    #[doc(alias = "g_credentials_get_unix_user")]
    #[doc(alias = "get_unix_user")]
    pub fn unix_user(&self) -> Result<libc::uid_t, glib::Error> {
        unsafe {
            let mut error = std::ptr::null_mut();
            let ret = ffi::g_credentials_get_unix_user(self.to_glib_none().0, &mut error);
            if error.is_null() {
                Ok(ret)
            } else {
                Err(from_glib_full(error))
            }
        }
    }

    #[doc(alias = "g_credentials_set_native")]
    pub unsafe fn set_native(&self, native_type: CredentialsType, native: glib::ffi::gpointer) {
        unsafe {
            ffi::g_credentials_set_native(self.to_glib_none().0, native_type.into_glib(), native)
        }
    }

    #[cfg(unix)]
    #[cfg_attr(docsrs, doc(cfg(unix)))]
    #[doc(alias = "g_credentials_set_unix_user")]
    pub fn set_unix_user(&self, uid: libc::uid_t) -> Result<(), glib::Error> {
        unsafe {
            let mut error = std::ptr::null_mut();
            let is_ok = ffi::g_credentials_set_unix_user(self.to_glib_none().0, uid, &mut error);
            debug_assert_eq!(is_ok == glib::ffi::GFALSE, !error.is_null());
            if error.is_null() {
                Ok(())
            } else {
                Err(from_glib_full(error))
            }
        }
    }
}
