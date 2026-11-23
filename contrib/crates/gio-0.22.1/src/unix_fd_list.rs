// Take a look at the license at the top of the repository in the LICENSE file.

#[cfg(unix)]
use std::os::unix::io::{AsFd, AsRawFd, FromRawFd, IntoRawFd, OwnedFd, RawFd};
use std::{mem, ptr};

use glib::{prelude::*, translate::*};
#[cfg(all(not(unix), docsrs))]
use socket::{AsFd, AsRawFd, FromRawFd, IntoRawFd, OwnedFd, RawFd};

use crate::{UnixFDList, ffi};

impl UnixFDList {
    #[doc(alias = "g_unix_fd_list_new_from_array")]
    pub fn from_array(fds: impl IntoIterator<Item = impl IntoRawFd>) -> UnixFDList {
        let fds = fds.into_iter().map(|t| t.into_raw_fd()).collect::<Vec<_>>();
        unsafe {
            from_glib_full(ffi::g_unix_fd_list_new_from_array(
                fds.to_glib_none().0,
                fds.len() as i32,
            ))
        }
    }
}

pub trait UnixFDListExtManual: IsA<UnixFDList> + Sized {
    #[doc(alias = "g_unix_fd_list_append")]
    fn append(&self, fd: impl AsFd) -> Result<i32, glib::Error> {
        unsafe {
            let mut error = ptr::null_mut();
            let ret = ffi::g_unix_fd_list_append(
                self.as_ref().to_glib_none().0,
                fd.as_fd().as_raw_fd(),
                &mut error,
            );
            if error.is_null() {
                Ok(ret)
            } else {
                Err(from_glib_full(error))
            }
        }
    }

    #[doc(alias = "g_unix_fd_list_get")]
    fn get(&self, index_: i32) -> Result<OwnedFd, glib::Error> {
        unsafe {
            let mut error = ptr::null_mut();
            let raw_fd =
                ffi::g_unix_fd_list_get(self.as_ref().to_glib_none().0, index_, &mut error);
            if error.is_null() {
                let fd = OwnedFd::from_raw_fd(raw_fd);
                Ok(fd)
            } else {
                Err(from_glib_full(error))
            }
        }
    }

    #[doc(alias = "g_unix_fd_list_peek_fds")]
    fn peek_fds(&self) -> Vec<RawFd> {
        unsafe {
            let mut length = mem::MaybeUninit::uninit();

            FromGlibContainer::from_glib_none_num(
                ffi::g_unix_fd_list_peek_fds(self.as_ref().to_glib_none().0, length.as_mut_ptr()),
                length.assume_init() as usize,
            )
        }
    }

    #[doc(alias = "g_unix_fd_list_steal_fds")]
    fn steal_fds(&self) -> Vec<RawFd> {
        unsafe {
            let mut length = mem::MaybeUninit::uninit();

            FromGlibContainer::from_glib_full_num(
                ffi::g_unix_fd_list_steal_fds(self.as_ref().to_glib_none().0, length.as_mut_ptr()),
                length.assume_init() as usize,
            )
        }
    }
}

impl<O: IsA<UnixFDList>> UnixFDListExtManual for O {}
