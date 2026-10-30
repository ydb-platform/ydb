// Take a look at the license at the top of the repository in the LICENSE file.

#[cfg(any(unix, all(docsrs, unix)))]
use std::os::unix::io::{AsFd, AsRawFd, IntoRawFd, OwnedFd};

use glib::translate::*;

use crate::SubprocessLauncher;
use crate::ffi;

#[cfg(all(docsrs, not(unix)))]
pub trait IntoRawFd: Sized {
    fn into_raw_fd(self) -> i32 {
        0
    }
}

impl SubprocessLauncher {
    #[doc(alias = "g_subprocess_launcher_set_environ")]
    pub fn set_environ(&self, env: &[std::ffi::OsString]) {
        unsafe {
            ffi::g_subprocess_launcher_set_environ(self.to_glib_none().0, env.to_glib_none().0);
        }
    }

    #[cfg(unix)]
    #[cfg_attr(docsrs, doc(cfg(unix)))]
    #[doc(alias = "g_subprocess_launcher_take_fd")]
    pub fn take_fd(&self, source_fd: OwnedFd, target_fd: impl AsFd) {
        let source_raw_fd = source_fd.into_raw_fd();
        let target_raw_fd = target_fd.as_fd().as_raw_fd();
        unsafe {
            ffi::g_subprocess_launcher_take_fd(self.to_glib_none().0, source_raw_fd, target_raw_fd);
        }
    }

    #[cfg(unix)]
    #[cfg_attr(docsrs, doc(cfg(unix)))]
    #[doc(alias = "g_subprocess_launcher_take_stderr_fd")]
    pub fn take_stderr_fd(&self, fd: Option<OwnedFd>) {
        unsafe {
            let raw_fd = fd.map_or(-1, |fd| fd.into_raw_fd());
            ffi::g_subprocess_launcher_take_stderr_fd(self.to_glib_none().0, raw_fd);
        }
    }

    #[cfg(unix)]
    #[cfg_attr(docsrs, doc(cfg(unix)))]
    #[doc(alias = "g_subprocess_launcher_take_stdin_fd")]
    pub fn take_stdin_fd(&self, fd: Option<OwnedFd>) {
        let raw_fd = fd.map_or(-1, |fd| fd.into_raw_fd());
        unsafe {
            ffi::g_subprocess_launcher_take_stdin_fd(self.to_glib_none().0, raw_fd);
        }
    }

    #[cfg(unix)]
    #[cfg_attr(docsrs, doc(cfg(unix)))]
    #[doc(alias = "g_subprocess_launcher_take_stdout_fd")]
    pub fn take_stdout_fd(&self, fd: Option<OwnedFd>) {
        let raw_fd = fd.map_or(-1, |fd| fd.into_raw_fd());
        unsafe {
            ffi::g_subprocess_launcher_take_stdout_fd(self.to_glib_none().0, raw_fd);
        }
    }
}
