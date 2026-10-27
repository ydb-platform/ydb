// Take a look at the license at the top of the repository in the LICENSE file.

use std::{ffi::CStr, fmt};

use crate::{Error, ffi};

// rustdoc-stripper-ignore-next
/// Resets all static data within cairo to its original state (i.e. identical to the state at program
/// invocation). For example, all caches within cairo will be flushed empty.
///
/// # Safety
/// It is only safe to call this function when there are no active cairo objects remaining (all
/// cairo objects have been dropped).
///
/// This function is thread safe.
#[doc(alias = "cairo_debug_reset_static_data")]
pub unsafe fn debug_reset_static_data() {
    unsafe { ffi::cairo_debug_reset_static_data() }
}

pub fn status_to_result(status: ffi::cairo_status_t) -> Result<(), Error> {
    match status {
        ffi::STATUS_SUCCESS => Ok(()),
        err => Err(err.into()),
    }
}

#[doc(alias = "cairo_version_string")]
#[doc(alias = "get_version_string")]
pub fn version_string() -> &'static str {
    unsafe {
        let ptr = ffi::cairo_version_string();
        CStr::from_ptr(ptr)
            .to_str()
            .expect("invalid version string")
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
pub struct Version {
    major: u8,
    minor: u8,
    micro: u8,
}

impl Version {
    #[doc(alias = "cairo_version")]
    #[doc(alias = "get_version")]
    pub fn new() -> Version {
        let version = unsafe { ffi::cairo_version() };
        Version {
            major: (version / 10_000 % 100) as _,
            minor: (version / 100 % 100) as _,
            micro: (version % 100) as _,
        }
    }

    pub fn major(self) -> u8 {
        self.major
    }
    pub fn minor(self) -> u8 {
        self.minor
    }
    pub fn micro(self) -> u8 {
        self.micro
    }
}

impl Default for Version {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for Version {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}.{}.{}", self.major, self.minor, self.micro)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn check_versions() {
        assert_eq!(version_string(), Version::new().to_string());
    }
}
