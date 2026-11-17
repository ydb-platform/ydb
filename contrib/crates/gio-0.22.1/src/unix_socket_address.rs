// Take a look at the license at the top of the repository in the LICENSE file.

#[cfg(not(docsrs))]
use std::ffi::OsStr;
#[cfg(unix)]
#[cfg(not(docsrs))]
use std::os::unix::ffi::OsStrExt;
use std::{path, ptr, slice};

use glib::translate::*;

use crate::{SocketAddress, UnixSocketAddress, UnixSocketAddressType, ffi, prelude::*};

#[derive(Debug)]
pub enum UnixSocketAddressPath<'a> {
    Path(&'a path::Path),
    Anonymous,
    Abstract(&'a [u8]),
    AbstractPadded(&'a [u8]),
}

impl UnixSocketAddressPath<'_> {
    fn to_type(&self) -> UnixSocketAddressType {
        use self::UnixSocketAddressPath::*;

        match *self {
            Path(_) => UnixSocketAddressType::Path,
            Anonymous => UnixSocketAddressType::Anonymous,
            Abstract(_) => UnixSocketAddressType::Abstract,
            AbstractPadded(_) => UnixSocketAddressType::AbstractPadded,
        }
    }
}

impl UnixSocketAddress {
    #[doc(alias = "g_unix_socket_address_new")]
    pub fn new(path: &path::Path) -> UnixSocketAddress {
        unsafe {
            SocketAddress::from_glib_full(ffi::g_unix_socket_address_new(path.to_glib_none().0))
                .unsafe_cast()
        }
    }

    #[doc(alias = "g_unix_socket_address_new_with_type")]
    pub fn with_type(address_type: UnixSocketAddressPath) -> Self {
        use self::UnixSocketAddressPath::*;

        let type_ = address_type.to_type();
        let new = |ptr, len| unsafe {
            SocketAddress::from_glib_full(ffi::g_unix_socket_address_new_with_type(
                ptr,
                len,
                type_.into_glib(),
            ))
            .unsafe_cast()
        };
        match address_type {
            Path(path) => new(path.to_glib_none().0, -1),
            Abstract(path) | AbstractPadded(path) => new(
                path.to_glib_none().0 as *mut libc::c_char,
                path.len() as i32,
            ),
            Anonymous => new(ptr::null_mut(), 0),
        }
    }
}

pub trait UnixSocketAddressExtManual: IsA<UnixSocketAddress> + 'static {
    #[doc(alias = "g_unix_socket_address_get_path")]
    #[doc(alias = "get_path")]
    fn path(&self) -> Option<UnixSocketAddressPath<'_>> {
        use self::UnixSocketAddressPath::*;

        let path = unsafe {
            let path = ffi::g_unix_socket_address_get_path(self.as_ref().to_glib_none().0);
            if path.is_null() || self.path_len() == 0 {
                &[]
            } else {
                slice::from_raw_parts(path as *const u8, self.path_len())
            }
        };
        match self.address_type() {
            UnixSocketAddressType::Anonymous => Some(Anonymous),
            #[cfg(not(docsrs))]
            UnixSocketAddressType::Path => Some(Path(path::Path::new(OsStr::from_bytes(path)))),
            #[cfg(docsrs)]
            UnixSocketAddressType::Path => unreachable!(),
            UnixSocketAddressType::Abstract => Some(Abstract(path)),
            UnixSocketAddressType::AbstractPadded => Some(AbstractPadded(path)),
            UnixSocketAddressType::Invalid | UnixSocketAddressType::__Unknown(_) => None,
        }
    }
}

impl<O: IsA<UnixSocketAddress>> UnixSocketAddressExtManual for O {}

#[cfg(test)]
mod test {
    use super::*;

    // Check the actual path and len are correct and are not the underlying OsString
    #[test]
    fn check_path() {
        let mut os_string = std::ffi::OsString::with_capacity(100);
        os_string.push("/tmp/foo");
        let path = os_string.as_ref();

        let addr = UnixSocketAddress::new(path);
        assert_eq!(addr.path_len(), 8);
        assert_eq!(addr.path_as_array().unwrap().as_ref(), b"/tmp/foo");

        let addr = UnixSocketAddress::with_type(UnixSocketAddressPath::Path(path));
        assert_eq!(addr.path_len(), 8);
        assert_eq!(addr.path_as_array().unwrap().as_ref(), b"/tmp/foo");
    }
}
