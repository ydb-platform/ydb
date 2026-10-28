// Take a look at the license at the top of the repository in the LICENSE file.

use std::io;

use crate::IOErrorEnum;
#[cfg(feature = "v2_74")]
use crate::glib::FileError;
#[cfg(feature = "v2_74")]
use crate::glib::translate::*;

impl From<IOErrorEnum> for io::ErrorKind {
    fn from(kind: IOErrorEnum) -> Self {
        match kind {
            IOErrorEnum::NotFound => Self::NotFound,
            IOErrorEnum::Exists => Self::AlreadyExists,
            IOErrorEnum::InvalidFilename => Self::InvalidInput,
            IOErrorEnum::InvalidArgument => Self::InvalidInput,
            IOErrorEnum::PermissionDenied => Self::PermissionDenied,
            IOErrorEnum::AddressInUse => Self::AddrInUse,
            IOErrorEnum::TimedOut => Self::TimedOut,
            IOErrorEnum::WouldBlock => Self::WouldBlock,
            IOErrorEnum::InvalidData => Self::InvalidData,
            IOErrorEnum::ConnectionRefused => Self::ConnectionRefused,
            IOErrorEnum::BrokenPipe => Self::BrokenPipe,
            IOErrorEnum::NotConnected => Self::NotConnected,
            _ => Self::Other,
        }
    }
}

pub(crate) fn to_std_io_result<T>(result: Result<T, glib::Error>) -> io::Result<T> {
    result.map_err(|g_error| match g_error.kind::<IOErrorEnum>() {
        Some(io_error_enum) => io::Error::new(io_error_enum.into(), g_error),
        None => io::Error::other(g_error),
    })
}

#[cfg(feature = "v2_74")]
#[cfg_attr(docsrs, doc(cfg(feature = "v2_74")))]
impl From<FileError> for IOErrorEnum {
    #[doc(alias = "g_io_error_from_file_error")]
    fn from(e: FileError) -> Self {
        unsafe { from_glib(crate::ffi::g_io_error_from_file_error(e.into_glib())) }
    }
}
