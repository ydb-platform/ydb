// Take a look at the license at the top of the repository in the LICENSE file.

use crate::ffi;
use std::{fmt, fmt::Debug, io};

#[derive(Debug, Clone, PartialEq, Copy, Eq)]
#[non_exhaustive]
#[doc(alias = "cairo_status_t")]
pub enum Error {
    #[doc(alias = "STATUS_NO_MEMORY")]
    NoMemory,
    #[doc(alias = "STATUS_INVALID_RESTORE")]
    InvalidRestore,
    #[doc(alias = "STATUS_INVALID_POP_GROUP")]
    InvalidPopGroup,
    #[doc(alias = "STATUS_NO_CURRENT_POINT")]
    NoCurrentPoint,
    #[doc(alias = "STATUS_INVALID_MATRIX")]
    InvalidMatrix,
    #[doc(alias = "STATUS_INVALID_STATUS")]
    InvalidStatus,
    #[doc(alias = "STATUS_NULL_POINTER")]
    NullPointer,
    #[doc(alias = "STATUS_INVALID_STRING")]
    InvalidString,
    #[doc(alias = "STATUS_INVALID_PATH_DATA")]
    InvalidPathData,
    #[doc(alias = "STATUS_READ_ERROR")]
    ReadError,
    #[doc(alias = "STATUS_WRITE_ERROR")]
    WriteError,
    #[doc(alias = "STATUS_SURFACE_FINISHED")]
    SurfaceFinished,
    #[doc(alias = "STATUS_SURFACE_TYPE_MISMATCH")]
    SurfaceTypeMismatch,
    #[doc(alias = "STATUS_PATTERN_TYPE_MISMATCH")]
    PatternTypeMismatch,
    #[doc(alias = "STATUS_INVALID_CONTENT")]
    InvalidContent,
    #[doc(alias = "STATUS_INVALID_FORMAT")]
    InvalidFormat,
    #[doc(alias = "STATUS_INVALID_VISUAL")]
    InvalidVisual,
    #[doc(alias = "STATUS_FILE_NOT_FOUND")]
    FileNotFound,
    #[doc(alias = "STATUS_INVALID_DASH")]
    InvalidDash,
    #[doc(alias = "STATUS_INVALID_DSC_COMMENT")]
    InvalidDscComment,
    #[doc(alias = "STATUS_INVALID_INDEX")]
    InvalidIndex,
    #[doc(alias = "STATUS_CLIP_NOT_REPRESENTABLE")]
    ClipNotRepresentable,
    #[doc(alias = "STATUS_TEMP_FILE_ERROR")]
    TempFileError,
    #[doc(alias = "STATUS_INVALID_STRIDE")]
    InvalidStride,
    #[doc(alias = "STATUS_FONT_TYPE_MISMATCH")]
    FontTypeMismatch,
    #[doc(alias = "STATUS_USER_FONT_IMMUTABLE")]
    UserFontImmutable,
    #[doc(alias = "STATUS_USER_FONT_ERROR")]
    UserFontError,
    #[doc(alias = "STATUS_NEGATIVE_COUNT")]
    NegativeCount,
    #[doc(alias = "STATUS_INVALID_CLUSTERS")]
    InvalidClusters,
    #[doc(alias = "STATUS_INVALID_SLANT")]
    InvalidSlant,
    #[doc(alias = "STATUS_INVALID_WEIGHT")]
    InvalidWeight,
    #[doc(alias = "STATUS_INVALID_SIZE")]
    InvalidSize,
    #[doc(alias = "STATUS_USER_FONT_NOT_IMPLEMENTED")]
    UserFontNotImplemented,
    #[doc(alias = "STATUS_DEVICE_TYPE_MISMATCH")]
    DeviceTypeMismatch,
    #[doc(alias = "STATUS_DEVICE_ERROR")]
    DeviceError,
    #[doc(alias = "STATUS_INVALID_MESH_CONSTRUCTION")]
    InvalidMeshConstruction,
    #[doc(alias = "STATUS_DEVICE_FINISHED")]
    DeviceFinished,
    #[doc(alias = "STATUS_J_BIG2_GLOBAL_MISSING")]
    JBig2GlobalMissing,
    #[doc(alias = "STATUS_PNG_ERROR")]
    PngError,
    #[doc(alias = "STATUS_FREETYPE_ERROR")]
    FreetypeError,
    #[doc(alias = "STATUS_WIN32_GDI_ERROR")]
    Win32GdiError,
    #[cfg(feature = "v1_16")]
    #[cfg_attr(docsrs, doc(cfg(feature = "v1_16")))]
    #[doc(alias = "STATUS_TAG_ERROR")]
    TagError,
    #[cfg(feature = "v1_18")]
    #[cfg_attr(docsrs, doc(cfg(feature = "v1_18")))]
    #[doc(alias = "STATUS_DWRITE_ERROR")]
    DwriteError,
    #[doc(alias = "STATUS_LAST_STATUS")]
    LastStatus,
    #[doc(hidden)]
    __Unknown(i32),
}

impl std::error::Error for Error {}

impl fmt::Display for Error {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::NoMemory => fmt.write_str("No Memory"),
            Error::InvalidRestore => fmt.write_str("Invalid Restore"),
            Error::InvalidPopGroup => fmt.write_str("Invalid Pop Group"),
            Error::NoCurrentPoint => fmt.write_str("No Current Point"),
            Error::InvalidMatrix => fmt.write_str("Invalid Matrix"),
            Error::InvalidStatus => fmt.write_str("Invalid Status"),
            Error::NullPointer => fmt.write_str("Null Pointer"),
            Error::InvalidString => fmt.write_str("Invalid String"),
            Error::InvalidPathData => fmt.write_str("Invalid Path Data"),
            Error::ReadError => fmt.write_str("Read Error"),
            Error::WriteError => fmt.write_str("Write Error"),
            Error::SurfaceFinished => fmt.write_str("Surface Finished"),
            Error::SurfaceTypeMismatch => fmt.write_str("Surface Type Mismatch"),
            Error::PatternTypeMismatch => fmt.write_str("Pattern Type Mismatch"),
            Error::InvalidContent => fmt.write_str("Invalid Content"),
            Error::InvalidFormat => fmt.write_str("Invalid Format"),
            Error::InvalidVisual => fmt.write_str("Invalid Visual"),
            Error::FileNotFound => fmt.write_str("File Not Found"),
            Error::InvalidDash => fmt.write_str("Invalid Dash"),
            Error::InvalidDscComment => fmt.write_str("Invalid DSC Comment"),
            Error::InvalidIndex => fmt.write_str("Invalid Index"),
            Error::ClipNotRepresentable => fmt.write_str("Clip Not Representable"),
            Error::TempFileError => fmt.write_str("Temp File Error"),
            Error::InvalidStride => fmt.write_str("Invalid Stride"),
            Error::FontTypeMismatch => fmt.write_str("Font Type Mismatch"),
            Error::UserFontImmutable => fmt.write_str("User Font Immutable"),
            Error::UserFontError => fmt.write_str("User Font Error"),
            Error::NegativeCount => fmt.write_str("Negative Count"),
            Error::InvalidClusters => fmt.write_str("Invalid Clusters"),
            Error::InvalidSlant => fmt.write_str("Invalid Slant"),
            Error::InvalidWeight => fmt.write_str("Invalid Weight"),
            Error::InvalidSize => fmt.write_str("Invalid Size"),
            Error::UserFontNotImplemented => fmt.write_str("User Font Not Implemented"),
            Error::DeviceTypeMismatch => fmt.write_str("Device Type Mismatch"),
            Error::DeviceError => fmt.write_str("Device Error"),
            Error::InvalidMeshConstruction => fmt.write_str("Invalid Mesh Construction"),
            Error::DeviceFinished => fmt.write_str("Device Finished"),
            Error::JBig2GlobalMissing => fmt.write_str("JBig2Global Missing"),
            Error::PngError => fmt.write_str("PNG Error"),
            Error::FreetypeError => fmt.write_str("Freetype Error"),
            Error::Win32GdiError => fmt.write_str("Win32Gdi Error"),
            #[cfg(feature = "v1_16")]
            Error::TagError => fmt.write_str("Tag error"),
            #[cfg(feature = "v1_18")]
            Error::DwriteError => fmt.write_str("Dwrite error"),
            Error::LastStatus => fmt.write_str("LastStatus"),
            Error::__Unknown(value) => write!(fmt, "Unknown {value}"),
        }
    }
}

#[doc(hidden)]
impl From<Error> for ffi::cairo_status_t {
    fn from(err: Error) -> Self {
        match err {
            Error::NoMemory => ffi::STATUS_NO_MEMORY,
            Error::InvalidRestore => ffi::STATUS_INVALID_RESTORE,
            Error::InvalidPopGroup => ffi::STATUS_INVALID_POP_GROUP,
            Error::NoCurrentPoint => ffi::STATUS_NO_CURRENT_POINT,
            Error::InvalidMatrix => ffi::STATUS_INVALID_MATRIX,
            Error::InvalidStatus => ffi::STATUS_INVALID_STATUS,
            Error::NullPointer => ffi::STATUS_NULL_POINTER,
            Error::InvalidString => ffi::STATUS_INVALID_STRING,
            Error::InvalidPathData => ffi::STATUS_INVALID_PATH_DATA,
            Error::ReadError => ffi::STATUS_READ_ERROR,
            Error::WriteError => ffi::STATUS_WRITE_ERROR,
            Error::SurfaceFinished => ffi::STATUS_SURFACE_FINISHED,
            Error::SurfaceTypeMismatch => ffi::STATUS_SURFACE_TYPE_MISMATCH,
            Error::PatternTypeMismatch => ffi::STATUS_PATTERN_TYPE_MISMATCH,
            Error::InvalidContent => ffi::STATUS_INVALID_CONTENT,
            Error::InvalidFormat => ffi::STATUS_INVALID_FORMAT,
            Error::InvalidVisual => ffi::STATUS_INVALID_VISUAL,
            Error::FileNotFound => ffi::STATUS_FILE_NOT_FOUND,
            Error::InvalidDash => ffi::STATUS_INVALID_DASH,
            Error::InvalidDscComment => ffi::STATUS_INVALID_DSC_COMMENT,
            Error::InvalidIndex => ffi::STATUS_INVALID_INDEX,
            Error::ClipNotRepresentable => ffi::STATUS_CLIP_NOT_REPRESENTABLE,
            Error::TempFileError => ffi::STATUS_TEMP_FILE_ERROR,
            Error::InvalidStride => ffi::STATUS_INVALID_STRIDE,
            Error::FontTypeMismatch => ffi::STATUS_FONT_TYPE_MISMATCH,
            Error::UserFontImmutable => ffi::STATUS_USER_FONT_IMMUTABLE,
            Error::UserFontError => ffi::STATUS_USER_FONT_ERROR,
            Error::NegativeCount => ffi::STATUS_NEGATIVE_COUNT,
            Error::InvalidClusters => ffi::STATUS_INVALID_CLUSTERS,
            Error::InvalidSlant => ffi::STATUS_INVALID_SLANT,
            Error::InvalidWeight => ffi::STATUS_INVALID_WEIGHT,
            Error::InvalidSize => ffi::STATUS_INVALID_SIZE,
            Error::UserFontNotImplemented => ffi::STATUS_USER_FONT_NOT_IMPLEMENTED,
            Error::DeviceTypeMismatch => ffi::STATUS_DEVICE_TYPE_MISMATCH,
            Error::DeviceError => ffi::STATUS_DEVICE_ERROR,
            Error::InvalidMeshConstruction => ffi::STATUS_INVALID_MESH_CONSTRUCTION,
            Error::DeviceFinished => ffi::STATUS_DEVICE_FINISHED,
            Error::JBig2GlobalMissing => ffi::STATUS_J_BIG2_GLOBAL_MISSING,
            Error::PngError => ffi::STATUS_PNG_ERROR,
            Error::FreetypeError => ffi::STATUS_FREETYPE_ERROR,
            Error::Win32GdiError => ffi::STATUS_WIN32_GDI_ERROR,
            #[cfg(feature = "v1_16")]
            Error::TagError => ffi::STATUS_TAG_ERROR,
            #[cfg(feature = "v1_18")]
            Error::DwriteError => ffi::STATUS_DWRITE_ERROR,
            Error::LastStatus => ffi::STATUS_LAST_STATUS,
            Error::__Unknown(value) => value,
        }
    }
}

#[doc(hidden)]
impl From<ffi::cairo_status_t> for Error {
    fn from(value: ffi::cairo_status_t) -> Self {
        match value {
            ffi::STATUS_NO_MEMORY => Self::NoMemory,
            ffi::STATUS_INVALID_RESTORE => Self::InvalidRestore,
            ffi::STATUS_INVALID_POP_GROUP => Self::InvalidPopGroup,
            ffi::STATUS_NO_CURRENT_POINT => Self::NoCurrentPoint,
            ffi::STATUS_INVALID_MATRIX => Self::InvalidMatrix,
            ffi::STATUS_INVALID_STATUS => Self::InvalidStatus,
            ffi::STATUS_NULL_POINTER => Self::NullPointer,
            ffi::STATUS_INVALID_STRING => Self::InvalidString,
            ffi::STATUS_INVALID_PATH_DATA => Self::InvalidPathData,
            ffi::STATUS_READ_ERROR => Self::ReadError,
            ffi::STATUS_WRITE_ERROR => Self::WriteError,
            ffi::STATUS_SURFACE_FINISHED => Self::SurfaceFinished,
            ffi::STATUS_SURFACE_TYPE_MISMATCH => Self::SurfaceTypeMismatch,
            ffi::STATUS_PATTERN_TYPE_MISMATCH => Self::PatternTypeMismatch,
            ffi::STATUS_INVALID_CONTENT => Self::InvalidContent,
            ffi::STATUS_INVALID_FORMAT => Self::InvalidFormat,
            ffi::STATUS_INVALID_VISUAL => Self::InvalidVisual,
            ffi::STATUS_FILE_NOT_FOUND => Self::FileNotFound,
            ffi::STATUS_INVALID_DASH => Self::InvalidDash,
            ffi::STATUS_INVALID_DSC_COMMENT => Self::InvalidDscComment,
            ffi::STATUS_INVALID_INDEX => Self::InvalidIndex,
            ffi::STATUS_CLIP_NOT_REPRESENTABLE => Self::ClipNotRepresentable,
            ffi::STATUS_TEMP_FILE_ERROR => Self::TempFileError,
            ffi::STATUS_INVALID_STRIDE => Self::InvalidStride,
            ffi::STATUS_FONT_TYPE_MISMATCH => Self::FontTypeMismatch,
            ffi::STATUS_USER_FONT_IMMUTABLE => Self::UserFontImmutable,
            ffi::STATUS_USER_FONT_ERROR => Self::UserFontError,
            ffi::STATUS_NEGATIVE_COUNT => Self::NegativeCount,
            ffi::STATUS_INVALID_CLUSTERS => Self::InvalidClusters,
            ffi::STATUS_INVALID_SLANT => Self::InvalidSlant,
            ffi::STATUS_INVALID_WEIGHT => Self::InvalidWeight,
            ffi::STATUS_INVALID_SIZE => Self::InvalidSize,
            ffi::STATUS_USER_FONT_NOT_IMPLEMENTED => Self::UserFontNotImplemented,
            ffi::STATUS_DEVICE_TYPE_MISMATCH => Self::DeviceTypeMismatch,
            ffi::STATUS_DEVICE_ERROR => Self::DeviceError,
            ffi::STATUS_INVALID_MESH_CONSTRUCTION => Self::InvalidMeshConstruction,
            ffi::STATUS_DEVICE_FINISHED => Self::DeviceFinished,
            ffi::STATUS_J_BIG2_GLOBAL_MISSING => Self::JBig2GlobalMissing,
            ffi::STATUS_PNG_ERROR => Self::PngError,
            ffi::STATUS_FREETYPE_ERROR => Self::FreetypeError,
            ffi::STATUS_WIN32_GDI_ERROR => Self::Win32GdiError,
            #[cfg(feature = "v1_16")]
            ffi::STATUS_TAG_ERROR => Error::TagError,
            #[cfg(feature = "v1_18")]
            ffi::STATUS_DWRITE_ERROR => Error::DwriteError,
            ffi::STATUS_LAST_STATUS => Self::LastStatus,
            value => Self::__Unknown(value),
        }
    }
}

#[derive(Debug)]
pub enum IoError {
    Cairo(Error),
    Io(io::Error),
}

impl std::error::Error for IoError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            IoError::Cairo(err) => Some(err),
            IoError::Io(err) => Some(err),
        }
    }
}

impl fmt::Display for IoError {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> ::core::fmt::Result {
        match self {
            IoError::Cairo(err) => write!(fmt, "Cairo error: {err}"),
            IoError::Io(err) => write!(fmt, "IO error: {err}"),
        }
    }
}

impl std::convert::From<Error> for IoError {
    fn from(source: Error) -> Self {
        IoError::Cairo(source)
    }
}

impl std::convert::From<io::Error> for IoError {
    fn from(source: io::Error) -> Self {
        IoError::Io(source)
    }
}

#[derive(Debug)]
pub enum BorrowError {
    Cairo(crate::Error),
    NonExclusive,
}

impl std::error::Error for BorrowError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            BorrowError::Cairo(err) => Some(err),
            BorrowError::NonExclusive => None,
        }
    }
}

impl fmt::Display for BorrowError {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match self {
            BorrowError::Cairo(err) => write!(fmt, "Failed to borrow with Cairo error: {err}"),
            BorrowError::NonExclusive => fmt.write_str("Can't get exclusive access"),
        }
    }
}

impl std::convert::From<crate::Error> for BorrowError {
    fn from(err: crate::Error) -> Self {
        BorrowError::Cairo(err)
    }
}

pub type Result<T> = std::result::Result<T, Error>;
