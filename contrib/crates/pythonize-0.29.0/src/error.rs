use pyo3::PyErr;
use pyo3::{exceptions::*, CastError, CastIntoError};
use serde::{de, ser};
use std::convert::Infallible;
use std::error;
use std::fmt::{self, Debug, Display};
use std::result;

/// Alias for `std::result::Result` with error type `PythonizeError`
pub type Result<T> = result::Result<T, PythonizeError>;

/// Errors that can occur when serializing/deserializing Python objects
pub struct PythonizeError {
    pub(crate) inner: Box<ErrorImpl>,
}

impl PythonizeError {
    pub(crate) fn msg<T>(text: T) -> Self
    where
        T: ToString,
    {
        Self {
            inner: Box::new(ErrorImpl::Message(text.to_string())),
        }
    }

    pub(crate) fn unsupported_type<T>(t: T) -> Self
    where
        T: ToString,
    {
        Self {
            inner: Box::new(ErrorImpl::UnsupportedType(t.to_string())),
        }
    }

    pub(crate) fn dict_key_not_string() -> Self {
        Self {
            inner: Box::new(ErrorImpl::DictKeyNotString),
        }
    }

    pub(crate) fn incorrect_sequence_length(expected: usize, got: usize) -> Self {
        Self {
            inner: Box::new(ErrorImpl::IncorrectSequenceLength { expected, got }),
        }
    }

    pub(crate) fn invalid_enum_type() -> Self {
        Self {
            inner: Box::new(ErrorImpl::InvalidEnumType),
        }
    }

    pub(crate) fn invalid_length_enum() -> Self {
        Self {
            inner: Box::new(ErrorImpl::InvalidLengthEnum),
        }
    }

    pub(crate) fn invalid_length_char() -> Self {
        Self {
            inner: Box::new(ErrorImpl::InvalidLengthChar),
        }
    }
}

/// Error codes for problems that can occur when serializing/deserializing Python objects
#[derive(Debug)]
pub enum ErrorImpl {
    /// An error originating from the Python runtime
    PyErr(PyErr),
    /// Generic error message
    Message(String),
    /// A Python type not supported by the deserializer
    UnsupportedType(String),
    /// A `PyAny` object that failed to cast to an expected Python type
    UnexpectedType(String),
    /// Dict keys should be strings to deserialize to struct fields
    DictKeyNotString,
    /// Sequence length did not match expected tuple or tuple struct length.
    IncorrectSequenceLength { expected: usize, got: usize },
    /// Enum variants should either be dict (tagged) or str (variant)
    InvalidEnumType,
    /// Tagged enum variants should be a dict with exactly 1 key
    InvalidLengthEnum,
    /// Expected a `char`, but got a Python str that was not length 1
    InvalidLengthChar,
}

impl error::Error for PythonizeError {}

impl Display for PythonizeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.inner.as_ref() {
            ErrorImpl::PyErr(e) => Display::fmt(e, f),
            ErrorImpl::Message(s) => Display::fmt(s, f),
            ErrorImpl::UnsupportedType(s) => write!(f, "unsupported type {}", s),
            ErrorImpl::UnexpectedType(s) => write!(f, "unexpected type: {}", s),
            ErrorImpl::DictKeyNotString => f.write_str("dict keys must have type str"),
            ErrorImpl::IncorrectSequenceLength { expected, got } => {
                write!(f, "expected sequence of length {}, got {}", expected, got)
            }
            ErrorImpl::InvalidEnumType => f.write_str("expected either a str or dict for enum"),
            ErrorImpl::InvalidLengthEnum => {
                f.write_str("expected tagged enum dict to have exactly 1 key")
            }
            ErrorImpl::InvalidLengthChar => f.write_str("expected a str of length 1 for char"),
        }
    }
}

impl Debug for PythonizeError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.inner.as_ref().fmt(f)
    }
}

impl ser::Error for PythonizeError {
    fn custom<T>(msg: T) -> Self
    where
        T: Display,
    {
        Self {
            inner: Box::new(ErrorImpl::Message(msg.to_string())),
        }
    }
}

impl de::Error for PythonizeError {
    fn custom<T>(msg: T) -> Self
    where
        T: Display,
    {
        Self {
            inner: Box::new(ErrorImpl::Message(msg.to_string())),
        }
    }
}

/// Convert an exception raised in Python to a `PythonizeError`
impl From<Infallible> for PythonizeError {
    fn from(other: Infallible) -> Self {
        match other {}
    }
}

/// Convert an exception raised in Python to a `PythonizeError`
impl From<PyErr> for PythonizeError {
    fn from(other: PyErr) -> Self {
        Self {
            inner: Box::new(ErrorImpl::PyErr(other)),
        }
    }
}

/// Handle errors that occur when attempting to use `PyAny::cast`
impl<'a, 'py> From<CastError<'a, 'py>> for PythonizeError {
    fn from(other: CastError<'a, 'py>) -> Self {
        Self {
            inner: Box::new(ErrorImpl::UnexpectedType(other.to_string())),
        }
    }
}

/// Handle errors that occur when attempting to use `PyAny::cast`
impl<'py> From<CastIntoError<'py>> for PythonizeError {
    fn from(other: CastIntoError<'py>) -> Self {
        Self {
            inner: Box::new(ErrorImpl::UnexpectedType(other.to_string())),
        }
    }
}

/// Convert a `PythonizeError` to a Python exception
impl From<PythonizeError> for PyErr {
    fn from(other: PythonizeError) -> Self {
        match *other.inner {
            ErrorImpl::PyErr(e) => e,
            ErrorImpl::Message(e) => PyException::new_err(e),
            ErrorImpl::UnsupportedType(_)
            | ErrorImpl::UnexpectedType(_)
            | ErrorImpl::DictKeyNotString
            | ErrorImpl::InvalidEnumType => PyTypeError::new_err(other.to_string()),
            ErrorImpl::IncorrectSequenceLength { .. }
            | ErrorImpl::InvalidLengthEnum
            | ErrorImpl::InvalidLengthChar => PyValueError::new_err(other.to_string()),
        }
    }
}
