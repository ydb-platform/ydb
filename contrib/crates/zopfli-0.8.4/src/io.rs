use alloc::vec::Vec;
use core::{
    cmp, fmt,
    fmt::{Display, Formatter},
    mem,
};

/// A trait for objects which are byte-oriented sinks, modeled after its
/// `std::io::Write` counterpart.
///
/// The documentation for this trait is taken from the aforementioned `std` trait.
pub trait Write {
    /// Write a buffer into this writer, returning how many bytes were written.
    ///
    /// This function will attempt to write the entire contents of `buf`, but
    /// the entire write might not succeed, or the write may also generate an
    /// error. A call to `write` represents *at most one* attempt to write to
    /// any wrapped object.
    ///
    /// Calls to `write` are not guaranteed to block waiting for data to be
    /// written, and a write which would otherwise block can be indicated through
    /// an [`Err`] variant.
    ///
    /// If the return value is `Ok(n)` then it must be guaranteed that
    /// `n <= buf.len()`. A return value of `0` typically means that the
    /// underlying object is no longer able to accept bytes and will likely not
    /// be able to in the future as well, or that the buffer provided is empty.
    ///
    /// # Errors
    ///
    /// Each call to `write` may generate an I/O error indicating that the
    /// operation could not be completed. If an error is returned then no bytes
    /// in the buffer were written to this writer.
    ///
    /// It is **not** considered an error if the entire buffer could not be
    /// written to this writer.
    ///
    /// An error of the [`ErrorKind::Interrupted`] kind is non-fatal and the
    /// write operation should be retried if there is nothing else to do.
    fn write(&mut self, buf: &[u8]) -> Result<usize, Error>;

    /// Flush this output stream, ensuring that all intermediately buffered
    /// contents reach their destination.
    ///
    /// # Errors
    ///
    /// It is considered an error if not all bytes could be written due to
    /// I/O errors or EOF being reached.
    fn flush(&mut self) -> Result<(), Error>;

    /// Attempts to write an entire buffer into this writer.
    ///
    /// This method will continuously call [`write`] until there is no more data
    /// to be written or an error of non-[`ErrorKind::Interrupted`] kind is
    /// returned. This method will not return until the entire buffer has been
    /// successfully written or such an error occurs. The first error that is
    /// not of [`ErrorKind::Interrupted`] kind generated from this method will be
    /// returned.
    ///
    /// If the buffer contains no data, this will never call [`write`].
    ///
    /// # Errors
    ///
    /// This function will return the first error of
    /// non-[`ErrorKind::Interrupted`] kind that [`write`] returns.
    ///
    /// [`write`]: Write::write
    // Implementation taken from Rust's stdlib
    fn write_all(&mut self, mut buf: &[u8]) -> Result<(), Error> {
        while !buf.is_empty() {
            match self.write(buf) {
                Ok(0) => {
                    return Err(ErrorKind::WriteZero.into());
                }
                Ok(n) => buf = &buf[n..],
                Err(ref e) if e.kind() == ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    /// Creates a "by reference" adapter for this instance of `Write`.
    ///
    /// The returned adapter also implements `Write` and will simply borrow this
    /// current writer.
    // Implementation taken from Rust's stdlib
    fn by_ref(&mut self) -> &mut Self
    where
        Self: Sized,
    {
        self
    }
}

impl Write for &mut [u8] {
    // Implementation taken from Rust's stdlib
    #[inline]
    fn write(&mut self, data: &[u8]) -> Result<usize, Error> {
        let amt = cmp::min(data.len(), self.len());
        let (a, b) = mem::take(self).split_at_mut(amt);
        a.copy_from_slice(&data[..amt]);
        *self = b;
        Ok(amt)
    }

    #[inline]
    fn flush(&mut self) -> Result<(), Error> {
        Ok(())
    }

    // Implementation taken from Rust's stdlib
    #[inline]
    fn write_all(&mut self, data: &[u8]) -> Result<(), Error> {
        if self.write(data)? == data.len() {
            Ok(())
        } else {
            Err(ErrorKind::WriteZero.into())
        }
    }
}

impl Write for Vec<u8> {
    // Implementation taken from Rust's stdlib
    #[inline]
    fn write(&mut self, buf: &[u8]) -> Result<usize, Error> {
        self.extend_from_slice(buf);
        Ok(buf.len())
    }

    #[inline]
    fn flush(&mut self) -> Result<(), Error> {
        Ok(())
    }

    // Implementation taken from Rust's stdlib
    #[inline]
    fn write_all(&mut self, buf: &[u8]) -> Result<(), Error> {
        self.extend_from_slice(buf);
        Ok(())
    }
}

impl<W: Write + ?Sized> Write for &mut W {
    fn write(&mut self, buf: &[u8]) -> Result<usize, Error> {
        (**self).write(buf)
    }

    fn flush(&mut self) -> Result<(), Error> {
        (**self).flush()
    }

    fn write_all(&mut self, buf: &[u8]) -> Result<(), Error> {
        (**self).write_all(buf)
    }
}

/// The error type for I/O operations of the `Write` trait.
#[derive(Debug)]
pub struct Error {
    kind: ErrorKind,
}

impl Error {
    /// Returns the corresponding `ErrorKind` for this error.
    pub fn kind(&self) -> ErrorKind {
        self.kind
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_fmt(format_args!("{:?}", self.kind))
    }
}

impl From<ErrorKind> for Error {
    fn from(value: ErrorKind) -> Self {
        Self { kind: value }
    }
}

#[cfg(feature = "nightly")]
impl core::error::Error for Error {}

/// A list specifying general categories of I/O error.
///
/// This list is intended to grow over time and it is not recommended to
/// exhaustively match against it.
///
/// The documentation for this enum is taken from the `std::io::ErrorKind`.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
#[non_exhaustive]
pub enum ErrorKind {
    /// This operation was interrupted.
    ///
    /// Interrupted operations can typically be retried.
    Interrupted,
    /// An error returned when an operation could not be completed because a
    /// call to [`write`] returned [`Ok(0)`].
    ///
    /// This typically means that an operation could only succeed if it wrote a
    /// particular number of bytes but only a smaller number of bytes could be
    /// written.
    ///
    /// [`write`]: Write::write
    /// [`Ok(0)`]: Ok
    WriteZero,
    /// An error that does not fall under any other I/O error kind.
    Other,
}
