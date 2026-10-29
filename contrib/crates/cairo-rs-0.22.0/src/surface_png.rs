// Take a look at the license at the top of the repository in the LICENSE file.

use std::{
    any::Any,
    io::{self, Read, Write},
    panic::AssertUnwindSafe,
    slice,
};

use crate::ffi;
use libc::{c_uint, c_void};

use crate::{Error, ImageSurface, IoError, Surface, utils::status_to_result};

struct ReadEnv<'a, R: 'a + Read> {
    reader: &'a mut R,
    io_error: Option<io::Error>,
    unwind_payload: Option<Box<dyn Any + Send + 'static>>,
}

unsafe extern "C" fn read_func<R: Read>(
    closure: *mut c_void,
    data: *mut u8,
    len: c_uint,
) -> crate::ffi::cairo_status_t {
    unsafe {
        let read_env: &mut ReadEnv<R> = &mut *(closure as *mut ReadEnv<R>);

        // Don’t attempt another read, if a previous one errored or panicked:
        if read_env.io_error.is_some() || read_env.unwind_payload.is_some() {
            return Error::ReadError.into();
        }

        let buffer = if data.is_null() || len == 0 {
            &mut []
        } else {
            slice::from_raw_parts_mut(data, len as usize)
        };
        let result =
            std::panic::catch_unwind(AssertUnwindSafe(|| read_env.reader.read_exact(buffer)));
        match result {
            Ok(Ok(())) => ffi::STATUS_SUCCESS,
            Ok(Err(error)) => {
                read_env.io_error = Some(error);
                Error::ReadError.into()
            }
            Err(payload) => {
                read_env.unwind_payload = Some(payload);
                Error::ReadError.into()
            }
        }
    }
}

struct WriteEnv<'a, W: 'a + Write> {
    writer: &'a mut W,
    io_error: Option<io::Error>,
    unwind_payload: Option<Box<dyn Any + Send + 'static>>,
}

unsafe extern "C" fn write_func<W: Write>(
    closure: *mut c_void,
    data: *const u8,
    len: c_uint,
) -> crate::ffi::cairo_status_t {
    unsafe {
        let write_env: &mut WriteEnv<W> = &mut *(closure as *mut WriteEnv<W>);

        // Don’t attempt another write, if a previous one errored or panicked:
        if write_env.io_error.is_some() || write_env.unwind_payload.is_some() {
            return Error::WriteError.into();
        }

        let buffer = if data.is_null() || len == 0 {
            &[]
        } else {
            slice::from_raw_parts(data, len as usize)
        };
        let result =
            std::panic::catch_unwind(AssertUnwindSafe(|| write_env.writer.write_all(buffer)));
        match result {
            Ok(Ok(())) => ffi::STATUS_SUCCESS,
            Ok(Err(error)) => {
                write_env.io_error = Some(error);
                Error::WriteError.into()
            }
            Err(payload) => {
                write_env.unwind_payload = Some(payload);
                Error::WriteError.into()
            }
        }
    }
}

impl ImageSurface {
    #[doc(alias = "cairo_image_surface_create_from_png_stream")]
    pub fn create_from_png<R: Read>(stream: &mut R) -> Result<ImageSurface, IoError> {
        let mut env = ReadEnv {
            reader: stream,
            io_error: None,
            unwind_payload: None,
        };
        unsafe {
            let raw_surface = ffi::cairo_image_surface_create_from_png_stream(
                Some(read_func::<R>),
                &mut env as *mut ReadEnv<R> as *mut c_void,
            );

            let surface = ImageSurface::from_raw_full(raw_surface)?;

            if let Some(payload) = env.unwind_payload {
                std::panic::resume_unwind(payload)
            }

            match env.io_error {
                None => Ok(surface),
                Some(err) => Err(IoError::Io(err)),
            }
        }
    }
}

impl Surface {
    // rustdoc-stripper-ignore-next
    /// This function writes the surface as a PNG image to the given stream.
    ///
    /// If the underlying surface does not support being written as a PNG, this will return
    /// [`Error::SurfaceTypeMismatch`]
    #[doc(alias = "cairo_surface_write_to_png_stream")]
    #[doc(alias = "cairo_surface_write_to_png")]
    pub fn write_to_png<W: Write>(&self, stream: &mut W) -> Result<(), IoError> {
        let mut env = WriteEnv {
            writer: stream,
            io_error: None,
            unwind_payload: None,
        };
        let status = unsafe {
            ffi::cairo_surface_write_to_png_stream(
                self.to_raw_none(),
                Some(write_func::<W>),
                &mut env as *mut WriteEnv<W> as *mut c_void,
            )
        };

        if let Some(payload) = env.unwind_payload {
            std::panic::resume_unwind(payload)
        }

        match env.io_error {
            None => match status_to_result(status) {
                Err(err) => Err(IoError::Cairo(err)),
                Ok(_) => Ok(()),
            },
            Some(err) => Err(IoError::Io(err)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::enums::Format;

    struct IoErrorReader;

    // A reader that always returns an error
    impl Read for IoErrorReader {
        fn read(&mut self, _: &mut [u8]) -> Result<usize, io::Error> {
            Err(io::Error::other("yikes!"))
        }
    }

    #[test]
    fn valid_png_reads_correctly() {
        // A 1x1 PNG, RGB, no alpha, with a single pixel with (42, 42, 42) values
        let png_data: Vec<u8> = vec![
            0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a, 0x00, 0x00, 0x00, 0x0d, 0x49, 0x48,
            0x44, 0x52, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x08, 0x02, 0x00, 0x00,
            0x00, 0x90, 0x77, 0x53, 0xde, 0x00, 0x00, 0x00, 0x0c, 0x49, 0x44, 0x41, 0x54, 0x08,
            0xd7, 0x63, 0xd0, 0xd2, 0xd2, 0x02, 0x00, 0x01, 0x00, 0x00, 0x7f, 0x09, 0xa9, 0x5a,
            0x4d, 0x00, 0x00, 0x00, 0x00, 0x49, 0x45, 0x4e, 0x44, 0xae, 0x42, 0x60, 0x82,
        ];

        let r = ImageSurface::create_from_png(&mut &png_data[..]);
        assert!(r.is_ok());

        let mut surface = r.unwrap();
        assert_eq!(surface.width(), 1);
        assert_eq!(surface.height(), 1);
        assert_eq!(surface.format(), Format::Rgb24);

        let data = surface.data().unwrap();
        assert!(data.len() >= 3);

        let slice = &data[0..3];
        assert_eq!(slice[0], 42);
        assert_eq!(slice[1], 42);
        assert_eq!(slice[2], 42);
    }

    #[cfg(not(target_os = "macos"))]
    #[test]
    fn invalid_png_yields_error() {
        let png_data: Vec<u8> = vec![
            //      v--- this byte is modified
            0x89, 0x40, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a, 0x00, 0x00, 0x00, 0x0d, 0x49, 0x48,
            0x44, 0x52, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x08, 0x02, 0x00, 0x00,
            0x00, 0x90, 0x77, 0x53, 0xde, 0x00, 0x00, 0x00, 0x0c, 0x49, 0x44, 0x41, 0x54, 0x08,
            0xd7, 0x63, 0xd0, 0xd2, 0xd2, 0x02, 0x00, 0x01, 0x00, 0x00, 0x7f, 0x09, 0xa9, 0x5a,
            0x4d, 0x00, 0x00, 0x00, 0x00, 0x49, 0x45, 0x4e, 0x44, 0xae, 0x42, 0x60, 0x82,
        ];

        match ImageSurface::create_from_png(&mut &png_data[..]) {
            Err(IoError::Cairo(_)) => (),
            _ => unreachable!(),
        }
    }

    #[cfg(not(target_os = "macos"))]
    #[test]
    fn io_error_yields_cairo_read_error() {
        let mut r = IoErrorReader;

        match ImageSurface::create_from_png(&mut r) {
            Err(IoError::Cairo(Error::ReadError)) => (),
            _ => unreachable!(),
        }
    }
}
