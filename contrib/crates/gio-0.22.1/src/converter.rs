// Take a look at the license at the top of the repository in the LICENSE file.

use std::{mem, ptr};

use glib::{prelude::*, translate::*};

use crate::{Converter, ConverterFlags, ConverterResult, ffi};

pub trait ConverterExtManual: IsA<Converter> + 'static {
    #[doc(alias = "g_converter_convert")]
    fn convert<IN: AsRef<[u8]>, OUT: AsMut<[u8]>>(
        &self,
        inbuf: IN,
        outbuf: OUT,
        flags: ConverterFlags,
    ) -> Result<(ConverterResult, usize, usize), glib::Error> {
        let inbuf: Box<IN> = Box::new(inbuf);
        let (inbuf_size, inbuf) = {
            let slice = (*inbuf).as_ref();
            (slice.len(), slice.as_ptr())
        };
        let mut outbuf: Box<OUT> = Box::new(outbuf);
        let (outbuf_size, outbuf) = {
            let slice = (*outbuf).as_mut();
            (slice.len(), slice.as_mut_ptr())
        };
        unsafe {
            let mut bytes_read = mem::MaybeUninit::uninit();
            let mut bytes_written = mem::MaybeUninit::uninit();
            let mut error = ptr::null_mut();
            let ret = ffi::g_converter_convert(
                self.as_ref().to_glib_none().0,
                mut_override(inbuf),
                inbuf_size,
                outbuf,
                outbuf_size,
                flags.into_glib(),
                bytes_read.as_mut_ptr(),
                bytes_written.as_mut_ptr(),
                &mut error,
            );
            if error.is_null() {
                Ok((
                    from_glib(ret),
                    bytes_read.assume_init(),
                    bytes_written.assume_init(),
                ))
            } else {
                Err(from_glib_full(error))
            }
        }
    }
}

impl<O: IsA<Converter>> ConverterExtManual for O {}
