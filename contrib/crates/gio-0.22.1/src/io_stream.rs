// Take a look at the license at the top of the repository in the LICENSE file.

use std::{io, pin::Pin};

use futures_core::task::{Context, Poll};
use futures_io::{AsyncRead, AsyncWrite};
use glib::prelude::*;

use crate::{
    IOStream, InputStreamAsyncRead, OutputStreamAsyncWrite, PollableInputStream,
    PollableOutputStream, prelude::*,
};

pub trait IOStreamExtManual: Sized + IsA<IOStream> {
    fn into_async_read_write(self) -> Result<IOStreamAsyncReadWrite<Self>, Self> {
        let write = self
            .output_stream()
            .dynamic_cast::<PollableOutputStream>()
            .ok()
            .and_then(|s| s.into_async_write().ok());

        let read = self
            .input_stream()
            .dynamic_cast::<PollableInputStream>()
            .ok()
            .and_then(|s| s.into_async_read().ok());

        let (read, write) = match (read, write) {
            (Some(read), Some(write)) => (read, write),
            _ => return Err(self),
        };

        Ok(IOStreamAsyncReadWrite {
            io_stream: self,
            read,
            write,
        })
    }
}

impl<O: IsA<IOStream>> IOStreamExtManual for O {}

#[derive(Debug)]
pub struct IOStreamAsyncReadWrite<T> {
    io_stream: T,
    read: InputStreamAsyncRead<PollableInputStream>,
    write: OutputStreamAsyncWrite<PollableOutputStream>,
}

impl<T: IsA<IOStream>> IOStreamAsyncReadWrite<T> {
    pub fn input_stream(&self) -> &PollableInputStream {
        self.read.input_stream()
    }

    pub fn output_stream(&self) -> &PollableOutputStream {
        self.write.output_stream()
    }

    pub fn into_io_stream(self) -> T {
        self.io_stream
    }

    pub fn io_stream(&self) -> &T {
        &self.io_stream
    }
}

impl<T: IsA<IOStream> + std::marker::Unpin> AsyncRead for IOStreamAsyncReadWrite<T> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<Result<usize, io::Error>> {
        Pin::new(&mut Pin::get_mut(self).read).poll_read(cx, buf)
    }
}

impl<T: IsA<IOStream> + std::marker::Unpin> AsyncWrite for IOStreamAsyncReadWrite<T> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        Pin::new(&mut Pin::get_mut(self).write).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        Pin::new(&mut Pin::get_mut(self).write).poll_flush(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        Pin::new(&mut Pin::get_mut(self).write).poll_close(cx)
    }
}
