// Take a look at the license at the top of the repository in the LICENSE file.

use std::{fmt, future::Future, io, mem, pin::Pin, ptr};

use futures_core::task::{Context, Poll};
use futures_io::{AsyncBufRead, AsyncRead};
use glib::{Priority, prelude::*, translate::*};

use crate::{Cancellable, InputStream, Seekable, error::to_std_io_result, ffi, prelude::*};

pub trait InputStreamExtManual: IsA<InputStream> + Sized {
    #[doc(alias = "g_input_stream_read")]
    fn read<B: AsMut<[u8]>, C: IsA<Cancellable>>(
        &self,
        mut buffer: B,
        cancellable: Option<&C>,
    ) -> Result<usize, glib::Error> {
        let cancellable = cancellable.map(|c| c.as_ref());
        let gcancellable = cancellable.to_glib_none();
        let buffer = buffer.as_mut();
        let buffer_ptr = buffer.as_mut_ptr();
        let count = buffer.len();
        unsafe {
            let mut error = ptr::null_mut();
            let ret = ffi::g_input_stream_read(
                self.as_ref().to_glib_none().0,
                buffer_ptr,
                count,
                gcancellable.0,
                &mut error,
            );
            if error.is_null() {
                Ok(ret as usize)
            } else {
                Err(from_glib_full(error))
            }
        }
    }

    #[doc(alias = "g_input_stream_read_all")]
    fn read_all<B: AsMut<[u8]>, C: IsA<Cancellable>>(
        &self,
        mut buffer: B,
        cancellable: Option<&C>,
    ) -> Result<(usize, Option<glib::Error>), glib::Error> {
        let cancellable = cancellable.map(|c| c.as_ref());
        let gcancellable = cancellable.to_glib_none();
        let buffer = buffer.as_mut();
        let buffer_ptr = buffer.as_mut_ptr();
        let count = buffer.len();
        unsafe {
            let mut bytes_read = mem::MaybeUninit::uninit();
            let mut error = ptr::null_mut();
            let _ = ffi::g_input_stream_read_all(
                self.as_ref().to_glib_none().0,
                buffer_ptr,
                count,
                bytes_read.as_mut_ptr(),
                gcancellable.0,
                &mut error,
            );

            let bytes_read = bytes_read.assume_init();
            if error.is_null() {
                Ok((bytes_read, None))
            } else if bytes_read != 0 {
                Ok((bytes_read, Some(from_glib_full(error))))
            } else {
                Err(from_glib_full(error))
            }
        }
    }

    #[doc(alias = "g_input_stream_read_all_async")]
    fn read_all_async<
        B: AsMut<[u8]> + Send + 'static,
        Q: FnOnce(Result<(B, usize, Option<glib::Error>), (B, glib::Error)>) + 'static,
        C: IsA<Cancellable>,
    >(
        &self,
        buffer: B,
        io_priority: Priority,
        cancellable: Option<&C>,
        callback: Q,
    ) {
        let main_context = glib::MainContext::ref_thread_default();
        let is_main_context_owner = main_context.is_owner();
        let has_acquired_main_context = (!is_main_context_owner)
            .then(|| main_context.acquire().ok())
            .flatten();
        assert!(
            is_main_context_owner || has_acquired_main_context.is_some(),
            "Async operations only allowed if the thread is owning the MainContext"
        );

        let cancellable = cancellable.map(|c| c.as_ref());
        let gcancellable = cancellable.to_glib_none();
        let mut user_data: Box<(glib::thread_guard::ThreadGuard<Q>, B)> =
            Box::new((glib::thread_guard::ThreadGuard::new(callback), buffer));
        // Need to do this after boxing as the contents pointer might change by moving into the box
        let (count, buffer_ptr) = {
            let buffer = &mut user_data.1;
            let slice = (*buffer).as_mut();
            (slice.len(), slice.as_mut_ptr())
        };
        unsafe extern "C" fn read_all_async_trampoline<
            B: AsMut<[u8]> + Send + 'static,
            Q: FnOnce(Result<(B, usize, Option<glib::Error>), (B, glib::Error)>) + 'static,
        >(
            _source_object: *mut glib::gobject_ffi::GObject,
            res: *mut ffi::GAsyncResult,
            user_data: glib::ffi::gpointer,
        ) {
            unsafe {
                let user_data: Box<(glib::thread_guard::ThreadGuard<Q>, B)> =
                    Box::from_raw(user_data as *mut _);
                let (callback, buffer) = *user_data;
                let callback = callback.into_inner();

                let mut error = ptr::null_mut();
                let mut bytes_read = mem::MaybeUninit::uninit();
                let _ = ffi::g_input_stream_read_all_finish(
                    _source_object as *mut _,
                    res,
                    bytes_read.as_mut_ptr(),
                    &mut error,
                );

                let bytes_read = bytes_read.assume_init();
                let result = if error.is_null() {
                    Ok((buffer, bytes_read, None))
                } else if bytes_read != 0 {
                    Ok((buffer, bytes_read, Some(from_glib_full(error))))
                } else {
                    Err((buffer, from_glib_full(error)))
                };

                callback(result);
            }
        }
        let callback = read_all_async_trampoline::<B, Q>;
        unsafe {
            ffi::g_input_stream_read_all_async(
                self.as_ref().to_glib_none().0,
                buffer_ptr,
                count,
                io_priority.into_glib(),
                gcancellable.0,
                Some(callback),
                Box::into_raw(user_data) as *mut _,
            );
        }
    }

    #[doc(alias = "g_input_stream_read_async")]
    fn read_async<
        B: AsMut<[u8]> + Send + 'static,
        Q: FnOnce(Result<(B, usize), (B, glib::Error)>) + 'static,
        C: IsA<Cancellable>,
    >(
        &self,
        buffer: B,
        io_priority: Priority,
        cancellable: Option<&C>,
        callback: Q,
    ) {
        let main_context = glib::MainContext::ref_thread_default();
        let is_main_context_owner = main_context.is_owner();
        let has_acquired_main_context = (!is_main_context_owner)
            .then(|| main_context.acquire().ok())
            .flatten();
        assert!(
            is_main_context_owner || has_acquired_main_context.is_some(),
            "Async operations only allowed if the thread is owning the MainContext"
        );

        let cancellable = cancellable.map(|c| c.as_ref());
        let gcancellable = cancellable.to_glib_none();
        let mut user_data: Box<(glib::thread_guard::ThreadGuard<Q>, B)> =
            Box::new((glib::thread_guard::ThreadGuard::new(callback), buffer));
        // Need to do this after boxing as the contents pointer might change by moving into the box
        let (count, buffer_ptr) = {
            let buffer = &mut user_data.1;
            let slice = (*buffer).as_mut();
            (slice.len(), slice.as_mut_ptr())
        };
        unsafe extern "C" fn read_async_trampoline<
            B: AsMut<[u8]> + Send + 'static,
            Q: FnOnce(Result<(B, usize), (B, glib::Error)>) + 'static,
        >(
            _source_object: *mut glib::gobject_ffi::GObject,
            res: *mut ffi::GAsyncResult,
            user_data: glib::ffi::gpointer,
        ) {
            unsafe {
                let user_data: Box<(glib::thread_guard::ThreadGuard<Q>, B)> =
                    Box::from_raw(user_data as *mut _);
                let (callback, buffer) = *user_data;
                let callback = callback.into_inner();

                let mut error = ptr::null_mut();
                let ret =
                    ffi::g_input_stream_read_finish(_source_object as *mut _, res, &mut error);

                let result = if error.is_null() {
                    Ok((buffer, ret as usize))
                } else {
                    Err((buffer, from_glib_full(error)))
                };

                callback(result);
            }
        }
        let callback = read_async_trampoline::<B, Q>;
        unsafe {
            ffi::g_input_stream_read_async(
                self.as_ref().to_glib_none().0,
                buffer_ptr,
                count,
                io_priority.into_glib(),
                gcancellable.0,
                Some(callback),
                Box::into_raw(user_data) as *mut _,
            );
        }
    }

    fn read_all_future<B: AsMut<[u8]> + Send + 'static>(
        &self,
        buffer: B,
        io_priority: Priority,
    ) -> Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<(B, usize, Option<glib::Error>), (B, glib::Error)>,
                > + 'static,
        >,
    > {
        Box::pin(crate::GioFuture::new(
            self,
            move |obj, cancellable, send| {
                obj.read_all_async(buffer, io_priority, Some(cancellable), move |res| {
                    send.resolve(res);
                });
            },
        ))
    }

    fn read_future<B: AsMut<[u8]> + Send + 'static>(
        &self,
        buffer: B,
        io_priority: Priority,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<(B, usize), (B, glib::Error)>> + 'static>>
    {
        Box::pin(crate::GioFuture::new(
            self,
            move |obj, cancellable, send| {
                obj.read_async(buffer, io_priority, Some(cancellable), move |res| {
                    send.resolve(res);
                });
            },
        ))
    }

    fn into_read(self) -> InputStreamRead<Self>
    where
        Self: IsA<InputStream>,
    {
        InputStreamRead(self)
    }

    fn into_async_buf_read(self, buffer_size: usize) -> InputStreamAsyncBufRead<Self>
    where
        Self: IsA<InputStream>,
    {
        InputStreamAsyncBufRead::new(self, buffer_size)
    }
}

impl<O: IsA<InputStream>> InputStreamExtManual for O {}

#[derive(Debug)]
pub struct InputStreamRead<T: IsA<InputStream>>(T);

impl<T: IsA<InputStream>> InputStreamRead<T> {
    pub fn into_input_stream(self) -> T {
        self.0
    }

    pub fn input_stream(&self) -> &T {
        &self.0
    }
}

impl<T: IsA<InputStream>> io::Read for InputStreamRead<T> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let gio_result = self.0.as_ref().read(buf, crate::Cancellable::NONE);
        to_std_io_result(gio_result)
    }
}

impl<T: IsA<InputStream> + IsA<Seekable>> io::Seek for InputStreamRead<T> {
    fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
        let (pos, type_) = match pos {
            io::SeekFrom::Start(pos) => (pos as i64, glib::SeekType::Set),
            io::SeekFrom::End(pos) => (pos, glib::SeekType::End),
            io::SeekFrom::Current(pos) => (pos, glib::SeekType::Cur),
        };
        let seekable: &Seekable = self.0.as_ref();
        let gio_result = seekable
            .seek(pos, type_, crate::Cancellable::NONE)
            .map(|_| seekable.tell() as u64);
        to_std_io_result(gio_result)
    }
}

enum State {
    Waiting {
        buffer: Vec<u8>,
    },
    Transitioning,
    Reading {
        pending: Pin<
            Box<
                dyn std::future::Future<Output = Result<(Vec<u8>, usize), (Vec<u8>, glib::Error)>>
                    + 'static,
            >,
        >,
    },
    HasData {
        buffer: Vec<u8>,
        valid: (usize, usize), // first index is inclusive, second is exclusive
    },
    Failed(crate::IOErrorEnum),
}

impl State {
    fn into_buffer(self) -> Vec<u8> {
        match self {
            State::Waiting { buffer } => buffer,
            _ => panic!("Invalid state"),
        }
    }

    #[doc(alias = "get_pending")]
    fn pending(
        &mut self,
    ) -> &mut Pin<
        Box<
            dyn std::future::Future<Output = Result<(Vec<u8>, usize), (Vec<u8>, glib::Error)>>
                + 'static,
        >,
    > {
        match self {
            State::Reading { pending } => pending,
            _ => panic!("Invalid state"),
        }
    }
}
pub struct InputStreamAsyncBufRead<T: IsA<InputStream>> {
    stream: T,
    state: State,
}

impl<T: IsA<InputStream>> InputStreamAsyncBufRead<T> {
    pub fn into_input_stream(self) -> T {
        self.stream
    }

    pub fn input_stream(&self) -> &T {
        &self.stream
    }

    fn new(stream: T, buffer_size: usize) -> Self {
        let buffer = vec![0; buffer_size];

        Self {
            stream,
            state: State::Waiting { buffer },
        }
    }
    fn set_reading(
        &mut self,
    ) -> &mut Pin<
        Box<
            dyn std::future::Future<Output = Result<(Vec<u8>, usize), (Vec<u8>, glib::Error)>>
                + 'static,
        >,
    > {
        match self.state {
            State::Waiting { .. } => {
                let waiting = mem::replace(&mut self.state, State::Transitioning);
                let buffer = waiting.into_buffer();
                let pending = self.input_stream().read_future(buffer, Priority::default());
                self.state = State::Reading { pending };
            }
            State::Reading { .. } => {}
            _ => panic!("Invalid state"),
        };

        self.state.pending()
    }

    #[doc(alias = "get_data")]
    fn data(&self) -> Poll<io::Result<&[u8]>> {
        if let State::HasData {
            ref buffer,
            valid: (i, j),
        } = self.state
        {
            return Poll::Ready(Ok(&buffer[i..j]));
        }
        panic!("Invalid state")
    }

    fn set_waiting(&mut self, buffer: Vec<u8>) {
        match self.state {
            State::Reading { .. } | State::Transitioning => self.state = State::Waiting { buffer },
            _ => panic!("Invalid state"),
        }
    }

    fn set_has_data(&mut self, buffer: Vec<u8>, valid: (usize, usize)) {
        match self.state {
            State::Reading { .. } | State::Transitioning => {
                self.state = State::HasData { buffer, valid }
            }
            _ => panic!("Invalid state"),
        }
    }

    fn poll_fill_buf(&mut self, cx: &mut Context) -> Poll<Result<&[u8], futures_io::Error>> {
        match self.state {
            State::Failed(kind) => Poll::Ready(Err(io::Error::new(
                io::ErrorKind::from(kind),
                BufReadError::Failed,
            ))),
            State::HasData { .. } => self.data(),
            State::Transitioning => panic!("Invalid state"),
            State::Waiting { .. } | State::Reading { .. } => {
                let pending = self.set_reading();
                match Pin::new(pending).poll(cx) {
                    Poll::Ready(Ok((buffer, res))) => {
                        if res == 0 {
                            self.set_waiting(buffer);
                            Poll::Ready(Ok(&[]))
                        } else {
                            self.set_has_data(buffer, (0, res));
                            self.data()
                        }
                    }
                    Poll::Ready(Err((_, err))) => {
                        let kind = err
                            .kind::<crate::IOErrorEnum>()
                            .unwrap_or(crate::IOErrorEnum::Failed);
                        self.state = State::Failed(kind);
                        Poll::Ready(Err(io::Error::new(io::ErrorKind::from(kind), err)))
                    }
                    Poll::Pending => Poll::Pending,
                }
            }
        }
    }

    fn consume(&mut self, amt: usize) {
        if amt == 0 {
            return;
        }

        if let State::HasData { .. } = self.state {
            let has_data = mem::replace(&mut self.state, State::Transitioning);
            if let State::HasData {
                buffer,
                valid: (i, j),
            } = has_data
            {
                let available = j - i;
                if amt > available {
                    panic!("Cannot consume {amt} bytes as only {available} are available",)
                }
                let remaining = available - amt;
                if remaining == 0 {
                    return self.set_waiting(buffer);
                } else {
                    return self.set_has_data(buffer, (i + amt, j));
                }
            }
        }

        panic!("Invalid state")
    }
}

#[derive(Debug)]
enum BufReadError {
    Failed,
}

impl std::error::Error for BufReadError {}

impl fmt::Display for BufReadError {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Failed => fmt.write_str("Previous read operation failed"),
        }
    }
}

impl<T: IsA<InputStream>> AsyncRead for InputStreamAsyncBufRead<T> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context,
        out_buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        let reader = self.get_mut();
        let poll = reader.poll_fill_buf(cx);

        let poll = poll.map_ok(|buffer| {
            let copied = buffer.len().min(out_buf.len());
            out_buf[..copied].copy_from_slice(&buffer[..copied]);
            copied
        });

        if let Poll::Ready(Ok(consumed)) = poll {
            reader.consume(consumed);
        }
        poll
    }
}

impl<T: IsA<InputStream>> AsyncBufRead for InputStreamAsyncBufRead<T> {
    fn poll_fill_buf(
        self: Pin<&mut Self>,
        cx: &mut Context,
    ) -> Poll<Result<&[u8], futures_io::Error>> {
        self.get_mut().poll_fill_buf(cx)
    }

    fn consume(self: Pin<&mut Self>, amt: usize) {
        self.get_mut().consume(amt);
    }
}

impl<T: IsA<InputStream>> Unpin for InputStreamAsyncBufRead<T> {}

#[cfg(test)]
mod tests {
    use std::io::Read;

    use glib::Bytes;

    use crate::{MemoryInputStream, prelude::*, test_util::run_async};

    #[test]
    fn read_all_async() {
        let ret = run_async(|tx, l| {
            let b = Bytes::from_owned(vec![1, 2, 3]);
            let strm = MemoryInputStream::from_bytes(&b);

            let buf = vec![0; 10];
            strm.read_all_async(
                buf,
                glib::Priority::DEFAULT_IDLE,
                crate::Cancellable::NONE,
                move |ret| {
                    tx.send(ret).unwrap();
                    l.quit();
                },
            );
        });

        let (buf, count, err) = ret.unwrap();
        assert_eq!(count, 3);
        assert!(err.is_none());
        assert_eq!(buf[0], 1);
        assert_eq!(buf[1], 2);
        assert_eq!(buf[2], 3);
    }

    #[test]
    fn read_all() {
        let b = Bytes::from_owned(vec![1, 2, 3]);
        let strm = MemoryInputStream::from_bytes(&b);
        let mut buf = vec![0; 10];

        let ret = strm.read_all(&mut buf, crate::Cancellable::NONE).unwrap();

        assert_eq!(ret.0, 3);
        assert!(ret.1.is_none());
        assert_eq!(buf[0], 1);
        assert_eq!(buf[1], 2);
        assert_eq!(buf[2], 3);
    }

    #[test]
    fn read() {
        let b = Bytes::from_owned(vec![1, 2, 3]);
        let strm = MemoryInputStream::from_bytes(&b);
        let mut buf = vec![0; 10];

        let ret = strm.read(&mut buf, crate::Cancellable::NONE);

        assert_eq!(ret.unwrap(), 3);
        assert_eq!(buf[0], 1);
        assert_eq!(buf[1], 2);
        assert_eq!(buf[2], 3);
    }

    #[test]
    fn read_async() {
        let ret = run_async(|tx, l| {
            let b = Bytes::from_owned(vec![1, 2, 3]);
            let strm = MemoryInputStream::from_bytes(&b);

            let buf = vec![0; 10];
            strm.read_async(
                buf,
                glib::Priority::DEFAULT_IDLE,
                crate::Cancellable::NONE,
                move |ret| {
                    tx.send(ret).unwrap();
                    l.quit();
                },
            );
        });

        let (buf, count) = ret.unwrap();
        assert_eq!(count, 3);
        assert_eq!(buf[0], 1);
        assert_eq!(buf[1], 2);
        assert_eq!(buf[2], 3);
    }

    #[test]
    fn read_bytes_async() {
        let ret = run_async(|tx, l| {
            let b = Bytes::from_owned(vec![1, 2, 3]);
            let strm = MemoryInputStream::from_bytes(&b);

            strm.read_bytes_async(
                10,
                glib::Priority::DEFAULT_IDLE,
                crate::Cancellable::NONE,
                move |ret| {
                    tx.send(ret).unwrap();
                    l.quit();
                },
            );
        });

        let bytes = ret.unwrap();
        assert_eq!(bytes, vec![1, 2, 3]);
    }

    #[test]
    fn skip_async() {
        let ret = run_async(|tx, l| {
            let b = Bytes::from_owned(vec![1, 2, 3]);
            let strm = MemoryInputStream::from_bytes(&b);

            strm.skip_async(
                10,
                glib::Priority::DEFAULT_IDLE,
                crate::Cancellable::NONE,
                move |ret| {
                    tx.send(ret).unwrap();
                    l.quit();
                },
            );
        });

        let skipped = ret.unwrap();
        assert_eq!(skipped, 3);
    }

    #[test]
    fn std_io_read() {
        let b = Bytes::from_owned(vec![1, 2, 3]);
        let mut read = MemoryInputStream::from_bytes(&b).into_read();
        let mut buf = [0u8; 10];

        let ret = read.read(&mut buf);

        assert_eq!(ret.unwrap(), 3);
        assert_eq!(buf[0], 1);
        assert_eq!(buf[1], 2);
        assert_eq!(buf[2], 3);
    }

    #[test]
    fn into_input_stream() {
        let b = Bytes::from_owned(vec![1, 2, 3]);
        let stream = MemoryInputStream::from_bytes(&b);
        let stream_clone = stream.clone();
        let stream = stream.into_read().into_input_stream();

        assert_eq!(stream, stream_clone);
    }
}
