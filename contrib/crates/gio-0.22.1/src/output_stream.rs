// Take a look at the license at the top of the repository in the LICENSE file.

use std::{io, mem, pin::Pin, ptr};

use glib::{Priority, prelude::*, translate::*};

#[cfg(feature = "v2_60")]
use crate::OutputVector;
use crate::{Cancellable, OutputStream, Seekable, error::to_std_io_result, ffi, prelude::*};

pub trait OutputStreamExtManual: IsA<OutputStream> + Sized {
    #[doc(alias = "g_output_stream_write_async")]
    fn write_async<
        B: AsRef<[u8]> + Send + 'static,
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
        let user_data: Box<(glib::thread_guard::ThreadGuard<Q>, B)> =
            Box::new((glib::thread_guard::ThreadGuard::new(callback), buffer));
        // Need to do this after boxing as the contents pointer might change by moving into the box
        let (count, buffer_ptr) = {
            let buffer = &user_data.1;
            let slice = buffer.as_ref();
            (slice.len(), slice.as_ptr())
        };
        unsafe extern "C" fn write_async_trampoline<
            B: AsRef<[u8]> + Send + 'static,
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
                    ffi::g_output_stream_write_finish(_source_object as *mut _, res, &mut error);
                let result = if error.is_null() {
                    Ok((buffer, ret as usize))
                } else {
                    Err((buffer, from_glib_full(error)))
                };
                callback(result);
            }
        }
        let callback = write_async_trampoline::<B, Q>;
        unsafe {
            ffi::g_output_stream_write_async(
                self.as_ref().to_glib_none().0,
                mut_override(buffer_ptr),
                count,
                io_priority.into_glib(),
                gcancellable.0,
                Some(callback),
                Box::into_raw(user_data) as *mut _,
            );
        }
    }

    #[doc(alias = "g_output_stream_write_all")]
    fn write_all<C: IsA<Cancellable>>(
        &self,
        buffer: &[u8],
        cancellable: Option<&C>,
    ) -> Result<(usize, Option<glib::Error>), glib::Error> {
        let cancellable = cancellable.map(|c| c.as_ref());
        let gcancellable = cancellable.to_glib_none();
        let count = buffer.len();
        unsafe {
            let mut bytes_written = mem::MaybeUninit::uninit();
            let mut error = ptr::null_mut();
            let _ = ffi::g_output_stream_write_all(
                self.as_ref().to_glib_none().0,
                buffer.to_glib_none().0,
                count,
                bytes_written.as_mut_ptr(),
                gcancellable.0,
                &mut error,
            );

            let bytes_written = bytes_written.assume_init();
            if error.is_null() {
                Ok((bytes_written, None))
            } else if bytes_written != 0 {
                Ok((bytes_written, Some(from_glib_full(error))))
            } else {
                Err(from_glib_full(error))
            }
        }
    }

    #[doc(alias = "g_output_stream_write_all_async")]
    fn write_all_async<
        B: AsRef<[u8]> + Send + 'static,
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
        let user_data: Box<(glib::thread_guard::ThreadGuard<Q>, B)> =
            Box::new((glib::thread_guard::ThreadGuard::new(callback), buffer));
        // Need to do this after boxing as the contents pointer might change by moving into the box
        let (count, buffer_ptr) = {
            let buffer = &user_data.1;
            let slice = buffer.as_ref();
            (slice.len(), slice.as_ptr())
        };
        unsafe extern "C" fn write_all_async_trampoline<
            B: AsRef<[u8]> + Send + 'static,
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
                let mut bytes_written = mem::MaybeUninit::uninit();
                let _ = ffi::g_output_stream_write_all_finish(
                    _source_object as *mut _,
                    res,
                    bytes_written.as_mut_ptr(),
                    &mut error,
                );
                let bytes_written = bytes_written.assume_init();
                let result = if error.is_null() {
                    Ok((buffer, bytes_written, None))
                } else if bytes_written != 0 {
                    Ok((buffer, bytes_written, from_glib_full(error)))
                } else {
                    Err((buffer, from_glib_full(error)))
                };
                callback(result);
            }
        }
        let callback = write_all_async_trampoline::<B, Q>;
        unsafe {
            ffi::g_output_stream_write_all_async(
                self.as_ref().to_glib_none().0,
                mut_override(buffer_ptr),
                count,
                io_priority.into_glib(),
                gcancellable.0,
                Some(callback),
                Box::into_raw(user_data) as *mut _,
            );
        }
    }

    fn write_future<B: AsRef<[u8]> + Send + 'static>(
        &self,
        buffer: B,
        io_priority: Priority,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<(B, usize), (B, glib::Error)>> + 'static>>
    {
        Box::pin(crate::GioFuture::new(
            self,
            move |obj, cancellable, send| {
                obj.write_async(buffer, io_priority, Some(cancellable), move |res| {
                    send.resolve(res);
                });
            },
        ))
    }

    fn write_all_future<B: AsRef<[u8]> + Send + 'static>(
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
                obj.write_all_async(buffer, io_priority, Some(cancellable), move |res| {
                    send.resolve(res);
                });
            },
        ))
    }

    #[cfg(feature = "v2_60")]
    #[cfg_attr(docsrs, doc(cfg(feature = "v2_60")))]
    #[doc(alias = "g_output_stream_writev")]
    fn writev(
        &self,
        vectors: &[OutputVector],
        cancellable: Option<&impl IsA<Cancellable>>,
    ) -> Result<usize, glib::Error> {
        unsafe {
            let mut error = ptr::null_mut();
            let mut bytes_written = mem::MaybeUninit::uninit();

            ffi::g_output_stream_writev(
                self.as_ref().to_glib_none().0,
                vectors.as_ptr() as *const _,
                vectors.len(),
                bytes_written.as_mut_ptr(),
                cancellable.map(|p| p.as_ref()).to_glib_none().0,
                &mut error,
            );
            if error.is_null() {
                Ok(bytes_written.assume_init())
            } else {
                Err(from_glib_full(error))
            }
        }
    }

    #[cfg(feature = "v2_60")]
    #[cfg_attr(docsrs, doc(cfg(feature = "v2_60")))]
    #[doc(alias = "g_output_stream_writev_async")]
    fn writev_async<
        B: AsRef<[u8]> + Send + 'static,
        P: FnOnce(Result<(Vec<B>, usize), (Vec<B>, glib::Error)>) + 'static,
    >(
        &self,
        vectors: impl IntoIterator<Item = B> + 'static,
        io_priority: glib::Priority,
        cancellable: Option<&impl IsA<Cancellable>>,
        callback: P,
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
        let buffers = vectors.into_iter().collect::<Vec<_>>();
        let vectors = buffers
            .iter()
            .map(|v| ffi::GOutputVector {
                buffer: v.as_ref().as_ptr() as *const _,
                size: v.as_ref().len(),
            })
            .collect::<Vec<_>>();
        let vectors_ptr = vectors.as_ptr();
        let num_vectors = vectors.len();
        let user_data: Box<(
            glib::thread_guard::ThreadGuard<P>,
            Vec<B>,
            Vec<ffi::GOutputVector>,
        )> = Box::new((
            glib::thread_guard::ThreadGuard::new(callback),
            buffers,
            vectors,
        ));

        unsafe extern "C" fn writev_async_trampoline<
            B: AsRef<[u8]> + Send + 'static,
            P: FnOnce(Result<(Vec<B>, usize), (Vec<B>, glib::Error)>) + 'static,
        >(
            _source_object: *mut glib::gobject_ffi::GObject,
            res: *mut ffi::GAsyncResult,
            user_data: glib::ffi::gpointer,
        ) {
            unsafe {
                let user_data: Box<(
                    glib::thread_guard::ThreadGuard<P>,
                    Vec<B>,
                    Vec<ffi::GOutputVector>,
                )> = Box::from_raw(user_data as *mut _);
                let (callback, buffers, _) = *user_data;
                let callback = callback.into_inner();

                let mut error = ptr::null_mut();
                let mut bytes_written = mem::MaybeUninit::uninit();
                ffi::g_output_stream_writev_finish(
                    _source_object as *mut _,
                    res,
                    bytes_written.as_mut_ptr(),
                    &mut error,
                );
                let bytes_written = bytes_written.assume_init();
                let result = if error.is_null() {
                    Ok((buffers, bytes_written))
                } else {
                    Err((buffers, from_glib_full(error)))
                };
                callback(result);
            }
        }
        let callback = writev_async_trampoline::<B, P>;
        unsafe {
            ffi::g_output_stream_writev_async(
                self.as_ref().to_glib_none().0,
                vectors_ptr,
                num_vectors,
                io_priority.into_glib(),
                gcancellable.0,
                Some(callback),
                Box::into_raw(user_data) as *mut _,
            );
        }
    }

    #[cfg(feature = "v2_60")]
    #[cfg_attr(docsrs, doc(cfg(feature = "v2_60")))]
    fn writev_future<B: AsRef<[u8]> + Send + 'static>(
        &self,
        vectors: impl IntoIterator<Item = B> + 'static,
        io_priority: glib::Priority,
    ) -> Pin<
        Box<
            dyn std::future::Future<Output = Result<(Vec<B>, usize), (Vec<B>, glib::Error)>>
                + 'static,
        >,
    > {
        Box::pin(crate::GioFuture::new(
            self,
            move |obj, cancellable, send| {
                obj.writev_async(vectors, io_priority, Some(cancellable), move |res| {
                    send.resolve(res);
                });
            },
        ))
    }

    #[cfg(feature = "v2_60")]
    #[cfg_attr(docsrs, doc(cfg(feature = "v2_60")))]
    #[doc(alias = "g_output_stream_writev_all")]
    fn writev_all(
        &self,
        vectors: &[OutputVector],
        cancellable: Option<&impl IsA<Cancellable>>,
    ) -> Result<(usize, Option<glib::Error>), glib::Error> {
        unsafe {
            let mut error = ptr::null_mut();
            let mut bytes_written = mem::MaybeUninit::uninit();

            ffi::g_output_stream_writev_all(
                self.as_ref().to_glib_none().0,
                mut_override(vectors.as_ptr() as *const _),
                vectors.len(),
                bytes_written.as_mut_ptr(),
                cancellable.map(|p| p.as_ref()).to_glib_none().0,
                &mut error,
            );
            let bytes_written = bytes_written.assume_init();
            if error.is_null() {
                Ok((bytes_written, None))
            } else if bytes_written != 0 {
                Ok((bytes_written, Some(from_glib_full(error))))
            } else {
                Err(from_glib_full(error))
            }
        }
    }

    #[cfg(feature = "v2_60")]
    #[cfg_attr(docsrs, doc(cfg(feature = "v2_60")))]
    #[doc(alias = "g_output_stream_writev_all_async")]
    fn writev_all_async<
        B: AsRef<[u8]> + Send + 'static,
        P: FnOnce(Result<(Vec<B>, usize, Option<glib::Error>), (Vec<B>, glib::Error)>) + 'static,
    >(
        &self,
        vectors: impl IntoIterator<Item = B> + 'static,
        io_priority: glib::Priority,
        cancellable: Option<&impl IsA<Cancellable>>,
        callback: P,
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
        let buffers = vectors.into_iter().collect::<Vec<_>>();
        let vectors = buffers
            .iter()
            .map(|v| ffi::GOutputVector {
                buffer: v.as_ref().as_ptr() as *const _,
                size: v.as_ref().len(),
            })
            .collect::<Vec<_>>();
        let vectors_ptr = vectors.as_ptr();
        let num_vectors = vectors.len();
        let user_data: Box<(
            glib::thread_guard::ThreadGuard<P>,
            Vec<B>,
            Vec<ffi::GOutputVector>,
        )> = Box::new((
            glib::thread_guard::ThreadGuard::new(callback),
            buffers,
            vectors,
        ));

        unsafe extern "C" fn writev_all_async_trampoline<
            B: AsRef<[u8]> + Send + 'static,
            P: FnOnce(Result<(Vec<B>, usize, Option<glib::Error>), (Vec<B>, glib::Error)>) + 'static,
        >(
            _source_object: *mut glib::gobject_ffi::GObject,
            res: *mut ffi::GAsyncResult,
            user_data: glib::ffi::gpointer,
        ) {
            unsafe {
                let user_data: Box<(
                    glib::thread_guard::ThreadGuard<P>,
                    Vec<B>,
                    Vec<ffi::GOutputVector>,
                )> = Box::from_raw(user_data as *mut _);
                let (callback, buffers, _) = *user_data;
                let callback = callback.into_inner();

                let mut error = ptr::null_mut();
                let mut bytes_written = mem::MaybeUninit::uninit();
                ffi::g_output_stream_writev_all_finish(
                    _source_object as *mut _,
                    res,
                    bytes_written.as_mut_ptr(),
                    &mut error,
                );
                let bytes_written = bytes_written.assume_init();
                let result = if error.is_null() {
                    Ok((buffers, bytes_written, None))
                } else if bytes_written != 0 {
                    Ok((buffers, bytes_written, from_glib_full(error)))
                } else {
                    Err((buffers, from_glib_full(error)))
                };
                callback(result);
            }
        }
        let callback = writev_all_async_trampoline::<B, P>;
        unsafe {
            ffi::g_output_stream_writev_all_async(
                self.as_ref().to_glib_none().0,
                mut_override(vectors_ptr),
                num_vectors,
                io_priority.into_glib(),
                gcancellable.0,
                Some(callback),
                Box::into_raw(user_data) as *mut _,
            );
        }
    }

    #[cfg(feature = "v2_60")]
    #[cfg_attr(docsrs, doc(cfg(feature = "v2_60")))]
    fn writev_all_future<B: AsRef<[u8]> + Send + 'static>(
        &self,
        vectors: impl IntoIterator<Item = B> + 'static,
        io_priority: glib::Priority,
    ) -> Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<(Vec<B>, usize, Option<glib::Error>), (Vec<B>, glib::Error)>,
                > + 'static,
        >,
    > {
        Box::pin(crate::GioFuture::new(
            self,
            move |obj, cancellable, send| {
                obj.writev_all_async(vectors, io_priority, Some(cancellable), move |res| {
                    send.resolve(res);
                });
            },
        ))
    }

    fn into_write(self) -> OutputStreamWrite<Self>
    where
        Self: IsA<OutputStream>,
    {
        OutputStreamWrite(self)
    }
}

impl<O: IsA<OutputStream>> OutputStreamExtManual for O {}

#[derive(Debug)]
pub struct OutputStreamWrite<T: IsA<OutputStream>>(T);

impl<T: IsA<OutputStream>> OutputStreamWrite<T> {
    pub fn into_output_stream(self) -> T {
        self.0
    }

    pub fn output_stream(&self) -> &T {
        &self.0
    }
}

impl<T: IsA<OutputStream>> io::Write for OutputStreamWrite<T> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let result = self
            .0
            .as_ref()
            .write(buf, crate::Cancellable::NONE)
            .map(|size| size as usize);
        to_std_io_result(result)
    }

    fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        let result = self
            .0
            .as_ref()
            .write_all(buf, crate::Cancellable::NONE)
            .and_then(|(_, e)| e.map(Err).unwrap_or(Ok(())));
        to_std_io_result(result)
    }

    #[cfg(feature = "v2_60")]
    #[cfg_attr(docsrs, doc(cfg(feature = "v2_60")))]
    fn write_vectored(&mut self, bufs: &[io::IoSlice<'_>]) -> io::Result<usize> {
        let vectors = bufs
            .iter()
            .map(|v| OutputVector::new(v))
            .collect::<smallvec::SmallVec<[_; 2]>>();
        let result = self.0.as_ref().writev(&vectors, crate::Cancellable::NONE);
        to_std_io_result(result)
    }

    fn flush(&mut self) -> io::Result<()> {
        let gio_result = self.0.as_ref().flush(crate::Cancellable::NONE);
        to_std_io_result(gio_result)
    }
}

impl<T: IsA<OutputStream> + IsA<Seekable>> io::Seek for OutputStreamWrite<T> {
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

#[cfg(test)]
mod tests {
    use std::io::Write;

    use glib::Bytes;

    #[cfg(feature = "v2_60")]
    use crate::OutputVector;
    use crate::{MemoryInputStream, MemoryOutputStream, prelude::*, test_util::run_async};

    #[test]
    fn splice_async() {
        let ret = run_async(|tx, l| {
            let input = MemoryInputStream::new();
            let b = Bytes::from_owned(vec![1, 2, 3]);
            input.add_bytes(&b);

            let strm = MemoryOutputStream::new_resizable();
            strm.splice_async(
                &input,
                crate::OutputStreamSpliceFlags::CLOSE_SOURCE,
                glib::Priority::DEFAULT_IDLE,
                crate::Cancellable::NONE,
                move |ret| {
                    tx.send(ret).unwrap();
                    l.quit();
                },
            );
        });

        assert_eq!(ret.unwrap(), 3);
    }

    #[test]
    fn write_async() {
        let ret = run_async(|tx, l| {
            let strm = MemoryOutputStream::new_resizable();

            let buf = vec![1, 2, 3];
            strm.write_async(
                buf,
                glib::Priority::DEFAULT_IDLE,
                crate::Cancellable::NONE,
                move |ret| {
                    tx.send(ret).unwrap();
                    l.quit();
                },
            );
        });

        let (buf, size) = ret.unwrap();
        assert_eq!(buf, vec![1, 2, 3]);
        assert_eq!(size, 3);
    }

    #[test]
    fn write_all_async() {
        let ret = run_async(|tx, l| {
            let strm = MemoryOutputStream::new_resizable();

            let buf = vec![1, 2, 3];
            strm.write_all_async(
                buf,
                glib::Priority::DEFAULT_IDLE,
                crate::Cancellable::NONE,
                move |ret| {
                    tx.send(ret).unwrap();
                    l.quit();
                },
            );
        });

        let (buf, size, err) = ret.unwrap();
        assert_eq!(buf, vec![1, 2, 3]);
        assert_eq!(size, 3);
        assert!(err.is_none());
    }

    #[test]
    fn write_bytes_async() {
        let ret = run_async(|tx, l| {
            let strm = MemoryOutputStream::new_resizable();

            let b = Bytes::from_owned(vec![1, 2, 3]);
            strm.write_bytes_async(
                &b,
                glib::Priority::DEFAULT_IDLE,
                crate::Cancellable::NONE,
                move |ret| {
                    tx.send(ret).unwrap();
                    l.quit();
                },
            );
        });

        assert_eq!(ret.unwrap(), 3);
    }

    #[test]
    fn std_io_write() {
        let b = Bytes::from_owned(vec![1, 2, 3]);
        let mut write = MemoryOutputStream::new_resizable().into_write();

        let ret = write.write(&b);

        let stream = write.into_output_stream();
        stream.close(crate::Cancellable::NONE).unwrap();
        assert_eq!(ret.unwrap(), 3);
        assert_eq!(stream.steal_as_bytes(), [1, 2, 3].as_ref());
    }

    #[test]
    fn into_output_stream() {
        let stream = MemoryOutputStream::new_resizable();
        let stream_clone = stream.clone();
        let stream = stream.into_write().into_output_stream();

        assert_eq!(stream, stream_clone);
    }

    #[test]
    #[cfg(feature = "v2_60")]
    fn writev() {
        let stream = MemoryOutputStream::new_resizable();

        let ret = stream.writev(
            &[OutputVector::new(&[1, 2, 3]), OutputVector::new(&[4, 5, 6])],
            crate::Cancellable::NONE,
        );
        assert_eq!(ret.unwrap(), 6);
        stream.close(crate::Cancellable::NONE).unwrap();
        assert_eq!(stream.steal_as_bytes(), [1, 2, 3, 4, 5, 6].as_ref());
    }

    #[test]
    #[cfg(feature = "v2_60")]
    fn writev_async() {
        let ret = run_async(|tx, l| {
            let strm = MemoryOutputStream::new_resizable();

            let strm_clone = strm.clone();
            strm.writev_async(
                [vec![1, 2, 3], vec![4, 5, 6]],
                glib::Priority::DEFAULT_IDLE,
                crate::Cancellable::NONE,
                move |ret| {
                    tx.send(ret).unwrap();
                    strm_clone.close(crate::Cancellable::NONE).unwrap();
                    assert_eq!(strm_clone.steal_as_bytes(), [1, 2, 3, 4, 5, 6].as_ref());
                    l.quit();
                },
            );
        });

        let (buf, size) = ret.unwrap();
        assert_eq!(buf, [[1, 2, 3], [4, 5, 6]]);
        assert_eq!(size, 6);
    }

    #[test]
    #[cfg(feature = "v2_60")]
    fn writev_all_async() {
        let ret = run_async(|tx, l| {
            let strm = MemoryOutputStream::new_resizable();

            let strm_clone = strm.clone();
            strm.writev_all_async(
                [vec![1, 2, 3], vec![4, 5, 6]],
                glib::Priority::DEFAULT_IDLE,
                crate::Cancellable::NONE,
                move |ret| {
                    tx.send(ret).unwrap();
                    strm_clone.close(crate::Cancellable::NONE).unwrap();
                    assert_eq!(strm_clone.steal_as_bytes(), [1, 2, 3, 4, 5, 6].as_ref());
                    l.quit();
                },
            );
        });

        let (buf, size, err) = ret.unwrap();
        assert_eq!(buf, [[1, 2, 3], [4, 5, 6]]);
        assert_eq!(size, 6);
        assert!(err.is_none());
    }
}
