// Take a look at the license at the top of the repository in the LICENSE file.

use std::{cell::RefCell, io, mem::transmute, pin::Pin};

use futures_channel::oneshot;
use futures_core::{
    Future,
    stream::Stream,
    task::{Context, Poll},
};
use futures_io::AsyncWrite;
use glib::{prelude::*, translate::*};

use crate::{Cancellable, PollableOutputStream, error::to_std_io_result, ffi, prelude::*};
#[cfg(feature = "v2_60")]
use crate::{OutputVector, PollableReturn};

pub trait PollableOutputStreamExtManual: IsA<PollableOutputStream> {
    #[doc(alias = "g_pollable_output_stream_create_source")]
    fn create_source<F, C>(
        &self,
        cancellable: Option<&C>,
        name: Option<&str>,
        priority: glib::Priority,
        func: F,
    ) -> glib::Source
    where
        F: FnMut(&Self) -> glib::ControlFlow + 'static,
        C: IsA<Cancellable>,
    {
        unsafe extern "C" fn trampoline<
            O: IsA<PollableOutputStream>,
            F: FnMut(&O) -> glib::ControlFlow + 'static,
        >(
            stream: *mut ffi::GPollableOutputStream,
            func: glib::ffi::gpointer,
        ) -> glib::ffi::gboolean {
            unsafe {
                let func: &RefCell<F> = &*(func as *const RefCell<F>);
                let mut func = func.borrow_mut();
                (*func)(PollableOutputStream::from_glib_borrow(stream).unsafe_cast_ref())
                    .into_glib()
            }
        }
        unsafe extern "C" fn destroy_closure<F>(ptr: glib::ffi::gpointer) {
            unsafe {
                let _ = Box::<RefCell<F>>::from_raw(ptr as *mut _);
            }
        }
        let cancellable = cancellable.map(|c| c.as_ref());
        let gcancellable = cancellable.to_glib_none();
        unsafe {
            let source = ffi::g_pollable_output_stream_create_source(
                self.as_ref().to_glib_none().0,
                gcancellable.0,
            );

            let trampoline = trampoline::<Self, F> as glib::ffi::gpointer;
            glib::ffi::g_source_set_callback(
                source,
                Some(transmute::<
                    glib::ffi::gpointer,
                    unsafe extern "C" fn(glib::ffi::gpointer) -> glib::ffi::gboolean,
                >(trampoline)),
                Box::into_raw(Box::new(RefCell::new(func))) as glib::ffi::gpointer,
                Some(destroy_closure::<F>),
            );
            glib::ffi::g_source_set_priority(source, priority.into_glib());

            if let Some(name) = name {
                glib::ffi::g_source_set_name(source, name.to_glib_none().0);
            }

            from_glib_full(source)
        }
    }

    fn create_source_future<C: IsA<Cancellable>>(
        &self,
        cancellable: Option<&C>,
        priority: glib::Priority,
    ) -> Pin<Box<dyn std::future::Future<Output = ()> + 'static>> {
        let cancellable: Option<Cancellable> = cancellable.map(|c| c.as_ref()).cloned();

        let obj = self.clone();
        Box::pin(glib::SourceFuture::new(move |send| {
            let mut send = Some(send);
            obj.create_source(cancellable.as_ref(), None, priority, move |_| {
                let _ = send.take().unwrap().send(());
                glib::ControlFlow::Break
            })
        }))
    }

    fn create_source_stream<C: IsA<Cancellable>>(
        &self,
        cancellable: Option<&C>,
        priority: glib::Priority,
    ) -> Pin<Box<dyn Stream<Item = ()> + 'static>> {
        let cancellable: Option<Cancellable> = cancellable.map(|c| c.as_ref()).cloned();

        let obj = self.clone();
        Box::pin(glib::SourceStream::new(move |send| {
            let send = Some(send);
            obj.create_source(cancellable.as_ref(), None, priority, move |_| {
                if send.as_ref().unwrap().unbounded_send(()).is_err() {
                    glib::ControlFlow::Break
                } else {
                    glib::ControlFlow::Continue
                }
            })
        }))
    }

    #[cfg(feature = "v2_60")]
    #[cfg_attr(docsrs, doc(cfg(feature = "v2_60")))]
    #[doc(alias = "g_pollable_output_stream_writev_nonblocking")]
    fn writev_nonblocking(
        &self,
        vectors: &[OutputVector],
        cancellable: Option<&impl IsA<Cancellable>>,
    ) -> Result<(PollableReturn, usize), glib::Error> {
        unsafe {
            let mut error = std::ptr::null_mut();
            let mut bytes_written = 0;

            let ret = ffi::g_pollable_output_stream_writev_nonblocking(
                self.as_ref().to_glib_none().0,
                vectors.as_ptr() as *const _,
                vectors.len(),
                &mut bytes_written,
                cancellable.map(|p| p.as_ref()).to_glib_none().0,
                &mut error,
            );
            if error.is_null() {
                Ok((from_glib(ret), bytes_written))
            } else {
                Err(from_glib_full(error))
            }
        }
    }

    fn into_async_write(self) -> Result<OutputStreamAsyncWrite<Self>, Self>
    where
        Self: IsA<PollableOutputStream>,
    {
        if self.can_poll() {
            Ok(OutputStreamAsyncWrite(self, None))
        } else {
            Err(self)
        }
    }
}

impl<O: IsA<PollableOutputStream>> PollableOutputStreamExtManual for O {}

#[derive(Debug)]
pub struct OutputStreamAsyncWrite<T: IsA<PollableOutputStream>>(
    T,
    Option<oneshot::Receiver<Result<(), glib::Error>>>,
);

impl<T: IsA<PollableOutputStream>> OutputStreamAsyncWrite<T> {
    pub fn into_output_stream(self) -> T {
        self.0
    }

    pub fn output_stream(&self) -> &T {
        &self.0
    }
}

impl<T: IsA<PollableOutputStream>> AsyncWrite for OutputStreamAsyncWrite<T> {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context, buf: &[u8]) -> Poll<io::Result<usize>> {
        let stream = Pin::get_ref(self.as_ref());
        let gio_result = stream
            .0
            .as_ref()
            .write_nonblocking(buf, crate::Cancellable::NONE);

        match gio_result {
            Ok(size) => Poll::Ready(Ok(size as usize)),
            Err(err) => {
                let kind = err
                    .kind::<crate::IOErrorEnum>()
                    .unwrap_or(crate::IOErrorEnum::Failed);
                if kind == crate::IOErrorEnum::WouldBlock {
                    let mut waker = Some(cx.waker().clone());
                    let source = stream.0.as_ref().create_source(
                        crate::Cancellable::NONE,
                        None,
                        glib::Priority::default(),
                        move |_| {
                            if let Some(waker) = waker.take() {
                                waker.wake();
                            }
                            glib::ControlFlow::Break
                        },
                    );
                    let main_context = glib::MainContext::ref_thread_default();
                    source.attach(Some(&main_context));

                    Poll::Pending
                } else {
                    Poll::Ready(Err(io::Error::new(io::ErrorKind::from(kind), err)))
                }
            }
        }
    }

    #[cfg(feature = "v2_60")]
    #[cfg_attr(docsrs, doc(cfg(feature = "v2_60")))]
    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let stream = Pin::get_ref(self.as_ref());
        let vectors = bufs
            .iter()
            .map(|v| OutputVector::new(v))
            .collect::<smallvec::SmallVec<[_; 2]>>();
        let gio_result = stream
            .0
            .as_ref()
            .writev_nonblocking(&vectors, crate::Cancellable::NONE);

        match gio_result {
            Ok((PollableReturn::Ok, size)) => Poll::Ready(Ok(size)),
            Ok((PollableReturn::WouldBlock, _)) => {
                let mut waker = Some(cx.waker().clone());
                let source = stream.0.as_ref().create_source(
                    crate::Cancellable::NONE,
                    None,
                    glib::Priority::default(),
                    move |_| {
                        if let Some(waker) = waker.take() {
                            waker.wake();
                        }
                        glib::ControlFlow::Break
                    },
                );
                let main_context = glib::MainContext::ref_thread_default();
                source.attach(Some(&main_context));

                Poll::Pending
            }
            Ok((_, _)) => unreachable!(),
            Err(err) => Poll::Ready(Err(io::Error::new(
                io::ErrorKind::from(
                    err.kind::<crate::IOErrorEnum>()
                        .unwrap_or(crate::IOErrorEnum::Failed),
                ),
                err,
            ))),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context) -> Poll<io::Result<()>> {
        let stream = unsafe { Pin::get_unchecked_mut(self) };

        let rx = if let Some(ref mut rx) = stream.1 {
            rx
        } else {
            let (tx, rx) = oneshot::channel();
            stream.0.as_ref().flush_async(
                glib::Priority::default(),
                crate::Cancellable::NONE,
                move |res| {
                    let _ = tx.send(res);
                },
            );

            stream.1 = Some(rx);
            stream.1.as_mut().unwrap()
        };

        match Pin::new(rx).poll(cx) {
            Poll::Ready(Ok(res)) => {
                let _ = stream.1.take();
                Poll::Ready(to_std_io_result(res))
            }
            Poll::Ready(Err(_)) => {
                let _ = stream.1.take();
                Poll::Ready(Ok(()))
            }
            Poll::Pending => Poll::Pending,
        }
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context) -> Poll<io::Result<()>> {
        let stream = unsafe { Pin::get_unchecked_mut(self) };

        let rx = if let Some(ref mut rx) = stream.1 {
            rx
        } else {
            let (tx, rx) = oneshot::channel();
            stream.0.as_ref().close_async(
                glib::Priority::default(),
                crate::Cancellable::NONE,
                move |res| {
                    let _ = tx.send(res);
                },
            );

            stream.1 = Some(rx);
            stream.1.as_mut().unwrap()
        };

        match Pin::new(rx).poll(cx) {
            Poll::Ready(Ok(res)) => {
                let _ = stream.1.take();
                Poll::Ready(to_std_io_result(res))
            }
            Poll::Ready(Err(_)) => {
                let _ = stream.1.take();
                Poll::Ready(Ok(()))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}
