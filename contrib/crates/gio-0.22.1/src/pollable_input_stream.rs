// Take a look at the license at the top of the repository in the LICENSE file.

use std::{cell::RefCell, io, mem::transmute, pin::Pin, ptr};

use futures_core::{
    stream::Stream,
    task::{Context, Poll},
};
use futures_io::AsyncRead;
use glib::{prelude::*, translate::*};

use crate::{Cancellable, PollableInputStream, ffi, prelude::*};

pub trait PollableInputStreamExtManual: IsA<PollableInputStream> + Sized {
    #[doc(alias = "g_pollable_input_stream_create_source")]
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
            O: IsA<PollableInputStream>,
            F: FnMut(&O) -> glib::ControlFlow + 'static,
        >(
            stream: *mut ffi::GPollableInputStream,
            func: glib::ffi::gpointer,
        ) -> glib::ffi::gboolean {
            unsafe {
                let func: &RefCell<F> = &*(func as *const RefCell<F>);
                let mut func = func.borrow_mut();
                (*func)(PollableInputStream::from_glib_borrow(stream).unsafe_cast_ref()).into_glib()
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
            let source = ffi::g_pollable_input_stream_create_source(
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
            obj.create_source(cancellable.as_ref(), None, priority, move |_| {
                if send.unbounded_send(()).is_err() {
                    glib::ControlFlow::Break
                } else {
                    glib::ControlFlow::Continue
                }
            })
        }))
    }

    #[doc(alias = "g_pollable_input_stream_read_nonblocking")]
    fn read_nonblocking<C: IsA<Cancellable>>(
        &self,
        buffer: &mut [u8],
        cancellable: Option<&C>,
    ) -> Result<isize, glib::Error> {
        let cancellable = cancellable.map(|c| c.as_ref());
        let gcancellable = cancellable.to_glib_none();
        let count = buffer.len();
        unsafe {
            let mut error = ptr::null_mut();
            let ret = ffi::g_pollable_input_stream_read_nonblocking(
                self.as_ref().to_glib_none().0,
                buffer.to_glib_none().0,
                count,
                gcancellable.0,
                &mut error,
            );
            if error.is_null() {
                Ok(ret)
            } else {
                Err(from_glib_full(error))
            }
        }
    }

    fn into_async_read(self) -> Result<InputStreamAsyncRead<Self>, Self>
    where
        Self: IsA<PollableInputStream>,
    {
        if self.can_poll() {
            Ok(InputStreamAsyncRead(self))
        } else {
            Err(self)
        }
    }
}

impl<O: IsA<PollableInputStream>> PollableInputStreamExtManual for O {}

#[derive(Debug)]
pub struct InputStreamAsyncRead<T: IsA<PollableInputStream>>(T);

impl<T: IsA<PollableInputStream>> InputStreamAsyncRead<T> {
    pub fn into_input_stream(self) -> T {
        self.0
    }

    pub fn input_stream(&self) -> &T {
        &self.0
    }
}

impl<T: IsA<PollableInputStream>> AsyncRead for InputStreamAsyncRead<T> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        let stream = Pin::get_ref(self.as_ref());
        let gio_result = stream
            .0
            .as_ref()
            .read_nonblocking(buf, crate::Cancellable::NONE);

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
}
