// Take a look at the license at the top of the repository in the LICENSE file.

use std::{cell::RefCell, mem::transmute, pin::Pin, ptr, time::Duration};

use futures_core::stream::Stream;
use glib::{prelude::*, translate::*};

use crate::{Cancellable, DatagramBased, InputMessage, OutputMessage, ffi};

pub trait DatagramBasedExtManual: IsA<DatagramBased> + Sized {
    #[doc(alias = "g_datagram_based_create_source")]
    fn create_source<F, C>(
        &self,
        condition: glib::IOCondition,
        cancellable: Option<&C>,
        name: Option<&str>,
        priority: glib::Priority,
        func: F,
    ) -> glib::Source
    where
        F: FnMut(&Self, glib::IOCondition) -> glib::ControlFlow + 'static,
        C: IsA<Cancellable>,
    {
        unsafe extern "C" fn trampoline<
            O: IsA<DatagramBased>,
            F: FnMut(&O, glib::IOCondition) -> glib::ControlFlow + 'static,
        >(
            datagram_based: *mut ffi::GDatagramBased,
            condition: glib::ffi::GIOCondition,
            func: glib::ffi::gpointer,
        ) -> glib::ffi::gboolean {
            unsafe {
                let func: &RefCell<F> = &*(func as *const RefCell<F>);
                let mut func = func.borrow_mut();
                (*func)(
                    DatagramBased::from_glib_borrow(datagram_based).unsafe_cast_ref(),
                    from_glib(condition),
                )
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
            let source = ffi::g_datagram_based_create_source(
                self.as_ref().to_glib_none().0,
                condition.into_glib(),
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
        condition: glib::IOCondition,
        cancellable: Option<&C>,
        priority: glib::Priority,
    ) -> Pin<Box<dyn std::future::Future<Output = glib::IOCondition> + 'static>> {
        let cancellable: Option<Cancellable> = cancellable.map(|c| c.as_ref()).cloned();

        let obj = self.clone();
        Box::pin(glib::SourceFuture::new(move |send| {
            let mut send = Some(send);
            obj.create_source(
                condition,
                cancellable.as_ref(),
                None,
                priority,
                move |_, condition| {
                    let _ = send.take().unwrap().send(condition);
                    glib::ControlFlow::Break
                },
            )
        }))
    }

    fn create_source_stream<C: IsA<Cancellable>>(
        &self,
        condition: glib::IOCondition,
        cancellable: Option<&C>,
        priority: glib::Priority,
    ) -> Pin<Box<dyn Stream<Item = glib::IOCondition> + 'static>> {
        let cancellable: Option<Cancellable> = cancellable.map(|c| c.as_ref()).cloned();

        let obj = self.clone();
        Box::pin(glib::SourceStream::new(move |send| {
            let send = Some(send);
            obj.create_source(
                condition,
                cancellable.as_ref(),
                None,
                priority,
                move |_, condition| {
                    if send.as_ref().unwrap().unbounded_send(condition).is_err() {
                        glib::ControlFlow::Break
                    } else {
                        glib::ControlFlow::Continue
                    }
                },
            )
        }))
    }

    #[doc(alias = "g_datagram_based_condition_wait")]
    fn condition_wait(
        &self,
        condition: glib::IOCondition,
        timeout: Option<Duration>,
        cancellable: Option<&impl IsA<Cancellable>>,
    ) -> Result<(), glib::Error> {
        unsafe {
            let mut error = ptr::null_mut();
            let is_ok = ffi::g_datagram_based_condition_wait(
                self.as_ref().to_glib_none().0,
                condition.into_glib(),
                timeout
                    .map(|t| t.as_micros().try_into().unwrap())
                    .unwrap_or(-1),
                cancellable.map(|p| p.as_ref()).to_glib_none().0,
                &mut error,
            );
            debug_assert_eq!(is_ok == glib::ffi::GFALSE, !error.is_null());
            if error.is_null() {
                Ok(())
            } else {
                Err(from_glib_full(error))
            }
        }
    }

    #[doc(alias = "g_datagram_based_receive_messages")]
    fn receive_messages<'v, V: IntoIterator<Item = &'v mut [&'v mut [u8]]>, C: IsA<Cancellable>>(
        &self,
        messages: &mut [InputMessage],
        flags: i32,
        timeout: Option<Duration>,
        cancellable: Option<&C>,
    ) -> Result<usize, glib::Error> {
        let cancellable = cancellable.map(|c| c.as_ref());
        unsafe {
            let mut error = ptr::null_mut();

            let count = ffi::g_datagram_based_receive_messages(
                self.as_ref().to_glib_none().0,
                messages.as_mut_ptr() as *mut _,
                messages.len().try_into().unwrap(),
                flags,
                timeout
                    .map(|t| t.as_micros().try_into().unwrap())
                    .unwrap_or(-1),
                cancellable.to_glib_none().0,
                &mut error,
            );
            if error.is_null() {
                Ok(count as usize)
            } else {
                Err(from_glib_full(error))
            }
        }
    }

    #[doc(alias = "g_datagram_based_send_messages")]
    fn send_messages<C: IsA<Cancellable>>(
        &self,
        messages: &mut [OutputMessage],
        flags: i32,
        timeout: Option<Duration>,
        cancellable: Option<&C>,
    ) -> Result<usize, glib::Error> {
        let cancellable = cancellable.map(|c| c.as_ref());
        unsafe {
            let mut error = ptr::null_mut();
            let count = ffi::g_datagram_based_send_messages(
                self.as_ref().to_glib_none().0,
                messages.as_mut_ptr() as *mut _,
                messages.len().try_into().unwrap(),
                flags,
                timeout
                    .map(|t| t.as_micros().try_into().unwrap())
                    .unwrap_or(-1),
                cancellable.to_glib_none().0,
                &mut error,
            );
            if error.is_null() {
                Ok(count as usize)
            } else {
                Err(from_glib_full(error))
            }
        }
    }
}

impl<O: IsA<DatagramBased>> DatagramBasedExtManual for O {}
