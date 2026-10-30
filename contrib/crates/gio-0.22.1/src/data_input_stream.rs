// Take a look at the license at the top of the repository in the LICENSE file.

use std::{boxed::Box as Box_, mem, pin::Pin, ptr};

use glib::{GString, prelude::*, translate::*};

use crate::{Cancellable, DataInputStream, ffi};

pub trait DataInputStreamExtManual: IsA<DataInputStream> + 'static {
    #[doc(alias = "g_data_input_stream_read_line")]
    fn read_line<P: IsA<Cancellable>>(
        &self,
        cancellable: Option<&P>,
    ) -> Result<Option<glib::collections::Slice<u8>>, glib::Error> {
        unsafe {
            let mut length = mem::MaybeUninit::uninit();
            let mut error = ptr::null_mut();
            let ret = ffi::g_data_input_stream_read_line(
                self.as_ref().to_glib_none().0,
                length.as_mut_ptr(),
                cancellable.map(|p| p.as_ref()).to_glib_none().0,
                &mut error,
            );
            if error.is_null() {
                if ret.is_null() {
                    Ok(None)
                } else {
                    let length = length.assume_init();
                    Ok(Some(FromGlibContainer::from_glib_full_num(ret, length)))
                }
            } else {
                Err(from_glib_full(error))
            }
        }
    }

    #[doc(alias = "g_data_input_stream_read_line_async")]
    fn read_line_async<
        P: IsA<Cancellable>,
        Q: FnOnce(Result<Option<glib::collections::Slice<u8>>, glib::Error>) + 'static,
    >(
        &self,
        io_priority: glib::Priority,
        cancellable: Option<&P>,
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

        let user_data: Box_<glib::thread_guard::ThreadGuard<Q>> =
            Box_::new(glib::thread_guard::ThreadGuard::new(callback));
        unsafe extern "C" fn read_line_async_trampoline<
            Q: FnOnce(Result<Option<glib::collections::Slice<u8>>, glib::Error>) + 'static,
        >(
            _source_object: *mut glib::gobject_ffi::GObject,
            res: *mut ffi::GAsyncResult,
            user_data: glib::ffi::gpointer,
        ) {
            unsafe {
                let mut error = ptr::null_mut();
                let mut length = mem::MaybeUninit::uninit();
                let ret = ffi::g_data_input_stream_read_line_finish(
                    _source_object as *mut _,
                    res,
                    length.as_mut_ptr(),
                    &mut error,
                );
                let result = if error.is_null() {
                    if ret.is_null() {
                        Ok(None)
                    } else {
                        let length = length.assume_init();
                        Ok(Some(FromGlibContainer::from_glib_full_num(ret, length)))
                    }
                } else {
                    Err(from_glib_full(error))
                };
                let callback: Box_<glib::thread_guard::ThreadGuard<Q>> =
                    Box_::from_raw(user_data as *mut _);
                let callback = callback.into_inner();
                callback(result);
            }
        }
        let callback = read_line_async_trampoline::<Q>;
        unsafe {
            ffi::g_data_input_stream_read_line_async(
                self.as_ref().to_glib_none().0,
                io_priority.into_glib(),
                cancellable.map(|p| p.as_ref()).to_glib_none().0,
                Some(callback),
                Box_::into_raw(user_data) as *mut _,
            );
        }
    }

    fn read_line_future(
        &self,
        io_priority: glib::Priority,
    ) -> Pin<
        Box_<
            dyn std::future::Future<
                    Output = Result<Option<glib::collections::Slice<u8>>, glib::Error>,
                > + 'static,
        >,
    > {
        Box_::pin(crate::GioFuture::new(
            self,
            move |obj, cancellable, send| {
                obj.read_line_async(io_priority, Some(cancellable), move |res| {
                    send.resolve(res);
                });
            },
        ))
    }

    #[doc(alias = "g_data_input_stream_read_line_utf8")]
    fn read_line_utf8<P: IsA<Cancellable>>(
        &self,
        cancellable: Option<&P>,
    ) -> Result<Option<GString>, glib::Error> {
        unsafe {
            let mut error = ptr::null_mut();
            let ret = ffi::g_data_input_stream_read_line_utf8(
                self.as_ref().to_glib_none().0,
                ptr::null_mut(),
                cancellable.map(|p| p.as_ref()).to_glib_none().0,
                &mut error,
            );
            if error.is_null() {
                Ok(from_glib_full(ret))
            } else {
                Err(from_glib_full(error))
            }
        }
    }

    fn read_line_utf8_async<
        P: IsA<Cancellable>,
        Q: FnOnce(Result<Option<GString>, glib::Error>) + 'static,
    >(
        &self,
        io_priority: glib::Priority,
        cancellable: Option<&P>,
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

        let user_data: Box_<glib::thread_guard::ThreadGuard<Q>> =
            Box_::new(glib::thread_guard::ThreadGuard::new(callback));
        unsafe extern "C" fn read_line_async_trampoline<
            Q: FnOnce(Result<Option<GString>, glib::Error>) + 'static,
        >(
            _source_object: *mut glib::gobject_ffi::GObject,
            res: *mut ffi::GAsyncResult,
            user_data: glib::ffi::gpointer,
        ) {
            unsafe {
                let mut error = ptr::null_mut();
                let ret = ffi::g_data_input_stream_read_line_finish_utf8(
                    _source_object as *mut _,
                    res,
                    ptr::null_mut(),
                    &mut error,
                );
                let result = if error.is_null() {
                    Ok(from_glib_full(ret))
                } else {
                    Err(from_glib_full(error))
                };
                let callback: Box_<glib::thread_guard::ThreadGuard<Q>> =
                    Box_::from_raw(user_data as *mut _);
                let callback = callback.into_inner();
                callback(result);
            }
        }
        let callback = read_line_async_trampoline::<Q>;
        unsafe {
            ffi::g_data_input_stream_read_line_async(
                self.as_ref().to_glib_none().0,
                io_priority.into_glib(),
                cancellable.map(|p| p.as_ref()).to_glib_none().0,
                Some(callback),
                Box_::into_raw(user_data) as *mut _,
            );
        }
    }

    fn read_line_utf8_future(
        &self,
        io_priority: glib::Priority,
    ) -> Pin<Box_<dyn std::future::Future<Output = Result<Option<GString>, glib::Error>> + 'static>>
    {
        Box_::pin(crate::GioFuture::new(
            self,
            move |obj, cancellable, send| {
                obj.read_line_utf8_async(io_priority, Some(cancellable), move |res| {
                    send.resolve(res);
                });
            },
        ))
    }

    #[doc(alias = "g_data_input_stream_read_upto")]
    fn read_upto<P: IsA<Cancellable>>(
        &self,
        stop_chars: &[u8],
        cancellable: Option<&P>,
    ) -> Result<glib::collections::Slice<u8>, glib::Error> {
        let stop_chars_len = stop_chars.len() as isize;
        unsafe {
            let mut error = ptr::null_mut();
            let mut length = mem::MaybeUninit::uninit();
            let ret = ffi::g_data_input_stream_read_upto(
                self.as_ref().to_glib_none().0,
                stop_chars.to_glib_none().0 as *const _,
                stop_chars_len,
                length.as_mut_ptr(),
                cancellable.map(|p| p.as_ref()).to_glib_none().0,
                &mut error,
            );
            if error.is_null() {
                let length = length.assume_init();
                Ok(FromGlibContainer::from_glib_full_num(
                    ret as *mut u8,
                    length,
                ))
            } else {
                Err(from_glib_full(error))
            }
        }
    }

    #[doc(alias = "g_data_input_stream_read_upto_async")]
    fn read_upto_async<
        P: IsA<Cancellable>,
        Q: FnOnce(Result<glib::collections::Slice<u8>, glib::Error>) + 'static,
    >(
        &self,
        stop_chars: &[u8],
        io_priority: glib::Priority,
        cancellable: Option<&P>,
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

        let stop_chars_len = stop_chars.len() as isize;
        let user_data: Box_<glib::thread_guard::ThreadGuard<Q>> =
            Box_::new(glib::thread_guard::ThreadGuard::new(callback));
        unsafe extern "C" fn read_upto_async_trampoline<
            Q: FnOnce(Result<glib::collections::Slice<u8>, glib::Error>) + 'static,
        >(
            _source_object: *mut glib::gobject_ffi::GObject,
            res: *mut ffi::GAsyncResult,
            user_data: glib::ffi::gpointer,
        ) {
            unsafe {
                let mut error = ptr::null_mut();
                let mut length = mem::MaybeUninit::uninit();
                let ret = ffi::g_data_input_stream_read_upto_finish(
                    _source_object as *mut _,
                    res,
                    length.as_mut_ptr(),
                    &mut error,
                );
                let result = if error.is_null() {
                    let length = length.assume_init();
                    Ok(FromGlibContainer::from_glib_full_num(
                        ret as *mut u8,
                        length,
                    ))
                } else {
                    Err(from_glib_full(error))
                };
                let callback: Box_<glib::thread_guard::ThreadGuard<Q>> =
                    Box_::from_raw(user_data as *mut _);
                let callback = callback.into_inner();
                callback(result);
            }
        }
        let callback = read_upto_async_trampoline::<Q>;
        unsafe {
            ffi::g_data_input_stream_read_upto_async(
                self.as_ref().to_glib_none().0,
                stop_chars.to_glib_none().0 as *const _,
                stop_chars_len,
                io_priority.into_glib(),
                cancellable.map(|p| p.as_ref()).to_glib_none().0,
                Some(callback),
                Box_::into_raw(user_data) as *mut _,
            );
        }
    }

    fn read_upto_future(
        &self,
        stop_chars: &[u8],
        io_priority: glib::Priority,
    ) -> Pin<
        Box_<
            dyn std::future::Future<Output = Result<glib::collections::Slice<u8>, glib::Error>>
                + 'static,
        >,
    > {
        let stop_chars = Vec::from(stop_chars);
        Box_::pin(crate::GioFuture::new(
            self,
            move |obj, cancellable, send| {
                obj.read_upto_async(&stop_chars, io_priority, Some(cancellable), move |res| {
                    send.resolve(res);
                });
            },
        ))
    }
}

impl<O: IsA<DataInputStream>> DataInputStreamExtManual for O {}
