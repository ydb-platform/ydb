// Take a look at the license at the top of the repository in the LICENSE file.

use std::{pin::Pin, ptr};

use glib::{GString, prelude::*, translate::*};
use libc::c_char;

use crate::{Cancellable, Subprocess, ffi};

impl Subprocess {
    #[doc(alias = "g_subprocess_communicate_utf8_async")]
    pub fn communicate_utf8_async<
        R: FnOnce(Result<(Option<GString>, Option<GString>), glib::Error>) + 'static,
        C: IsA<Cancellable>,
    >(
        &self,
        stdin_buf: Option<String>,
        cancellable: Option<&C>,
        callback: R,
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

        let stdin_buf = stdin_buf.to_glib_full();
        let cancellable = cancellable.map(|c| c.as_ref());
        let gcancellable = cancellable.to_glib_none();
        let user_data: Box<(glib::thread_guard::ThreadGuard<R>, *mut c_char)> =
            Box::new((glib::thread_guard::ThreadGuard::new(callback), stdin_buf));
        unsafe extern "C" fn communicate_utf8_async_trampoline<
            R: FnOnce(Result<(Option<GString>, Option<GString>), glib::Error>) + 'static,
        >(
            _source_object: *mut glib::gobject_ffi::GObject,
            res: *mut ffi::GAsyncResult,
            user_data: glib::ffi::gpointer,
        ) {
            unsafe {
                let mut error = ptr::null_mut();
                let mut stdout_buf = ptr::null_mut();
                let mut stderr_buf = ptr::null_mut();
                let _ = ffi::g_subprocess_communicate_utf8_finish(
                    _source_object as *mut _,
                    res,
                    &mut stdout_buf,
                    &mut stderr_buf,
                    &mut error,
                );
                let result = if error.is_null() {
                    Ok((from_glib_full(stdout_buf), from_glib_full(stderr_buf)))
                } else {
                    Err(from_glib_full(error))
                };
                let callback: Box<(glib::thread_guard::ThreadGuard<R>, *mut c_char)> =
                    Box::from_raw(user_data as *mut _);
                glib::ffi::g_free(callback.1 as *mut _);
                (callback.0.into_inner())(result);
            }
        }
        unsafe {
            ffi::g_subprocess_communicate_utf8_async(
                self.to_glib_none().0,
                stdin_buf,
                gcancellable.0,
                Some(communicate_utf8_async_trampoline::<R>),
                Box::into_raw(user_data) as *mut _,
            );
        }
    }

    pub fn communicate_utf8_future(
        &self,
        stdin_buf: Option<String>,
    ) -> Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<(Option<GString>, Option<GString>), glib::Error>,
                > + 'static,
        >,
    > {
        Box::pin(crate::GioFuture::new(
            self,
            move |obj, cancellable, send| {
                obj.communicate_utf8_async(stdin_buf, Some(cancellable), move |res| {
                    send.resolve(res);
                });
            },
        ))
    }
}
