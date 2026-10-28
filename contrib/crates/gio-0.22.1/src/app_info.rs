// Take a look at the license at the top of the repository in the LICENSE file.

use std::boxed::Box as Box_;
use std::pin::Pin;
use std::ptr;

use glib::prelude::*;
use glib::translate::*;

use crate::{AppInfo, AppLaunchContext, Cancellable, ffi};

pub trait AppInfoExtManual: IsA<AppInfo> + 'static {
    #[cfg_attr(docsrs, doc(cfg(feature = "v2_60")))]
    #[doc(alias = "g_app_info_launch_uris_async")]
    fn launch_uris_async<
        P: IsA<AppLaunchContext>,
        Q: IsA<Cancellable>,
        R: FnOnce(Result<(), glib::Error>) + 'static,
    >(
        &self,
        uris: &[&str],
        context: Option<&P>,
        cancellable: Option<&Q>,
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

        let user_data: Box_<(glib::thread_guard::ThreadGuard<R>, *mut *mut libc::c_char)> =
            Box_::new((
                glib::thread_guard::ThreadGuard::new(callback),
                uris.to_glib_full(),
            ));
        unsafe extern "C" fn launch_uris_async_trampoline<
            R: FnOnce(Result<(), glib::Error>) + 'static,
        >(
            _source_object: *mut glib::gobject_ffi::GObject,
            res: *mut ffi::GAsyncResult,
            user_data: glib::ffi::gpointer,
        ) {
            unsafe {
                let mut error = ptr::null_mut();
                let _ =
                    ffi::g_app_info_launch_uris_finish(_source_object as *mut _, res, &mut error);
                let result = if error.is_null() {
                    Ok(())
                } else {
                    Err(from_glib_full(error))
                };
                let callback: Box_<(glib::thread_guard::ThreadGuard<R>, *mut *mut libc::c_char)> =
                    Box_::from_raw(user_data as *mut _);
                let (callback, uris) = *callback;
                let callback = callback.into_inner();
                callback(result);
                glib::ffi::g_strfreev(uris);
            }
        }
        let callback = launch_uris_async_trampoline::<R>;
        unsafe {
            ffi::g_app_info_launch_uris_async(
                self.as_ref().to_glib_none().0,
                uris.to_glib_none().0,
                context.map(|p| p.as_ref()).to_glib_none().0,
                cancellable.map(|p| p.as_ref()).to_glib_none().0,
                Some(callback),
                Box_::into_raw(user_data) as *mut _,
            );
        }
    }

    #[cfg_attr(docsrs, doc(cfg(feature = "v2_60")))]
    fn launch_uris_future<P: IsA<AppLaunchContext> + Clone + 'static>(
        &self,
        uris: &[&str],
        context: Option<&P>,
    ) -> Pin<Box_<dyn std::future::Future<Output = Result<(), glib::Error>> + 'static>> {
        let uris = uris.iter().copied().map(String::from).collect::<Vec<_>>();
        let context = context.map(ToOwned::to_owned);
        Box_::pin(crate::GioFuture::new(
            self,
            move |obj, cancellable, send| {
                let uris = uris
                    .iter()
                    .map(::std::borrow::Borrow::borrow)
                    .collect::<Vec<_>>();
                obj.launch_uris_async(
                    uris.as_ref(),
                    context.as_ref().map(::std::borrow::Borrow::borrow),
                    Some(cancellable),
                    move |res| {
                        send.resolve(res);
                    },
                );
            },
        ))
    }
}

impl<O: IsA<AppInfo>> AppInfoExtManual for O {}
