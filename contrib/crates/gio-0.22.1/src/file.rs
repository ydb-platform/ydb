// Take a look at the license at the top of the repository in the LICENSE file.

use std::{cell::RefCell, mem, pin::Pin, ptr};

use glib::{prelude::*, translate::*};

#[cfg(feature = "v2_74")]
use crate::FileIOStream;
use crate::{
    Cancellable, File, FileAttributeValue, FileCreateFlags, FileEnumerator, FileQueryInfoFlags, ffi,
};

impl File {
    #[cfg(feature = "v2_74")]
    #[cfg_attr(docsrs, doc(cfg(feature = "v2_74")))]
    #[doc(alias = "g_file_new_tmp_async")]
    pub fn new_tmp_async<P: FnOnce(Result<(File, FileIOStream), glib::Error>) + 'static>(
        tmpl: Option<impl AsRef<std::path::Path>>,
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

        let user_data: Box<glib::thread_guard::ThreadGuard<P>> =
            Box::new(glib::thread_guard::ThreadGuard::new(callback));
        unsafe extern "C" fn new_tmp_async_trampoline<
            P: FnOnce(Result<(File, FileIOStream), glib::Error>) + 'static,
        >(
            _source_object: *mut glib::gobject_ffi::GObject,
            res: *mut crate::ffi::GAsyncResult,
            user_data: glib::ffi::gpointer,
        ) {
            unsafe {
                let mut error = ptr::null_mut();
                let mut iostream = ptr::null_mut();
                let ret = ffi::g_file_new_tmp_finish(res, &mut iostream, &mut error);
                let result = if error.is_null() {
                    Ok((from_glib_full(ret), from_glib_full(iostream)))
                } else {
                    Err(from_glib_full(error))
                };
                let callback: Box<glib::thread_guard::ThreadGuard<P>> =
                    Box::from_raw(user_data as *mut _);
                let callback: P = callback.into_inner();
                callback(result);
            }
        }
        let callback = new_tmp_async_trampoline::<P>;
        unsafe {
            ffi::g_file_new_tmp_async(
                tmpl.as_ref().map(|p| p.as_ref()).to_glib_none().0,
                io_priority.into_glib(),
                cancellable.map(|p| p.as_ref()).to_glib_none().0,
                Some(callback),
                Box::into_raw(user_data) as *mut _,
            );
        }
    }

    #[cfg(feature = "v2_74")]
    #[cfg_attr(docsrs, doc(cfg(feature = "v2_74")))]
    pub fn new_tmp_future(
        tmpl: Option<impl AsRef<std::path::Path>>,
        io_priority: glib::Priority,
    ) -> Pin<
        Box<dyn std::future::Future<Output = Result<(File, FileIOStream), glib::Error>> + 'static>,
    > {
        let tmpl = tmpl.map(|tmpl| tmpl.as_ref().to_owned());
        Box::pin(crate::GioFuture::new(
            &(),
            move |_obj, cancellable, send| {
                Self::new_tmp_async(
                    tmpl.as_ref()
                        .map(<std::path::PathBuf as std::borrow::Borrow<std::path::Path>>::borrow),
                    io_priority,
                    Some(cancellable),
                    move |res| {
                        send.resolve(res);
                    },
                );
            },
        ))
    }

    #[cfg(feature = "v2_74")]
    #[cfg_attr(docsrs, doc(cfg(feature = "v2_74")))]
    #[doc(alias = "g_file_new_tmp_dir_async")]
    pub fn new_tmp_dir_async<P: FnOnce(Result<File, glib::Error>) + 'static>(
        tmpl: Option<impl AsRef<std::path::Path>>,
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

        let user_data: Box<glib::thread_guard::ThreadGuard<P>> =
            Box::new(glib::thread_guard::ThreadGuard::new(callback));
        unsafe extern "C" fn new_tmp_dir_async_trampoline<
            P: FnOnce(Result<File, glib::Error>) + 'static,
        >(
            _source_object: *mut glib::gobject_ffi::GObject,
            res: *mut crate::ffi::GAsyncResult,
            user_data: glib::ffi::gpointer,
        ) {
            unsafe {
                let mut error = ptr::null_mut();
                let ret = ffi::g_file_new_tmp_dir_finish(res, &mut error);
                let result = if error.is_null() {
                    Ok(from_glib_full(ret))
                } else {
                    Err(from_glib_full(error))
                };
                let callback: Box<glib::thread_guard::ThreadGuard<P>> =
                    Box::from_raw(user_data as *mut _);
                let callback: P = callback.into_inner();
                callback(result);
            }
        }
        let callback = new_tmp_dir_async_trampoline::<P>;
        unsafe {
            ffi::g_file_new_tmp_dir_async(
                tmpl.as_ref().map(|p| p.as_ref()).to_glib_none().0,
                io_priority.into_glib(),
                cancellable.map(|p| p.as_ref()).to_glib_none().0,
                Some(callback),
                Box::into_raw(user_data) as *mut _,
            );
        }
    }

    #[cfg(feature = "v2_74")]
    #[cfg_attr(docsrs, doc(cfg(feature = "v2_74")))]
    pub fn new_tmp_dir_future(
        tmpl: Option<impl AsRef<std::path::Path>>,
        io_priority: glib::Priority,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<File, glib::Error>> + 'static>> {
        let tmpl = tmpl.map(|tmpl| tmpl.as_ref().to_owned());
        Box::pin(crate::GioFuture::new(
            &(),
            move |_obj, cancellable, send| {
                Self::new_tmp_dir_async(
                    tmpl.as_ref()
                        .map(<std::path::PathBuf as std::borrow::Borrow<std::path::Path>>::borrow),
                    io_priority,
                    Some(cancellable),
                    move |res| {
                        send.resolve(res);
                    },
                );
            },
        ))
    }
}

pub trait FileExtManual: IsA<File> + Sized {
    #[doc(alias = "g_file_replace_contents_async")]
    fn replace_contents_async<
        B: AsRef<[u8]> + Send + 'static,
        R: FnOnce(Result<(B, Option<glib::GString>), (B, glib::Error)>) + 'static,
        C: IsA<Cancellable>,
    >(
        &self,
        contents: B,
        etag: Option<&str>,
        make_backup: bool,
        flags: FileCreateFlags,
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

        let etag = etag.to_glib_none();
        let cancellable = cancellable.map(|c| c.as_ref());
        let gcancellable = cancellable.to_glib_none();
        let user_data: Box<(glib::thread_guard::ThreadGuard<R>, B)> =
            Box::new((glib::thread_guard::ThreadGuard::new(callback), contents));
        // Need to do this after boxing as the contents pointer might change by moving into the box
        let (count, contents_ptr) = {
            let contents = &user_data.1;
            let slice = contents.as_ref();
            (slice.len(), slice.as_ptr())
        };
        unsafe extern "C" fn replace_contents_async_trampoline<
            B: AsRef<[u8]> + Send + 'static,
            R: FnOnce(Result<(B, Option<glib::GString>), (B, glib::Error)>) + 'static,
        >(
            _source_object: *mut glib::gobject_ffi::GObject,
            res: *mut ffi::GAsyncResult,
            user_data: glib::ffi::gpointer,
        ) {
            unsafe {
                let user_data: Box<(glib::thread_guard::ThreadGuard<R>, B)> =
                    Box::from_raw(user_data as *mut _);
                let (callback, contents) = *user_data;
                let callback = callback.into_inner();

                let mut error = ptr::null_mut();
                let mut new_etag = ptr::null_mut();
                let _ = ffi::g_file_replace_contents_finish(
                    _source_object as *mut _,
                    res,
                    &mut new_etag,
                    &mut error,
                );
                let result = if error.is_null() {
                    Ok((contents, from_glib_full(new_etag)))
                } else {
                    Err((contents, from_glib_full(error)))
                };
                callback(result);
            }
        }
        let callback = replace_contents_async_trampoline::<B, R>;
        unsafe {
            ffi::g_file_replace_contents_async(
                self.as_ref().to_glib_none().0,
                mut_override(contents_ptr),
                count,
                etag.0,
                make_backup.into_glib(),
                flags.into_glib(),
                gcancellable.0,
                Some(callback),
                Box::into_raw(user_data) as *mut _,
            );
        }
    }

    fn replace_contents_future<B: AsRef<[u8]> + Send + 'static>(
        &self,
        contents: B,
        etag: Option<&str>,
        make_backup: bool,
        flags: FileCreateFlags,
    ) -> Pin<
        Box<
            dyn std::future::Future<Output = Result<(B, Option<glib::GString>), (B, glib::Error)>>
                + 'static,
        >,
    > {
        let etag = etag.map(glib::GString::from);
        Box::pin(crate::GioFuture::new(
            self,
            move |obj, cancellable, send| {
                obj.replace_contents_async(
                    contents,
                    etag.as_ref().map(|s| s.as_str()),
                    make_backup,
                    flags,
                    Some(cancellable),
                    move |res| {
                        send.resolve(res);
                    },
                );
            },
        ))
    }

    #[doc(alias = "g_file_enumerate_children_async")]
    fn enumerate_children_async<
        P: IsA<Cancellable>,
        Q: FnOnce(Result<FileEnumerator, glib::Error>) + 'static,
    >(
        &self,
        attributes: &str,
        flags: FileQueryInfoFlags,
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

        let user_data: Box<glib::thread_guard::ThreadGuard<Q>> =
            Box::new(glib::thread_guard::ThreadGuard::new(callback));
        unsafe extern "C" fn create_async_trampoline<
            Q: FnOnce(Result<FileEnumerator, glib::Error>) + 'static,
        >(
            _source_object: *mut glib::gobject_ffi::GObject,
            res: *mut crate::ffi::GAsyncResult,
            user_data: glib::ffi::gpointer,
        ) {
            unsafe {
                let mut error = ptr::null_mut();
                let ret = ffi::g_file_enumerate_children_finish(
                    _source_object as *mut _,
                    res,
                    &mut error,
                );
                let result = if error.is_null() {
                    Ok(from_glib_full(ret))
                } else {
                    Err(from_glib_full(error))
                };
                let callback: Box<glib::thread_guard::ThreadGuard<Q>> =
                    Box::from_raw(user_data as *mut _);
                let callback = callback.into_inner();
                callback(result);
            }
        }
        let callback = create_async_trampoline::<Q>;
        unsafe {
            ffi::g_file_enumerate_children_async(
                self.as_ref().to_glib_none().0,
                attributes.to_glib_none().0,
                flags.into_glib(),
                io_priority.into_glib(),
                cancellable.map(|p| p.as_ref()).to_glib_none().0,
                Some(callback),
                Box::into_raw(user_data) as *mut _,
            );
        }
    }

    fn enumerate_children_future(
        &self,
        attributes: &str,
        flags: FileQueryInfoFlags,
        io_priority: glib::Priority,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<FileEnumerator, glib::Error>> + 'static>>
    {
        let attributes = attributes.to_owned();
        Box::pin(crate::GioFuture::new(
            self,
            move |obj, cancellable, send| {
                obj.enumerate_children_async(
                    &attributes,
                    flags,
                    io_priority,
                    Some(cancellable),
                    move |res| {
                        send.resolve(res);
                    },
                );
            },
        ))
    }

    #[doc(alias = "g_file_copy_async")]
    fn copy_async<Q: FnOnce(Result<(), glib::Error>) + 'static>(
        &self,
        destination: &impl IsA<File>,
        flags: crate::FileCopyFlags,
        io_priority: glib::Priority,
        cancellable: Option<&impl IsA<Cancellable>>,
        progress_callback: Option<Box<dyn FnMut(i64, i64)>>,
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

        let progress_trampoline = if progress_callback.is_some() {
            Some(copy_async_progress_trampoline::<Q> as _)
        } else {
            None
        };

        let user_data: Box<(
            glib::thread_guard::ThreadGuard<Q>,
            RefCell<Option<glib::thread_guard::ThreadGuard<Box<dyn FnMut(i64, i64)>>>>,
        )> = Box::new((
            glib::thread_guard::ThreadGuard::new(callback),
            RefCell::new(progress_callback.map(glib::thread_guard::ThreadGuard::new)),
        ));
        unsafe extern "C" fn copy_async_trampoline<Q: FnOnce(Result<(), glib::Error>) + 'static>(
            _source_object: *mut glib::gobject_ffi::GObject,
            res: *mut crate::ffi::GAsyncResult,
            user_data: glib::ffi::gpointer,
        ) {
            unsafe {
                let mut error = ptr::null_mut();
                ffi::g_file_copy_finish(_source_object as *mut _, res, &mut error);
                let result = if error.is_null() {
                    Ok(())
                } else {
                    Err(from_glib_full(error))
                };
                let callback: Box<(
                    glib::thread_guard::ThreadGuard<Q>,
                    RefCell<Option<glib::thread_guard::ThreadGuard<Box<dyn FnMut(i64, i64)>>>>,
                )> = Box::from_raw(user_data as *mut _);
                let callback = callback.0.into_inner();
                callback(result);
            }
        }
        unsafe extern "C" fn copy_async_progress_trampoline<
            Q: FnOnce(Result<(), glib::Error>) + 'static,
        >(
            current_num_bytes: i64,
            total_num_bytes: i64,
            user_data: glib::ffi::gpointer,
        ) {
            unsafe {
                let callback: &(
                    glib::thread_guard::ThreadGuard<Q>,
                    RefCell<Option<glib::thread_guard::ThreadGuard<Box<dyn FnMut(i64, i64)>>>>,
                ) = &*(user_data as *const _);
                (callback
                    .1
                    .borrow_mut()
                    .as_mut()
                    .expect("no closure")
                    .get_mut())(current_num_bytes, total_num_bytes);
            }
        }

        let user_data = Box::into_raw(user_data) as *mut _;

        unsafe {
            ffi::g_file_copy_async(
                self.as_ref().to_glib_none().0,
                destination.as_ref().to_glib_none().0,
                flags.into_glib(),
                io_priority.into_glib(),
                cancellable.map(|p| p.as_ref()).to_glib_none().0,
                progress_trampoline,
                user_data,
                Some(copy_async_trampoline::<Q>),
                user_data,
            );
        }
    }

    fn copy_future(
        &self,
        destination: &(impl IsA<File> + Clone + 'static),
        flags: crate::FileCopyFlags,
        io_priority: glib::Priority,
    ) -> (
        Pin<Box<dyn std::future::Future<Output = Result<(), glib::Error>> + 'static>>,
        Pin<Box<dyn futures_core::stream::Stream<Item = (i64, i64)> + 'static>>,
    ) {
        let destination = destination.clone();

        let (sender, receiver) = futures_channel::mpsc::unbounded();

        let fut = Box::pin(crate::GioFuture::new(
            self,
            move |obj, cancellable, send| {
                obj.copy_async(
                    &destination,
                    flags,
                    io_priority,
                    Some(cancellable),
                    Some(Box::new(move |current_num_bytes, total_num_bytes| {
                        let _ = sender.unbounded_send((current_num_bytes, total_num_bytes));
                    })),
                    move |res| {
                        send.resolve(res);
                    },
                );
            },
        ));

        (fut, Box::pin(receiver))
    }

    #[doc(alias = "g_file_load_contents")]
    fn load_contents(
        &self,
        cancellable: Option<&impl IsA<Cancellable>>,
    ) -> Result<(glib::collections::Slice<u8>, Option<glib::GString>), glib::Error> {
        unsafe {
            let mut contents = std::ptr::null_mut();
            let mut length = std::mem::MaybeUninit::uninit();
            let mut etag_out = std::ptr::null_mut();
            let mut error = std::ptr::null_mut();
            let is_ok = ffi::g_file_load_contents(
                self.as_ref().to_glib_none().0,
                cancellable.map(|p| p.as_ref()).to_glib_none().0,
                &mut contents,
                length.as_mut_ptr(),
                &mut etag_out,
                &mut error,
            );
            debug_assert_eq!(is_ok == glib::ffi::GFALSE, !error.is_null());
            if error.is_null() {
                Ok((
                    FromGlibContainer::from_glib_full_num(contents, length.assume_init() as _),
                    from_glib_full(etag_out),
                ))
            } else {
                Err(from_glib_full(error))
            }
        }
    }

    #[doc(alias = "g_file_load_contents_async")]
    fn load_contents_async<
        P: FnOnce(Result<(glib::collections::Slice<u8>, Option<glib::GString>), glib::Error>)
            + 'static,
    >(
        &self,
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

        let user_data: Box<glib::thread_guard::ThreadGuard<P>> =
            Box::new(glib::thread_guard::ThreadGuard::new(callback));
        unsafe extern "C" fn load_contents_async_trampoline<
            P: FnOnce(Result<(glib::collections::Slice<u8>, Option<glib::GString>), glib::Error>)
                + 'static,
        >(
            _source_object: *mut glib::gobject_ffi::GObject,
            res: *mut crate::ffi::GAsyncResult,
            user_data: glib::ffi::gpointer,
        ) {
            unsafe {
                let mut error = std::ptr::null_mut();
                let mut contents = std::ptr::null_mut();
                let mut length = std::mem::MaybeUninit::uninit();
                let mut etag_out = std::ptr::null_mut();
                let _ = ffi::g_file_load_contents_finish(
                    _source_object as *mut _,
                    res,
                    &mut contents,
                    length.as_mut_ptr(),
                    &mut etag_out,
                    &mut error,
                );
                let result = if error.is_null() {
                    Ok((
                        FromGlibContainer::from_glib_full_num(contents, length.assume_init() as _),
                        from_glib_full(etag_out),
                    ))
                } else {
                    Err(from_glib_full(error))
                };
                let callback: Box<glib::thread_guard::ThreadGuard<P>> =
                    Box::from_raw(user_data as *mut _);
                let callback: P = callback.into_inner();
                callback(result);
            }
        }
        let callback = load_contents_async_trampoline::<P>;
        unsafe {
            ffi::g_file_load_contents_async(
                self.as_ref().to_glib_none().0,
                cancellable.map(|p| p.as_ref()).to_glib_none().0,
                Some(callback),
                Box::into_raw(user_data) as *mut _,
            );
        }
    }

    fn load_contents_future(
        &self,
    ) -> impl std::future::Future<
        Output = Result<(glib::collections::Slice<u8>, Option<glib::GString>), glib::Error>,
    > + Unpin
    + 'static {
        crate::GioFuture::new(self, move |obj, cancellable, send| {
            obj.load_contents_async(Some(cancellable), move |res| {
                send.resolve(res);
            });
        })
    }

    #[doc(alias = "g_file_load_partial_contents_async")]
    fn load_partial_contents_async<
        P: FnMut(&[u8]) -> bool + 'static,
        Q: FnOnce(Result<(glib::collections::Slice<u8>, Option<glib::GString>), glib::Error>)
            + 'static,
    >(
        &self,
        cancellable: Option<&impl IsA<Cancellable>>,
        read_more_callback: P,
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

        let user_data: Box<(
            glib::thread_guard::ThreadGuard<Q>,
            RefCell<glib::thread_guard::ThreadGuard<P>>,
        )> = Box::new((
            glib::thread_guard::ThreadGuard::new(callback),
            RefCell::new(glib::thread_guard::ThreadGuard::new(read_more_callback)),
        ));
        unsafe extern "C" fn load_partial_contents_async_trampoline<
            P: FnMut(&[u8]) -> bool + 'static,
            Q: FnOnce(Result<(glib::collections::Slice<u8>, Option<glib::GString>), glib::Error>)
                + 'static,
        >(
            _source_object: *mut glib::gobject_ffi::GObject,
            res: *mut crate::ffi::GAsyncResult,
            user_data: glib::ffi::gpointer,
        ) {
            unsafe {
                let mut contents = ptr::null_mut();
                let mut length = mem::MaybeUninit::uninit();
                let mut etag_out = ptr::null_mut();
                let mut error = ptr::null_mut();
                ffi::g_file_load_partial_contents_finish(
                    _source_object as *mut _,
                    res,
                    &mut contents,
                    length.as_mut_ptr(),
                    &mut etag_out,
                    &mut error,
                );
                let result = if error.is_null() {
                    Ok((
                        FromGlibContainer::from_glib_full_num(contents, length.assume_init() as _),
                        from_glib_full(etag_out),
                    ))
                } else {
                    Err(from_glib_full(error))
                };
                let callback: Box<(
                    glib::thread_guard::ThreadGuard<Q>,
                    RefCell<glib::thread_guard::ThreadGuard<P>>,
                )> = Box::from_raw(user_data as *mut _);
                let callback = callback.0.into_inner();
                callback(result);
            }
        }
        unsafe extern "C" fn load_partial_contents_async_read_more_trampoline<
            P: FnMut(&[u8]) -> bool + 'static,
            Q: FnOnce(Result<(glib::collections::Slice<u8>, Option<glib::GString>), glib::Error>)
                + 'static,
        >(
            file_contents: *const libc::c_char,
            file_size: i64,
            user_data: glib::ffi::gpointer,
        ) -> glib::ffi::gboolean {
            unsafe {
                use std::slice;

                let callback: &(
                    glib::thread_guard::ThreadGuard<Q>,
                    RefCell<glib::thread_guard::ThreadGuard<P>>,
                ) = &*(user_data as *const _);
                let data = if file_size == 0 {
                    &[]
                } else {
                    slice::from_raw_parts(file_contents as *const u8, file_size as usize)
                };

                (*callback.1.borrow_mut().get_mut())(data).into_glib()
            }
        }

        let user_data = Box::into_raw(user_data) as *mut _;

        unsafe {
            ffi::g_file_load_partial_contents_async(
                self.as_ref().to_glib_none().0,
                cancellable.map(|p| p.as_ref()).to_glib_none().0,
                Some(load_partial_contents_async_read_more_trampoline::<P, Q>),
                Some(load_partial_contents_async_trampoline::<P, Q>),
                user_data,
            );
        }
    }

    #[doc(alias = "g_file_measure_disk_usage_async")]
    fn measure_disk_usage_async<P: FnOnce(Result<(u64, u64, u64), glib::Error>) + 'static>(
        &self,
        flags: crate::FileMeasureFlags,
        io_priority: glib::Priority,
        cancellable: Option<&impl IsA<Cancellable>>,
        progress_callback: Option<Box<dyn FnMut(bool, u64, u64, u64) + 'static>>,
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

        let progress_callback_trampoline = if progress_callback.is_some() {
            Some(measure_disk_usage_async_progress_trampoline::<P> as _)
        } else {
            None
        };

        let user_data: Box<(
            glib::thread_guard::ThreadGuard<P>,
            RefCell<
                Option<
                    glib::thread_guard::ThreadGuard<Box<dyn FnMut(bool, u64, u64, u64) + 'static>>,
                >,
            >,
        )> = Box::new((
            glib::thread_guard::ThreadGuard::new(callback),
            RefCell::new(progress_callback.map(glib::thread_guard::ThreadGuard::new)),
        ));
        unsafe extern "C" fn measure_disk_usage_async_trampoline<
            P: FnOnce(Result<(u64, u64, u64), glib::Error>) + 'static,
        >(
            _source_object: *mut glib::gobject_ffi::GObject,
            res: *mut crate::ffi::GAsyncResult,
            user_data: glib::ffi::gpointer,
        ) {
            unsafe {
                let mut disk_usage = mem::MaybeUninit::uninit();
                let mut num_dirs = mem::MaybeUninit::uninit();
                let mut num_files = mem::MaybeUninit::uninit();
                let mut error = ptr::null_mut();
                ffi::g_file_measure_disk_usage_finish(
                    _source_object as *mut _,
                    res,
                    disk_usage.as_mut_ptr(),
                    num_dirs.as_mut_ptr(),
                    num_files.as_mut_ptr(),
                    &mut error,
                );
                let result = if error.is_null() {
                    Ok((
                        disk_usage.assume_init(),
                        num_dirs.assume_init(),
                        num_files.assume_init(),
                    ))
                } else {
                    Err(from_glib_full(error))
                };
                let callback: Box<(
                    glib::thread_guard::ThreadGuard<P>,
                    RefCell<
                        Option<
                            glib::thread_guard::ThreadGuard<
                                Box<dyn FnMut(bool, u64, u64, u64) + 'static>,
                            >,
                        >,
                    >,
                )> = Box::from_raw(user_data as *mut _);
                let callback = callback.0.into_inner();
                callback(result);
            }
        }
        unsafe extern "C" fn measure_disk_usage_async_progress_trampoline<
            P: FnOnce(Result<(u64, u64, u64), glib::Error>) + 'static,
        >(
            reporting: glib::ffi::gboolean,
            disk_usage: u64,
            num_dirs: u64,
            num_files: u64,
            user_data: glib::ffi::gpointer,
        ) {
            unsafe {
                let callback: &(
                    glib::thread_guard::ThreadGuard<P>,
                    RefCell<
                        Option<
                            glib::thread_guard::ThreadGuard<
                                Box<dyn FnMut(bool, u64, u64, u64) + 'static>,
                            >,
                        >,
                    >,
                ) = &*(user_data as *const _);
                (callback
                    .1
                    .borrow_mut()
                    .as_mut()
                    .expect("can't get callback")
                    .get_mut())(
                    from_glib(reporting), disk_usage, num_dirs, num_files
                );
            }
        }

        let user_data = Box::into_raw(user_data) as *mut _;

        unsafe {
            ffi::g_file_measure_disk_usage_async(
                self.as_ref().to_glib_none().0,
                flags.into_glib(),
                io_priority.into_glib(),
                cancellable.map(|p| p.as_ref()).to_glib_none().0,
                progress_callback_trampoline,
                user_data,
                Some(measure_disk_usage_async_trampoline::<P>),
                user_data,
            );
        }
    }

    fn measure_disk_usage_future(
        &self,
        flags: crate::FileMeasureFlags,
        io_priority: glib::Priority,
    ) -> (
        Pin<Box<dyn std::future::Future<Output = Result<(u64, u64, u64), glib::Error>> + 'static>>,
        Pin<Box<dyn futures_core::stream::Stream<Item = (bool, u64, u64, u64)> + 'static>>,
    ) {
        let (sender, receiver) = futures_channel::mpsc::unbounded();

        let fut = Box::pin(crate::GioFuture::new(
            self,
            move |obj, cancellable, send| {
                obj.measure_disk_usage_async(
                    flags,
                    io_priority,
                    Some(cancellable),
                    Some(Box::new(
                        move |reporting, disk_usage, num_dirs, num_files| {
                            let _ =
                                sender.unbounded_send((reporting, disk_usage, num_dirs, num_files));
                        },
                    )),
                    move |res| {
                        send.resolve(res);
                    },
                );
            },
        ));

        (fut, Box::pin(receiver))
    }

    #[cfg(feature = "v2_72")]
    #[cfg_attr(docsrs, doc(cfg(feature = "v2_72")))]
    #[doc(alias = "g_file_move_async")]
    fn move_async<Q: FnOnce(Result<(), glib::Error>) + 'static>(
        &self,
        destination: &impl IsA<File>,
        flags: crate::FileCopyFlags,
        io_priority: glib::Priority,
        cancellable: Option<&impl IsA<Cancellable>>,
        progress_callback: Option<Box<dyn FnMut(i64, i64)>>,
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

        let progress_trampoline = if progress_callback.is_some() {
            Some(move_async_progress_trampoline::<Q> as _)
        } else {
            None
        };

        let user_data: Box<(
            glib::thread_guard::ThreadGuard<Q>,
            RefCell<Option<glib::thread_guard::ThreadGuard<Box<dyn FnMut(i64, i64)>>>>,
        )> = Box::new((
            glib::thread_guard::ThreadGuard::new(callback),
            RefCell::new(progress_callback.map(glib::thread_guard::ThreadGuard::new)),
        ));
        unsafe extern "C" fn move_async_trampoline<Q: FnOnce(Result<(), glib::Error>) + 'static>(
            _source_object: *mut glib::gobject_ffi::GObject,
            res: *mut crate::ffi::GAsyncResult,
            user_data: glib::ffi::gpointer,
        ) {
            unsafe {
                let mut error = ptr::null_mut();
                ffi::g_file_move_finish(_source_object as *mut _, res, &mut error);
                let result = if error.is_null() {
                    Ok(())
                } else {
                    Err(from_glib_full(error))
                };
                let callback: Box<(
                    glib::thread_guard::ThreadGuard<Q>,
                    RefCell<Option<glib::thread_guard::ThreadGuard<Box<dyn FnMut(i64, i64)>>>>,
                )> = Box::from_raw(user_data as *mut _);
                let callback = callback.0.into_inner();
                callback(result);
            }
        }
        unsafe extern "C" fn move_async_progress_trampoline<
            Q: FnOnce(Result<(), glib::Error>) + 'static,
        >(
            current_num_bytes: i64,
            total_num_bytes: i64,
            user_data: glib::ffi::gpointer,
        ) {
            unsafe {
                let callback: &(
                    glib::thread_guard::ThreadGuard<Q>,
                    RefCell<Option<glib::thread_guard::ThreadGuard<Box<dyn FnMut(i64, i64)>>>>,
                ) = &*(user_data as *const _);
                (callback
                    .1
                    .borrow_mut()
                    .as_mut()
                    .expect("no closure")
                    .get_mut())(current_num_bytes, total_num_bytes);
            }
        }

        let user_data = Box::into_raw(user_data) as *mut _;

        unsafe {
            ffi::g_file_move_async(
                self.as_ref().to_glib_none().0,
                destination.as_ref().to_glib_none().0,
                flags.into_glib(),
                io_priority.into_glib(),
                cancellable.map(|p| p.as_ref()).to_glib_none().0,
                progress_trampoline,
                user_data,
                Some(move_async_trampoline::<Q>),
                user_data,
            );
        }
    }

    #[cfg(feature = "v2_74")]
    #[cfg_attr(docsrs, doc(cfg(feature = "v2_74")))]
    #[doc(alias = "g_file_make_symbolic_link_async")]
    fn make_symbolic_link_async<P: FnOnce(Result<(), glib::Error>) + 'static>(
        &self,
        symlink_value: impl AsRef<std::path::Path>,
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

        let user_data: Box<glib::thread_guard::ThreadGuard<P>> =
            Box::new(glib::thread_guard::ThreadGuard::new(callback));
        unsafe extern "C" fn make_symbolic_link_async_trampoline<
            P: FnOnce(Result<(), glib::Error>) + 'static,
        >(
            _source_object: *mut glib::gobject_ffi::GObject,
            res: *mut crate::ffi::GAsyncResult,
            user_data: glib::ffi::gpointer,
        ) {
            unsafe {
                let mut error = ptr::null_mut();
                let _ = ffi::g_file_make_symbolic_link_finish(
                    _source_object as *mut _,
                    res,
                    &mut error,
                );
                let result = if error.is_null() {
                    Ok(())
                } else {
                    Err(from_glib_full(error))
                };
                let callback: Box<glib::thread_guard::ThreadGuard<P>> =
                    Box::from_raw(user_data as *mut _);
                let callback: P = callback.into_inner();
                callback(result);
            }
        }
        let callback = make_symbolic_link_async_trampoline::<P>;
        unsafe {
            ffi::g_file_make_symbolic_link_async(
                self.as_ref().to_glib_none().0,
                symlink_value.as_ref().to_glib_none().0,
                io_priority.into_glib(),
                cancellable.map(|p| p.as_ref()).to_glib_none().0,
                Some(callback),
                Box::into_raw(user_data) as *mut _,
            );
        }
    }

    #[cfg(feature = "v2_74")]
    #[cfg_attr(docsrs, doc(cfg(feature = "v2_74")))]
    fn make_symbolic_link_future(
        &self,
        symlink_value: impl AsRef<std::path::Path>,
        io_priority: glib::Priority,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<(), glib::Error>> + 'static>> {
        let symlink_value = symlink_value.as_ref().to_owned();
        Box::pin(crate::GioFuture::new(
            self,
            move |obj, cancellable, send| {
                obj.make_symbolic_link_async(
                    &symlink_value,
                    io_priority,
                    Some(cancellable),
                    move |res| {
                        send.resolve(res);
                    },
                );
            },
        ))
    }

    #[cfg(feature = "v2_72")]
    #[cfg_attr(docsrs, doc(cfg(feature = "v2_72")))]
    fn move_future(
        &self,
        destination: &(impl IsA<File> + Clone + 'static),
        flags: crate::FileCopyFlags,
        io_priority: glib::Priority,
    ) -> (
        Pin<Box<dyn std::future::Future<Output = Result<(), glib::Error>> + 'static>>,
        Pin<Box<dyn futures_core::stream::Stream<Item = (i64, i64)> + 'static>>,
    ) {
        let destination = destination.clone();

        let (sender, receiver) = futures_channel::mpsc::unbounded();

        let fut = Box::pin(crate::GioFuture::new(
            self,
            move |obj, cancellable, send| {
                obj.move_async(
                    &destination,
                    flags,
                    io_priority,
                    Some(cancellable),
                    Some(Box::new(move |current_num_bytes, total_num_bytes| {
                        let _ = sender.unbounded_send((current_num_bytes, total_num_bytes));
                    })),
                    move |res| {
                        send.resolve(res);
                    },
                );
            },
        ));

        (fut, Box::pin(receiver))
    }

    #[doc(alias = "g_file_set_attribute")]
    fn set_attribute<'a>(
        &self,
        attribute: &str,
        value: impl Into<FileAttributeValue<'a>>,
        flags: FileQueryInfoFlags,
        cancellable: Option<&impl IsA<Cancellable>>,
    ) -> Result<(), glib::Error> {
        unsafe {
            let mut error = std::ptr::null_mut();
            let value: FileAttributeValue<'a> = value.into();
            let is_ok = ffi::g_file_set_attribute(
                self.as_ref().to_glib_none().0,
                attribute.to_glib_none().0,
                value.type_().into_glib(),
                value.as_ptr(),
                flags.into_glib(),
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
}

impl<O: IsA<File>> FileExtManual for O {}
