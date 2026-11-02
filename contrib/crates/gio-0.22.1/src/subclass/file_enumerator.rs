// Take a look at the license at the top of the repository in the LICENSE file.

use glib::{prelude::*, subclass::prelude::*, translate::*};
use std::pin::Pin;

use crate::{
    AsyncResult, Cancellable, FileEnumerator, FileInfo, GioFuture, IOErrorEnum, LocalTask, ffi,
    prelude::*,
};

// Support custom implementation of virtual functions defined in `gio::ffi::GFileEnumeratorClass` except pairs `xxx_async/xxx_finish` for which GIO provides a default implementation (which should be ok).
// TODO: overriding these default implementations might still be useful for subclasses (if they can do something better than blocking IO).
pub trait FileEnumeratorImpl: ObjectImpl + ObjectSubclass<Type: IsA<FileEnumerator>> {
    fn next_file(
        &self,
        cancellable: Option<&Cancellable>,
    ) -> Result<Option<FileInfo>, glib::Error> {
        self.parent_next_file(cancellable)
    }

    fn next_files_future(
        &self,
        num_files: i32,
        priority: glib::Priority,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<glib::List<FileInfo>, glib::Error>> + 'static>,
    > {
        self.parent_next_files_future(num_files, priority)
    }

    // rustdoc-stripper-ignore-next
    /// Closes the enumerator (see [`FileEnumeratorExt::close`]).
    ///
    /// NOTE: If the enumerator has not been explicitly closed, GIO closes it when the object is dropped.
    /// But GIO does it by calling `close` vfunc in `finalize`, which is not safe and could lead to undefined behavior,
    /// such as accessing freed memory or resources, which can cause crashes or other unexpected behavior.
    ///
    /// An issue has been opened in GLib to address this: <https://gitlab.gnome.org/GNOME/glib/-/issues/3713> and a MR has been opened to fix it: <https://gitlab.gnome.org/GNOME/glib/-/merge_requests/4672>.
    ///
    /// Until this is fixed, it is unsafe to rely on the enumerator being closed when the object is dropped.
    /// It is recommended to close the enumerator explicitly before dropping it, by calling [`FileEnumeratorExt::close`],
    /// or to implement the [`ObjectImpl::dispose`] method and call [`FileEnumeratorExt::close`] there (it is safe to access the object there):
    /// ```ignore
    /// pub struct MyFileEnumerator();
    ///
    /// #[glib::object_subclass]
    /// impl ObjectSubclass for MyFileEnumerator { ... }
    ///
    /// impl ObjectImpl for MyFileEnumerator {
    ///     fn dispose(&self) {
    ///         // close the enumerator here is safe and avoids `finalize` to call close.
    ///         let _ = self.obj().close(Cancellable::NONE);
    ///     }
    /// }
    ///
    /// impl FileEnumeratorImpl for MyFileEnumerator { ... }
    /// ```
    ///
    /// [`FileEnumeratorExt::close`]: ../auto/file_enumerator/trait.FileEnumeratorExt.html#method.close
    /// [`ObjectImpl::dispose`]: ../../glib/subclass/object/trait.ObjectImpl.html#method.dispose
    fn close(&self, cancellable: Option<&Cancellable>) -> (bool, Option<glib::Error>) {
        self.parent_close(cancellable)
    }

    fn close_future(
        &self,
        io_priority: glib::Priority,
    ) -> Pin<Box<dyn Future<Output = Result<(), glib::Error>> + 'static>> {
        self.parent_close_future(io_priority)
    }
}

// Support parent implementation of virtual functions defined in `gio::ffi::GFileEnumeratorClass` except pairs `xxx_async/xxx_finish` for which GIO provides a default implementation (which should be ok).
// TODO: add parent implementation of `xxx_async/xxx_finish` virtual functions if overriding these default implementations is supported.
pub trait FileEnumeratorImplExt: FileEnumeratorImpl {
    fn parent_next_file(
        &self,
        cancellable: Option<&Cancellable>,
    ) -> Result<Option<FileInfo>, glib::Error> {
        if self.obj().is_closed() {
            Err(glib::Error::new::<IOErrorEnum>(
                IOErrorEnum::Closed,
                "Enumerator is closed",
            ))
        } else {
            unsafe {
                let data = Self::type_data();
                let parent_class = data.as_ref().parent_class() as *const ffi::GFileEnumeratorClass;

                let f = (*parent_class)
                    .next_file
                    .expect("No parent class implementation for \"next_file\"");

                let mut error = std::ptr::null_mut();
                let res = f(
                    self.obj()
                        .unsafe_cast_ref::<FileEnumerator>()
                        .to_glib_none()
                        .0,
                    cancellable.as_ref().to_glib_none().0,
                    &mut error,
                );
                if error.is_null() {
                    Ok(from_glib_full(res))
                } else {
                    Err(from_glib_full(error))
                }
            }
        }
    }

    fn parent_next_files_async<R: FnOnce(Result<glib::List<FileInfo>, glib::Error>) + 'static>(
        &self,
        num_files: i32,
        priority: glib::Priority,
        cancellable: Option<&Cancellable>,
        callback: R,
    ) {
        unsafe {
            let main_context = glib::MainContext::ref_thread_default();
            let is_main_context_owner = main_context.is_owner();
            let has_acquired_main_context = (!is_main_context_owner)
                .then(|| main_context.acquire().ok())
                .flatten();
            assert!(
                is_main_context_owner || has_acquired_main_context.is_some(),
                "Async operations only allowed if the thread is owning the MainContext"
            );

            let data = Self::type_data();
            let parent_class = data.as_ref().parent_class() as *const ffi::GFileEnumeratorClass;

            let f = (*parent_class)
                .next_files_async
                .expect("No parent class implementation for \"next_files_async\"");
            let finish = (*parent_class)
                .next_files_finish
                .expect("no parent \"next_files_finish\" implementation");

            let user_data: Box<(glib::thread_guard::ThreadGuard<R>, _)> =
                Box::new((glib::thread_guard::ThreadGuard::new(callback), finish));

            unsafe extern "C" fn next_files_async_trampoline<
                R: FnOnce(Result<glib::List<FileInfo>, glib::Error>) + 'static,
            >(
                source_object_ptr: *mut glib::gobject_ffi::GObject,
                res: *mut ffi::GAsyncResult,
                user_data: glib::ffi::gpointer,
            ) {
                unsafe {
                    let mut error = std::ptr::null_mut();
                    let cb: Box<(
                        glib::thread_guard::ThreadGuard<R>,
                        fn(
                            *mut ffi::GFileEnumerator,
                            *mut ffi::GAsyncResult,
                            *mut *mut glib::ffi::GError,
                        ) -> *mut glib::ffi::GList,
                    )> = Box::from_raw(user_data as *mut _);
                    let ret = cb.1(source_object_ptr as _, res, &mut error);
                    let result = if error.is_null() {
                        Ok(glib::List::<FileInfo>::from_glib_full(ret))
                    } else {
                        Err(from_glib_full(error))
                    };
                    let cb = cb.0.into_inner();
                    cb(result);
                }
            }

            f(
                self.obj()
                    .unsafe_cast_ref::<FileEnumerator>()
                    .to_glib_none()
                    .0,
                num_files,
                priority.into_glib(),
                cancellable.to_glib_none().0,
                Some(next_files_async_trampoline::<R>),
                Box::into_raw(user_data) as *mut _,
            );
        }
    }

    fn parent_next_files_future(
        &self,
        num_files: i32,
        priority: glib::Priority,
    ) -> Pin<
        Box<dyn std::future::Future<Output = Result<glib::List<FileInfo>, glib::Error>> + 'static>,
    > {
        Box::pin(GioFuture::new(
            &self.ref_counted(),
            move |imp, cancellable, send| {
                imp.parent_next_files_async(num_files, priority, Some(cancellable), move |res| {
                    send.resolve(res);
                });
            },
        ))
    }

    fn parent_close(&self, cancellable: Option<&Cancellable>) -> (bool, Option<glib::Error>) {
        unsafe {
            let data = Self::type_data();
            let parent_class = data.as_ref().parent_class() as *const ffi::GFileEnumeratorClass;

            let f = (*parent_class)
                .close_fn
                .expect("No parent class implementation for \"close_fn\"");

            let mut error = std::ptr::null_mut();
            let is_ok = f(
                self.obj()
                    .unsafe_cast_ref::<FileEnumerator>()
                    .to_glib_none()
                    .0,
                cancellable.as_ref().to_glib_none().0,
                &mut error,
            );
            (from_glib(is_ok), from_glib_full(error))
        }
    }

    fn parent_close_async<R: FnOnce(Result<(), glib::Error>) + 'static>(
        &self,
        io_priority: glib::Priority,
        cancellable: Option<&Cancellable>,
        callback: R,
    ) {
        unsafe {
            let main_context = glib::MainContext::ref_thread_default();
            let is_main_context_owner = main_context.is_owner();
            let has_acquired_main_context = (!is_main_context_owner)
                .then(|| main_context.acquire().ok())
                .flatten();
            assert!(
                is_main_context_owner || has_acquired_main_context.is_some(),
                "Async operations only allowed if the thread is owning the MainContext"
            );

            let data = Self::type_data();
            let parent_class = data.as_ref().parent_class() as *mut ffi::GFileEnumeratorClass;
            let f = (*parent_class)
                .close_async
                .expect("no parent \"close_async\" implementation");
            let finish = (*parent_class)
                .close_finish
                .expect("no parent \"close_finish\" implementation");

            let user_data: Box<(glib::thread_guard::ThreadGuard<R>, _)> =
                Box::new((glib::thread_guard::ThreadGuard::new(callback), finish));

            unsafe extern "C" fn close_async_trampoline<
                R: FnOnce(Result<(), glib::Error>) + 'static,
            >(
                source_object_ptr: *mut glib::gobject_ffi::GObject,
                res: *mut ffi::GAsyncResult,
                user_data: glib::ffi::gpointer,
            ) {
                unsafe {
                    let mut error = std::ptr::null_mut();
                    let cb: Box<(
                        glib::thread_guard::ThreadGuard<R>,
                        fn(
                            *mut ffi::GFileEnumerator,
                            *mut ffi::GAsyncResult,
                            *mut *mut glib::ffi::GError,
                        ) -> glib::ffi::gboolean,
                    )> = Box::from_raw(user_data as *mut _);
                    cb.1(source_object_ptr as _, res, &mut error);
                    let result = if error.is_null() {
                        Ok(())
                    } else {
                        Err(from_glib_full(error))
                    };
                    let cb = cb.0.into_inner();
                    cb(result);
                }
            }

            f(
                self.obj()
                    .unsafe_cast_ref::<FileEnumerator>()
                    .to_glib_none()
                    .0,
                io_priority.into_glib(),
                cancellable.to_glib_none().0,
                Some(close_async_trampoline::<R>),
                Box::into_raw(user_data) as *mut _,
            );
        }
    }

    fn parent_close_future(
        &self,
        io_priority: glib::Priority,
    ) -> Pin<Box<dyn Future<Output = Result<(), glib::Error>> + 'static>> {
        Box::pin(GioFuture::new(
            &self.ref_counted(),
            move |imp, cancellable, send| {
                imp.parent_close_async(io_priority, Some(cancellable), move |res| {
                    send.resolve(res);
                });
            },
        ))
    }
}

impl<T: FileEnumeratorImpl> FileEnumeratorImplExt for T {}

// Implement virtual functions defined in `gio::ffi::GFileEnumeratorClass` except pairs `xxx_async/xxx_finish` for which GIO provides a default implementation.
unsafe impl<T: FileEnumeratorImpl> IsSubclassable<T> for FileEnumerator {
    fn class_init(class: &mut ::glib::Class<Self>) {
        Self::parent_class_init::<T>(class);

        let klass = class.as_mut();
        klass.next_file = Some(next_file::<T>);
        klass.close_fn = Some(close_fn::<T>);
        klass.next_files_async = Some(next_files_async::<T>);
        klass.next_files_finish = Some(next_files_finish);
        klass.close_async = Some(close_async::<T>);
        klass.close_finish = Some(close_finish);
    }
}

unsafe extern "C" fn next_file<T: FileEnumeratorImpl>(
    enumerator: *mut ffi::GFileEnumerator,
    cancellable: *mut ffi::GCancellable,
    error: *mut *mut glib::ffi::GError,
) -> *mut ffi::GFileInfo {
    unsafe {
        let instance = &*(enumerator as *mut T::Instance);
        let imp = instance.imp();
        let cancellable = Option::<Cancellable>::from_glib_none(cancellable);

        let res = imp.next_file(cancellable.as_ref());

        match res {
            Ok(fileinfo) => fileinfo.to_glib_full(),
            Err(err) => {
                if !error.is_null() {
                    *error = err.to_glib_full()
                }
                std::ptr::null_mut()
            }
        }
    }
}

unsafe extern "C" fn next_files_async<T: FileEnumeratorImpl>(
    enumerator: *mut ffi::GFileEnumerator,
    num_files: i32,
    priority: i32,
    cancellable: *mut ffi::GCancellable,
    callback: ffi::GAsyncReadyCallback,
    user_data: glib::ffi::gpointer,
) {
    unsafe {
        let instance = &*(enumerator as *mut T::Instance);
        let imp = instance.imp();
        let wrap: FileEnumerator = from_glib_none(enumerator);
        let cancellable: Option<Cancellable> = from_glib_none(cancellable);

        // Closure that will invoke the C callback when the LocalTask completes
        let closure = move |task: LocalTask<glib::ValueArray>,
                            source_object: Option<&glib::Object>| {
            let result: *mut ffi::GAsyncResult = task.upcast_ref::<AsyncResult>().to_glib_none().0;
            let source_object: *mut glib::gobject_ffi::GObject = source_object.to_glib_none().0;
            callback.unwrap()(source_object, result, user_data)
        };

        let t = LocalTask::new(
            Some(wrap.upcast_ref::<glib::Object>()),
            cancellable.as_ref(),
            closure,
        );

        // Spawn the async work on the main context
        glib::MainContext::ref_thread_default().spawn_local(async move {
            // Call the trait method's future version
            let res = imp.next_files_future(num_files, from_glib(priority)).await;

            // Store result in the task
            t.return_result(res.map(|files| {
                let values: Vec<glib::Value> = files
                    .into_iter()
                    .map(|file_info| file_info.to_value())
                    .collect();
                glib::ValueArray::from_values(values)
            }));
        });
    }
}

unsafe extern "C" fn next_files_finish(
    _enumerator: *mut ffi::GFileEnumerator,
    res_ptr: *mut ffi::GAsyncResult,
    error_ptr: *mut *mut glib::ffi::GError,
) -> *mut glib::ffi::GList {
    unsafe {
        let res: AsyncResult = from_glib_none(res_ptr);
        let t = res.downcast::<LocalTask<glib::ValueArray>>().unwrap();
        let ret = t.propagate();
        match ret {
            Ok(files) => {
                let files = files
                    .iter()
                    .map(|v| <FileInfo as glib::value::FromValue>::from_value(v))
                    .collect::<Vec<FileInfo>>();
                files.to_glib_full()
            }
            Err(e) => {
                if !error_ptr.is_null() {
                    *error_ptr = e.into_glib_ptr();
                }
                std::ptr::null_mut()
            }
        }
    }
}

unsafe extern "C" fn close_fn<T: FileEnumeratorImpl>(
    enumerator: *mut ffi::GFileEnumerator,
    cancellable: *mut ffi::GCancellable,
    error: *mut *mut glib::ffi::GError,
) -> glib::ffi::gboolean {
    unsafe {
        let instance = &*(enumerator as *mut T::Instance);
        let imp = instance.imp();
        let cancellable = Option::<Cancellable>::from_glib_none(cancellable);

        let res = imp.close(cancellable.as_ref());

        if !error.is_null() {
            *error = res.1.to_glib_full()
        }

        res.0.into_glib()
    }
}

unsafe extern "C" fn close_async<T: FileEnumeratorImpl>(
    enumerator: *mut ffi::GFileEnumerator,
    priority: i32,
    cancellable: *mut ffi::GCancellable,
    callback: ffi::GAsyncReadyCallback,
    user_data: glib::ffi::gpointer,
) {
    unsafe {
        let instance = &*(enumerator as *mut T::Instance);
        let imp = instance.imp();
        let wrap: FileEnumerator = from_glib_none(enumerator);
        let cancellable: Option<Cancellable> = from_glib_none(cancellable);

        // Closure that will invoke the C callback when the LocalTask completes
        let closure = move |task: LocalTask<bool>, source_object: Option<&glib::Object>| {
            let result: *mut ffi::GAsyncResult = task.upcast_ref::<AsyncResult>().to_glib_none().0;
            let source_object: *mut glib::gobject_ffi::GObject = source_object.to_glib_none().0;
            callback.unwrap()(source_object, result, user_data)
        };

        let t = LocalTask::new(
            Some(wrap.upcast_ref::<glib::Object>()),
            cancellable.as_ref(),
            closure,
        );

        // Spawn the async work on the main context
        glib::MainContext::ref_thread_default().spawn_local(async move {
            // Call the trait method's future version
            let res = imp.close_future(from_glib(priority)).await;

            // Store result in the task (bool indicates success/failure)
            t.return_result(res.map(|_t| true));
        });
    }
}

unsafe extern "C" fn close_finish(
    _enumerator: *mut ffi::GFileEnumerator,
    res_ptr: *mut ffi::GAsyncResult,
    error_ptr: *mut *mut glib::ffi::GError,
) -> glib::ffi::gboolean {
    unsafe {
        let res: AsyncResult = from_glib_none(res_ptr);
        let t = res.downcast::<LocalTask<bool>>().unwrap();
        match t.propagate() {
            Ok(_) => glib::ffi::GTRUE,
            Err(e) => {
                if !error_ptr.is_null() {
                    *error_ptr = e.into_glib_ptr();
                }
                glib::ffi::GFALSE
            }
        }
    }
}

#[cfg(test)]
mod tests {
    // The following tests rely on a custom type `MyCustomFileEnumerator` that extends another custom type `MyFileEnumerator`.
    // For each virtual method defined in class `gio::ffi::GFileEnumeratorClass`, a test checks that `MyCustomFileEnumerator` and `MyFileEnumerator` return the same results.

    use super::*;

    // Define `MyCustomFileEnumerator` as a subclass of `MyFileEnumerator`.
    mod imp {
        use std::cell::Cell;

        use super::*;

        #[derive(Default)]
        pub struct MyFileEnumerator(Cell<i32>);

        #[glib::object_subclass]
        impl ObjectSubclass for MyFileEnumerator {
            const NAME: &'static str = "MyFileEnumerator";
            type Type = super::MyFileEnumerator;
            type ParentType = FileEnumerator;
        }

        impl ObjectImpl for MyFileEnumerator {
            fn dispose(&self) {
                let _ = self.obj().close(Cancellable::NONE);
            }
        }

        // Implements `FileEnumeratorImpl` with custom implementation.
        impl FileEnumeratorImpl for MyFileEnumerator {
            fn next_file(
                &self,
                cancellable: Option<&Cancellable>,
            ) -> Result<Option<FileInfo>, glib::Error> {
                if cancellable.is_some_and(|c| c.is_cancelled()) {
                    Err(glib::Error::new::<IOErrorEnum>(
                        IOErrorEnum::Cancelled,
                        "Operation was cancelled",
                    ))
                } else {
                    match self.0.get() {
                        -1 => Err(glib::Error::new::<IOErrorEnum>(
                            IOErrorEnum::Closed,
                            "Enumerator is closed",
                        )),
                        i if i < 3 => {
                            let file_info = FileInfo::new();
                            file_info.set_display_name(&format!("file{i}"));
                            self.0.set(i + 1);
                            Ok(Some(file_info))
                        }
                        _ => Ok(None),
                    }
                }
            }

            fn next_files_future(
                &self,
                num_files: i32,
                _priority: glib::Priority,
            ) -> std::pin::Pin<
                Box<
                    dyn std::future::Future<Output = Result<glib::List<FileInfo>, glib::Error>>
                        + 'static,
                >,
            > {
                Box::pin(GioFuture::new(
                    &self.ref_counted(),
                    move |self_, cancellable, send| {
                        let mut res: Result<glib::List<FileInfo>, glib::Error> =
                            Ok(glib::List::new());
                        for _ in 0..num_files {
                            match self_.next_file(Some(cancellable)) {
                                Ok(Some(fi)) => res.as_mut().unwrap().push_back(fi),
                                Ok(None) => break,
                                Err(e) => {
                                    res = Err(e);
                                    break;
                                }
                            }
                        }
                        send.resolve(res);
                    },
                ))
            }

            fn close(&self, cancellable: Option<&Cancellable>) -> (bool, Option<glib::Error>) {
                if cancellable.is_some_and(|c| c.is_cancelled()) {
                    (
                        false,
                        Some(glib::Error::new::<IOErrorEnum>(
                            IOErrorEnum::Cancelled,
                            "Operation was cancelled",
                        )),
                    )
                } else {
                    self.0.set(-1);
                    (true, None)
                }
            }

            fn close_future(
                &self,
                _priority: glib::Priority,
            ) -> std::pin::Pin<
                Box<dyn std::future::Future<Output = Result<(), glib::Error>> + 'static>,
            > {
                Box::pin(GioFuture::new(
                    &self.ref_counted(),
                    move |self_, cancellable, send| {
                        let (is_ok, error) = self_.close(Some(cancellable));
                        debug_assert_eq!(!is_ok, error.is_some());
                        let res = if is_ok { Ok(()) } else { Err(error.unwrap()) };
                        send.resolve(res);
                    },
                ))
            }
        }

        #[derive(Default)]
        pub struct MyCustomFileEnumerator;

        #[glib::object_subclass]
        impl ObjectSubclass for MyCustomFileEnumerator {
            const NAME: &'static str = "MyCustomFileEnumerator";
            type Type = super::MyCustomFileEnumerator;
            type ParentType = super::MyFileEnumerator;
        }

        impl ObjectImpl for MyCustomFileEnumerator {}

        // Implements `FileEnumeratorImpl` with default implementation, which calls the parent's implementation.
        impl FileEnumeratorImpl for MyCustomFileEnumerator {}

        impl MyFileEnumeratorImpl for MyCustomFileEnumerator {}
    }

    glib::wrapper! {
        pub struct MyFileEnumerator(ObjectSubclass<imp::MyFileEnumerator>) @extends FileEnumerator;
    }

    pub trait MyFileEnumeratorImpl:
        ObjectImpl + ObjectSubclass<Type: IsA<MyFileEnumerator> + IsA<FileEnumerator>>
    {
    }

    // To make this class subclassable we need to implement IsSubclassable
    unsafe impl<T: MyFileEnumeratorImpl + FileEnumeratorImpl> IsSubclassable<T> for MyFileEnumerator {}

    glib::wrapper! {
        pub struct MyCustomFileEnumerator(ObjectSubclass<imp::MyCustomFileEnumerator>) @extends MyFileEnumerator, FileEnumerator;
    }

    #[test]
    fn file_enumerator_next_file() {
        // invoke `MyCustomFileEnumerator` implementation of `gio::ffi::GFileEnumeratorClass::next_file`
        let my_custom_file_enumerator = glib::Object::new::<MyCustomFileEnumerator>();
        let res = my_custom_file_enumerator.next_file(Cancellable::NONE);
        assert!(res.as_ref().is_ok_and(|res| res.is_some()));
        let filename = res.unwrap().unwrap().display_name();

        // invoke `MyFileEnumerator` implementation of `gio::ffi::GFileEnumeratorClass::next_file`
        let my_file_enumerator = glib::Object::new::<MyFileEnumerator>();
        let res = my_file_enumerator.next_file(Cancellable::NONE);
        assert!(res.as_ref().is_ok_and(|res| res.is_some()));
        let expected = res.unwrap().unwrap().display_name();

        // both filenames should equal
        assert_eq!(filename, expected);

        // and also next results until there is no more file info
        for res in my_custom_file_enumerator.upcast::<FileEnumerator>() {
            assert!(res.as_ref().is_ok());
            let filename = res.unwrap().display_name();

            let res = my_file_enumerator.next_file(Cancellable::NONE);
            assert!(res.as_ref().is_ok_and(|res| res.is_some()));
            let expected = res.unwrap().unwrap().display_name();

            // both filenames should equal
            assert_eq!(filename, expected);
        }
    }

    #[test]
    fn file_enumerator_next_files_future() {
        // run test in a main context dedicated and configured as the thread default one
        let _ = glib::MainContext::new().with_thread_default(|| {
            // invoke `MyCustomFileEnumerator` implementation of `gio::ffi::GFileEnumeratorClass::next_files_future` asynchronously
            let my_custom_file_enumerator = glib::Object::new::<MyCustomFileEnumerator>();
            let res = glib::MainContext::ref_thread_default()
                .block_on(my_custom_file_enumerator.next_files_future(1, glib::Priority::DEFAULT));
            assert!(res.as_ref().is_ok_and(|res| res.len() == 1));
            let filename = res.unwrap().first().unwrap().display_name();

            // invoke `MyFileEnumerator` implementation of `gio::ffi::GFileEnumeratorClass::next_files_future` asynchronously
            let my_file_enumerator = glib::Object::new::<MyFileEnumerator>();
            let res = glib::MainContext::ref_thread_default()
                .block_on(my_file_enumerator.next_files_future(1, glib::Priority::DEFAULT));
            assert!(res.as_ref().is_ok_and(|res| res.len() == 1));
            let expected = res.unwrap().first().unwrap().display_name();

            // both filenames should equal
            assert_eq!(filename, expected);

            // and also next results until there is no more file info
            let res = glib::MainContext::ref_thread_default()
                .block_on(my_custom_file_enumerator.next_files_future(10, glib::Priority::DEFAULT));
            assert!(res.as_ref().is_ok());
            let filenames = res
                .unwrap()
                .into_iter()
                .map(|fi| fi.display_name())
                .collect::<Vec<_>>();

            let res = glib::MainContext::ref_thread_default()
                .block_on(my_file_enumerator.next_files_future(10, glib::Priority::DEFAULT));
            assert!(res.as_ref().is_ok());
            let expected = res
                .unwrap()
                .into_iter()
                .map(|fi| fi.display_name())
                .collect::<Vec<_>>();

            // both filenames should equal
            assert_eq!(filenames, expected);
        });
    }

    #[test]
    fn file_enumerator_close() {
        // invoke `MyCustomFileEnumerator` implementation of `gio::ffi::GFileEnumeratorClass::next_file`
        let my_custom_file_enumerator = glib::Object::new::<MyCustomFileEnumerator>();
        let res = my_custom_file_enumerator.next_file(Cancellable::NONE);
        assert!(res.as_ref().is_ok_and(|res| res.is_some()));
        let filename = res.unwrap().unwrap().display_name();

        // invoke `MyFileEnumerator` implementation of `gio::ffi::GFileEnumeratorClass::next_file`
        let my_file_enumerator = glib::Object::new::<MyFileEnumerator>();
        let res = my_file_enumerator.next_file(Cancellable::NONE);
        assert!(res.as_ref().is_ok_and(|res| res.is_some()));
        let expected = res.unwrap().unwrap().display_name();

        // both filenames should equal
        assert_eq!(filename, expected);

        // invoke `MyCustomFileEnumerator` implementation of `gio::ffi::GFileEnumeratorClass::close`
        let res = my_custom_file_enumerator.close(Cancellable::NONE);
        assert_eq!(res.1, None);
        let closed = res.0;

        // invoke `MyFileEnumerator` implementation of `gio::ffi::GFileEnumeratorClass::close`
        let res = my_file_enumerator.close(Cancellable::NONE);
        assert_eq!(res.1, None);
        let expected = res.0;

        // both results should equal
        assert_eq!(closed, expected);

        // invoke `MyCustomFileEnumerator` implementation of `gio::ffi::GFileEnumeratorClass::next_file`
        let res = my_custom_file_enumerator.next_file(Cancellable::NONE);
        assert!(res.is_err());
        let err = res.unwrap_err();

        // invoke `MyFileEnumerator` implementation of `gio::ffi::GFileEnumeratorClass::next_file`
        let res = my_file_enumerator.next_file(Cancellable::NONE);
        assert!(res.is_err());
        let expected = res.unwrap_err();

        // both errors should equal
        assert_eq!(err.domain(), expected.domain());
        assert!(err.matches::<IOErrorEnum>(IOErrorEnum::Closed));
        assert!(expected.matches::<IOErrorEnum>(IOErrorEnum::Closed));
        assert_eq!(err.message(), expected.message());
    }

    #[test]
    fn file_enumerator_close_future() {
        // run test in a main context dedicated and configured as the thread default one
        let _ = glib::MainContext::new().with_thread_default(|| {
            // invoke `MyCustomFileEnumerator` implementation of `gio::ffi::GFileEnumeratorClass::next_files_future` asynchronously
            let my_custom_file_enumerator = glib::Object::new::<MyCustomFileEnumerator>();
            let res = glib::MainContext::ref_thread_default()
                .block_on(my_custom_file_enumerator.next_files_future(1, glib::Priority::DEFAULT));
            assert!(res.as_ref().is_ok_and(|res| res.len() == 1));
            let filename = res.unwrap().first().unwrap().display_name();

            // invoke `MyFileEnumerator` implementation of `gio::ffi::GFileEnumeratorClass::next_files_future` asynchronously
            let my_file_enumerator = glib::Object::new::<MyFileEnumerator>();
            let res = glib::MainContext::ref_thread_default()
                .block_on(my_file_enumerator.next_files_future(1, glib::Priority::DEFAULT));
            assert!(res.as_ref().is_ok_and(|res| res.len() == 1));
            let expected = res.unwrap().first().unwrap().display_name();

            // both filenames should equal
            assert_eq!(filename, expected);

            // invoke `MyCustomFileEnumerator` implementation of `gio::ffi::GFileEnumeratorClass::close_future` asynchronously
            let res = glib::MainContext::ref_thread_default()
                .block_on(my_custom_file_enumerator.close_future(glib::Priority::DEFAULT));
            assert!(res.is_ok());
            let closed = true;

            // invoke `MyFileEnumerator` implementation of `gio::ffi::GFileEnumeratorClass::close_future` asynchronously
            let res = glib::MainContext::ref_thread_default()
                .block_on(my_file_enumerator.close_future(glib::Priority::DEFAULT));
            assert!(res.is_ok());
            let expected = true;

            // both results should equal
            assert_eq!(closed, expected);

            // invoke `MyCustomFileEnumerator` implementation of `gio::ffi::GFileEnumeratorClass::next_files_future` asynchronously
            let res = glib::MainContext::ref_thread_default()
                .block_on(my_custom_file_enumerator.next_files_future(1, glib::Priority::DEFAULT));
            assert!(res.is_err());
            let err = res.unwrap_err();

            // invoke `MyFileEnumerator` implementation of `gio::ffi::GFileEnumeratorClass::next_files_future` asynchronously
            let res = glib::MainContext::ref_thread_default()
                .block_on(my_file_enumerator.next_files_future(1, glib::Priority::DEFAULT));
            assert!(res.is_err());
            let expected = res.unwrap_err();

            // both errors should equal
            assert_eq!(err.domain(), expected.domain());
            assert!(err.matches::<IOErrorEnum>(IOErrorEnum::Closed));
            assert!(expected.matches::<IOErrorEnum>(IOErrorEnum::Closed));
            assert_eq!(err.message(), expected.message());
        });
    }

    #[test]
    fn file_enumerator_cancel() {
        // invoke `MyCustomFileEnumerator` implementation of `gio::ffi::GFileEnumeratorClass::next_file`
        let my_custom_file_enumerator = glib::Object::new::<MyCustomFileEnumerator>();
        let res = my_custom_file_enumerator.next_file(Cancellable::NONE);
        assert!(res.as_ref().is_ok_and(|res| res.is_some()));
        let filename = res.unwrap().unwrap().display_name();

        // invoke `MyFileEnumerator` implementation of `gio::ffi::GFileEnumeratorClass::next_file`
        let my_file_enumerator = glib::Object::new::<MyFileEnumerator>();
        let res = my_file_enumerator.next_file(Cancellable::NONE);
        assert!(res.as_ref().is_ok_and(|res| res.is_some()));
        let expected = res.unwrap().unwrap().display_name();

        // both filenames should equal
        assert_eq!(filename, expected);

        // invoke `MyCustomFileEnumerator` implementation of `gio::ffi::GFileEnumeratorClass::next_file` with cancel
        let cancellable = Cancellable::new();
        cancellable.cancel();
        let res = my_custom_file_enumerator.next_file(Some(&cancellable));
        assert!(res.as_ref().is_err());
        let err = res.unwrap_err();

        // invoke `MyFileEnumerator` implementation of `gio::ffi::GFileEnumeratorClass::next_file` with cancel
        let cancellable = Cancellable::new();
        cancellable.cancel();
        let res = my_file_enumerator.next_file(Some(&cancellable));
        assert!(res.as_ref().is_err());
        let expected = res.unwrap_err();

        // both errors should equal
        assert_eq!(err.domain(), expected.domain());
        assert!(err.matches::<IOErrorEnum>(IOErrorEnum::Cancelled));
        assert!(expected.matches::<IOErrorEnum>(IOErrorEnum::Cancelled));
        assert_eq!(err.message(), expected.message());

        // invoke `MyCustomFileEnumerator` implementation of `gio::ffi::GFileEnumeratorClass::close` with cancel
        let cancellable = Cancellable::new();
        cancellable.cancel();
        let res = my_custom_file_enumerator.close(Some(&cancellable));
        assert!(res.1.is_some());
        let err = res.1.unwrap();

        // invoke `MyFileEnumerator` implementation of `gio::ffi::GFileEnumeratorClass::close` with cancel
        let cancellable = Cancellable::new();
        cancellable.cancel();
        let res = my_file_enumerator.close(Some(&cancellable));
        assert!(res.1.is_some());
        let expected = res.1.unwrap();

        // both errors should equal
        assert_eq!(err.domain(), expected.domain());
        assert!(err.matches::<IOErrorEnum>(IOErrorEnum::Cancelled));
        assert!(expected.matches::<IOErrorEnum>(IOErrorEnum::Cancelled));
        assert_eq!(err.message(), expected.message());
    }
}
