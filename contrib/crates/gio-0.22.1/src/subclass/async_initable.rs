// Take a look at the license at the top of the repository in the LICENSE file.

use std::{future::Future, pin::Pin, ptr};

use glib::{Error, prelude::*, subclass::prelude::*, thread_guard::ThreadGuard, translate::*};

use crate::{
    AsyncInitable, AsyncResult, Cancellable, CancellableFuture, GioFutureResult, LocalTask, ffi,
};

pub trait AsyncInitableImpl: ObjectImpl + ObjectSubclass<Type: IsA<AsyncInitable>> {
    fn init_future(
        &self,
        io_priority: glib::Priority,
    ) -> Pin<Box<dyn Future<Output = Result<(), Error>> + 'static>> {
        self.parent_init_future(io_priority)
    }
}

pub trait AsyncInitableImplExt: AsyncInitableImpl {
    fn parent_init_future(
        &self,
        io_priority: glib::Priority,
    ) -> Pin<Box<dyn Future<Output = Result<(), Error>> + 'static>> {
        unsafe {
            let type_data = Self::type_data();
            let parent_iface = type_data.as_ref().parent_interface::<AsyncInitable>()
                as *const ffi::GAsyncInitableIface;

            let init_async = (*parent_iface)
                .init_async
                .expect("no parent \"init_async\" implementation");

            unsafe extern "C" fn parent_init_future_callback<
                T: ObjectSubclass<Type: IsA<glib::Object>>,
            >(
                source_object: *mut glib::gobject_ffi::GObject,
                res: *mut crate::ffi::GAsyncResult,
                user_data: glib::ffi::gpointer,
            ) {
                unsafe {
                    let type_data = T::type_data();
                    let parent_iface = type_data.as_ref().parent_interface::<AsyncInitable>()
                        as *const ffi::GAsyncInitableIface;
                    let init_finish = (*parent_iface)
                        .init_finish
                        .expect("no parent \"init_finish\" implementation");

                    let r: Box<ThreadGuard<GioFutureResult<Result<(), Error>>>> =
                        Box::from_raw(user_data as *mut _);
                    let r = r.into_inner();

                    let mut error = ptr::null_mut();
                    init_finish(source_object as *mut _, res, &mut error);
                    let result = if error.is_null() {
                        Ok(())
                    } else {
                        Err(from_glib_full(error))
                    };
                    r.resolve(result);
                }
            }

            Box::pin(crate::GioFuture::new(
                &*self.obj(),
                move |obj, cancellable, res| {
                    let user_data: Box<ThreadGuard<GioFutureResult<Result<(), Error>>>> =
                        Box::new(ThreadGuard::new(res));
                    let user_data = Box::into_raw(user_data);
                    init_async(
                        obj.unsafe_cast_ref::<AsyncInitable>().to_glib_none().0,
                        io_priority.into_glib(),
                        cancellable.to_glib_none().0,
                        Some(parent_init_future_callback::<Self>),
                        user_data as *mut _,
                    );
                },
            ))
        }
    }
}

impl<T: AsyncInitableImpl> AsyncInitableImplExt for T {}

unsafe impl<T: AsyncInitableImpl> IsImplementable<T> for AsyncInitable {
    fn interface_init(iface: &mut glib::Interface<Self>) {
        let iface = iface.as_mut();
        iface.init_async = Some(async_initable_init_async::<T>);
        iface.init_finish = Some(async_initable_init_finish);
    }
}

unsafe extern "C" fn async_initable_init_async<T: AsyncInitableImpl>(
    initable: *mut ffi::GAsyncInitable,
    io_priority: std::os::raw::c_int,
    cancellable: *mut ffi::GCancellable,
    callback: ffi::GAsyncReadyCallback,
    user_data: glib::ffi::gpointer,
) {
    unsafe {
        let instance = &*(initable as *mut T::Instance);
        let imp = instance.imp();
        let cancellable = Option::<Cancellable>::from_glib_none(cancellable);

        let task = callback.map(|callback| {
            let task = LocalTask::new(
                Some(imp.obj().unsafe_cast_ref::<glib::Object>()),
                cancellable.as_ref(),
                move |task, obj| {
                    let result: *mut crate::ffi::GAsyncResult =
                        task.upcast_ref::<AsyncResult>().to_glib_none().0;
                    let obj: *mut glib::gobject_ffi::GObject = obj.to_glib_none().0;
                    callback(obj, result, user_data);
                },
            );
            task.set_check_cancellable(true);
            task.set_return_on_cancel(true);
            task
        });

        glib::MainContext::ref_thread_default().spawn_local(async move {
            let io_priority = from_glib(io_priority);
            let res = if let Some(cancellable) = cancellable {
                CancellableFuture::new(imp.init_future(io_priority), cancellable)
                    .await
                    .map_err(|cancelled| cancelled.into())
                    .and_then(|res| res)
            } else {
                imp.init_future(io_priority).await
            };
            if let Some(task) = task {
                task.return_result(res.map(|_t| true));
            }
        });
    }
}

unsafe extern "C" fn async_initable_init_finish(
    initable: *mut ffi::GAsyncInitable,
    res: *mut ffi::GAsyncResult,
    error: *mut *mut glib::ffi::GError,
) -> glib::ffi::gboolean {
    unsafe {
        let res = from_glib_none::<_, AsyncResult>(res);

        let task = res
            .downcast::<LocalTask<bool>>()
            .expect("GAsyncResult is not a GTask");
        if !LocalTask::<bool>::is_valid(
            &task,
            Some(from_glib_borrow::<_, AsyncInitable>(initable).as_ref()),
        ) {
            panic!("Task is not valid for source object");
        }

        match task.propagate() {
            Ok(v) => {
                debug_assert!(v);
                true.into_glib()
            }
            Err(e) => {
                if !error.is_null() {
                    *error = e.into_glib_ptr();
                }
                false.into_glib()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prelude::*;

    pub mod imp {
        use std::cell::Cell;

        use super::*;

        pub struct AsyncInitableTestType(pub Cell<u64>);

        #[glib::object_subclass]
        impl ObjectSubclass for AsyncInitableTestType {
            const NAME: &'static str = "AsyncInitableTestType";
            type Type = super::AsyncInitableTestType;
            type Interfaces = (AsyncInitable,);

            fn new() -> Self {
                Self(Cell::new(0))
            }
        }

        impl AsyncInitableImpl for AsyncInitableTestType {
            fn init_future(
                &self,
                _io_priority: glib::Priority,
            ) -> Pin<Box<dyn Future<Output = Result<(), Error>> + 'static>> {
                let imp = glib::subclass::ObjectImplRef::new(self);
                Box::pin(async move {
                    glib::timeout_future_seconds(0).await;
                    imp.0.set(0x123456789abcdef);
                    Ok(())
                })
            }
        }

        impl ObjectImpl for AsyncInitableTestType {}
    }

    pub mod ffi {
        use super::*;
        pub type AsyncInitableTestType = <imp::AsyncInitableTestType as ObjectSubclass>::Instance;

        pub unsafe extern "C" fn async_initable_test_type_get_value(
            this: *mut AsyncInitableTestType,
        ) -> u64 {
            unsafe {
                let this = super::AsyncInitableTestType::from_glib_borrow(this);
                this.imp().0.get()
            }
        }
    }

    glib::wrapper! {
        pub struct AsyncInitableTestType(ObjectSubclass<imp::AsyncInitableTestType>)
            @implements AsyncInitable;
    }

    #[allow(clippy::new_without_default)]
    impl AsyncInitableTestType {
        pub async fn new() -> Self {
            AsyncInitable::new_future(glib::Priority::default())
                .await
                .expect("Failed creation/initialization of AsyncInitableTestType object")
        }

        pub unsafe fn new_uninit() -> Self {
            unsafe {
                // This creates an uninitialized AsyncInitableTestType object, for testing
                // purposes. In real code, using AsyncInitable::new_future (like the new() method
                // does) is recommended.
                glib::Object::new_internal(Self::static_type(), &mut [])
                    .downcast()
                    .unwrap()
            }
        }

        pub fn value(&self) -> u64 {
            self.imp().0.get()
        }
    }

    #[test]
    fn test_async_initable_with_init() {
        glib::MainContext::new().block_on(async {
            let res = unsafe {
                let test = AsyncInitableTestType::new_uninit();

                assert_ne!(0x123456789abcdef, test.value());

                test.init_future(glib::Priority::default())
                    .await
                    .map(|_| test)
            };
            assert!(res.is_ok());
            let test = res.unwrap();

            assert_eq!(0x123456789abcdef, test.value());
        });
    }

    #[test]
    fn test_async_initable_with_initable_new() {
        glib::MainContext::new().block_on(async {
            let test = AsyncInitableTestType::new().await;
            assert_eq!(0x123456789abcdef, test.value());
        });
    }

    #[test]
    #[should_panic = ""]
    fn test_async_initable_new_failure() {
        glib::MainContext::new().block_on(async {
            let value: u32 = 2;
            let _ = AsyncInitable::builder::<AsyncInitableTestType>()
                .property("invalid-property", value)
                .build_future(glib::Priority::default())
                .await;
            unreachable!();
        });
    }

    #[test]
    fn test_async_initable_with_initable_with_type() {
        glib::MainContext::new().block_on(async {
            let test = AsyncInitable::with_type_future(
                AsyncInitableTestType::static_type(),
                glib::Priority::default(),
            )
            .await
            .expect("Failed creation/initialization of AsyncInitableTestType object from type")
            .downcast::<AsyncInitableTestType>()
            .expect("Failed downcast of AsyncInitableTestType object");
            assert_eq!(0x123456789abcdef, test.value());
        });
    }

    #[test]
    fn test_async_initable_with_async_initable_with_values() {
        glib::MainContext::new().block_on(async {
            let test = AsyncInitable::with_type_future(
                AsyncInitableTestType::static_type(),
                glib::Priority::default(),
            )
            .await
            .expect("Failed creation/initialization of AsyncInitableTestType object from values")
            .downcast::<AsyncInitableTestType>()
            .expect("Failed downcast of AsyncInitableTestType object");
            assert_eq!(0x123456789abcdef, test.value());
        });
    }

    #[test]
    fn test_async_initable_through_ffi() {
        use futures_channel::oneshot;

        glib::MainContext::new().block_on(async {
            unsafe {
                let test = AsyncInitableTestType::new_uninit();
                let test: *mut ffi::AsyncInitableTestType = test.as_ptr();

                assert_ne!(
                    0x123456789abcdef,
                    ffi::async_initable_test_type_get_value(test)
                );

                let (tx, rx) = oneshot::channel::<Result<(), glib::Error>>();
                let user_data = Box::new(ThreadGuard::new(tx));
                unsafe extern "C" fn init_async_callback(
                    source_object: *mut glib::gobject_ffi::GObject,
                    res: *mut crate::ffi::GAsyncResult,
                    user_data: glib::ffi::gpointer,
                ) {
                    unsafe {
                        let tx: Box<ThreadGuard<oneshot::Sender<Result<(), glib::Error>>>> =
                            Box::from_raw(user_data as *mut _);
                        let tx = tx.into_inner();
                        let mut error = ptr::null_mut();
                        let ret = crate::ffi::g_async_initable_init_finish(
                            source_object as *mut _,
                            res,
                            &mut error,
                        );
                        assert_eq!(ret, glib::ffi::GTRUE);
                        let result = if error.is_null() {
                            Ok(())
                        } else {
                            Err(from_glib_full(error))
                        };
                        tx.send(result).unwrap();
                    }
                }

                crate::ffi::g_async_initable_init_async(
                    test as *mut crate::ffi::GAsyncInitable,
                    glib::ffi::G_PRIORITY_DEFAULT,
                    std::ptr::null_mut(),
                    Some(init_async_callback),
                    Box::into_raw(user_data) as *mut _,
                );

                let result = rx.await.unwrap();
                assert!(result.is_ok());
                assert_eq!(
                    0x123456789abcdef,
                    ffi::async_initable_test_type_get_value(test)
                );
            }
        });
    }
}
