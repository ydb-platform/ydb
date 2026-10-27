// Take a look at the license at the top of the repository in the LICENSE file.

use std::{boxed::Box as Box_, future::Future, mem::transmute, panic, ptr};

use glib::{
    prelude::*,
    signal::{SignalHandlerId, connect_raw},
    translate::*,
};

use futures_channel::oneshot;

use crate::{AsyncResult, Cancellable, ffi};

glib::wrapper! {
    // rustdoc-stripper-ignore-next
    /// `LocalTask` provides idiomatic access to gio's `GTask` API, for
    /// instance by being generic over their value type, while not completely departing
    /// from the underlying C API. `LocalTask` does not require its value to be `Send`
    /// and `Sync` and thus is useful to to implement gio style asynchronous
    /// tasks that run in the glib main loop. If you need to run tasks in threads
    /// see the `Task` type.
    ///
    /// The constructors of `LocalTask` and `Task` is marked as unsafe because this API does
    /// not allow to automatically enforce all the invariants required to be a completely
    /// safe abstraction. See the `Task` type for more details.
    #[doc(alias = "GTask")]
    pub struct LocalTask<V: ValueType>(Object<ffi::GTask, ffi::GTaskClass>) @implements AsyncResult;

    match fn {
        type_ => || ffi::g_task_get_type(),
    }
}

glib::wrapper! {
    // rustdoc-stripper-ignore-next
    /// `Task` provides idiomatic access to gio's `GTask` API, for
    /// instance by being generic over their value type, while not completely departing
    /// from the underlying C API. `Task` is `Send` and `Sync` and requires its value to
    /// also be `Send` and `Sync`, thus is useful to to implement gio style asynchronous
    /// tasks that run in threads. If you need to only run tasks in glib main loop
    /// see the `LocalTask` type.
    ///
    /// The constructors of `LocalTask` and `Task` is marked as unsafe because this API does
    /// not allow to automatically enforce all the invariants required to be a completely
    /// safe abstraction. The caller is responsible to ensure the following requirements
    /// are satisfied
    ///
    /// * You should not create a `LocalTask`, upcast it to a `glib::Object` and then
    ///   downcast it to a `Task`, as this will bypass the thread safety requirements
    /// * You should ensure that the `return_result`, `return_error_if_cancelled` and
    ///   `propagate()` methods are only called once.
    #[doc(alias = "GTask")]
    pub struct Task<V: ValueType + Send>(Object<ffi::GTask, ffi::GTaskClass>) @implements AsyncResult;

    match fn {
        type_ => || ffi::g_task_get_type(),
    }
}

macro_rules! task_impl {
    ($name:ident $(, @bound: $bound:tt)? $(, @safety: $safety:tt)?) => {
        impl <V: Into<glib::Value> + ValueType $(+ $bound)?> $name<V> {
            #[doc(alias = "g_task_new")]
            #[allow(unused_unsafe)]
            pub unsafe fn new<S, P, Q>(
                source_object: Option<&S>,
                cancellable: Option<&P>,
                callback: Q,
            ) -> Self
            where
                S: IsA<glib::Object> $(+ $bound)?,
                P: IsA<Cancellable>,
                Q: FnOnce($name<V>, Option<&S>) $(+ $bound)? + 'static,
            {
                let callback_data = Box_::new(callback);
                unsafe extern "C" fn trampoline<
                    S: IsA<glib::Object> $(+ $bound)?,
                    V: ValueType $(+ $bound)?,
                    Q: FnOnce($name<V>, Option<&S>) $(+ $bound)? + 'static,
                >(
                    source_object: *mut glib::gobject_ffi::GObject,
                    res: *mut ffi::GAsyncResult,
                    user_data: glib::ffi::gpointer,
                ) { unsafe {
                    let callback: Box_<Q> = Box::from_raw(user_data as *mut _);
                    let task = AsyncResult::from_glib_none(res)
                        .downcast::<$name<V>>()
                        .unwrap();
                    let source_object = Option::<glib::Object>::from_glib_borrow(source_object);
                    callback(
                        task,
                        source_object.as_ref().as_ref().map(|s| s.unsafe_cast_ref()),
                    );
                }}
                let callback = trampoline::<S, V, Q>;
                unsafe {
                    from_glib_full(ffi::g_task_new(
                        source_object.map(|p| p.as_ref()).to_glib_none().0,
                        cancellable.map(|p| p.as_ref()).to_glib_none().0,
                        Some(callback),
                        Box_::into_raw(callback_data) as *mut _,
                    ))
                }
            }

            #[doc(alias = "g_task_get_cancellable")]
            #[doc(alias = "get_cancellable")]
            pub fn cancellable(&self) -> Option<Cancellable> {
                unsafe { from_glib_none(ffi::g_task_get_cancellable(self.to_glib_none().0)) }
            }

            #[doc(alias = "g_task_get_check_cancellable")]
            #[doc(alias = "get_check_cancellable")]
            pub fn is_check_cancellable(&self) -> bool {
                unsafe { from_glib(ffi::g_task_get_check_cancellable(self.to_glib_none().0)) }
            }

            #[doc(alias = "g_task_set_check_cancellable")]
            pub fn set_check_cancellable(&self, check_cancellable: bool) {
                unsafe {
                    ffi::g_task_set_check_cancellable(self.to_glib_none().0, check_cancellable.into_glib());
                }
            }

            #[cfg(feature = "v2_60")]
            #[cfg_attr(docsrs, doc(cfg(feature = "v2_60")))]
            #[doc(alias = "g_task_set_name")]
            pub fn set_name(&self, name: Option<&str>) {
                unsafe {
                    ffi::g_task_set_name(self.to_glib_none().0, name.to_glib_none().0);
                }
            }

            #[doc(alias = "g_task_set_return_on_cancel")]
            pub fn set_return_on_cancel(&self, return_on_cancel: bool) -> bool {
                unsafe {
                    from_glib(ffi::g_task_set_return_on_cancel(
                        self.to_glib_none().0,
                        return_on_cancel.into_glib(),
                    ))
                }
            }

            #[doc(alias = "g_task_is_valid")]
            pub fn is_valid(
                result: &impl IsA<AsyncResult>,
                source_object: Option<&impl IsA<glib::Object>>,
            ) -> bool {
                unsafe {
                    from_glib(ffi::g_task_is_valid(
                        result.as_ref().to_glib_none().0,
                        source_object.map(|p| p.as_ref()).to_glib_none().0,
                    ))
                }
            }

            #[doc(alias = "get_priority")]
            #[doc(alias = "g_task_get_priority")]
            pub fn priority(&self) -> glib::source::Priority {
                unsafe { FromGlib::from_glib(ffi::g_task_get_priority(self.to_glib_none().0)) }
            }

            #[doc(alias = "g_task_set_priority")]
            pub fn set_priority(&self, priority: glib::source::Priority) {
                unsafe {
                    ffi::g_task_set_priority(self.to_glib_none().0, priority.into_glib());
                }
            }

            #[doc(alias = "g_task_get_completed")]
            #[doc(alias = "get_completed")]
            pub fn is_completed(&self) -> bool {
                unsafe { from_glib(ffi::g_task_get_completed(self.to_glib_none().0)) }
            }

            #[doc(alias = "g_task_get_context")]
            #[doc(alias = "get_context")]
            pub fn context(&self) -> glib::MainContext {
                unsafe { from_glib_none(ffi::g_task_get_context(self.to_glib_none().0)) }
            }

            #[cfg(feature = "v2_60")]
            #[cfg_attr(docsrs, doc(cfg(feature = "v2_60")))]
            #[doc(alias = "g_task_get_name")]
            #[doc(alias = "get_name")]
            pub fn name(&self) -> Option<glib::GString> {
                unsafe { from_glib_none(ffi::g_task_get_name(self.to_glib_none().0)) }
            }

            #[doc(alias = "g_task_get_return_on_cancel")]
            #[doc(alias = "get_return_on_cancel")]
            pub fn is_return_on_cancel(&self) -> bool {
                unsafe { from_glib(ffi::g_task_get_return_on_cancel(self.to_glib_none().0)) }
            }

            #[doc(alias = "g_task_had_error")]
            pub fn had_error(&self) -> bool {
                unsafe { from_glib(ffi::g_task_had_error(self.to_glib_none().0)) }
            }

            #[doc(alias = "completed")]
            pub fn connect_completed_notify<F>(&self, f: F) -> SignalHandlerId
            where
                F: Fn(&$name<V>) $(+ $bound)? + 'static,
            {
                unsafe extern "C" fn notify_completed_trampoline<V, F>(
                    this: *mut ffi::GTask,
                    _param_spec: glib::ffi::gpointer,
                    f: glib::ffi::gpointer,
                ) where
                    V: ValueType $(+ $bound)?,
                    F: Fn(&$name<V>) + 'static,
                { unsafe {
                    let f: &F = &*(f as *const F);
                    f(&from_glib_borrow(this))
                }}
                unsafe {
                    let f: Box_<F> = Box_::new(f);
                    connect_raw(
                        self.as_ptr() as *mut _,
                        b"notify::completed\0".as_ptr() as *const _,
                        Some(transmute::<*const (), unsafe extern "C" fn()>(
                            notify_completed_trampoline::<V, F> as *const (),
                        )),
                        Box_::into_raw(f),
                    )
                }
            }

            // the following functions are marked unsafe since they cannot be called
            // more than once, but we have no way to enforce that since the task can be cloned

            #[doc(alias = "g_task_return_error_if_cancelled")]
            #[allow(unused_unsafe)]
            pub $($safety)? fn return_error_if_cancelled(&self) -> bool {
                unsafe { from_glib(ffi::g_task_return_error_if_cancelled(self.to_glib_none().0)) }
            }

            // rustdoc-stripper-ignore-next
            /// Set the result of the task
            ///
            /// # Safety
            ///
            /// The value must be read with [`Task::propagate`],
            /// `g_task_propagate_value` or `g_task_propagate_pointer`.
            #[doc(alias = "g_task_return_value")]
            #[doc(alias = "g_task_return_pointer")]
            #[doc(alias = "g_task_return_error")]
            #[allow(unused_unsafe)]
            pub $($safety)? fn return_result(self, result: Result<V, glib::Error>) {
                #[cfg(not(feature = "v2_64"))]
                unsafe extern "C" fn value_free(value: *mut libc::c_void) { unsafe {
                    let _: glib::Value = from_glib_full(value as *mut glib::gobject_ffi::GValue);
                }}

                match result {
                    #[cfg(feature = "v2_64")]
                    Ok(v) => unsafe {
                        ffi::g_task_return_value(
                            self.to_glib_none().0,
                            v.to_value().to_glib_none().0 as *mut _,
                        )
                    },
                    #[cfg(not(feature = "v2_64"))]
                    Ok(v) => unsafe {
                        let v: glib::Value = v.into();
                        ffi::g_task_return_pointer(
                            self.to_glib_none().0,
                            <glib::Value as glib::translate::IntoGlibPtr::<*mut glib::gobject_ffi::GValue>>::into_glib_ptr(v) as glib::ffi::gpointer,
                            Some(value_free),
                        )
                    },
                    Err(e) => unsafe {
                        ffi::g_task_return_error(self.to_glib_none().0, e.into_glib_ptr());
                    },
                }
            }

            // rustdoc-stripper-ignore-next
            /// Set the result of the task as a boolean
            ///
            /// # Safety
            ///
            /// The value must be read with [`Task::propagate_boolean`],
            /// or `g_task_propagate_boolean`.
            #[doc(alias = "g_task_return_boolean")]
            #[allow(unused_unsafe)]
            pub $($safety)? fn return_boolean_result(self, result: Result<bool, glib::Error>) {
                match result {
                    Ok(v) =>  unsafe { ffi::g_task_return_boolean(self.to_glib_none().0, v as i32) },
                    Err(e) => unsafe { ffi::g_task_return_error(self.to_glib_none().0, e.into_glib_ptr()) },
                }
            }

            // rustdoc-stripper-ignore-next
            /// Set the result of the task as an int
            ///
            /// # Safety
            ///
            /// The value must be read with [`Task::propagate_int`],
            /// or `g_task_propagate_int`.
            #[doc(alias = "g_task_return_int")]
            #[allow(unused_unsafe)]
            pub $($safety)? fn return_int_result(self, result: Result<isize, glib::Error>) {
                match result {
                    Ok(v) =>  unsafe { ffi::g_task_return_int(self.to_glib_none().0, v) },
                    Err(e) => unsafe { ffi::g_task_return_error(self.to_glib_none().0, e.into_glib_ptr()) },
                }
            }


            // rustdoc-stripper-ignore-next
            /// Gets the result of the task and transfers ownership of it
            ///
            /// # Safety
            ///
            /// This must only be called once, and only if the result was set
            /// via [`Task::return_result`], `g_task_return_value` or
            /// `g_task_return_pointer`.
            #[doc(alias = "g_task_propagate_value")]
            #[doc(alias = "g_task_propagate_pointer")]
            #[allow(unused_unsafe)]
            pub unsafe fn propagate(self) -> Result<V, glib::Error> {
                let mut error = ptr::null_mut();

                unsafe {
                    #[cfg(feature = "v2_64")]
                    {
                        let mut value = glib::Value::uninitialized();
                        ffi::g_task_propagate_value(
                            self.to_glib_none().0,
                            value.to_glib_none_mut().0,
                            &mut error,
                        );

                        if error.is_null() {
                            Ok(V::from_value(&value))
                        } else {
                            Err(from_glib_full(error))
                        }
                    }

                    #[cfg(not(feature = "v2_64"))]
                    {
                        let value = ffi::g_task_propagate_pointer(self.to_glib_none().0, &mut error);

                        if error.is_null() {
                            let value = Option::<glib::Value>::from_glib_full(
                                value as *mut glib::gobject_ffi::GValue,
                            )
                            .expect("Task::propagate() called before Task::return_result()");
                            Ok(V::from_value(&value))
                        } else {
                            Err(from_glib_full(error))
                        }
                    }
                }
            }

            // rustdoc-stripper-ignore-next
            /// Gets the result of the task as a boolean, or the error
            ///
            /// # Safety
            ///
            /// This must only be called once, and only if the result was set
            /// via [`Task::return_boolean_result`], or `g_task_return_boolean`.
            #[doc(alias = "g_task_propagate_boolean")]
            #[allow(unused_unsafe)]
            pub unsafe fn propagate_boolean(self) -> Result<bool, glib::Error> {
                let mut error = ptr::null_mut();

                unsafe {
                    let res = ffi::g_task_propagate_boolean(self.to_glib_none().0, &mut error);

                    if error.is_null() {
                        Ok(res != 0)
                    } else {
                        Err(from_glib_full(error))
                    }
                }
            }

            // rustdoc-stripper-ignore-next
            /// Gets the result of the task as an int, or the error
            ///
            /// # Safety
            ///
            /// This must only be called once, and only if the result was set
            /// via [`Task::return_int_result`], or `g_task_return_int`.
            #[doc(alias = "g_task_propagate_int")]
            #[allow(unused_unsafe)]
            pub unsafe fn propagate_int(self) -> Result<isize, glib::Error> {
                let mut error = ptr::null_mut();

                unsafe {
                    let res = ffi::g_task_propagate_int(self.to_glib_none().0, &mut error);

                    if error.is_null() {
                        Ok(res)
                    } else {
                        Err(from_glib_full(error))
                    }
                }
            }
        }
    }
}

task_impl!(LocalTask);
task_impl!(Task, @bound: Send, @safety: unsafe);

impl<V: ValueType + Send> Task<V> {
    #[doc(alias = "g_task_run_in_thread")]
    pub fn run_in_thread<S, Q>(&self, task_func: Q)
    where
        S: IsA<glib::Object> + Send,
        Q: FnOnce(Self, Option<&S>, Option<&Cancellable>) + Send + 'static,
    {
        let task_func_data = Box_::new(task_func);

        // We store the func pointer into the task data.
        // We intentionally do not expose a way to set the task data in the bindings.
        // If we detect that the task data is set, there is not much we can do, so we panic.
        unsafe {
            assert!(
                ffi::g_task_get_task_data(self.to_glib_none().0).is_null(),
                "Task data was manually set or the task was run thread multiple times"
            );

            ffi::g_task_set_task_data(
                self.to_glib_none().0,
                Box_::into_raw(task_func_data) as *mut _,
                None,
            );
        }

        unsafe extern "C" fn trampoline<V, S, Q>(
            task: *mut ffi::GTask,
            source_object: *mut glib::gobject_ffi::GObject,
            user_data: glib::ffi::gpointer,
            cancellable: *mut ffi::GCancellable,
        ) where
            V: ValueType + Send,
            S: IsA<glib::Object> + Send,
            Q: FnOnce(Task<V>, Option<&S>, Option<&Cancellable>) + Send + 'static,
        {
            unsafe {
                let task = Task::from_glib_none(task);
                let source_object = Option::<glib::Object>::from_glib_borrow(source_object);
                let cancellable = Option::<Cancellable>::from_glib_borrow(cancellable);
                let task_func: Box_<Q> = Box::from_raw(user_data as *mut _);
                task_func(
                    task,
                    source_object.as_ref().as_ref().map(|s| s.unsafe_cast_ref()),
                    cancellable.as_ref().as_ref(),
                );
            }
        }

        let task_func = trampoline::<V, S, Q>;
        unsafe {
            ffi::g_task_run_in_thread(self.to_glib_none().0, Some(task_func));
        }
    }
}

unsafe impl<V: ValueType + Send> Send for Task<V> {}
unsafe impl<V: ValueType + Send> Sync for Task<V> {}

// rustdoc-stripper-ignore-next
/// A handle to a task running on the I/O thread pool.
///
/// Like [`std::thread::JoinHandle`] for a blocking I/O task rather than a thread. The return value
/// from the task can be retrieved by awaiting on this handle. Dropping the handle "detaches" the
/// task, allowing it to complete but discarding the return value.
#[derive(Debug)]
pub struct JoinHandle<T> {
    rx: oneshot::Receiver<std::thread::Result<T>>,
}

impl<T> JoinHandle<T> {
    #[inline]
    fn new() -> (Self, oneshot::Sender<std::thread::Result<T>>) {
        let (tx, rx) = oneshot::channel();
        (Self { rx }, tx)
    }
}

impl<T> Future for JoinHandle<T> {
    type Output = std::thread::Result<T>;
    #[inline]
    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        std::pin::Pin::new(&mut self.rx)
            .poll(cx)
            .map(|r| r.unwrap())
    }
}

impl<T> futures_core::FusedFuture for JoinHandle<T> {
    #[inline]
    fn is_terminated(&self) -> bool {
        self.rx.is_terminated()
    }
}

// rustdoc-stripper-ignore-next
/// Runs a blocking I/O task on the I/O thread pool.
///
/// Calls `func` on the internal Gio thread pool for blocking I/O operations. The thread pool is
/// shared with other Gio async I/O operations, and may rate-limit the tasks it receives. Callers
/// may want to avoid blocking indefinitely by making sure blocking calls eventually time out.
///
/// This function should not be used to spawn async tasks. Instead, use
/// [`glib::MainContext::spawn`] or [`glib::MainContext::spawn_local`] to run a future.
pub fn spawn_blocking<T, F>(func: F) -> JoinHandle<T>
where
    T: Send + 'static,
    F: FnOnce() -> T + Send + 'static,
{
    // use Cancellable::NONE as source obj to fulfill `Send` requirement
    let task = unsafe { Task::<bool>::new(Cancellable::NONE, Cancellable::NONE, |_, _| {}) };
    let (join, tx) = JoinHandle::new();
    task.run_in_thread(move |task, _: Option<&Cancellable>, _| {
        let res = panic::catch_unwind(panic::AssertUnwindSafe(func));
        let _ = tx.send(res);
        unsafe { ffi::g_task_return_pointer(task.to_glib_none().0, ptr::null_mut(), None) }
    });

    join
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{prelude::*, test_util::run_async_local};

    #[test]
    fn test_int_value_async_result() {
        let fut = run_async_local(|tx, l| {
            let cancellable = crate::Cancellable::new();
            let task = unsafe {
                crate::LocalTask::new(
                    None,
                    Some(&cancellable),
                    move |t: LocalTask<i32>, _b: Option<&glib::Object>| {
                        tx.send(t.propagate()).unwrap();
                        l.quit();
                    },
                )
            };
            task.return_result(Ok(100_i32));
        });

        match fut {
            Err(_) => panic!(),
            Ok(i) => assert_eq!(i, 100),
        }
    }

    #[test]
    fn test_boolean_async_result() {
        let fut = run_async_local(|tx, l| {
            let cancellable = crate::Cancellable::new();
            let task = unsafe {
                crate::LocalTask::new(
                    None,
                    Some(&cancellable),
                    move |t: LocalTask<bool>, _b: Option<&glib::Object>| {
                        tx.send(t.propagate_boolean()).unwrap();
                        l.quit();
                    },
                )
            };
            task.return_boolean_result(Ok(true));
        });

        match fut {
            Err(_) => panic!(),
            Ok(i) => assert!(i),
        }
    }

    #[test]
    fn test_int_async_result() {
        let fut = run_async_local(|tx, l| {
            let cancellable = crate::Cancellable::new();
            let task = unsafe {
                crate::LocalTask::new(
                    None,
                    Some(&cancellable),
                    move |t: LocalTask<i32>, _b: Option<&glib::Object>| {
                        tx.send(t.propagate_int()).unwrap();
                        l.quit();
                    },
                )
            };
            task.return_int_result(Ok(100_isize));
        });

        match fut {
            Err(_) => panic!(),
            Ok(i) => assert_eq!(i, 100),
        }
    }

    #[test]
    fn test_object_async_result() {
        use glib::subclass::prelude::*;
        pub struct MySimpleObjectPrivate {
            pub size: std::cell::RefCell<Option<i64>>,
        }

        #[glib::object_subclass]
        impl ObjectSubclass for MySimpleObjectPrivate {
            const NAME: &'static str = "MySimpleObjectPrivate";
            type Type = MySimpleObject;

            fn new() -> Self {
                Self {
                    size: std::cell::RefCell::new(Some(100)),
                }
            }
        }

        impl ObjectImpl for MySimpleObjectPrivate {}

        glib::wrapper! {
            pub struct MySimpleObject(ObjectSubclass<MySimpleObjectPrivate>);
        }

        impl MySimpleObject {
            pub fn new() -> Self {
                glib::Object::new()
            }

            #[doc(alias = "get_size")]
            pub fn size(&self) -> Option<i64> {
                *self.imp().size.borrow()
            }

            pub fn set_size(&self, size: i64) {
                self.imp().size.borrow_mut().replace(size);
            }
        }

        impl Default for MySimpleObject {
            fn default() -> Self {
                Self::new()
            }
        }

        let fut = run_async_local(|tx, l| {
            let cancellable = crate::Cancellable::new();
            let task = unsafe {
                crate::LocalTask::new(
                    None,
                    Some(&cancellable),
                    move |t: LocalTask<glib::Object>, _b: Option<&glib::Object>| {
                        tx.send(t.propagate()).unwrap();
                        l.quit();
                    },
                )
            };
            let my_object = MySimpleObject::new();
            my_object.set_size(100);
            task.return_result(Ok(my_object.upcast::<glib::Object>()));
        });

        match fut {
            Err(_) => panic!(),
            Ok(o) => {
                let o = o.downcast::<MySimpleObject>().unwrap();
                assert_eq!(o.size(), Some(100));
            }
        }
    }

    #[test]
    fn test_error() {
        let fut = run_async_local(|tx, l| {
            let cancellable = crate::Cancellable::new();
            let task = unsafe {
                crate::LocalTask::new(
                    None,
                    Some(&cancellable),
                    move |t: LocalTask<i32>, _b: Option<&glib::Object>| {
                        tx.send(t.propagate()).unwrap();
                        l.quit();
                    },
                )
            };
            task.return_result(Err(glib::Error::new(
                crate::IOErrorEnum::WouldBlock,
                "WouldBlock",
            )));
        });

        match fut {
            Err(e) => match e.kind().unwrap() {
                crate::IOErrorEnum::WouldBlock => {}
                _ => panic!(),
            },
            Ok(_) => panic!(),
        }
    }

    #[test]
    fn test_cancelled() {
        let fut = run_async_local(|tx, l| {
            let cancellable = crate::Cancellable::new();
            let task = unsafe {
                crate::LocalTask::new(
                    None,
                    Some(&cancellable),
                    move |t: LocalTask<i32>, _b: Option<&glib::Object>| {
                        tx.send(t.propagate()).unwrap();
                        l.quit();
                    },
                )
            };
            cancellable.cancel();
            task.return_error_if_cancelled();
        });

        match fut {
            Err(e) => match e.kind().unwrap() {
                crate::IOErrorEnum::Cancelled => {}
                _ => panic!(),
            },
            Ok(_) => panic!(),
        }
    }
}
