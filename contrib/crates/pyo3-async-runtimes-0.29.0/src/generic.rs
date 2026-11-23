//! Generic implementations of PyO3 Asyncio utilities that can be used for any Rust runtime
//!
//! Items marked with
//! <span
//!   class="module-item stab portability"
//!   style="display: inline; border-radius: 3px; padding: 2px; font-size: 80%; line-height: 1.2;"
//! ><code>unstable-streams</code></span>
//! > are only available when the `unstable-streams` Cargo feature is enabled:
//!
//! ```toml
//! [dependencies.pyo3-async-runtimes]
//! version = "0.24"
//! features = ["unstable-streams"]
//! ```

use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

use crate::{
    asyncio, call_soon_threadsafe, close, create_future, dump_err, err::RustPanic,
    get_running_loop, into_future_with_locals, TaskLocals,
};
#[cfg(feature = "unstable-streams")]
use futures_channel::mpsc;
use futures_channel::oneshot;
#[cfg(feature = "unstable-streams")]
use futures_util::SinkExt;
use pin_project_lite::pin_project;
use pyo3::prelude::*;
use pyo3::IntoPyObjectExt;
#[cfg(feature = "unstable-streams")]
use std::marker::PhantomData;

/// Generic utilities for a JoinError
pub trait JoinError {
    /// Check if the spawned task exited because of a panic
    fn is_panic(&self) -> bool;
    /// Get the panic object associated with the error.  Panics if `is_panic` is not true.
    fn into_panic(self) -> Box<dyn std::any::Any + Send + 'static>;
}

/// Generic Rust async/await runtime
pub trait Runtime: Send + 'static {
    /// The error returned by a JoinHandle after being awaited
    type JoinError: JoinError + Send;
    /// A future that completes with the result of the spawned task
    type JoinHandle: Future<Output = Result<(), Self::JoinError>> + Send;

    /// Spawn a future onto this runtime's event loop
    fn spawn<F>(fut: F) -> Self::JoinHandle
    where
        F: Future<Output = ()> + Send + 'static;

    /// Spawn a function onto this runtime's blocking event loop
    fn spawn_blocking<F>(f: F) -> Self::JoinHandle
    where
        F: FnOnce() + Send + 'static;
}

/// Extension trait for async/await runtimes that support spawning local tasks
pub trait SpawnLocalExt: Runtime {
    /// Spawn a !Send future onto this runtime's event loop
    fn spawn_local<F>(fut: F) -> Self::JoinHandle
    where
        F: Future<Output = ()> + 'static;
}

/// Exposes the utilities necessary for using task-local data in the Runtime
pub trait ContextExt: Runtime {
    /// Set the task locals for the given future
    fn scope<F, R>(locals: TaskLocals, fut: F) -> Pin<Box<dyn Future<Output = R> + Send>>
    where
        F: Future<Output = R> + Send + 'static;

    /// Get the task locals for the current task
    fn get_task_locals() -> Option<TaskLocals>;
}

/// Adds the ability to scope task-local data for !Send futures
pub trait LocalContextExt: Runtime {
    /// Set the task locals for the given !Send future
    fn scope_local<F, R>(locals: TaskLocals, fut: F) -> Pin<Box<dyn Future<Output = R>>>
    where
        F: Future<Output = R> + 'static;
}

/// Get the current event loop from either Python or Rust async task local context
///
/// This function first checks if the runtime has a task-local reference to the Python event loop.
/// If not, it calls [`get_running_loop`](crate::get_running_loop`) to get the event loop associated
/// with the current OS thread.
pub fn get_current_loop<R>(py: Python) -> PyResult<Bound<PyAny>>
where
    R: ContextExt,
{
    if let Some(locals) = R::get_task_locals() {
        Ok(locals.0.event_loop.clone_ref(py).into_bound(py))
    } else {
        get_running_loop(py)
    }
}

/// Either copy the task locals from the current task OR get the current running loop and
/// contextvars from Python.
pub fn get_current_locals<R>(py: Python) -> PyResult<TaskLocals>
where
    R: ContextExt,
{
    if let Some(locals) = R::get_task_locals() {
        Ok(locals)
    } else {
        Ok(TaskLocals::with_running_loop(py)?.copy_context(py)?)
    }
}

/// Run the event loop until the given Future completes
///
/// After this function returns, the event loop can be resumed with [`run_until_complete`]
///
/// # Arguments
/// * `event_loop` - The Python event loop that should run the future
/// * `fut` - The future to drive to completion
///
/// # Examples
///
/// ```no_run
/// # use std::{any::Any, task::{Context, Poll}, pin::Pin, future::Future};
/// #
/// # use pyo3_async_runtimes::{
/// #     TaskLocals,
/// #     generic::{JoinError, SpawnLocalExt, ContextExt, LocalContextExt, Runtime}
/// # };
/// #
/// # struct MyCustomJoinError;
/// #
/// # impl JoinError for MyCustomJoinError {
/// #     fn is_panic(&self) -> bool {
/// #         unreachable!()
/// #     }
/// #     fn into_panic(self) -> Box<(dyn Any + Send + 'static)> {
/// #         unreachable!()
/// #     }
/// # }
/// #
/// # struct MyCustomJoinHandle;
/// #
/// # impl Future for MyCustomJoinHandle {
/// #     type Output = Result<(), MyCustomJoinError>;
/// #
/// #     fn poll(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Self::Output> {
/// #         unreachable!()
/// #     }
/// # }
/// #
/// # struct MyCustomRuntime;
/// #
/// # impl Runtime for MyCustomRuntime {
/// #     type JoinError = MyCustomJoinError;
/// #     type JoinHandle = MyCustomJoinHandle;
/// #
/// #     fn spawn<F>(fut: F) -> Self::JoinHandle
/// #     where
/// #         F: Future<Output = ()> + Send + 'static
/// #     {
/// #         unreachable!()
/// #     }
/// #
/// #     fn spawn_blocking<F>(f: F) -> Self::JoinHandle where F: FnOnce() + Send + 'static {
/// #         unreachable!()
/// #     }
/// # }
/// #
/// # impl ContextExt for MyCustomRuntime {
/// #     fn scope<F, R>(locals: TaskLocals, fut: F) -> Pin<Box<dyn Future<Output = R> + Send>>
/// #     where
/// #         F: Future<Output = R> + Send + 'static
/// #     {
/// #         unreachable!()
/// #     }
/// #     fn get_task_locals() -> Option<TaskLocals> {
/// #         unreachable!()
/// #     }
/// # }
/// #
/// # use std::time::Duration;
/// #
/// # use pyo3::prelude::*;
/// #
/// # Python::attach(|py| -> PyResult<()> {
/// # let event_loop = py.import("asyncio")?.call_method0("new_event_loop")?;
/// # #[cfg(feature = "tokio-runtime")]
/// pyo3_async_runtimes::generic::run_until_complete::<MyCustomRuntime, _, _>(&event_loop, async move {
///     tokio::time::sleep(Duration::from_secs(1)).await;
///     Ok(())
/// })?;
/// # Ok(())
/// # }).unwrap();
/// ```
pub fn run_until_complete<R, F, T>(event_loop: &Bound<PyAny>, fut: F) -> PyResult<T>
where
    R: Runtime + ContextExt,
    F: Future<Output = PyResult<T>> + Send + 'static,
    T: Send + Sync + 'static,
{
    let py = event_loop.py();
    let result_tx = Arc::new(Mutex::new(None));
    let result_rx = Arc::clone(&result_tx);
    let coro = future_into_py_with_locals::<R, _, ()>(
        py,
        TaskLocals::new(event_loop.clone()).copy_context(py)?,
        async move {
            let val = fut.await?;
            if let Ok(mut result) = result_tx.lock() {
                *result = Some(val);
            }
            Ok(())
        },
    )?;

    event_loop.call_method1(pyo3::intern!(py, "run_until_complete"), (coro,))?;

    let result = result_rx.lock().unwrap().take().unwrap();
    Ok(result)
}

/// Run the event loop until the given Future completes
///
/// # Arguments
/// * `py` - The current PyO3 GIL guard
/// * `fut` - The future to drive to completion
///
/// # Examples
///
/// ```no_run
/// # use std::{any::Any, task::{Context, Poll}, pin::Pin, future::Future};
/// #
/// # use pyo3_async_runtimes::{
/// #     TaskLocals,
/// #     generic::{JoinError, SpawnLocalExt, ContextExt, LocalContextExt, Runtime}
/// # };
/// #
/// # struct MyCustomJoinError;
/// #
/// # impl JoinError for MyCustomJoinError {
/// #     fn is_panic(&self) -> bool {
/// #         unreachable!()
/// #     }
/// #     fn into_panic(self) -> Box<(dyn Any + Send + 'static)> {
/// #         unreachable!()
/// #     }
/// # }
/// #
/// # struct MyCustomJoinHandle;
/// #
/// # impl Future for MyCustomJoinHandle {
/// #     type Output = Result<(), MyCustomJoinError>;
/// #
/// #     fn poll(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Self::Output> {
/// #         unreachable!()
/// #     }
/// # }
/// #
/// # struct MyCustomRuntime;
/// #
/// # impl Runtime for MyCustomRuntime {
/// #     type JoinError = MyCustomJoinError;
/// #     type JoinHandle = MyCustomJoinHandle;
/// #
/// #     fn spawn<F>(fut: F) -> Self::JoinHandle
/// #     where
/// #         F: Future<Output = ()> + Send + 'static
/// #     {
/// #         unreachable!()
/// #     }
/// #
/// #     fn spawn_blocking<F>(f: F) -> Self::JoinHandle where F: FnOnce() + Send + 'static {
/// #         unreachable!()
/// #     }
/// # }
/// #
/// # impl ContextExt for MyCustomRuntime {
/// #     fn scope<F, R>(locals: TaskLocals, fut: F) -> Pin<Box<dyn Future<Output = R> + Send>>
/// #     where
/// #         F: Future<Output = R> + Send + 'static
/// #     {
/// #         unreachable!()
/// #     }
/// #     fn get_task_locals() -> Option<TaskLocals> {
/// #         unreachable!()
/// #     }
/// # }
/// #
/// # use std::time::Duration;
/// # async fn custom_sleep(_duration: Duration) { }
/// #
/// # use pyo3::prelude::*;
/// #
/// fn main() {
///     Python::attach(|py| {
///         pyo3_async_runtimes::generic::run::<MyCustomRuntime, _, _>(py, async move {
///             custom_sleep(Duration::from_secs(1)).await;
///             Ok(())
///         })
///         .map_err(|e| {
///             e.print_and_set_sys_last_vars(py);
///         })
///         .unwrap();
///     })
/// }
/// ```
pub fn run<R, F, T>(py: Python, fut: F) -> PyResult<T>
where
    R: Runtime + ContextExt,
    F: Future<Output = PyResult<T>> + Send + 'static,
    T: Send + Sync + 'static,
{
    let event_loop = asyncio(py)?.call_method0(pyo3::intern!(py, "new_event_loop"))?;

    let result = run_until_complete::<R, F, T>(&event_loop, fut);

    close(event_loop)?;

    result
}

fn cancelled(future: &Bound<PyAny>) -> PyResult<bool> {
    future
        .getattr(pyo3::intern!(future.py(), "cancelled"))?
        .call0()?
        .is_truthy()
}

#[pyclass]
struct CheckedCompletor;

#[pymethods]
impl CheckedCompletor {
    fn __call__(
        &self,
        future: &Bound<PyAny>,
        complete: &Bound<PyAny>,
        value: &Bound<PyAny>,
    ) -> PyResult<()> {
        if cancelled(future)? {
            return Ok(());
        }

        complete.call1((value,))?;

        Ok(())
    }
}

fn set_result(
    event_loop: &Bound<PyAny>,
    future: &Bound<PyAny>,
    result: PyResult<Py<PyAny>>,
) -> PyResult<()> {
    let py = event_loop.py();
    let none = py.None().into_bound(py);

    let (complete, val) = match result {
        Ok(val) => (
            future.getattr(pyo3::intern!(py, "set_result"))?,
            val.into_pyobject(py)?,
        ),
        Err(err) => (
            future.getattr(pyo3::intern!(py, "set_exception"))?,
            err.into_bound_py_any(py)?,
        ),
    };
    call_soon_threadsafe(event_loop, &none, (CheckedCompletor, future, complete, val))?;

    Ok(())
}

/// Convert a Python `awaitable` into a Rust Future
///
/// This function simply forwards the future and the task locals returned by [`get_current_locals`]
/// to [`into_future_with_locals`](`crate::into_future_with_locals`). See
/// [`into_future_with_locals`](`crate::into_future_with_locals`) for more details.
///
/// # Arguments
/// * `awaitable` - The Python `awaitable` to be converted
///
/// # Examples
///
/// ```no_run
/// # use std::{any::Any, pin::Pin, future::Future, task::{Context, Poll}, time::Duration};
/// # use std::ffi::CString;
/// #
/// # use pyo3::prelude::*;
/// #
/// # use pyo3_async_runtimes::{
/// #     TaskLocals,
/// #     generic::{JoinError, SpawnLocalExt, ContextExt, LocalContextExt, Runtime}
/// # };
/// #
/// # struct MyCustomJoinError;
/// #
/// # impl JoinError for MyCustomJoinError {
/// #     fn is_panic(&self) -> bool {
/// #         unreachable!()
/// #     }
/// #     fn into_panic(self) -> Box<(dyn Any + Send + 'static)> {
/// #         unreachable!()
/// #     }
/// # }
/// #
/// # struct MyCustomJoinHandle;
/// #
/// # impl Future for MyCustomJoinHandle {
/// #     type Output = Result<(), MyCustomJoinError>;
/// #
/// #     fn poll(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Self::Output> {
/// #         unreachable!()
/// #     }
/// # }
/// #
/// # struct MyCustomRuntime;
/// #
/// # impl MyCustomRuntime {
/// #     async fn sleep(_: Duration) {
/// #         unreachable!()
/// #     }
/// # }
/// #
/// # impl Runtime for MyCustomRuntime {
/// #     type JoinError = MyCustomJoinError;
/// #     type JoinHandle = MyCustomJoinHandle;
/// #
/// #     fn spawn<F>(fut: F) -> Self::JoinHandle
/// #     where
/// #         F: Future<Output = ()> + Send + 'static
/// #     {
/// #         unreachable!()
/// #     }
/// #
/// #     fn spawn_blocking<F>(f: F) -> Self::JoinHandle where F: FnOnce() + Send + 'static {
/// #         unreachable!()
/// #     }
/// # }
/// #
/// # impl ContextExt for MyCustomRuntime {
/// #     fn scope<F, R>(locals: TaskLocals, fut: F) -> Pin<Box<dyn Future<Output = R> + Send>>
/// #     where
/// #         F: Future<Output = R> + Send + 'static
/// #     {
/// #         unreachable!()
/// #     }
/// #     fn get_task_locals() -> Option<TaskLocals> {
/// #         unreachable!()
/// #     }
/// # }
/// #
/// const PYTHON_CODE: &'static str = r#"
/// import asyncio
///
/// async def py_sleep(duration):
///     await asyncio.sleep(duration)
/// "#;
///
/// async fn py_sleep(seconds: f32) -> PyResult<()> {
///     let test_mod = Python::attach(|py| -> PyResult<Py<PyAny>> {
///         Ok(
///             PyModule::from_code(
///                 py,
///                 &CString::new(PYTHON_CODE).unwrap(),
///                 &CString::new("test_into_future/test_mod.py").unwrap(),
///                 &CString::new("test_mod").unwrap(),
///             )?
///             .into()
///         )
///     })?;
///
///     Python::attach(|py| {
///         pyo3_async_runtimes::generic::into_future::<MyCustomRuntime>(
///             test_mod
///                 .call_method1(py, "py_sleep", (seconds,))?
///                 .into_bound(py),
///         )
///     })?
///     .await?;
///     Ok(())
/// }
/// ```
pub fn into_future<R>(
    awaitable: Bound<PyAny>,
) -> PyResult<impl Future<Output = PyResult<Py<PyAny>>> + Send>
where
    R: Runtime + ContextExt,
{
    into_future_with_locals(&get_current_locals::<R>(awaitable.py())?, awaitable)
}

/// Convert a Rust Future into a Python awaitable with a generic runtime
///
/// If the `asyncio.Future` returned by this conversion is cancelled via `asyncio.Future.cancel`,
/// the Rust future will be cancelled as well (new behaviour in `v0.15`).
///
/// Python `contextvars` are preserved when calling async Python functions within the Rust future
/// via [`into_future`] (new behaviour in `v0.15`).
///
/// > Although `contextvars` are preserved for async Python functions, synchronous functions will
/// > unfortunately fail to resolve them when called within the Rust future. This is because the
/// > function is being called from a Rust thread, not inside an actual Python coroutine context.
/// >
/// > As a workaround, you can get the `contextvars` from the current task locals using
/// > [`get_current_locals`] and [`TaskLocals::context`](`crate::TaskLocals::context`), then wrap your
/// > synchronous function in a call to `contextvars.Context.run`. This will set the context, call the
/// > synchronous function, and restore the previous context when it returns or raises an exception.
///
/// # Arguments
/// * `py` - PyO3 GIL guard
/// * `locals` - The task-local data for Python
/// * `fut` - The Rust future to be converted
///
/// # Examples
///
/// ```no_run
/// # use std::{any::Any, task::{Context, Poll}, pin::Pin, future::Future};
/// #
/// # use pyo3_async_runtimes::{
/// #     TaskLocals,
/// #     generic::{JoinError, SpawnLocalExt, ContextExt, LocalContextExt, Runtime}
/// # };
/// #
/// # struct MyCustomJoinError;
/// #
/// # impl JoinError for MyCustomJoinError {
/// #     fn is_panic(&self) -> bool {
/// #         unreachable!()
/// #     }
/// #     fn into_panic(self) -> Box<(dyn Any + Send + 'static)> {
/// #         unreachable!()
/// #     }
/// # }
/// #
/// # struct MyCustomJoinHandle;
/// #
/// # impl Future for MyCustomJoinHandle {
/// #     type Output = Result<(), MyCustomJoinError>;
/// #
/// #     fn poll(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Self::Output> {
/// #         unreachable!()
/// #     }
/// # }
/// #
/// # struct MyCustomRuntime;
/// #
/// # impl MyCustomRuntime {
/// #     async fn sleep(_: Duration) {
/// #         unreachable!()
/// #     }
/// # }
/// #
/// # impl Runtime for MyCustomRuntime {
/// #     type JoinError = MyCustomJoinError;
/// #     type JoinHandle = MyCustomJoinHandle;
/// #
/// #     fn spawn<F>(fut: F) -> Self::JoinHandle
/// #     where
/// #         F: Future<Output = ()> + Send + 'static
/// #     {
/// #         unreachable!()
/// #     }
/// #
/// #     fn spawn_blocking<F>(f: F) -> Self::JoinHandle where F: FnOnce() + Send + 'static {
/// #         unreachable!()
/// #     }
/// # }
/// #
/// # impl ContextExt for MyCustomRuntime {
/// #     fn scope<F, R>(locals: TaskLocals, fut: F) -> Pin<Box<dyn Future<Output = R> + Send>>
/// #     where
/// #         F: Future<Output = R> + Send + 'static
/// #     {
/// #         unreachable!()
/// #     }
/// #     fn get_task_locals() -> Option<TaskLocals> {
/// #         unreachable!()
/// #     }
/// # }
/// #
/// use std::time::Duration;
///
/// use pyo3::prelude::*;
///
/// /// Awaitable sleep function
/// #[pyfunction]
/// fn sleep_for<'p>(py: Python<'p>, secs: Bound<'p, PyAny>) -> PyResult<Bound<'p, PyAny>> {
///     let secs = secs.extract()?;
///     pyo3_async_runtimes::generic::future_into_py_with_locals::<MyCustomRuntime, _, _>(
///         py,
///         pyo3_async_runtimes::generic::get_current_locals::<MyCustomRuntime>(py)?,
///         async move {
///             MyCustomRuntime::sleep(Duration::from_secs(secs)).await;
///             Ok(())
///         }
///     )
/// }
/// ```
#[allow(unused_must_use)]
pub fn future_into_py_with_locals<R, F, T>(
    py: Python,
    locals: TaskLocals,
    fut: F,
) -> PyResult<Bound<PyAny>>
where
    R: Runtime + ContextExt,
    F: Future<Output = PyResult<T>> + Send + 'static,
    T: for<'py> IntoPyObject<'py> + Send + 'static,
{
    let (cancel_tx, cancel_rx) = oneshot::channel();

    let py_fut = create_future(locals.0.event_loop.bind(py).clone())?;
    py_fut.call_method1(
        pyo3::intern!(py, "add_done_callback"),
        (PyDoneCallback {
            cancel_tx: Some(cancel_tx),
        },),
    )?;

    let future_tx1: Py<PyAny> = py_fut.clone().into();
    let future_tx2 = future_tx1.clone_ref(py);

    R::spawn(async move {
        let locals2 = locals.clone();

        if let Err(e) = R::spawn(async move {
            let result = R::scope(
                locals2.clone(),
                Cancellable::new_with_cancel_rx(fut, cancel_rx),
            )
            .await;

            // We should not hold GIL inside async-std/tokio event loop,
            // because a blocked task may prevent other tasks from progressing.
            R::spawn_blocking(|| {
                Python::attach(move |py| {
                    if cancelled(future_tx1.bind(py))
                        .map_err(dump_err(py))
                        .unwrap_or(false)
                    {
                        return;
                    }

                    let _ = set_result(
                        &locals2.event_loop(py),
                        future_tx1.bind(py),
                        result.and_then(|val| val.into_py_any(py)),
                    )
                    .map_err(dump_err(py));
                });
            });
        })
        .await
        {
            if e.is_panic() {
                R::spawn_blocking(|| {
                    Python::attach(move |py| {
                        if cancelled(future_tx2.bind(py))
                            .map_err(dump_err(py))
                            .unwrap_or(false)
                        {
                            return;
                        }

                        let panic_message = format!(
                            "rust future panicked: {}",
                            get_panic_message(&e.into_panic())
                        );
                        let _ = set_result(
                            locals.0.event_loop.bind(py),
                            future_tx2.bind(py),
                            Err(RustPanic::new_err(panic_message)),
                        )
                        .map_err(dump_err(py));
                    });
                });
            }
        }
    });

    Ok(py_fut)
}

fn get_panic_message(any: &dyn std::any::Any) -> &str {
    if let Some(str_slice) = any.downcast_ref::<&str>() {
        str_slice
    } else if let Some(string) = any.downcast_ref::<String>() {
        string.as_str()
    } else {
        "unknown error"
    }
}

pin_project! {
    /// Future returned by [`timeout`](timeout) and [`timeout_at`](timeout_at).
    #[must_use = "futures do nothing unless you `.await` or poll them"]
    #[derive(Debug)]
    struct Cancellable<T> {
        #[pin]
        future: T,
        #[pin]
        cancel_rx: oneshot::Receiver<()>,

        poll_cancel_rx: bool
    }
}

impl<T> Cancellable<T> {
    fn new_with_cancel_rx(future: T, cancel_rx: oneshot::Receiver<()>) -> Self {
        Self {
            future,
            cancel_rx,

            poll_cancel_rx: true,
        }
    }
}

impl<'py, F, T> Future for Cancellable<F>
where
    F: Future<Output = PyResult<T>>,
    T: IntoPyObject<'py>,
{
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();

        // First, try polling the future
        if let Poll::Ready(v) = this.future.poll(cx) {
            return Poll::Ready(v);
        }

        // Now check for cancellation
        if *this.poll_cancel_rx {
            match this.cancel_rx.poll(cx) {
                Poll::Ready(Ok(())) => {
                    *this.poll_cancel_rx = false;
                    // The python future has already been cancelled, so this return value will never
                    // be used.
                    Poll::Ready(Err(pyo3::exceptions::PyBaseException::new_err(
                        "unreachable",
                    )))
                }
                Poll::Ready(Err(_)) => {
                    *this.poll_cancel_rx = false;
                    Poll::Pending
                }
                Poll::Pending => Poll::Pending,
            }
        } else {
            Poll::Pending
        }
    }
}

#[pyclass]
struct PyDoneCallback {
    cancel_tx: Option<oneshot::Sender<()>>,
}

#[pymethods]
impl PyDoneCallback {
    pub fn __call__(&mut self, fut: &Bound<PyAny>) -> PyResult<()> {
        let py = fut.py();

        if cancelled(fut).map_err(dump_err(py)).unwrap_or(false) {
            let _ = self.cancel_tx.take().unwrap().send(());
        }

        Ok(())
    }
}

/// Convert a Rust Future into a Python awaitable with a generic runtime
///
/// If the `asyncio.Future` returned by this conversion is cancelled via `asyncio.Future.cancel`,
/// the Rust future will be cancelled as well (new behaviour in `v0.15`).
///
/// Python `contextvars` are preserved when calling async Python functions within the Rust future
/// via [`into_future`] (new behaviour in `v0.15`).
///
/// > Although `contextvars` are preserved for async Python functions, synchronous functions will
/// > unfortunately fail to resolve them when called within the Rust future. This is because the
/// > function is being called from a Rust thread, not inside an actual Python coroutine context.
/// >
/// > As a workaround, you can get the `contextvars` from the current task locals using
/// > [`get_current_locals`] and [`TaskLocals::context`](`crate::TaskLocals::context`), then wrap your
/// > synchronous function in a call to `contextvars.Context.run`. This will set the context, call the
/// > synchronous function, and restore the previous context when it returns or raises an exception.
///
/// # Arguments
/// * `py` - The current PyO3 GIL guard
/// * `fut` - The Rust future to be converted
///
/// # Examples
///
/// ```no_run
/// # use std::{any::Any, task::{Context, Poll}, pin::Pin, future::Future};
/// #
/// # use pyo3_async_runtimes::{
/// #     TaskLocals,
/// #     generic::{JoinError, SpawnLocalExt, ContextExt, LocalContextExt, Runtime}
/// # };
/// #
/// # struct MyCustomJoinError;
/// #
/// # impl JoinError for MyCustomJoinError {
/// #     fn is_panic(&self) -> bool {
/// #         unreachable!()
/// #     }
/// #     fn into_panic(self) -> Box<(dyn Any + Send + 'static)> {
/// #         unreachable!()
/// #     }
/// # }
/// #
/// # struct MyCustomJoinHandle;
/// #
/// # impl Future for MyCustomJoinHandle {
/// #     type Output = Result<(), MyCustomJoinError>;
/// #
/// #     fn poll(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Self::Output> {
/// #         unreachable!()
/// #     }
/// # }
/// #
/// # struct MyCustomRuntime;
/// #
/// # impl MyCustomRuntime {
/// #     async fn sleep(_: Duration) {
/// #         unreachable!()
/// #     }
/// # }
/// #
/// # impl Runtime for MyCustomRuntime {
/// #     type JoinError = MyCustomJoinError;
/// #     type JoinHandle = MyCustomJoinHandle;
/// #
/// #     fn spawn<F>(fut: F) -> Self::JoinHandle
/// #     where
/// #         F: Future<Output = ()> + Send + 'static
/// #     {
/// #         unreachable!()
/// #     }
/// #
/// #     fn spawn_blocking<F>(f: F) -> Self::JoinHandle where F: FnOnce() + Send + 'static {
/// #         unreachable!()
/// #     }
/// # }
/// #
/// # impl ContextExt for MyCustomRuntime {
/// #     fn scope<F, R>(locals: TaskLocals, fut: F) -> Pin<Box<dyn Future<Output = R> + Send>>
/// #     where
/// #         F: Future<Output = R> + Send + 'static
/// #     {
/// #         unreachable!()
/// #     }
/// #     fn get_task_locals() -> Option<TaskLocals> {
/// #         unreachable!()
/// #     }
/// # }
/// #
/// use std::time::Duration;
///
/// use pyo3::prelude::*;
///
/// /// Awaitable sleep function
/// #[pyfunction]
/// fn sleep_for<'p>(py: Python<'p>, secs: Bound<'p, PyAny>) -> PyResult<Bound<'p, PyAny>> {
///     let secs = secs.extract()?;
///     pyo3_async_runtimes::generic::future_into_py::<MyCustomRuntime, _, _>(py, async move {
///         MyCustomRuntime::sleep(Duration::from_secs(secs)).await;
///         Ok(())
///     })
/// }
/// ```
pub fn future_into_py<R, F, T>(py: Python, fut: F) -> PyResult<Bound<PyAny>>
where
    R: Runtime + ContextExt,
    F: Future<Output = PyResult<T>> + Send + 'static,
    T: for<'py> IntoPyObject<'py> + Send + 'static,
{
    future_into_py_with_locals::<R, F, T>(py, get_current_locals::<R>(py)?, fut)
}

/// Convert a `!Send` Rust Future into a Python awaitable with a generic runtime and manual
/// specification of task locals.
///
/// If the `asyncio.Future` returned by this conversion is cancelled via `asyncio.Future.cancel`,
/// the Rust future will be cancelled as well (new behaviour in `v0.15`).
///
/// Python `contextvars` are preserved when calling async Python functions within the Rust future
/// via [`into_future`] (new behaviour in `v0.15`).
///
/// > Although `contextvars` are preserved for async Python functions, synchronous functions will
/// > unfortunately fail to resolve them when called within the Rust future. This is because the
/// > function is being called from a Rust thread, not inside an actual Python coroutine context.
/// >
/// > As a workaround, you can get the `contextvars` from the current task locals using
/// > [`get_current_locals`] and [`TaskLocals::context`](`crate::TaskLocals::context`), then wrap your
/// > synchronous function in a call to `contextvars.Context.run`. This will set the context, call the
/// > synchronous function, and restore the previous context when it returns or raises an exception.
///
/// # Arguments
/// * `py` - PyO3 GIL guard
/// * `locals` - The task locals for the future
/// * `fut` - The Rust future to be converted
///
/// # Examples
///
/// ```no_run
/// # use std::{any::Any, task::{Context, Poll}, pin::Pin, future::Future};
/// #
/// # use pyo3_async_runtimes::{
/// #     TaskLocals,
/// #     generic::{JoinError, SpawnLocalExt, ContextExt, LocalContextExt, Runtime}
/// # };
/// #
/// # struct MyCustomJoinError;
/// #
/// # impl JoinError for MyCustomJoinError {
/// #     fn is_panic(&self) -> bool {
/// #         unreachable!()
/// #     }
/// #     fn into_panic(self) -> Box<(dyn Any + Send + 'static)> {
/// #         unreachable!()
/// #     }
/// # }
/// #
/// # struct MyCustomJoinHandle;
/// #
/// # impl Future for MyCustomJoinHandle {
/// #     type Output = Result<(), MyCustomJoinError>;
/// #
/// #     fn poll(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Self::Output> {
/// #         unreachable!()
/// #     }
/// # }
/// #
/// # struct MyCustomRuntime;
/// #
/// # impl MyCustomRuntime {
/// #     async fn sleep(_: Duration) {
/// #         unreachable!()
/// #     }
/// # }
/// #
/// # impl Runtime for MyCustomRuntime {
/// #     type JoinError = MyCustomJoinError;
/// #     type JoinHandle = MyCustomJoinHandle;
/// #
/// #     fn spawn<F>(fut: F) -> Self::JoinHandle
/// #     where
/// #         F: Future<Output = ()> + Send + 'static
/// #     {
/// #         unreachable!()
/// #     }
/// #
/// #     fn spawn_blocking<F>(f: F) -> Self::JoinHandle where F: FnOnce() + Send + 'static {
/// #         unreachable!()
/// #     }
/// # }
/// #
/// # impl ContextExt for MyCustomRuntime {
/// #     fn scope<F, R>(locals: TaskLocals, fut: F) -> Pin<Box<dyn Future<Output = R> + Send>>
/// #     where
/// #         F: Future<Output = R> + Send + 'static
/// #     {
/// #         unreachable!()
/// #     }
/// #     fn get_task_locals() -> Option<TaskLocals> {
/// #         unreachable!()
/// #     }
/// # }
/// #
/// # impl SpawnLocalExt for MyCustomRuntime {
/// #     fn spawn_local<F>(fut: F) -> Self::JoinHandle
/// #     where
/// #         F: Future<Output = ()> + 'static
/// #     {
/// #         unreachable!()
/// #     }
/// # }
/// #
/// # impl LocalContextExt for MyCustomRuntime {
/// #     fn scope_local<F, R>(locals: TaskLocals, fut: F) -> Pin<Box<dyn Future<Output = R>>>
/// #     where
/// #         F: Future<Output = R> + 'static
/// #     {
/// #         unreachable!()
/// #     }
/// # }
/// #
/// use std::{rc::Rc, time::Duration};
///
/// use pyo3::prelude::*;
///
/// /// Awaitable sleep function
/// #[pyfunction]
/// fn sleep_for(py: Python, secs: u64) -> PyResult<Bound<PyAny>> {
///     // Rc is !Send so it cannot be passed into pyo3_async_runtimes::generic::future_into_py
///     let secs = Rc::new(secs);
///
///     pyo3_async_runtimes::generic::local_future_into_py_with_locals::<MyCustomRuntime, _, _>(
///         py,
///         pyo3_async_runtimes::generic::get_current_locals::<MyCustomRuntime>(py)?,
///         async move {
///             MyCustomRuntime::sleep(Duration::from_secs(*secs)).await;
///             Ok(())
///         }
///     )
/// }
/// ```
#[deprecated(
    since = "0.18.0",
    note = "Questionable whether these conversions have real-world utility (see https://github.com/awestlake87/pyo3-asyncio/issues/59#issuecomment-1008038497 and let me know if you disagree!)"
)]
#[allow(unused_must_use)]
pub fn local_future_into_py_with_locals<R, F, T>(
    py: Python,
    locals: TaskLocals,
    fut: F,
) -> PyResult<Bound<PyAny>>
where
    R: Runtime + SpawnLocalExt + LocalContextExt,
    F: Future<Output = PyResult<T>> + 'static,
    T: for<'py> IntoPyObject<'py>,
{
    let (cancel_tx, cancel_rx) = oneshot::channel();

    let py_fut = create_future(locals.0.event_loop.clone_ref(py).into_bound(py))?;
    py_fut.call_method1(
        pyo3::intern!(py, "add_done_callback"),
        (PyDoneCallback {
            cancel_tx: Some(cancel_tx),
        },),
    )?;

    let future_tx1: Py<PyAny> = py_fut.clone().into();
    let future_tx2 = future_tx1.clone_ref(py);

    R::spawn_local(async move {
        let locals2 = locals.clone();

        if let Err(e) = R::spawn_local(async move {
            let result = R::scope_local(
                locals2.clone(),
                Cancellable::new_with_cancel_rx(fut, cancel_rx),
            )
            .await;

            Python::attach(move |py| {
                if cancelled(future_tx1.bind(py))
                    .map_err(dump_err(py))
                    .unwrap_or(false)
                {
                    return;
                }

                let _ = set_result(
                    locals2.0.event_loop.bind(py),
                    future_tx1.bind(py),
                    result.and_then(|val| val.into_py_any(py)),
                )
                .map_err(dump_err(py));
            });
        })
        .await
        {
            if e.is_panic() {
                Python::attach(move |py| {
                    if cancelled(future_tx2.bind(py))
                        .map_err(dump_err(py))
                        .unwrap_or(false)
                    {
                        return;
                    }

                    let panic_message = format!(
                        "rust future panicked: {}",
                        get_panic_message(&e.into_panic())
                    );
                    let _ = set_result(
                        locals.0.event_loop.bind(py),
                        future_tx2.bind(py),
                        Err(RustPanic::new_err(panic_message)),
                    )
                    .map_err(dump_err(py));
                });
            }
        }
    });

    Ok(py_fut)
}

/// Convert a `!Send` Rust Future into a Python awaitable with a generic runtime
///
/// If the `asyncio.Future` returned by this conversion is cancelled via `asyncio.Future.cancel`,
/// the Rust future will be cancelled as well (new behaviour in `v0.15`).
///
/// Python `contextvars` are preserved when calling async Python functions within the Rust future
/// via [`into_future`] (new behaviour in `v0.15`).
///
/// > Although `contextvars` are preserved for async Python functions, synchronous functions will
/// > unfortunately fail to resolve them when called within the Rust future. This is because the
/// > function is being called from a Rust thread, not inside an actual Python coroutine context.
/// >
/// > As a workaround, you can get the `contextvars` from the current task locals using
/// > [`get_current_locals`] and [`TaskLocals::context`](`crate::TaskLocals::context`), then wrap your
/// > synchronous function in a call to `contextvars.Context.run`. This will set the context, call the
/// > synchronous function, and restore the previous context when it returns or raises an exception.
///
/// # Arguments
/// * `py` - The current PyO3 GIL guard
/// * `fut` - The Rust future to be converted
///
/// # Examples
///
/// ```no_run
/// # use std::{any::Any, task::{Context, Poll}, pin::Pin, future::Future};
/// #
/// # use pyo3_async_runtimes::{
/// #     TaskLocals,
/// #     generic::{JoinError, SpawnLocalExt, ContextExt, LocalContextExt, Runtime}
/// # };
/// #
/// # struct MyCustomJoinError;
/// #
/// # impl JoinError for MyCustomJoinError {
/// #     fn is_panic(&self) -> bool {
/// #         unreachable!()
/// #     }
/// #     fn into_panic(self) -> Box<(dyn Any + Send + 'static)> {
/// #         unreachable!()
/// #     }
/// # }
/// #
/// # struct MyCustomJoinHandle;
/// #
/// # impl Future for MyCustomJoinHandle {
/// #     type Output = Result<(), MyCustomJoinError>;
/// #
/// #     fn poll(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Self::Output> {
/// #         unreachable!()
/// #     }
/// # }
/// #
/// # struct MyCustomRuntime;
/// #
/// # impl MyCustomRuntime {
/// #     async fn sleep(_: Duration) {
/// #         unreachable!()
/// #     }
/// # }
/// #
/// # impl Runtime for MyCustomRuntime {
/// #     type JoinError = MyCustomJoinError;
/// #     type JoinHandle = MyCustomJoinHandle;
/// #
/// #     fn spawn<F>(fut: F) -> Self::JoinHandle
/// #     where
/// #         F: Future<Output = ()> + Send + 'static
/// #     {
/// #         unreachable!()
/// #     }
/// #
/// #     fn spawn_blocking<F>(f: F) -> Self::JoinHandle where F: FnOnce() + Send + 'static {
/// #         unreachable!()
/// #     }
/// # }
/// #
/// # impl ContextExt for MyCustomRuntime {
/// #     fn scope<F, R>(locals: TaskLocals, fut: F) -> Pin<Box<dyn Future<Output = R> + Send>>
/// #     where
/// #         F: Future<Output = R> + Send + 'static
/// #     {
/// #         unreachable!()
/// #     }
/// #     fn get_task_locals() -> Option<TaskLocals> {
/// #         unreachable!()
/// #     }
/// # }
/// #
/// # impl SpawnLocalExt for MyCustomRuntime {
/// #     fn spawn_local<F>(fut: F) -> Self::JoinHandle
/// #     where
/// #         F: Future<Output = ()> + 'static
/// #     {
/// #         unreachable!()
/// #     }
/// # }
/// #
/// # impl LocalContextExt for MyCustomRuntime {
/// #     fn scope_local<F, R>(locals: TaskLocals, fut: F) -> Pin<Box<dyn Future<Output = R>>>
/// #     where
/// #         F: Future<Output = R> + 'static
/// #     {
/// #         unreachable!()
/// #     }
/// # }
/// #
/// use std::{rc::Rc, time::Duration};
///
/// use pyo3::prelude::*;
///
/// /// Awaitable sleep function
/// #[pyfunction]
/// fn sleep_for(py: Python, secs: u64) -> PyResult<Bound<PyAny>> {
///     // Rc is !Send so it cannot be passed into pyo3_async_runtimes::generic::future_into_py
///     let secs = Rc::new(secs);
///
///     pyo3_async_runtimes::generic::local_future_into_py::<MyCustomRuntime, _, _>(py, async move {
///         MyCustomRuntime::sleep(Duration::from_secs(*secs)).await;
///         Ok(())
///     })
/// }
/// ```
#[deprecated(
    since = "0.18.0",
    note = "Questionable whether these conversions have real-world utility (see https://github.com/awestlake87/pyo3-asyncio/issues/59#issuecomment-1008038497 and let me know if you disagree!)"
)]
#[allow(deprecated)]
pub fn local_future_into_py<R, F, T>(py: Python, fut: F) -> PyResult<Bound<PyAny>>
where
    R: Runtime + ContextExt + SpawnLocalExt + LocalContextExt,
    F: Future<Output = PyResult<T>> + 'static,
    T: for<'py> IntoPyObject<'py>,
{
    local_future_into_py_with_locals::<R, F, T>(py, get_current_locals::<R>(py)?, fut)
}

/// <span class="module-item stab portability" style="display: inline; border-radius: 3px; padding: 2px; font-size: 80%; line-height: 1.2;"><code>unstable-streams</code></span> Convert an async generator into a stream
///
/// **This API is marked as unstable** and is only available when the
/// `unstable-streams` crate feature is enabled. This comes with no
/// stability guarantees, and could be changed or removed at any time.
///
/// # Arguments
/// * `locals` - The current task locals
/// * `gen` - The Python async generator to be converted
///
/// # Examples
/// ```no_run
/// # use std::{any::Any, task::{Context, Poll}, pin::Pin, future::Future};
/// #
/// # use pyo3_async_runtimes::{
/// #     TaskLocals,
/// #     generic::{JoinError, ContextExt, Runtime}
/// # };
/// #
/// # struct MyCustomJoinError;
/// #
/// # impl JoinError for MyCustomJoinError {
/// #     fn is_panic(&self) -> bool {
/// #         unreachable!()
/// #     }
/// #     fn into_panic(self) -> Box<(dyn Any + Send + 'static)> {
/// #         unreachable!()
/// #     }
/// # }
/// #
/// # struct MyCustomJoinHandle;
/// #
/// # impl Future for MyCustomJoinHandle {
/// #     type Output = Result<(), MyCustomJoinError>;
/// #
/// #     fn poll(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Self::Output> {
/// #         unreachable!()
/// #     }
/// # }
/// #
/// # struct MyCustomRuntime;
/// #
/// # impl Runtime for MyCustomRuntime {
/// #     type JoinError = MyCustomJoinError;
/// #     type JoinHandle = MyCustomJoinHandle;
/// #
/// #     fn spawn<F>(fut: F) -> Self::JoinHandle
/// #     where
/// #         F: Future<Output = ()> + Send + 'static
/// #     {
/// #         unreachable!()
/// #     }
/// #
/// #     fn spawn_blocking<F>(f: F) -> Self::JoinHandle where F: FnOnce() + Send + 'static {
/// #         unreachable!()
/// #     }
/// # }
/// #
/// # impl ContextExt for MyCustomRuntime {
/// #     fn scope<F, R>(locals: TaskLocals, fut: F) -> Pin<Box<dyn Future<Output = R> + Send>>
/// #     where
/// #         F: Future<Output = R> + Send + 'static
/// #     {
/// #         unreachable!()
/// #     }
/// #     fn get_task_locals() -> Option<TaskLocals> {
/// #         unreachable!()
/// #     }
/// # }
///
/// use pyo3::prelude::*;
/// use futures_util::stream::{StreamExt, TryStreamExt};
/// use std::ffi::CString;
///
/// const TEST_MOD: &str = r#"
/// import asyncio
///
/// async def gen():
///     for i in range(10):
///         await asyncio.sleep(0.1)
///         yield i
/// "#;
///
/// # async fn test_async_gen() -> PyResult<()> {
/// let stream = Python::attach(|py| {
///     let test_mod = PyModule::from_code(
///         py,
///         &CString::new(TEST_MOD).unwrap(),
///         &CString::new("test_rust_coroutine/test_mod.py").unwrap(),
///         &CString::new("test_mod").unwrap(),
///     )?;
///
///     pyo3_async_runtimes::generic::into_stream_with_locals_v1::<MyCustomRuntime>(
///         pyo3_async_runtimes::generic::get_current_locals::<MyCustomRuntime>(py)?,
///         test_mod.call_method0("gen")?
///     )
/// })?;
///
/// let vals = stream
///     .map(|item| Python::attach(|py| -> PyResult<i32> { Ok(item?.bind(py).extract()?) }))
///     .try_collect::<Vec<i32>>()
///     .await?;
///
/// assert_eq!((0..10).collect::<Vec<i32>>(), vals);
///
/// Ok(())
/// # }
/// ```
#[cfg(feature = "unstable-streams")]
#[allow(unused_must_use)] // False positive unused lint on `R::spawn`
pub fn into_stream_with_locals_v1<R>(
    locals: TaskLocals,
    gen: Bound<'_, PyAny>,
) -> PyResult<impl futures_util::stream::Stream<Item = PyResult<Py<PyAny>>> + 'static>
where
    R: Runtime,
{
    let (tx, rx) = async_channel::bounded(1);
    let py = gen.py();
    let anext: Py<PyAny> = gen.getattr(pyo3::intern!(py, "__anext__"))?.into();

    R::spawn(async move {
        loop {
            let fut = Python::attach(|py| -> PyResult<_> {
                into_future_with_locals(&locals, anext.bind(py).call0()?)
            });
            let item = match fut {
                Ok(fut) => match fut.await {
                    Ok(item) => Ok(item),
                    Err(e) => {
                        let stop_iter = Python::attach(|py| {
                            e.is_instance_of::<pyo3::exceptions::PyStopAsyncIteration>(py)
                        });

                        if stop_iter {
                            // end the iteration
                            break;
                        } else {
                            Err(e)
                        }
                    }
                },
                Err(e) => Err(e),
            };

            if tx.send(item).await.is_err() {
                // receiving side was dropped
                break;
            }
        }
    });

    Ok(rx)
}

/// <span class="module-item stab portability" style="display: inline; border-radius: 3px; padding: 2px; font-size: 80%; line-height: 1.2;"><code>unstable-streams</code></span> Convert an async generator into a stream
///
/// **This API is marked as unstable** and is only available when the
/// `unstable-streams` crate feature is enabled. This comes with no
/// stability guarantees, and could be changed or removed at any time.
///
/// # Arguments
/// * `gen` - The Python async generator to be converted
///
/// # Examples
/// ```no_run
/// # use std::{any::Any, task::{Context, Poll}, pin::Pin, future::Future};
/// #
/// # use pyo3_async_runtimes::{
/// #     TaskLocals,
/// #     generic::{JoinError, ContextExt, Runtime}
/// # };
/// #
/// # struct MyCustomJoinError;
/// #
/// # impl JoinError for MyCustomJoinError {
/// #     fn is_panic(&self) -> bool {
/// #         unreachable!()
/// #     }
/// #     fn into_panic(self) -> Box<(dyn Any + Send + 'static)> {
/// #         unreachable!()
/// #     }
/// # }
/// #
/// # struct MyCustomJoinHandle;
/// #
/// # impl Future for MyCustomJoinHandle {
/// #     type Output = Result<(), MyCustomJoinError>;
/// #
/// #     fn poll(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Self::Output> {
/// #         unreachable!()
/// #     }
/// # }
/// #
/// # struct MyCustomRuntime;
/// #
/// # impl Runtime for MyCustomRuntime {
/// #     type JoinError = MyCustomJoinError;
/// #     type JoinHandle = MyCustomJoinHandle;
/// #
/// #     fn spawn<F>(fut: F) -> Self::JoinHandle
/// #     where
/// #         F: Future<Output = ()> + Send + 'static
/// #     {
/// #         unreachable!()
/// #     }
/// #
/// #     fn spawn_blocking<F>(f: F) -> Self::JoinHandle where F: FnOnce() + Send + 'static {
/// #         unreachable!()
/// #     }
/// # }
/// #
/// # impl ContextExt for MyCustomRuntime {
/// #     fn scope<F, R>(locals: TaskLocals, fut: F) -> Pin<Box<dyn Future<Output = R> + Send>>
/// #     where
/// #         F: Future<Output = R> + Send + 'static
/// #     {
/// #         unreachable!()
/// #     }
/// #     fn get_task_locals() -> Option<TaskLocals> {
/// #         unreachable!()
/// #     }
/// # }
///
/// use pyo3::prelude::*;
/// use futures_util::stream::{StreamExt, TryStreamExt};
/// use std::ffi::CString;
///
/// const TEST_MOD: &str = r#"
/// import asyncio
///
/// async def gen():
///     for i in range(10):
///         await asyncio.sleep(0.1)
///         yield i
/// "#;
///
/// # async fn test_async_gen() -> PyResult<()> {
/// let stream = Python::attach(|py| {
///     let test_mod = PyModule::from_code(
///         py,
///         &CString::new(TEST_MOD).unwrap(),
///         &CString::new("test_rust_coroutine/test_mod.py").unwrap(),
///         &CString::new("test_mod").unwrap(),
///     )?;
///
///     pyo3_async_runtimes::generic::into_stream_v1::<MyCustomRuntime>(test_mod.call_method0("gen")?)
/// })?;
///
/// let vals = stream
///     .map(|item| Python::attach(|py| -> PyResult<i32> { Ok(item?.bind(py).extract()?) }))
///     .try_collect::<Vec<i32>>()
///     .await?;
///
/// assert_eq!((0..10).collect::<Vec<i32>>(), vals);
///
/// Ok(())
/// # }
/// ```
#[cfg(feature = "unstable-streams")]
pub fn into_stream_v1<R>(
    gen: Bound<'_, PyAny>,
) -> PyResult<impl futures_util::stream::Stream<Item = PyResult<Py<PyAny>>> + 'static>
where
    R: Runtime + ContextExt,
{
    into_stream_with_locals_v1::<R>(get_current_locals::<R>(gen.py())?, gen)
}

trait Sender: Send + 'static {
    fn send(&mut self, py: Python, locals: TaskLocals, item: Py<PyAny>) -> PyResult<Py<PyAny>>;
    fn close(&mut self) -> PyResult<()>;
}

#[cfg(feature = "unstable-streams")]
struct GenericSender<R>
where
    R: Runtime,
{
    runtime: PhantomData<R>,
    tx: mpsc::Sender<Py<PyAny>>,
}

#[cfg(feature = "unstable-streams")]
impl<R> Sender for GenericSender<R>
where
    R: Runtime + ContextExt,
{
    fn send(&mut self, py: Python, locals: TaskLocals, item: Py<PyAny>) -> PyResult<Py<PyAny>> {
        match self.tx.try_send(item.clone_ref(py)) {
            Ok(_) => true.into_py_any(py),
            Err(e) => {
                if e.is_full() {
                    let mut tx = self.tx.clone();

                    future_into_py_with_locals::<R, _, bool>(py, locals, async move {
                        if tx.flush().await.is_err() {
                            // receiving side disconnected
                            return Ok(false);
                        }
                        if tx.send(item).await.is_err() {
                            // receiving side disconnected
                            return Ok(false);
                        }
                        Ok(true)
                    })
                    .map(Bound::unbind)
                } else {
                    false.into_py_any(py)
                }
            }
        }
    }
    fn close(&mut self) -> PyResult<()> {
        self.tx.close_channel();
        Ok(())
    }
}

#[pyclass]
struct SenderGlue {
    locals: TaskLocals,
    tx: Arc<Mutex<dyn Sender>>,
}
#[pymethods]
impl SenderGlue {
    pub fn send(&mut self, item: Py<PyAny>) -> PyResult<Py<PyAny>> {
        Python::attach(|py| self.tx.lock().unwrap().send(py, self.locals.clone(), item))
    }
    pub fn close(&mut self) -> PyResult<()> {
        self.tx.lock().unwrap().close()
    }
}

#[cfg(feature = "unstable-streams")]
const STREAM_GLUE: &str = r#"
import inspect

async def forward(gen, sender):
    async for item in gen:
        should_continue = sender.send(item)

        if inspect.isawaitable(should_continue):
            should_continue = await should_continue

        if should_continue:
            continue
        else:
            break

    sender.close()
"#;

/// <span class="module-item stab portability" style="display: inline; border-radius: 3px; padding: 2px; font-size: 80%; line-height: 1.2;"><code>unstable-streams</code></span> Convert an async generator into a stream
///
/// **This API is marked as unstable** and is only available when the
/// `unstable-streams` crate feature is enabled. This comes with no
/// stability guarantees, and could be changed or removed at any time.
///
/// # Arguments
/// * `locals` - The current task locals
/// * `gen` - The Python async generator to be converted
///
/// # Examples
/// ```no_run
/// # use std::{any::Any, task::{Context, Poll}, pin::Pin, future::Future};
/// #
/// # use pyo3_async_runtimes::{
/// #     TaskLocals,
/// #     generic::{JoinError, ContextExt, Runtime}
/// # };
/// #
/// # struct MyCustomJoinError;
/// #
/// # impl JoinError for MyCustomJoinError {
/// #     fn is_panic(&self) -> bool {
/// #         unreachable!()
/// #     }
/// #     fn into_panic(self) -> Box<(dyn Any + Send + 'static)> {
/// #         unreachable!()
/// #     }
/// # }
/// #
/// # struct MyCustomJoinHandle;
/// #
/// # impl Future for MyCustomJoinHandle {
/// #     type Output = Result<(), MyCustomJoinError>;
/// #
/// #     fn poll(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Self::Output> {
/// #         unreachable!()
/// #     }
/// # }
/// #
/// # struct MyCustomRuntime;
/// #
/// # impl Runtime for MyCustomRuntime {
/// #     type JoinError = MyCustomJoinError;
/// #     type JoinHandle = MyCustomJoinHandle;
/// #
/// #     fn spawn<F>(fut: F) -> Self::JoinHandle
/// #     where
/// #         F: Future<Output = ()> + Send + 'static
/// #     {
/// #         unreachable!()
/// #     }
/// #
/// #     fn spawn_blocking<F>(f: F) -> Self::JoinHandle where F: FnOnce() + Send + 'static {
/// #         unreachable!()
/// #     }
/// # }
/// #
/// # impl ContextExt for MyCustomRuntime {
/// #     fn scope<F, R>(locals: TaskLocals, fut: F) -> Pin<Box<dyn Future<Output = R> + Send>>
/// #     where
/// #         F: Future<Output = R> + Send + 'static
/// #     {
/// #         unreachable!()
/// #     }
/// #     fn get_task_locals() -> Option<TaskLocals> {
/// #         unreachable!()
/// #     }
/// # }
///
/// use pyo3::prelude::*;
/// use futures_util::stream::{StreamExt, TryStreamExt};
/// use std::ffi::CString;
///
/// const TEST_MOD: &str = r#"
/// import asyncio
///
/// async def gen():
///     for i in range(10):
///         await asyncio.sleep(0.1)
///         yield i
/// "#;
///
/// # async fn test_async_gen() -> PyResult<()> {
/// let stream = Python::attach(|py| {
///     let test_mod = PyModule::from_code(
///         py,
///         &CString::new(TEST_MOD).unwrap(),
///         &CString::new("test_rust_coroutine/test_mod.py").unwrap(),
///         &CString::new("test_mod").unwrap(),
///     )?;
///
///     pyo3_async_runtimes::generic::into_stream_with_locals_v2::<MyCustomRuntime>(
///         pyo3_async_runtimes::generic::get_current_locals::<MyCustomRuntime>(py)?,
///         test_mod.call_method0("gen")?
///     )
/// })?;
///
/// let vals = stream
///     .map(|item| Python::attach(|py| -> PyResult<i32> { Ok(item.bind(py).extract()?) }))
///     .try_collect::<Vec<i32>>()
///     .await?;
///
/// assert_eq!((0..10).collect::<Vec<i32>>(), vals);
///
/// Ok(())
/// # }
/// ```
#[cfg(feature = "unstable-streams")]
pub fn into_stream_with_locals_v2<R>(
    locals: TaskLocals,
    gen: Bound<'_, PyAny>,
) -> PyResult<impl futures_util::stream::Stream<Item = Py<PyAny>> + 'static>
where
    R: Runtime + ContextExt,
{
    use std::ffi::CString;

    use pyo3::sync::PyOnceLock;

    static GLUE_MOD: PyOnceLock<Py<PyAny>> = PyOnceLock::new();
    let py = gen.py();
    let glue = GLUE_MOD
        .get_or_try_init(py, || -> PyResult<Py<PyAny>> {
            Ok(PyModule::from_code(
                py,
                &CString::new(STREAM_GLUE).unwrap(),
                &CString::new("pyo3_async_runtimes/pyo3_async_runtimes_glue.py").unwrap(),
                &CString::new("pyo3_async_runtimes_glue").unwrap(),
            )?
            .into())
        })?
        .bind(py);

    let (tx, rx) = mpsc::channel(10);

    locals.event_loop(py).call_method1(
        pyo3::intern!(py, "call_soon_threadsafe"),
        (
            locals
                .event_loop(py)
                .getattr(pyo3::intern!(py, "create_task"))?,
            glue.call_method1(
                pyo3::intern!(py, "forward"),
                (
                    gen,
                    SenderGlue {
                        locals,
                        tx: Arc::new(Mutex::new(GenericSender {
                            runtime: PhantomData::<R>,
                            tx,
                        })),
                    },
                ),
            )?,
        ),
    )?;
    Ok(rx)
}

/// <span class="module-item stab portability" style="display: inline; border-radius: 3px; padding: 2px; font-size: 80%; line-height: 1.2;"><code>unstable-streams</code></span> Convert an async generator into a stream
///
/// **This API is marked as unstable** and is only available when the
/// `unstable-streams` crate feature is enabled. This comes with no
/// stability guarantees, and could be changed or removed at any time.
///
/// # Arguments
/// * `gen` - The Python async generator to be converted
///
/// # Examples
/// ```no_run
/// # use std::{any::Any, task::{Context, Poll}, pin::Pin, future::Future};
/// #
/// # use pyo3_async_runtimes::{
/// #     TaskLocals,
/// #     generic::{JoinError, ContextExt, Runtime}
/// # };
/// #
/// # struct MyCustomJoinError;
/// #
/// # impl JoinError for MyCustomJoinError {
/// #     fn is_panic(&self) -> bool {
/// #         unreachable!()
/// #     }
/// #     fn into_panic(self) -> Box<(dyn Any + Send + 'static)> {
/// #         unreachable!()
/// #     }
/// # }
/// #
/// # struct MyCustomJoinHandle;
/// #
/// # impl Future for MyCustomJoinHandle {
/// #     type Output = Result<(), MyCustomJoinError>;
/// #
/// #     fn poll(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Self::Output> {
/// #         unreachable!()
/// #     }
/// # }
/// #
/// # struct MyCustomRuntime;
/// #
/// # impl Runtime for MyCustomRuntime {
/// #     type JoinError = MyCustomJoinError;
/// #     type JoinHandle = MyCustomJoinHandle;
/// #
/// #     fn spawn<F>(fut: F) -> Self::JoinHandle
/// #     where
/// #         F: Future<Output = ()> + Send + 'static
/// #     {
/// #         unreachable!()
/// #     }
/// #
/// #     fn spawn_blocking<F>(f: F) -> Self::JoinHandle where F: FnOnce() + Send + 'static {
/// #         unreachable!()
/// #     }
/// # }
/// #
/// # impl ContextExt for MyCustomRuntime {
/// #     fn scope<F, R>(locals: TaskLocals, fut: F) -> Pin<Box<dyn Future<Output = R> + Send>>
/// #     where
/// #         F: Future<Output = R> + Send + 'static
/// #     {
/// #         unreachable!()
/// #     }
/// #     fn get_task_locals() -> Option<TaskLocals> {
/// #         unreachable!()
/// #     }
/// # }
///
/// use pyo3::prelude::*;
/// use futures_util::stream::{StreamExt, TryStreamExt};
/// use std::ffi::CString;
///
/// const TEST_MOD: &str = r#"
/// import asyncio
///
/// async def gen():
///     for i in range(10):
///         await asyncio.sleep(0.1)
///         yield i
/// "#;
///
/// # async fn test_async_gen() -> PyResult<()> {
/// let stream = Python::attach(|py| {
///     let test_mod = PyModule::from_code(
///         py,
///         &CString::new(TEST_MOD).unwrap(),
///         &CString::new("test_rust_coroutine/test_mod.py").unwrap(),
///         &CString::new("test_mod").unwrap(),
///     )?;
///
///     pyo3_async_runtimes::generic::into_stream_v2::<MyCustomRuntime>(test_mod.call_method0("gen")?)
/// })?;
///
/// let vals = stream
///     .map(|item| Python::attach(|py| -> PyResult<i32> { Ok(item.bind(py).extract()?) }))
///     .try_collect::<Vec<i32>>()
///     .await?;
///
/// assert_eq!((0..10).collect::<Vec<i32>>(), vals);
///
/// Ok(())
/// # }
/// ```
#[cfg(feature = "unstable-streams")]
pub fn into_stream_v2<R>(
    gen: Bound<'_, PyAny>,
) -> PyResult<impl futures_util::stream::Stream<Item = Py<PyAny>> + 'static>
where
    R: Runtime + ContextExt,
{
    into_stream_with_locals_v2::<R>(get_current_locals::<R>(gen.py())?, gen)
}
