//! <span class="module-item stab portability" style="display: inline; border-radius: 3px; padding: 2px; font-size: 80%; line-height: 1.2;"><code>tokio-runtime</code></span> PyO3 Asyncio functions specific to the tokio runtime
//!
//! Items marked with
//! <span
//!   class="module-item stab portability"
//!   style="display: inline; border-radius: 3px; padding: 2px; font-size: 80%; line-height: 1.2;"
//! ><code>unstable-streams</code></span>
//! >are only available when the `unstable-streams` Cargo feature is enabled:
//!
//! ```toml
//! [dependencies.pyo3-async-runtimes]
//! version = "0.24"
//! features = ["unstable-streams"]
//! ```

use std::cell::OnceCell;
use std::ops::Deref;
use std::sync::OnceLock;
use std::{future::Future, pin::Pin, sync::Mutex};

use ::tokio::{
    runtime::{Builder, Runtime},
    task,
};
use once_cell::sync::Lazy;
use pyo3::prelude::*;

use crate::{
    generic::{self, ContextExt, LocalContextExt, Runtime as GenericRuntime, SpawnLocalExt},
    TaskLocals,
};

/// <span class="module-item stab portability" style="display: inline; border-radius: 3px; padding: 2px; font-size: 80%; line-height: 1.2;"><code>attributes</code></span>
/// re-exports for macros
#[cfg(feature = "attributes")]
pub mod re_exports {
    /// re-export pending to be used in tokio macros without additional dependency
    pub use futures_util::future::pending;
    /// re-export tokio::runtime to build runtimes in tokio macros without additional dependency
    pub use tokio::runtime;
}

/// <span class="module-item stab portability" style="display: inline; border-radius: 3px; padding: 2px; font-size: 80%; line-height: 1.2;"><code>attributes</code></span>
#[cfg(feature = "attributes")]
pub use pyo3_async_runtimes_macros::tokio_main as main;

/// <span class="module-item stab portability" style="display: inline; border-radius: 3px; padding: 2px; font-size: 80%; line-height: 1.2;"><code>attributes</code></span>
/// <span class="module-item stab portability" style="display: inline; border-radius: 3px; padding: 2px; font-size: 80%; line-height: 1.2;"><code>testing</code></span>
/// Registers a `tokio` test with the `pyo3-asyncio` test harness
#[cfg(all(feature = "attributes", feature = "testing"))]
pub use pyo3_async_runtimes_macros::tokio_test as test;

enum Pyo3Runtime {
    Borrowed(&'static Runtime),
    Owned(Runtime),
}
impl Deref for Pyo3Runtime {
    type Target = Runtime;
    fn deref(&self) -> &Self::Target {
        match self {
            Self::Borrowed(rt) => rt,
            Self::Owned(rt) => rt,
        }
    }
}

static TOKIO_BUILDER: Lazy<Mutex<Builder>> = Lazy::new(|| Mutex::new(multi_thread()));
static TOKIO_RUNTIME: OnceLock<Pyo3Runtime> = OnceLock::new();

impl generic::JoinError for task::JoinError {
    fn is_panic(&self) -> bool {
        task::JoinError::is_panic(self)
    }
    fn into_panic(self) -> Box<dyn std::any::Any + Send + 'static> {
        task::JoinError::into_panic(self)
    }
}

struct TokioRuntime;

tokio::task_local! {
    static TASK_LOCALS: OnceCell<TaskLocals>;
}

impl GenericRuntime for TokioRuntime {
    type JoinError = task::JoinError;
    type JoinHandle = task::JoinHandle<()>;

    fn spawn<F>(fut: F) -> Self::JoinHandle
    where
        F: Future<Output = ()> + Send + 'static,
    {
        get_runtime().spawn(async move {
            fut.await;
        })
    }

    fn spawn_blocking<F>(f: F) -> Self::JoinHandle
    where
        F: FnOnce() + Send + 'static,
    {
        get_runtime().spawn_blocking(f)
    }
}

impl ContextExt for TokioRuntime {
    fn scope<F, R>(locals: TaskLocals, fut: F) -> Pin<Box<dyn Future<Output = R> + Send>>
    where
        F: Future<Output = R> + Send + 'static,
    {
        let cell = OnceCell::new();
        cell.set(locals).unwrap();

        Box::pin(TASK_LOCALS.scope(cell, fut))
    }

    fn get_task_locals() -> Option<TaskLocals> {
        TASK_LOCALS
            .try_with(|c| c.get().map(|locals| locals.clone()))
            .unwrap_or_default()
    }
}

impl SpawnLocalExt for TokioRuntime {
    fn spawn_local<F>(fut: F) -> Self::JoinHandle
    where
        F: Future<Output = ()> + 'static,
    {
        tokio::task::spawn_local(fut)
    }
}

impl LocalContextExt for TokioRuntime {
    fn scope_local<F, R>(locals: TaskLocals, fut: F) -> Pin<Box<dyn Future<Output = R>>>
    where
        F: Future<Output = R> + 'static,
    {
        let cell = OnceCell::new();
        cell.set(locals).unwrap();

        Box::pin(TASK_LOCALS.scope(cell, fut))
    }
}

/// Set the task local event loop for the given future
pub async fn scope<F, R>(locals: TaskLocals, fut: F) -> R
where
    F: Future<Output = R> + Send + 'static,
{
    TokioRuntime::scope(locals, fut).await
}

/// Set the task local event loop for the given !Send future
pub async fn scope_local<F, R>(locals: TaskLocals, fut: F) -> R
where
    F: Future<Output = R> + 'static,
{
    TokioRuntime::scope_local(locals, fut).await
}

/// Get the current event loop from either Python or Rust async task local context
///
/// This function first checks if the runtime has a task-local reference to the Python event loop.
/// If not, it calls [`get_running_loop`](`crate::get_running_loop`) to get the event loop
/// associated with the current OS thread.
pub fn get_current_loop(py: Python) -> PyResult<Bound<PyAny>> {
    generic::get_current_loop::<TokioRuntime>(py)
}

/// Either copy the task locals from the current task OR get the current running loop and
/// contextvars from Python.
pub fn get_current_locals(py: Python) -> PyResult<TaskLocals> {
    generic::get_current_locals::<TokioRuntime>(py)
}

/// Initialize the Tokio runtime with a custom build
pub fn init(builder: Builder) {
    *TOKIO_BUILDER.lock().unwrap() = builder
}

/// Initialize the Tokio runtime with a custom Tokio runtime
///
/// Returns Ok(()) if success and Err(()) if it had been inited.
#[allow(clippy::result_unit_err)]
pub fn init_with_runtime(runtime: &'static Runtime) -> Result<(), ()> {
    TOKIO_RUNTIME
        .set(Pyo3Runtime::Borrowed(runtime))
        .map_err(|_| ())
}

/// Get a reference to the current tokio runtime
pub fn get_runtime<'a>() -> &'a Runtime {
    TOKIO_RUNTIME.get_or_init(|| {
        let rt = TOKIO_BUILDER
            .lock()
            .unwrap()
            .build()
            .expect("Unable to build Tokio runtime");
        Pyo3Runtime::Owned(rt)
    })
}

fn multi_thread() -> Builder {
    let mut builder = Builder::new_multi_thread();
    builder.enable_all();
    builder
}

/// Run the event loop until the given Future completes
///
/// The event loop runs until the given future is complete.
///
/// After this function returns, the event loop can be resumed with [`run_until_complete`]
///
/// # Arguments
/// * `event_loop` - The Python event loop that should run the future
/// * `fut` - The future to drive to completion
///
/// # Examples
///
/// ```
/// # use std::time::Duration;
/// #
/// # use pyo3::prelude::*;
/// #
/// # Python::initialize();
/// # Python::attach(|py| -> PyResult<()> {
/// # let event_loop = py.import("asyncio")?.call_method0("new_event_loop")?;
/// pyo3_async_runtimes::tokio::run_until_complete(event_loop, async move {
///     tokio::time::sleep(Duration::from_secs(1)).await;
///     Ok(())
/// })?;
/// # Ok(())
/// # }).unwrap();
/// ```
pub fn run_until_complete<F, T>(event_loop: Bound<PyAny>, fut: F) -> PyResult<T>
where
    F: Future<Output = PyResult<T>> + Send + 'static,
    T: Send + Sync + 'static,
{
    generic::run_until_complete::<TokioRuntime, _, T>(&event_loop, fut)
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
/// # use std::time::Duration;
/// #
/// # use pyo3::prelude::*;
/// #
/// fn main() {
///     Python::attach(|py| {
///         pyo3_async_runtimes::tokio::run(py, async move {
///             tokio::time::sleep(Duration::from_secs(1)).await;
///             Ok(())
///         })
///         .map_err(|e| {
///             e.print_and_set_sys_last_vars(py);
///         })
///         .unwrap();
///     })
/// }
/// ```
pub fn run<F, T>(py: Python, fut: F) -> PyResult<T>
where
    F: Future<Output = PyResult<T>> + Send + 'static,
    T: Send + Sync + 'static,
{
    generic::run::<TokioRuntime, F, T>(py, fut)
}

/// Convert a Rust Future into a Python awaitable
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
/// * `locals` - The task locals for the given future
/// * `fut` - The Rust future to be converted
///
/// # Examples
///
/// ```
/// use std::time::Duration;
///
/// use pyo3::prelude::*;
///
/// /// Awaitable sleep function
/// #[pyfunction]
/// fn sleep_for<'p>(py: Python<'p>, secs: Bound<'p, PyAny>) -> PyResult<Bound<'p, PyAny>> {
///     let secs = secs.extract()?;
///     pyo3_async_runtimes::tokio::future_into_py_with_locals(
///         py,
///         pyo3_async_runtimes::tokio::get_current_locals(py)?,
///         async move {
///             tokio::time::sleep(Duration::from_secs(secs)).await;
///             Python::attach(|py| Ok(py.None()))
///         }
///     )
/// }
/// ```
pub fn future_into_py_with_locals<F, T>(
    py: Python,
    locals: TaskLocals,
    fut: F,
) -> PyResult<Bound<PyAny>>
where
    F: Future<Output = PyResult<T>> + Send + 'static,
    T: for<'py> IntoPyObject<'py> + Send + 'static,
{
    generic::future_into_py_with_locals::<TokioRuntime, F, T>(py, locals, fut)
}

/// Convert a Rust Future into a Python awaitable
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
/// ```
/// use std::time::Duration;
///
/// use pyo3::prelude::*;
///
/// /// Awaitable sleep function
/// #[pyfunction]
/// fn sleep_for<'p>(py: Python<'p>, secs: Bound<'p, PyAny>) -> PyResult<Bound<'p, PyAny>> {
///     let secs = secs.extract()?;
///     pyo3_async_runtimes::tokio::future_into_py(py, async move {
///         tokio::time::sleep(Duration::from_secs(secs)).await;
///         Ok(())
///     })
/// }
/// ```
pub fn future_into_py<F, T>(py: Python, fut: F) -> PyResult<Bound<PyAny>>
where
    F: Future<Output = PyResult<T>> + Send + 'static,
    T: for<'py> IntoPyObject<'py> + Send + 'static,
{
    generic::future_into_py::<TokioRuntime, _, T>(py, fut)
}

/// Convert a `!Send` Rust Future into a Python awaitable
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
/// * `locals` - The task locals for the given future
/// * `fut` - The Rust future to be converted
///
/// # Examples
///
/// ```
/// use std::{rc::Rc, time::Duration};
///
/// use pyo3::prelude::*;
///
/// /// Awaitable non-send sleep function
/// #[pyfunction]
/// fn sleep_for(py: Python, secs: u64) -> PyResult<Bound<PyAny>> {
///     // Rc is non-send so it cannot be passed into pyo3_async_runtimes::tokio::future_into_py
///     let secs = Rc::new(secs);
///
///     pyo3_async_runtimes::tokio::local_future_into_py_with_locals(
///         py,
///         pyo3_async_runtimes::tokio::get_current_locals(py)?,
///         async move {
///             tokio::time::sleep(Duration::from_secs(*secs)).await;
///             Python::attach(|py| Ok(py.None()))
///         }
///     )
/// }
///
/// # #[cfg(all(feature = "tokio-runtime", feature = "attributes"))]
/// #[pyo3_async_runtimes::tokio::main]
/// async fn main() -> PyResult<()> {
///     let locals = Python::attach(|py| -> PyResult<_> {
///         pyo3_async_runtimes::tokio::get_current_locals(py)
///     })?;
///
///     // the main coroutine is running in a Send context, so we cannot use LocalSet here. Instead
///     // we use spawn_blocking in order to use LocalSet::block_on
///     tokio::task::spawn_blocking(move || {
///         // LocalSet allows us to work with !Send futures within tokio. Without it, any calls to
///         // pyo3_async_runtimes::tokio::local_future_into_py will panic.
///         tokio::task::LocalSet::new().block_on(
///             pyo3_async_runtimes::tokio::get_runtime(),
///             pyo3_async_runtimes::tokio::scope_local(locals, async {
///                 Python::attach(|py| {
///                     let py_future = sleep_for(py, 1)?;
///                     pyo3_async_runtimes::tokio::into_future(py_future)
///                 })?
///                 .await?;
///
///                 Ok(())
///             })
///         )
///     }).await.unwrap()
/// }
/// # #[cfg(not(all(feature = "tokio-runtime", feature = "attributes")))]
/// # fn main() {}
/// ```
#[deprecated(
    since = "0.18.0",
    note = "Questionable whether these conversions have real-world utility (see https://github.com/awestlake87/pyo3-asyncio/issues/59#issuecomment-1008038497 and let me know if you disagree!)"
)]
#[allow(deprecated)]
pub fn local_future_into_py_with_locals<F, T>(
    py: Python,
    locals: TaskLocals,
    fut: F,
) -> PyResult<Bound<PyAny>>
where
    F: Future<Output = PyResult<T>> + 'static,
    T: for<'py> IntoPyObject<'py>,
{
    generic::local_future_into_py_with_locals::<TokioRuntime, _, T>(py, locals, fut)
}

/// Convert a `!Send` Rust Future into a Python awaitable
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
/// ```
/// use std::{rc::Rc, time::Duration};
///
/// use pyo3::prelude::*;
///
/// /// Awaitable non-send sleep function
/// #[pyfunction]
/// fn sleep_for(py: Python, secs: u64) -> PyResult<Bound<PyAny>> {
///     // Rc is non-send so it cannot be passed into pyo3_async_runtimes::tokio::future_into_py
///     let secs = Rc::new(secs);
///     pyo3_async_runtimes::tokio::local_future_into_py(py, async move {
///         tokio::time::sleep(Duration::from_secs(*secs)).await;
///         Ok(())
///     })
/// }
///
/// # #[cfg(all(feature = "tokio-runtime", feature = "attributes"))]
/// #[pyo3_async_runtimes::tokio::main]
/// async fn main() -> PyResult<()> {
///     let locals = Python::attach(|py| {
///         pyo3_async_runtimes::tokio::get_current_locals(py).unwrap()
///     });
///
///     // the main coroutine is running in a Send context, so we cannot use LocalSet here. Instead
///     // we use spawn_blocking in order to use LocalSet::block_on
///     tokio::task::spawn_blocking(move || {
///         // LocalSet allows us to work with !Send futures within tokio. Without it, any calls to
///         // pyo3_async_runtimes::tokio::local_future_into_py will panic.
///         tokio::task::LocalSet::new().block_on(
///             pyo3_async_runtimes::tokio::get_runtime(),
///             pyo3_async_runtimes::tokio::scope_local(locals, async {
///                 Python::attach(|py| {
///                     let py_future = sleep_for(py, 1)?;
///                     pyo3_async_runtimes::tokio::into_future(py_future)
///                 })?
///                 .await?;
///
///                 Ok(())
///             })
///         )
///     }).await.unwrap()
/// }
/// # #[cfg(not(all(feature = "tokio-runtime", feature = "attributes")))]
/// # fn main() {}
/// ```
#[deprecated(
    since = "0.18.0",
    note = "Questionable whether these conversions have real-world utility (see https://github.com/awestlake87/pyo3-asyncio/issues/59#issuecomment-1008038497 and let me know if you disagree!)"
)]
#[allow(deprecated)]
pub fn local_future_into_py<F, T>(py: Python, fut: F) -> PyResult<Bound<PyAny>>
where
    F: Future<Output = PyResult<T>> + 'static,
    T: for<'py> IntoPyObject<'py>,
{
    generic::local_future_into_py::<TokioRuntime, _, T>(py, fut)
}

/// Convert a Python `awaitable` into a Rust Future
///
/// This function converts the `awaitable` into a Python Task using `run_coroutine_threadsafe`. A
/// completion handler sends the result of this Task through a
/// `futures_channel::oneshot::Sender<PyResult<Py<PyAny>>>` and the future returned by this function
/// simply awaits the result through the `futures_channel::oneshot::Receiver<PyResult<Py<PyAny>>>`.
///
/// # Arguments
/// * `awaitable` - The Python `awaitable` to be converted
///
/// # Examples
///
/// ```
/// use std::time::Duration;
/// use std::ffi::CString;
///
/// use pyo3::prelude::*;
///
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
///         pyo3_async_runtimes::tokio::into_future(
///             test_mod
///                 .call_method1(py, "py_sleep", (seconds,))?
///                 .into_bound(py),
///         )
///     })?
///     .await?;
///     Ok(())
/// }
/// ```
pub fn into_future(
    awaitable: Bound<PyAny>,
) -> PyResult<impl Future<Output = PyResult<Py<PyAny>>> + Send> {
    generic::into_future::<TokioRuntime>(awaitable)
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
/// ```
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
/// # #[cfg(all(feature = "unstable-streams", feature = "attributes"))]
/// # #[pyo3_async_runtimes::tokio::main]
/// # async fn main() -> PyResult<()> {
/// let stream = Python::attach(|py| {
///     let test_mod = PyModule::from_code(
///         py,
///         &CString::new(TEST_MOD).unwrap(),
///         &CString::new("test_rust_coroutine/test_mod.py").unwrap(),
///         &CString::new("test_mod").unwrap(),
///     )?;
///
///     pyo3_async_runtimes::tokio::into_stream_with_locals_v1(
///         pyo3_async_runtimes::tokio::get_current_locals(py)?,
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
/// # #[cfg(not(all(feature = "unstable-streams", feature = "attributes")))]
/// # fn main() {}
/// ```
#[cfg(feature = "unstable-streams")]
pub fn into_stream_with_locals_v1(
    locals: TaskLocals,
    gen: Bound<'_, PyAny>,
) -> PyResult<impl futures_util::stream::Stream<Item = PyResult<Py<PyAny>>> + 'static> {
    generic::into_stream_with_locals_v1::<TokioRuntime>(locals, gen)
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
/// ```
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
/// # #[cfg(all(feature = "unstable-streams", feature = "attributes"))]
/// # #[pyo3_async_runtimes::tokio::main]
/// # async fn main() -> PyResult<()> {
/// let stream = Python::attach(|py| {
///     let test_mod = PyModule::from_code(
///         py,
///         &CString::new(TEST_MOD).unwrap(),
///         &CString::new("test_rust_coroutine/test_mod.py").unwrap(),
///         &CString::new("test_mod").unwrap(),
///     )?;
///
///     pyo3_async_runtimes::tokio::into_stream_v1(test_mod.call_method0("gen")?)
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
/// # #[cfg(not(all(feature = "unstable-streams", feature = "attributes")))]
/// # fn main() {}
/// ```
#[cfg(feature = "unstable-streams")]
pub fn into_stream_v1(
    gen: Bound<'_, PyAny>,
) -> PyResult<impl futures_util::stream::Stream<Item = PyResult<Py<PyAny>>> + 'static> {
    generic::into_stream_v1::<TokioRuntime>(gen)
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
/// ```
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
/// # #[cfg(all(feature = "unstable-streams", feature = "attributes"))]
/// # #[pyo3_async_runtimes::tokio::main]
/// # async fn main() -> PyResult<()> {
/// let stream = Python::attach(|py| {
///     let test_mod = PyModule::from_code(
///         py,
///         &CString::new(TEST_MOD).unwrap(),
///         &CString::new("test_rust_coroutine/test_mod.py").unwrap(),
///         &CString::new("test_mod").unwrap(),
///     )?;
///
///     pyo3_async_runtimes::tokio::into_stream_with_locals_v2(
///         pyo3_async_runtimes::tokio::get_current_locals(py)?,
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
/// # #[cfg(not(all(feature = "unstable-streams", feature = "attributes")))]
/// # fn main() {}
/// ```
#[cfg(feature = "unstable-streams")]
pub fn into_stream_with_locals_v2(
    locals: TaskLocals,
    gen: Bound<'_, PyAny>,
) -> PyResult<impl futures_util::stream::Stream<Item = Py<PyAny>> + 'static> {
    generic::into_stream_with_locals_v2::<TokioRuntime>(locals, gen)
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
/// ```
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
/// # #[cfg(all(feature = "unstable-streams", feature = "attributes"))]
/// # #[pyo3_async_runtimes::tokio::main]
/// # async fn main() -> PyResult<()> {
/// let stream = Python::attach(|py| {
///     let test_mod = PyModule::from_code(
///         py,
///         &CString::new(TEST_MOD).unwrap(),
///         &CString::new("test_rust_coroutine/test_mod.py").unwrap(),
///         &CString::new("test_mod").unwrap(),
///     )?;
///
///     pyo3_async_runtimes::tokio::into_stream_v2(test_mod.call_method0("gen")?)
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
/// # #[cfg(not(all(feature = "unstable-streams", feature = "attributes")))]
/// # fn main() {}
/// ```
#[cfg(feature = "unstable-streams")]
pub fn into_stream_v2(
    gen: Bound<'_, PyAny>,
) -> PyResult<impl futures_util::stream::Stream<Item = Py<PyAny>> + 'static> {
    generic::into_stream_v2::<TokioRuntime>(gen)
}
