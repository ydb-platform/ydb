#![warn(missing_docs)]
#![allow(clippy::borrow_deref_ref)]

//! Rust Bindings to the Python Asyncio Event Loop
//!
//! # Motivation
//!
//! This crate aims to provide a convenient interface to manage the interop between Python and
//! Rust's async/await models. It supports conversions between Rust and Python futures and manages
//! the event loops for both languages. Python's threading model and GIL can make this interop a bit
//! trickier than one might expect, so there are a few caveats that users should be aware of.
//!
//! ## Why Two Event Loops
//!
//! Currently, we don't have a way to run Rust futures directly on Python's event loop. Likewise,
//! Python's coroutines cannot be directly spawned on a Rust event loop. The two coroutine models
//! require some additional assistance from their event loops, so in all likelihood they will need
//! a new _unique_ event loop that addresses the needs of both languages if the coroutines are to
//! be run on the same loop.
//!
//! It's not immediately clear that this would provide worthwhile performance wins either, so in the
//! interest of getting something simple out there to facilitate these conversions, this crate
//! handles the communication between _separate_ Python and Rust event loops.
//!
//! ## Python's Event Loop and the Main Thread
//!
//! Python is very picky about the threads used by the `asyncio` executor. In particular, it needs
//! to have control over the main thread in order to handle signals like CTRL-C correctly. This
//! means that Cargo's default test harness will no longer work since it doesn't provide a method of
//! overriding the main function to add our event loop initialization and finalization.
//!
//! ## Event Loop References and ContextVars
//!
//! One problem that arises when interacting with Python's asyncio library is that the functions we
//! use to get a reference to the Python event loop can only be called in certain contexts. Since
//! PyO3 Asyncio needs to interact with Python's event loop during conversions, the context of these
//! conversions can matter a lot.
//!
//! Likewise, Python's `contextvars` library can require some special treatment. Python functions
//! and coroutines can rely on the context of outer coroutines to function correctly, so this
//! library needs to be able to preserve `contextvars` during conversions.
//!
//! > The core conversions we've mentioned so far in the README should insulate you from these
//! > concerns in most cases. For the edge cases where they don't, this section should provide you
//! > with the information you need to solve these problems.
//!
//! ### The Main Dilemma
//!
//! Python programs can have many independent event loop instances throughout the lifetime of the
//! application (`asyncio.run` for example creates its own event loop each time it's called for
//! instance), and they can even run concurrent with other event loops. For this reason, the most
//! correct method of obtaining a reference to the Python event loop is via
//! `asyncio.get_running_loop`.
//!
//! `asyncio.get_running_loop` returns the event loop associated with the current OS thread. It can
//! be used inside Python coroutines to spawn concurrent tasks, interact with timers, or in our case
//! signal between Rust and Python. This is all well and good when we are operating on a Python
//! thread, but since Rust threads are not associated with a Python event loop,
//! `asyncio.get_running_loop` will fail when called on a Rust runtime.
//!
//! `contextvars` operates in a similar way, though the current context is not always associated
//! with the current OS thread. Different contexts can be associated with different coroutines even
//! if they run on the same OS thread.
//!
//! ### The Solution
//!
//! A really straightforward way of dealing with this problem is to pass references to the
//! associated Python event loop and context for every conversion. That's why we have a structure
//! called `TaskLocals` and a set of conversions that accept it.
//!
//! `TaskLocals` stores the current event loop, and allows the user to copy the current Python
//! context if necessary. The following conversions will use these references to perform the
//! necessary conversions and restore Python context when needed:
//!
//! - `pyo3_async_runtimes::into_future_with_locals` - Convert a Python awaitable into a Rust future.
//! - `pyo3_async_runtimes::<runtime>::future_into_py_with_locals` - Convert a Rust future into a Python
//!   awaitable.
//! - `pyo3_async_runtimes::<runtime>::local_future_into_py_with_locals` - Convert a `!Send` Rust future
//!   into a Python awaitable.
//!
//! One clear disadvantage to this approach is that the Rust application has to explicitly track
//! these references. In native libraries, we can't make any assumptions about the underlying event
//! loop, so the only reliable way to make sure our conversions work properly is to store these
//! references at the callsite to use later on.
//!
//! ```rust
//! use pyo3::{wrap_pyfunction, prelude::*};
//!
//! # #[cfg(feature = "tokio-runtime")]
//! #[pyfunction]
//! fn sleep(py: Python) -> PyResult<Bound<PyAny>> {
//!     // Construct the task locals structure with the current running loop and context
//!     let locals = pyo3_async_runtimes::TaskLocals::with_running_loop(py)?.copy_context(py)?;
//!
//!     // Convert the async move { } block to a Python awaitable
//!     pyo3_async_runtimes::tokio::future_into_py_with_locals(py, locals.clone(), async move {
//!         let py_sleep = Python::attach(|py| {
//!             // Sometimes we need to call other async Python functions within
//!             // this future. In order for this to work, we need to track the
//!             // event loop from earlier.
//!             pyo3_async_runtimes::into_future_with_locals(
//!                 &locals,
//!                 py.import("asyncio")?.call_method1("sleep", (1,))?
//!             )
//!         })?;
//!
//!         py_sleep.await?;
//!
//!         Ok(())
//!     })
//! }
//!
//! # #[cfg(feature = "tokio-runtime")]
//! #[pymodule]
//! fn my_mod(py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
//!     m.add_function(wrap_pyfunction!(sleep, m)?)?;
//!     Ok(())
//! }
//! ```
//!
//! > A naive solution to this tracking problem would be to cache a global reference to the asyncio
//! > event loop that all PyO3 Asyncio conversions can use. In fact this is what we did in PyO3
//! > Asyncio `v0.13`. This works well for applications, but it soon became clear that this is not
//! > so ideal for libraries. Libraries usually have no direct control over how the event loop is
//! > managed, they're just expected to work with any event loop at any point in the application.
//! > This problem is compounded further when multiple event loops are used in the application since
//! > the global reference will only point to one.
//!
//! Another disadvantage to this explicit approach that is less obvious is that we can no longer
//! call our `#[pyfunction] fn sleep` on a Rust runtime since `asyncio.get_running_loop` only works
//! on Python threads! It's clear that we need a slightly more flexible approach.
//!
//! In order to detect the Python event loop at the callsite, we need something like
//! `asyncio.get_running_loop` and `contextvars.copy_context` that works for _both Python and Rust_.
//! In Python, `asyncio.get_running_loop` uses thread-local data to retrieve the event loop
//! associated with the current thread. What we need in Rust is something that can retrieve the
//! Python event loop and contextvars associated with the current Rust _task_.
//!
//! Enter `pyo3_async_runtimes::<runtime>::get_current_locals`. This function first checks task-local data
//! for the `TaskLocals`, then falls back on `asyncio.get_running_loop` and
//! `contextvars.copy_context` if no task locals are found. This way both bases are
//! covered.
//!
//! Now, all we need is a way to store the `TaskLocals` for the Rust future. Since this is a
//! runtime-specific feature, you can find the following functions in each runtime module:
//!
//! - `pyo3_async_runtimes::<runtime>::scope` - Store the task-local data when executing the given Future.
//! - `pyo3_async_runtimes::<runtime>::scope_local` - Store the task-local data when executing the given
//!   `!Send` Future.
//!
//! With these new functions, we can make our previous example more correct:
//!
//! ```rust no_run
//! use pyo3::prelude::*;
//!
//! # #[cfg(feature = "tokio-runtime")]
//! #[pyfunction]
//! fn sleep(py: Python) -> PyResult<Bound<PyAny>> {
//!     // get the current event loop through task-local data
//!     // OR `asyncio.get_running_loop` and `contextvars.copy_context`
//!     let locals = pyo3_async_runtimes::tokio::get_current_locals(py)?;
//!
//!     pyo3_async_runtimes::tokio::future_into_py_with_locals(
//!         py,
//!         locals.clone(),
//!         // Store the current locals in task-local data
//!         pyo3_async_runtimes::tokio::scope(locals.clone(), async move {
//!             let py_sleep = Python::attach(|py| {
//!                 pyo3_async_runtimes::into_future_with_locals(
//!                     // Now we can get the current locals through task-local data
//!                     &pyo3_async_runtimes::tokio::get_current_locals(py)?,
//!                     py.import("asyncio")?.call_method1("sleep", (1,))?
//!                 )
//!             })?;
//!
//!             py_sleep.await?;
//!
//!             Ok(Python::attach(|py| py.None()))
//!         })
//!     )
//! }
//!
//! # #[cfg(feature = "tokio-runtime")]
//! #[pyfunction]
//! fn wrap_sleep(py: Python) -> PyResult<Bound<PyAny>> {
//!     // get the current event loop through task-local data
//!     // OR `asyncio.get_running_loop` and `contextvars.copy_context`
//!     let locals = pyo3_async_runtimes::tokio::get_current_locals(py)?;
//!
//!     pyo3_async_runtimes::tokio::future_into_py_with_locals(
//!         py,
//!         locals.clone(),
//!         // Store the current locals in task-local data
//!         pyo3_async_runtimes::tokio::scope(locals.clone(), async move {
//!             let py_sleep = Python::attach(|py| {
//!                 pyo3_async_runtimes::into_future_with_locals(
//!                     &pyo3_async_runtimes::tokio::get_current_locals(py)?,
//!                     // We can also call sleep within a Rust task since the
//!                     // locals are stored in task local data
//!                     sleep(py)?
//!                 )
//!             })?;
//!
//!             py_sleep.await?;
//!
//!             Ok(Python::attach(|py| py.None()))
//!         })
//!     )
//! }
//!
//! # #[cfg(feature = "tokio-runtime")]
//! #[pymodule]
//! fn my_mod(py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
//!     m.add_function(wrap_pyfunction!(sleep, m)?)?;
//!     m.add_function(wrap_pyfunction!(wrap_sleep, m)?)?;
//!     Ok(())
//! }
//! ```
//!
//! Even though this is more correct, it's clearly not more ergonomic. That's why we introduced a
//! set of functions with this functionality baked in:
//!
//! - `pyo3_async_runtimes::<runtime>::into_future`
//!   > Convert a Python awaitable into a Rust future (using
//!   > `pyo3_async_runtimes::<runtime>::get_current_locals`)
//! - `pyo3_async_runtimes::<runtime>::future_into_py`
//!   > Convert a Rust future into a Python awaitable (using
//!   > `pyo3_async_runtimes::<runtime>::get_current_locals` and `pyo3_async_runtimes::<runtime>::scope` to set the
//!   > task-local event loop for the given Rust future)
//! - `pyo3_async_runtimes::<runtime>::local_future_into_py`
//!   > Convert a `!Send` Rust future into a Python awaitable (using
//!   > `pyo3_async_runtimes::<runtime>::get_current_locals` and `pyo3_async_runtimes::<runtime>::scope_local` to
//!   > set the task-local event loop for the given Rust future).
//!
//! __These are the functions that we recommend using__. With these functions, the previous example
//! can be rewritten to be more compact:
//!
//! ```rust
//! use pyo3::prelude::*;
//!
//! # #[cfg(feature = "tokio-runtime")]
//! #[pyfunction]
//! fn sleep(py: Python) -> PyResult<Bound<PyAny>> {
//!     pyo3_async_runtimes::tokio::future_into_py(py, async move {
//!         let py_sleep = Python::attach(|py| {
//!             pyo3_async_runtimes::tokio::into_future(
//!                 py.import("asyncio")?.call_method1("sleep", (1,))?
//!             )
//!         })?;
//!
//!         py_sleep.await?;
//!
//!         Ok(Python::attach(|py| py.None()))
//!     })
//! }
//!
//! # #[cfg(feature = "tokio-runtime")]
//! #[pyfunction]
//! fn wrap_sleep(py: Python) -> PyResult<Bound<PyAny>> {
//!     pyo3_async_runtimes::tokio::future_into_py(py, async move {
//!         let py_sleep = Python::attach(|py| {
//!             pyo3_async_runtimes::tokio::into_future(sleep(py)?)
//!         })?;
//!
//!         py_sleep.await?;
//!
//!         Ok(Python::attach(|py| py.None()))
//!     })
//! }
//!
//! # #[cfg(feature = "tokio-runtime")]
//! #[pymodule]
//! fn my_mod(py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
//!     m.add_function(wrap_pyfunction!(sleep, m)?)?;
//!     m.add_function(wrap_pyfunction!(wrap_sleep, m)?)?;
//!     Ok(())
//! }
//! ```
//!
//! > A special thanks to [@ShadowJonathan](https://github.com/ShadowJonathan) for helping with the
//! > design and review of these changes!
//!
//! ## Rust's Event Loop
//!
//! Currently only the Async-Std and Tokio runtimes are supported by this crate. If you need support
//! for another runtime, feel free to make a request on GitHub (or attempt to add support yourself
//! with the [`generic`] module)!
//!
//! > _In the future, we may implement first class support for more Rust runtimes. Contributions are
//! > welcome as well!_
//!
//! ## Features
//!
//! Items marked with
//! <span
//!   class="module-item stab portability"
//!   style="display: inline; border-radius: 3px; padding: 2px; font-size: 80%; line-height: 1.2;"
//! ><code>attributes</code></span>
//! > are only available when the `attributes` Cargo feature is enabled:
//!
//! ```toml
//! [dependencies.pyo3-async-runtimes]
//! version = "0.24"
//! features = ["attributes"]
//! ```
//!
//! Items marked with
//! <span
//!   class="module-item stab portability"
//!   style="display: inline; border-radius: 3px; padding: 2px; font-size: 80%; line-height: 1.2;"
//! ><code>async-std-runtime</code></span>
//! > are only available when the `async-std-runtime` Cargo feature is enabled:
//!
//! ```toml
//! [dependencies.pyo3-async-runtimes]
//! version = "0.24"
//! features = ["async-std-runtime"]
//! ```
//!
//! Items marked with
//! <span
//!   class="module-item stab portability"
//!   style="display: inline; border-radius: 3px; padding: 2px; font-size: 80%; line-height: 1.2;"
//! ><code>tokio-runtime</code></span>
//! > are only available when the `tokio-runtime` Cargo feature is enabled:
//!
//! ```toml
//! [dependencies.pyo3-async-runtimes]
//! version = "0.24"
//! features = ["tokio-runtime"]
//! ```
//!
//! Items marked with
//! <span
//!   class="module-item stab portability"
//!   style="display: inline; border-radius: 3px; padding: 2px; font-size: 80%; line-height: 1.2;"
//! ><code>testing</code></span>
//! > are only available when the `testing` Cargo feature is enabled:
//!
//! ```toml
//! [dependencies.pyo3-async-runtimes]
//! version = "0.24"
//! features = ["testing"]
//! ```

/// Re-exported for #[test] attributes
#[cfg(all(feature = "attributes", feature = "testing"))]
pub use inventory;

/// <span class="module-item stab portability" style="display: inline; border-radius: 3px; padding: 2px; font-size: 80%; line-height: 1.2;"><code>testing</code></span> Utilities for writing PyO3 Asyncio tests
#[cfg(feature = "testing")]
pub mod testing;

#[cfg(feature = "async-std")]
pub mod async_std;

#[cfg(feature = "tokio-runtime")]
pub mod tokio;

/// Errors and exceptions related to PyO3 Asyncio
pub mod err;

pub mod generic;

#[pymodule]
fn pyo3_async_runtimes(py: Python, m: &Bound<PyModule>) -> PyResult<()> {
    m.add("RustPanic", py.get_type::<err::RustPanic>())?;
    Ok(())
}

/// Test README
#[doc(hidden)]
pub mod doc_test {
    #[allow(unused)]
    macro_rules! doc_comment {
        ($x:expr, $module:item) => {
            #[doc = $x]
            $module
        };
    }

    #[allow(unused)]
    macro_rules! doctest {
        ($x:expr, $y:ident) => {
            doc_comment!(include_str!($x), mod $y {});
        };
    }

    #[cfg(all(
        feature = "async-std-runtime",
        feature = "tokio-runtime",
        feature = "attributes"
    ))]
    doctest!("../README.md", readme_md);
}

use std::future::Future;
use std::sync::Arc;

use futures_channel::oneshot;
use pyo3::{call::PyCallArgs, prelude::*, sync::PyOnceLock, types::PyDict};

static ASYNCIO: PyOnceLock<Py<PyAny>> = PyOnceLock::new();
static CONTEXTVARS: PyOnceLock<Py<PyAny>> = PyOnceLock::new();
static ENSURE_FUTURE: PyOnceLock<Py<PyAny>> = PyOnceLock::new();
static GET_RUNNING_LOOP: PyOnceLock<Py<PyAny>> = PyOnceLock::new();

fn ensure_future<'p>(py: Python<'p>, awaitable: &Bound<'p, PyAny>) -> PyResult<Bound<'p, PyAny>> {
    ENSURE_FUTURE
        .get_or_try_init(py, || -> PyResult<Py<PyAny>> {
            Ok(asyncio(py)?
                .getattr(pyo3::intern!(py, "ensure_future"))?
                .into())
        })?
        .bind(py)
        .call1((awaitable,))
}

fn create_future(event_loop: Bound<'_, PyAny>) -> PyResult<Bound<'_, PyAny>> {
    event_loop.call_method0(pyo3::intern!(event_loop.py(), "create_future"))
}

fn close(event_loop: Bound<PyAny>) -> PyResult<()> {
    let py = event_loop.py();
    event_loop.call_method1(
        pyo3::intern!(py, "run_until_complete"),
        (event_loop.call_method0(pyo3::intern!(py, "shutdown_asyncgens"))?,),
    )?;

    // how to do this prior to 3.9?
    if event_loop.hasattr(pyo3::intern!(py, "shutdown_default_executor"))? {
        event_loop.call_method1(
            pyo3::intern!(py, "run_until_complete"),
            (event_loop.call_method0(pyo3::intern!(py, "shutdown_default_executor"))?,),
        )?;
    }

    event_loop.call_method0(pyo3::intern!(py, "close"))?;

    Ok(())
}

fn asyncio(py: Python<'_>) -> PyResult<&Bound<'_, PyAny>> {
    ASYNCIO
        .get_or_try_init(py, || Ok(py.import("asyncio")?.into()))
        .map(|asyncio| asyncio.bind(py))
}

/// Get a reference to the Python Event Loop from Rust
///
/// Equivalent to `asyncio.get_running_loop()` in Python 3.7+.
pub fn get_running_loop(py: Python) -> PyResult<Bound<PyAny>> {
    // Ideally should call get_running_loop, but calls get_event_loop for compatibility when
    // get_running_loop is not available.
    GET_RUNNING_LOOP
        .get_or_try_init(py, || -> PyResult<Py<PyAny>> {
            let asyncio = asyncio(py)?;

            Ok(asyncio
                .getattr(pyo3::intern!(py, "get_running_loop"))?
                .into())
        })?
        .bind(py)
        .call0()
}

fn contextvars(py: Python<'_>) -> PyResult<&Bound<'_, PyAny>> {
    Ok(CONTEXTVARS
        .get_or_try_init(py, || py.import("contextvars").map(|m| m.into()))?
        .bind(py))
}

fn copy_context(py: Python) -> PyResult<Bound<PyAny>> {
    contextvars(py)?.call_method0(pyo3::intern!(py, "copy_context"))
}

/// Task-local inner structure.
#[derive(Debug)]
struct TaskLocalsInner {
    /// Track the event loop of the Python task
    event_loop: Py<PyAny>,
    /// Track the contextvars of the Python task
    context: Py<PyAny>,
}

/// Task-local data to store for Python conversions.
#[derive(Debug)]
pub struct TaskLocals(Arc<TaskLocalsInner>);

impl TaskLocals {
    /// At a minimum, TaskLocals must store the event loop.
    pub fn new(event_loop: Bound<PyAny>) -> Self {
        Self(Arc::new(TaskLocalsInner {
            context: event_loop.py().None(),
            event_loop: event_loop.into(),
        }))
    }

    /// Construct TaskLocals with the event loop returned by `get_running_loop`
    pub fn with_running_loop(py: Python) -> PyResult<Self> {
        Ok(Self::new(get_running_loop(py)?))
    }

    /// Manually provide the contextvars for the current task.
    pub fn with_context(self, context: Bound<PyAny>) -> Self {
        Self(Arc::new(TaskLocalsInner {
            event_loop: self.0.event_loop.clone_ref(context.py()),
            context: context.into(),
        }))
    }

    /// Capture the current task's contextvars
    pub fn copy_context(self, py: Python) -> PyResult<Self> {
        Ok(self.with_context(copy_context(py)?))
    }

    /// Get a reference to the event loop
    pub fn event_loop<'p>(&self, py: Python<'p>) -> Bound<'p, PyAny> {
        self.0.event_loop.clone_ref(py).into_bound(py)
    }

    /// Get a reference to the python context
    pub fn context<'p>(&self, py: Python<'p>) -> Bound<'p, PyAny> {
        self.0.context.clone_ref(py).into_bound(py)
    }

    /// Create a clone of the TaskLocals. No longer uses the runtime, use `clone` instead.
    #[deprecated(note = "please use `clone` instead")]
    pub fn clone_ref(&self, _py: Python<'_>) -> Self {
        self.clone()
    }
}

impl Clone for TaskLocals {
    /// Create a clone of the TaskLocals by incrementing the reference counter of the inner
    /// structure.
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

#[pyclass]
struct PyTaskCompleter {
    tx: Option<oneshot::Sender<PyResult<Py<PyAny>>>>,
}

#[pymethods]
impl PyTaskCompleter {
    #[pyo3(signature = (task))]
    pub fn __call__(&mut self, task: &Bound<PyAny>) -> PyResult<()> {
        let py = task.py();
        debug_assert!(task.call_method0(pyo3::intern!(py, "done"))?.extract()?);
        let result = match task.call_method0(pyo3::intern!(py, "result")) {
            Ok(val) => Ok(val.into()),
            Err(e) => Err(e),
        };

        // unclear to me whether or not this should be a panic or silent error.
        //
        // calling PyTaskCompleter twice should not be possible, but I don't think it really hurts
        // anything if it happens.
        if let Some(tx) = self.tx.take() {
            if tx.send(result).is_err() {
                // cancellation is not an error
            }
        }

        Ok(())
    }
}

#[pyclass]
struct PyEnsureFuture {
    awaitable: Py<PyAny>,
    tx: Option<oneshot::Sender<PyResult<Py<PyAny>>>>,
}

#[pymethods]
impl PyEnsureFuture {
    pub fn __call__(&mut self) -> PyResult<()> {
        Python::attach(|py| {
            let task = ensure_future(py, self.awaitable.bind(py))?;
            let on_complete = PyTaskCompleter { tx: self.tx.take() };
            task.call_method1(pyo3::intern!(py, "add_done_callback"), (on_complete,))?;

            Ok(())
        })
    }
}

fn call_soon_threadsafe<'py>(
    event_loop: &Bound<'py, PyAny>,
    context: &Bound<PyAny>,
    args: impl PyCallArgs<'py>,
) -> PyResult<()> {
    let py = event_loop.py();

    let kwargs = PyDict::new(py);
    kwargs.set_item(pyo3::intern!(py, "context"), context)?;

    event_loop.call_method(
        pyo3::intern!(py, "call_soon_threadsafe"),
        args,
        Some(&kwargs),
    )?;
    Ok(())
}

/// Convert a Python `awaitable` into a Rust Future
///
/// This function converts the `awaitable` into a Python Task using `run_coroutine_threadsafe`. A
/// completion handler sends the result of this Task through a
/// `futures_channel::oneshot::Sender<PyResult<Py<PyAny>>>` and the future returned by this function
/// simply awaits the result through the `futures_channel::oneshot::Receiver<PyResult<Py<PyAny>>>`.
///
/// # Arguments
/// * `locals` - The Python event loop and context to be used for the provided awaitable
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
/// # #[cfg(feature = "tokio-runtime")]
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
///         pyo3_async_runtimes::into_future_with_locals(
///             &pyo3_async_runtimes::tokio::get_current_locals(py)?,
///             test_mod
///                 .call_method1(py, "py_sleep", (seconds,))?
///                 .into_bound(py),
///         )
///     })?
///     .await?;
///     Ok(())
/// }
/// ```
pub fn into_future_with_locals(
    locals: &TaskLocals,
    awaitable: Bound<PyAny>,
) -> PyResult<impl Future<Output = PyResult<Py<PyAny>>> + Send> {
    let py = awaitable.py();
    let (tx, rx) = oneshot::channel();

    call_soon_threadsafe(
        &locals.event_loop(py),
        &locals.context(py),
        (PyEnsureFuture {
            awaitable: awaitable.into(),
            tx: Some(tx),
        },),
    )?;

    Ok(async move {
        match rx.await {
            Ok(item) => item,
            Err(_) => Python::attach(|py| {
                Err(PyErr::from_value(
                    asyncio(py)?.call_method0(pyo3::intern!(py, "CancelledError"))?,
                ))
            }),
        }
    })
}

fn dump_err(py: Python<'_>) -> impl FnOnce(PyErr) + '_ {
    move |e| {
        // We can't display Python exceptions via std::fmt::Display,
        // so print the error here manually.
        e.print_and_set_sys_last_vars(py);
    }
}
