use std::ffi::CString;
use std::{thread, time::Duration};

use pyo3::prelude::*;
use pyo3_async_runtimes::TaskLocals;

pub(super) const TEST_MOD: &str = r#"
import asyncio

async def py_sleep(duration):
    await asyncio.sleep(duration)

async def sleep_for_1s(sleep_for):
    await sleep_for(1)
"#;

pub(super) async fn test_into_future(event_loop: Py<PyAny>) -> PyResult<()> {
    let fut = Python::attach(|py| {
        let test_mod = PyModule::from_code(
            py,
            &CString::new(TEST_MOD).unwrap(),
            &CString::new("test_rust_coroutine/test_mod.py").unwrap(),
            &CString::new("test_mod").unwrap(),
        )?;

        pyo3_async_runtimes::into_future_with_locals(
            &TaskLocals::new(event_loop.into_bound(py)),
            test_mod.call_method1("py_sleep", (1,))?,
        )
    })?;

    fut.await?;

    Ok(())
}

pub(super) fn test_blocking_sleep() -> PyResult<()> {
    thread::sleep(Duration::from_secs(1));
    Ok(())
}

pub(super) async fn test_other_awaitables(event_loop: Py<PyAny>) -> PyResult<()> {
    let fut = Python::attach(|py| {
        let functools = py.import("functools")?;
        let time = py.import("time")?;

        // spawn a blocking sleep in the threadpool executor - returns a task, not a coroutine
        let task = event_loop.bind(py).call_method1(
            "run_in_executor",
            (
                py.None(),
                functools.call_method1("partial", (time.getattr("sleep")?, 1))?,
            ),
        )?;

        pyo3_async_runtimes::into_future_with_locals(
            &TaskLocals::new(event_loop.into_bound(py)),
            task,
        )
    })?;

    fut.await?;

    Ok(())
}
