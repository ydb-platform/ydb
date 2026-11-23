use std::ffi::CString;

use pyo3::{prelude::*, wrap_pyfunction};

#[pyfunction]
fn sleep<'p>(py: Python<'p>, secs: Bound<'p, PyAny>) -> PyResult<Bound<'p, PyAny>> {
    let secs = secs.extract()?;

    pyo3_async_runtimes::async_std::future_into_py(py, async move {
        async_std::task::sleep(std::time::Duration::from_secs_f64(secs)).await;
        Ok(())
    })
}

const RACE_CONDITION_REGRESSION_TEST: &str = r#"
import asyncio
import random

async def trigger_race_condition(rust_sleeper, delay):
    coro = asyncio.wrap_future(rust_sleeper(0.1))
    await asyncio.sleep(delay)
    coro.cancel()

def main(rust_sleeper):
    race_condition_triggered = False

    for i in range(1000):
        delay = random.uniform(0.099, 0.101)
        loop = asyncio.new_event_loop()
        loop.set_debug(True)

        def custom_exception_handler(loop, context):
            nonlocal race_condition_triggered
            race_condition_triggered = True

        try:
            loop.set_exception_handler(custom_exception_handler)
            loop.run_until_complete(trigger_race_condition(rust_sleeper, delay))

            if race_condition_triggered:
                raise Exception("Race condition triggered")

        finally:
            loop.run_until_complete(loop.shutdown_asyncgens())
            if hasattr(loop, 'shutdown_default_executor'):
                loop.run_until_complete(loop.shutdown_default_executor())
            loop.close()
"#;

fn main() -> pyo3::PyResult<()> {
    Python::initialize();

    Python::attach(|py| -> PyResult<()> {
        let sleeper_mod = PyModule::new(py, "rust_sleeper")?;

        sleeper_mod.add_wrapped(wrap_pyfunction!(sleep))?;

        let test_mod = PyModule::from_code(
            py,
            &CString::new(RACE_CONDITION_REGRESSION_TEST).unwrap(),
            &CString::new("race_condition_regression_test.py").unwrap(),
            &CString::new("race_condition_regression_test").unwrap(),
        )?;

        test_mod.call_method1("main", (sleeper_mod.getattr("sleep")?,))?;
        Ok(())
    })
}
