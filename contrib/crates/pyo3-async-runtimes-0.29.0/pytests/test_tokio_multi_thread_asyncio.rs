mod common;
mod tokio_asyncio;

use pyo3::prelude::*;

fn main() -> pyo3::PyResult<()> {
    Python::initialize();

    Python::attach(|py| pyo3_async_runtimes::tokio::run(py, pyo3_async_runtimes::testing::main()))
}
