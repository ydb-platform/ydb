#[cfg(not(target_os = "windows"))]
fn main() -> pyo3::PyResult<()> {
    use pyo3::{prelude::*, types::PyType};

    Python::initialize();

    Python::attach(|py| {
        // uvloop not supported on the free-threaded build yet
        // https://github.com/MagicStack/uvloop/issues/642
        let sysconfig = py.import("sysconfig")?;
        let is_freethreaded = sysconfig.call_method1("get_config_var", ("Py_GIL_DISABLED",))?;
        if is_freethreaded.is_truthy()? {
            return Ok(());
        }

        // uvloop not yet supported on 3.14
        if py.version_info() >= (3, 14) {
            return Ok(());
        }

        let uvloop = py.import("uvloop")?;
        uvloop.call_method0("install")?;

        // store a reference for the assertion
        let uvloop: Py<PyAny> = uvloop.into();

        pyo3_async_runtimes::tokio::run(py, async move {
            // verify that we are on a uvloop.Loop
            Python::attach(|py| -> PyResult<()> {
                assert!(pyo3_async_runtimes::tokio::get_current_loop(py)?
                    .is_instance(uvloop.bind(py).getattr("Loop")?.cast::<PyType>().unwrap())?);
                Ok(())
            })?;

            tokio::time::sleep(std::time::Duration::from_secs(1)).await;

            println!("test test_tokio_multi_thread_uvloop ... ok");
            Ok(())
        })
    })
}

#[cfg(target_os = "windows")]
fn main() {}
