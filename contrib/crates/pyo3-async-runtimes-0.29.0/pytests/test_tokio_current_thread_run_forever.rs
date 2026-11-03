use pyo3::Python;

mod tokio_run_forever;

fn main() {
    Python::initialize();

    let mut builder = tokio::runtime::Builder::new_current_thread();
    builder.enable_all();

    pyo3_async_runtimes::tokio::init(builder);
    std::thread::spawn(move || {
        pyo3_async_runtimes::tokio::get_runtime().block_on(futures_util::future::pending::<()>());
    });

    tokio_run_forever::test_main();
    println!("test test_tokio_current_thread_run_forever ... ok");
}
