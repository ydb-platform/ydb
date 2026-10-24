use pyo3::Python;

mod tokio_run_forever;

fn main() {
    Python::initialize();

    tokio_run_forever::test_main();
    println!("test test_tokio_multi_thread_run_forever ... ok");
}
