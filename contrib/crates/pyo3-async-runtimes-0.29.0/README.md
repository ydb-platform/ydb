# Async Runtime Integrations for PyO3

[![Actions Status](https://github.com/PyO3/pyo3-async-runtimes/workflows/CI/badge.svg)](https://github.com/PyO3/pyo3-async-runtimes)
[![codecov](https://codecov.io/gh/davidhewitt/pyo3-async-runtimes/branch/main/graph/badge.svg)](https://codecov.io/gh/PyO3/pyo3-async-runtimes)
[![crates.io](https://img.shields.io/crates/v/pyo3-async-runtimes)](https://crates.io/crates/pyo3-async-runtimes)
[![minimum rustc 1.63](https://img.shields.io/badge/rustc-1.63+-blue.svg)](https://rust-lang.github.io/rfcs/2495-min-rust-version.html)

**_Forked from [`pyo3-asyncio`](https://github.com/awestlake87/pyo3-asyncio/) to deliver compatibility for PyO3 0.21+._**

[Rust](http://www.rust-lang.org/) bindings for [Python](https://www.python.org/)'s [Asyncio Library](https://docs.python.org/3/library/asyncio.html). This crate facilitates interactions between Rust Futures and Python Coroutines and manages the lifecycle of their corresponding event loops.

- PyO3 Project: [Homepage](https://pyo3.rs/) | [GitHub](https://github.com/PyO3/pyo3)

- `pyo3-async-runtimes` API Documentation: [stable](https://docs.rs/pyo3-async-runtimes/)

- Guide for Async / Await [stable](https://pyo3.rs/latest/ecosystem/async-await.html) | [main](https://pyo3.rs/main/ecosystem/async-await.html)

- Contributing Notes: [github](https://github.com/PyO3/pyo3-async-runtimes/blob/main/Contributing.md)

## Usage

`pyo3-async-runtimes` supports the following software versions:

- Python 3.9 and up (CPython and PyPy)
- Rust 1.83 and up

## `pyo3-async-runtimes` Primer

If you are working with a Python library that makes use of async functions or wish to provide
Python bindings for an async Rust library, [`pyo3-async-runtimes`](https://github.com/PyO3/pyo3-async-runtimes)
likely has the tools you need. It provides conversions between async functions in both Python and
Rust and was designed with first-class support for popular Rust runtimes such as
[`tokio`](https://tokio.rs/) and [`async-std`](https://async.rs/). In addition, all async Python
code runs on the default `asyncio` event loop, so `pyo3-async-runtimes` should work just fine with existing
Python libraries.

In the following sections, we'll give a general overview of `pyo3-async-runtimes` explaining how to call
async Python functions with PyO3, how to call async Rust functions from Python, and how to configure
your codebase to manage the runtimes of both.

### Quickstart

Here are some examples to get you started right away! A more detailed breakdown
of the concepts in these examples can be found in the following sections.

#### Rust Applications

Here we initialize the runtime, import Python's `asyncio` library and run the given future to completion using Python's default `EventLoop` and `async-std`. Inside the future, we convert `asyncio` sleep into a Rust future and await it.

```toml
# Cargo.toml dependencies
[dependencies]
pyo3 = { version = "0.29" }
pyo3-async-runtimes = { version = "0.29", features = ["attributes", "async-std-runtime"] }
async-std = "1.13"
```

```rust
//! main.rs

use pyo3::prelude::*;

#[pyo3_async_runtimes::async_std::main]
async fn main() -> PyResult<()> {
    let fut = Python::attach(|py| {
        let asyncio = py.import("asyncio")?;
        // convert asyncio.sleep into a Rust Future
        pyo3_async_runtimes::async_std::into_future(asyncio.call_method1("sleep", (1,))?)
    })?;

    fut.await?;

    Ok(())
}
```

The same application can be written to use `tokio` instead using the `#[pyo3_async_runtimes::tokio::main]`
attribute.

```toml
# Cargo.toml dependencies
[dependencies]
pyo3 = { version = "0.29" }
pyo3-async-runtimes = { version = "0.29", features = ["attributes", "tokio-runtime"] }
tokio = "1.40"
```

```rust
//! main.rs

use pyo3::prelude::*;

#[pyo3_async_runtimes::tokio::main]
async fn main() -> PyResult<()> {
    let fut = Python::attach(|py| {
        let asyncio = py.import("asyncio")?;
        // convert asyncio.sleep into a Rust Future
        pyo3_async_runtimes::tokio::into_future(asyncio.call_method1("sleep", (1,))?)
    })?;

    fut.await?;

    Ok(())
}
```

More details on the usage of this library can be found in the API docs
and the primer below.

#### PyO3 Native Rust Modules

`pyo3-async-runtimes` can also be used to write native modules with async functions.

Add the `[lib]` section to `Cargo.toml` to make your library a `cdylib` that Python can import.

```toml
[lib]
name = "my_async_module"
crate-type = ["cdylib"]
```

Make your project depend on `pyo3` with the `extension-module` feature enabled and select your
`pyo3-async-runtimes` runtime:

For `async-std`:

```toml
[dependencies]
pyo3 = { version = "0.29", features = ["extension-module"] }
pyo3-async-runtimes = { version = "0.29", features = ["async-std-runtime"] }
async-std = "1.13"
```

For `tokio`:

```toml
[dependencies]
pyo3 = { version = "0.29", features = ["extension-module"] }
pyo3-async-runtimes = { version = "0.29", features = ["tokio-runtime"] }
tokio = "1.40"
```

Export an async function that makes use of `async-std`:

```rust
//! lib.rs

use pyo3::{prelude::*, wrap_pyfunction};

#[pyfunction]
fn rust_sleep(py: Python) -> PyResult<Bound<PyAny>> {
    pyo3_async_runtimes::async_std::future_into_py(py, async {
        async_std::task::sleep(std::time::Duration::from_secs(1)).await;
        Ok(())
    })
}

#[pymodule]
fn my_async_module(py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(rust_sleep, m)?)?;

    Ok(())
}

```

If you want to use `tokio` instead, here's what your module should look like:

```rust
//! lib.rs

use pyo3::{prelude::*, wrap_pyfunction};

#[pyfunction]
fn rust_sleep(py: Python) -> PyResult<Bound<PyAny>> {
    pyo3_async_runtimes::tokio::future_into_py(py, async {
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        Ok(())
    })
}

#[pymodule]
fn my_async_module(py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(rust_sleep, m)?)?;
    Ok(())
}
```

You can build your module with maturin (see the [Using Rust in Python](https://pyo3.rs/main/#using-rust-from-python) section in the PyO3 guide for setup instructions). After that you should be able to run the Python REPL to try it out.

```bash
maturin develop && python3
🔗 Found pyo3 bindings
🐍 Found CPython 3.8 at python3
    Finished dev [unoptimized + debuginfo] target(s) in 0.04s
Python 3.8.5 (default, Jan 27 2021, 15:41:15)
[GCC 9.3.0] on linux
Type "help", "copyright", "credits" or "license" for more information.
>>> import asyncio
>>>
>>> from my_async_module import rust_sleep
>>>
>>> async def main():
>>>     await rust_sleep()
>>>
>>> # should sleep for 1s
>>> asyncio.run(main())
>>>
```

### Awaiting an Async Python Function in Rust

Let's take a look at a dead simple async Python function:

```python
# Sleep for 1 second
async def py_sleep():
    await asyncio.sleep(1)
```

**Async functions in Python are simply functions that return a `coroutine` object**. For our purposes,
we really don't need to know much about these `coroutine` objects. The key factor here is that calling
an `async` function is _just like calling a regular function_, the only difference is that we have
to do something special with the object that it returns.

Normally in Python, that something special is the `await` keyword, but in order to await this
coroutine in Rust, we first need to convert it into Rust's version of a `coroutine`: a `Future`.
That's where `pyo3-async-runtimes` comes in.
[`pyo3_async_runtimes::into_future`](https://docs.rs/pyo3-async-runtimes/latest/pyo3_async_runtimes/fn.into_future.html)
performs this conversion for us:

```rust no_run
use pyo3::prelude::*;

#[pyo3_async_runtimes::tokio::main]
async fn main() -> PyResult<()> {
    let future = Python::attach(|py| -> PyResult<_> {
        // import the module containing the py_sleep function
        let example = py.import("example")?;

        // calling the py_sleep method like a normal function
        // returns a coroutine
        let coroutine = example.call_method0("py_sleep")?;

        // convert the coroutine into a Rust future using the
        // tokio runtime
        pyo3_async_runtimes::tokio::into_future(coroutine)
    })?;

    // await the future
    future.await?;

    Ok(())
}
```

> If you're interested in learning more about `coroutines` and `awaitables` in general, check out the
> [Python 3 `asyncio` docs](https://docs.python.org/3/library/asyncio-task.html) for more information.

### Awaiting a Rust Future in Python

Here we have the same async function as before written in Rust using the
[`async-std`](https://async.rs/) runtime:

```rust
/// Sleep for 1 second
async fn rust_sleep() {
    async_std::task::sleep(std::time::Duration::from_secs(1)).await;
}
```

Similar to Python, Rust's async functions also return a special object called a
`Future`:

```rust compile_fail
let future = rust_sleep();
```

We can convert this `Future` object into Python to make it `awaitable`. This tells Python that you
can use the `await` keyword with it. In order to do this, we'll call
[`pyo3_async_runtimes::async_std::future_into_py`](https://docs.rs/pyo3-async-runtimes/latest/pyo3_async_runtimes/async_std/fn.future_into_py.html):

```rust
use pyo3::prelude::*;

async fn rust_sleep() {
    async_std::task::sleep(std::time::Duration::from_secs(1)).await;
}

#[pyfunction]
fn call_rust_sleep(py: Python) -> PyResult<Bound<PyAny>> {
    pyo3_async_runtimes::async_std::future_into_py(py, async move {
        rust_sleep().await;
        Ok(())
    })
}
```

In Python, we can call this pyo3 function just like any other async function:

```python
from example import call_rust_sleep

async def rust_sleep():
    await call_rust_sleep()
```

## Managing Event Loops

Python's event loop requires some special treatment, especially regarding the main thread. Some of
Python's `asyncio` features, like proper signal handling, require control over the main thread, which
doesn't always play well with Rust.

Luckily, Rust's event loops are pretty flexible and don't _need_ control over the main thread, so in
`pyo3-async-runtimes`, we decided the best way to handle Rust/Python interop was to just surrender the main
thread to Python and run Rust's event loops in the background. Unfortunately, since most event loop
implementations _prefer_ control over the main thread, this can still make some things awkward.

### `pyo3-async-runtimes` Initialization

Because Python needs to control the main thread, we can't use the convenient proc macros from Rust
runtimes to handle the `main` function or `#[test]` functions. Instead, the initialization for PyO3 has to be done from the `main` function and the main
thread must block on [`pyo3_async_runtimes::async_std::run_until_complete`](https://docs.rs/pyo3-async-runtimes/latest/pyo3_async_runtimes/async_std/fn.run_until_complete.html).

Because we have to block on one of those functions, we can't use [`#[async_std::main]`](https://docs.rs/async-std/latest/async_std/attr.main.html) or [`#[tokio::main]`](https://docs.rs/tokio/1.1.0/tokio/attr.main.html)
since it's not a good idea to make long blocking calls during an async function.

> Internally, these `#[main]` proc macros are expanded to something like this:
>
> ```rust compile_fail
> fn main() {
>     // your async main fn
>     async fn _main_impl() { /* ... */ }
>     Runtime::new().block_on(_main_impl());
> }
> ```
>
> Making a long blocking call inside the `Future` that's being driven by `block_on` prevents that
> thread from doing anything else and can spell trouble for some runtimes (also this will actually
> deadlock a single-threaded runtime!). Many runtimes have some sort of `spawn_blocking` mechanism
> that can avoid this problem, but again that's not something we can use here since we need it to
> block on the _main_ thread.

For this reason, `pyo3-async-runtimes` provides its own set of proc macros to provide you with this
initialization. These macros are intended to mirror the initialization of `async-std` and `tokio`
while also satisfying the Python runtime's needs.

Here's a full example of PyO3 initialization with the `async-std` runtime:

```rust no_run
use pyo3::prelude::*;

#[pyo3_async_runtimes::async_std::main]
async fn main() -> PyResult<()> {
    // PyO3 is initialized - Ready to go

    let fut = Python::attach(|py| -> PyResult<_> {
        let asyncio = py.import("asyncio")?;

        // convert asyncio.sleep into a Rust Future
        pyo3_async_runtimes::async_std::into_future(
            asyncio.call_method1("sleep", (1,))?
        )
    })?;

    fut.await?;

    Ok(())
}
```

#### A Note About `asyncio.run`

In Python 3.7+, the recommended way to run a top-level coroutine with `asyncio`
is with `asyncio.run`. In `v0.13` we recommended against using this function due to initialization issues, but in `v0.14` it's perfectly valid to use this function... with a caveat.

Since our Rust <--> Python conversions require a reference to the Python event loop, this poses a problem. Imagine we have a `pyo3-async-runtimes` module that defines
a `rust_sleep` function like in previous examples. You might rightfully assume that you can call pass this directly into `asyncio.run` like this:

```python
import asyncio

from my_async_module import rust_sleep

asyncio.run(rust_sleep())
```

You might be surprised to find out that this throws an error:

```bash
Traceback (most recent call last):
  File "example.py", line 5, in <module>
    asyncio.run(rust_sleep())
RuntimeError: no running event loop
```

What's happening here is that we are calling `rust_sleep` _before_ the future is
actually running on the event loop created by `asyncio.run`. This is counter-intuitive, but expected behaviour, and unfortunately there doesn't seem to be a good way of solving this problem within `pyo3-async-runtimes` itself.

However, we can make this example work with a simple workaround:

```python
import asyncio

from my_async_module import rust_sleep

# Calling main will just construct the coroutine that later calls rust_sleep.
# - This ensures that rust_sleep will be called when the event loop is running,
#   not before.
async def main():
    await rust_sleep()

# Run the main() coroutine at the top-level instead
asyncio.run(main())
```

#### Non-standard Python Event Loops

Python allows you to use alternatives to the default `asyncio` event loop. One
popular alternative is `uvloop`. In `v0.13` using non-standard event loops was
a bit of an ordeal, but in `v0.14` it's trivial.

#### Using `uvloop` in a PyO3 Native Extensions

```toml
# Cargo.toml

[lib]
name = "my_async_module"
crate-type = ["cdylib"]

[dependencies]
pyo3 = { version = "0.29", features = ["extension-module"] }
pyo3-async-runtimes = { version = "0.29", features = ["tokio-runtime"] }
async-std = "1.13"
tokio = "1.40"
```

```rust
//! lib.rs

use pyo3::{prelude::*, wrap_pyfunction};

#[pyfunction]
fn rust_sleep(py: Python) -> PyResult<Bound<PyAny>> {
    pyo3_async_runtimes::tokio::future_into_py(py, async {
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        Ok(())
    })
}

#[pymodule]
fn my_async_module(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(rust_sleep, m)?)?;

    Ok(())
}
```

```bash
$ maturin develop && python3
🔗 Found pyo3 bindings
🐍 Found CPython 3.8 at python3
    Finished dev [unoptimized + debuginfo] target(s) in 0.04s
Python 3.8.8 (default, Apr 13 2021, 19:58:27)
[GCC 7.3.0] :: Anaconda, Inc. on linux
Type "help", "copyright", "credits" or "license" for more information.
>>> import asyncio
>>> import uvloop
>>>
>>> import my_async_module
>>>
>>> uvloop.install()
>>>
>>> async def main():
...     await my_async_module.rust_sleep()
...
>>> asyncio.run(main())
>>>
```

#### Using `uvloop` in Rust Applications

Using `uvloop` in Rust applications is a bit trickier, but it's still possible
with relatively few modifications.

Unfortunately, we can't make use of the `#[pyo3_async_runtimes::<runtime>::main]` attribute with non-standard event loops. This is because the `#[pyo3_async_runtimes::<runtime>::main]` proc macro has to interact with the Python
event loop before we can install the `uvloop` policy.

```toml
[dependencies]
async-std = "1.13"
pyo3 = "0.29"
pyo3-async-runtimes = { version = "0.29", features = ["async-std-runtime"] }
```

```rust no_run
//! main.rs

use pyo3::{prelude::*, types::PyType};

fn main() -> PyResult<()> {
    Python::initialize();

    Python::attach(|py| {
        let uvloop = py.import("uvloop")?;
        uvloop.call_method0("install")?;

        // store a reference for the assertion
        let uvloop: Py<PyAny> = uvloop.into();

        pyo3_async_runtimes::async_std::run(py, async move {
            // verify that we are on a uvloop.Loop
            Python::attach(|py| -> PyResult<()> {
                assert!(uvloop
                    .bind(py)
                    .getattr("Loop")?
                    .cast::<PyType>()
                    .unwrap()
                    .is_instance(&pyo3_async_runtimes::async_std::get_current_loop(py)?)?);
                Ok(())
            })?;

            async_std::task::sleep(std::time::Duration::from_secs(1)).await;

            Ok(())
        })
    })
}
```

### Additional Information

- Managing event loop references can be tricky with `pyo3-async-runtimes`. See [Event Loop References and ContextVars](https://docs.rs/pyo3-async-runtimes/latest/pyo3_async_runtimes/#event-loop-references-and-contextvars) in the API docs to get a better intuition for how event loop references are managed in this library.
- Testing `pyo3-async-runtimes` libraries and applications requires a custom test harness since Python requires control over the main thread. You can find a testing guide in the [API docs for the `testing` module](https://docs.rs/pyo3-async-runtimes/latest/pyo3_async_runtimes/testing/index.html)
