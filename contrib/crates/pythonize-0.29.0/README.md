# Pythonize

This is a serializer for Rust's serde ecosystem, which can convert Rust objects to Python values and back.

At the moment the Python structures it produces should be _very_ similar to those which are produced by `serde_json`; i.e. calling Python's `json.loads()` on a value encoded by `serde_json` should produce an identical structure to
that which is produced directly by `pythonize`.

## Usage

This crate converts Rust types which implement the [Serde] serialization
traits into Python objects using the [PyO3] library.

Pythonize has two main public APIs: `pythonize` and `depythonize`.

</div>

[Serde]: https://github.com/serde-rs/serde
[PyO3]: https://github.com/PyO3/pyo3

## Examples

```rust
use serde::{Serialize, Deserialize};
use pyo3::prelude::*;
use pythonize::{depythonize, pythonize};

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct Sample {
    foo: String,
    bar: Option<usize>
}

let sample = Sample {
    foo: "Foo".to_string(),
    bar: None
};

Python::attach(|py| {
    // Rust -> Python
    let obj =  pythonize(py, &sample).unwrap();

    assert_eq!("{'foo': 'Foo', 'bar': None}", &format!("{}", obj.repr().unwrap()));

    // Python -> Rust
    let new_sample: Sample = depythonize(&obj).unwrap();

    assert_eq!(new_sample, sample);
})
```

## Features

### `arbitrary_precision`

Enable support for `serde_json`'s `arbitrary_precision` feature, which allows handling numbers that exceed the range of `i128`/`u128` when converting `serde_json::Value` to and from Python.

```toml
[dependencies]
pythonize = { version = "0.29", features = ["arbitrary_precision"] }
```
