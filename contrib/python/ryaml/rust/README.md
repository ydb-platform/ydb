ryaml
=====

### *Quickly and safely parse yaml*

### What is ryaml?

ryaml is a Python library that wraps a Rust yaml parser, [serde-yaml](https://github.com/dtolnay/serde-yaml), to quickly and safely parse and dump yaml to and from Python objects.

It is *not* fully compatible with PyYAML, and it has a similar design to the `json` module.
The hope is this will be used as a safe and fast yaml parser in lieu of PyYAML.

Like PyYAML, this library implements the YAML 1.1 specification.

## Installation

We ship binary wheels for Windows, Linux, and macOS, so as long as you are using Python 3.10+,
you can run:

```
$ python -m pip install ryaml
```

Otherwise, you will need to build from source. To do so, first install Rust 1.41 stable.

Then you should be able to just

```shell
$ git clone https://github.com/emmatyping/ryaml
$ cd ryaml
$ python -m pip install .
```

Or if you want to build a wheel:

```shell
$ git clone https://github.com/emmatyping/ryaml
$ cd ryaml
$ python -m pip install maturin
$ maturin build --release --no-sdist
# OR if you want an abi3 wheel (compatible with Python 3.10+)
$ maturin build --release --no-sdist --cargo-extra-args="--features=abi3"
```

And a wheel will be created in `target/wheels` which you can install.

## Usage

The API of `ryaml` is very similar to that of `json` in the standard library:

You can use `ryaml.loads` to read from a `str`:

```python
import ryaml
obj = ryaml.loads('key: [10, "hi"]')
assert isinstance(obj, dict) # True
assert obj['key'][1] == "hi" # True
```

And `ryaml.dumps` to dump an object into a yaml file:

```python
import ryaml
s = ryaml.dumps({ 'key' : None })
print(s)
# prints:
# ---
# key: ~
```

There are also `ryaml.load` and `ryaml.load_all` to read yaml document(s) from files:

```python
import ryaml
obj = {'a': [{'b': 1}]}
with open('test.yaml', 'w') as w:
    ryaml.dump(w, obj)
with open('test.yaml', 'r') as r:
    assert ryaml.load(r) == obj
with open('multidoc.yaml', 'w') as multi:
    multi.write('''
---
a:
  key:
...
---
b:
  key:
    ''')
with open('multidoc.yaml', 'r') as multi:
    docs = ryaml.load_all(multi)
assert len(docs) == 2
assert docs[0]['a']['key'] is None
```

`ryaml.load_all` will, as seen above, load multiple documents from a single file.


## Thanks

This project is standing on the shoulders of giants, and would not be possible without:

[pyo3](https://pyo3.rs/)

[serde-yaml](https://github.com/dtolnay/serde-yaml)

[yaml-rust](https://github.com/chyh1990/yaml-rust)

[pythonize](https://github.com/davidhewitt/pythonize)
