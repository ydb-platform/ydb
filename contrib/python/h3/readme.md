<img align="right" src="https://uber.github.io/img/h3Logo-color.svg" alt="H3 Logo" width="200">

# **h3-py**: Uber's H3 Hexagonal Hierarchical Geospatial Indexing System in Python

<!-- TODO: have a nice 3d image of hexagons up front -->

[![PyPI version](https://badge.fury.io/py/h3.svg)](https://badge.fury.io/py/h3)
[![PyPI downloads](https://img.shields.io/pypi/dm/h3.svg)](https://pypistats.org/packages/h3)
[![conda](https://img.shields.io/conda/vn/conda-forge/h3-py.svg)](https://anaconda.org/conda-forge/h3-py)
[![version](https://img.shields.io/badge/h3-v3.7.1-blue.svg)](https://github.com/uber/h3/releases/tag/v3.7.1)
[![version](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/uber/h3-py/blob/master/LICENSE)

[![Tests](https://github.com/uber/h3-py/workflows/tests/badge.svg)](https://github.com/uber/h3-py/actions)
[![codecov](https://codecov.io/gh/uber/h3-py/branch/master/graph/badge.svg)](https://codecov.io/gh/uber/h3-py)

Python bindings for the [H3 core library](https://h3geo.org/).

- Documentation: [uber.github.io/h3-py](https://uber.github.io/h3-py)
- GitHub repo: [github.com/uber/h3-py](https://github.com/uber/h3-py)


## Installation

From [PyPI](https://pypi.org/project/h3/):

```console
pip install h3
```

From [conda](https://github.com/conda-forge/h3-py-feedstock):

```console
conda config --add channels conda-forge
conda install h3-py
```


## Usage

```python
>>> import h3
>>> lat, lng = 37.769377, -122.388903
>>> resolution = 9
>>> h3.geo_to_h3(lat, lng, resolution)
'89283082e73ffff'
```


## APIs

[api_comparison]: https://uber.github.io/h3-py/api_comparison
[api_reference]: https://uber.github.io/h3-py/api_reference

We provide [multiple APIs][api_comparison] in `h3-py`.

- All APIs have the same set of functions;
  see the [API reference][api_reference].
- The APIs differ only in their input/output formats;
  see the [API comparison page][api_comparison].


## Example gallery

Browse [a collection of example notebooks](https://github.com/uber/h3-py-notebooks),
and if you have examples or visualizations of your own, please feel free
to contribute!

[walkthrough]: https://nbviewer.jupyter.org/github/uber/h3-py-notebooks/blob/master/notebooks/usage.ipynb

We also have an introductory [walkthrough of the API][walkthrough].


## Versioning

<!-- todo: this should just be the h3.versions() docstring, yeah? -->

`h3-py` wraps the [H3 core library](https://github.com/uber/h3),
which is written in C.
The C and Python projects each employ
[semantic versioning](https://semver.org/),
where versions take the form `X.Y.Z`.

The `h3-py` version string is guaranteed to match the C library string
in both *major* and *minor* numbers (`X.Y`), but may differ on the
*patch* (`Z`) number.
This convention provides users with information on breaking changes and
feature additions, while providing downstream bindings (like this one!)
with the versioning freedom to fix bugs.

Use `h3.versions()` to see the version numbers for both
`h3-py` and the C library. For example,

```python
>>> import h3
>>> h3.versions()
{'c': '3.6.3', 'python': '3.6.1'}
```
