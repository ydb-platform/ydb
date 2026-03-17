# fastavro
[![Build Status](https://github.com/fastavro/fastavro/workflows/Build/badge.svg)](https://github.com/fastavro/fastavro/actions)
[![Documentation Status](https://readthedocs.org/projects/fastavro/badge/?version=latest)](http://fastavro.readthedocs.io/en/latest/?badge=latest)
[![codecov](https://codecov.io/gh/fastavro/fastavro/branch/master/graph/badge.svg)](https://codecov.io/gh/fastavro/fastavro)


Because the Apache Python `avro` package is written in pure Python, it is
relatively slow. In one test case, it takes about 14 seconds to iterate through
a file of 10,000 records. By comparison, the JAVA `avro` SDK reads the same file in
1.9 seconds.

The `fastavro` library was written to offer performance comparable to the Java
library. With regular CPython, `fastavro` uses C extensions which allow it to
iterate the same 10,000 record file in 1.7 seconds. With PyPy, this drops to 1.5
seconds (to be fair, the JAVA benchmark is doing some extra JSON
encoding/decoding).

`fastavro` supports the following Python versions:

* Python 3.9
* Python 3.10
* Python 3.11
* Python 3.12
* Python 3.13
* Python 3.14
* PyPy3

## Supported Features

* File Writer
* File Reader (iterating via records or blocks)
* Schemaless Writer
* Schemaless Reader
* JSON Writer
* JSON Reader
* Codecs (Snappy, Deflate, Zstandard, Bzip2, LZ4, XZ)
* Schema resolution
* Aliases
* Logical Types
* Parsing schemas into the canonical form
* Schema fingerprinting

## Missing Features

* Anything involving Avro's RPC features

[Cython]: http://cython.org/

# Documentation

Documentation is available at http://fastavro.readthedocs.io/en/latest/

# Installing
`fastavro` is available both on [PyPI](http://pypi.python.org/pypi)

    pip install fastavro

and on [conda-forge](https://conda-forge.github.io) `conda` channel.

    conda install -c conda-forge fastavro

# Contributing

* Bugs and new feature requests typically start as GitHub issues where they can be discussed. I try to resolve these as time affords, but PRs are welcome from all.
* Get approval from discussing on the GitHub issue before opening the pull request
* Tests must be passing for pull request to be considered

Developer requirements can be installed with `pip install -r developer_requirements.txt`.
If those are installed, you can run the tests with `./run-tests.sh`. If you have trouble
installing those dependencies, you can run `docker build .` to run the tests inside
a Docker container. This won't test on all versions of Python or on PyPy, so it's possible
to still get CI failures after making a pull request, but we can work through those errors
if/when they happen. `.run-tests.sh` only covers the Cython tests. In order to test the
pure Python implementation, comment out `python setup.py build_ext --inplace`
and re-run.

NOTE: Some tests might fail when running the tests locally. An example of this
is this codec tests. If the supporting codec library is not available, the test
will fail. These failures can be ignored since the tests will on pull requests
and will be run in the correct environments with the correct dependencies set up.

### Releasing

We release both to [PyPI][pypi] and to [conda-forge][conda-forge].

We assume you have [twine][twine] installed and that you've created your own
fork of [fastavro-feedstock][feedstock].

* Make sure the tests pass
* Run `make tag`
* Wait for all artifacts to be built and published to the GitHub release.
* Run `make publish`
* The conda-forge PR should get created and merged automatically

[conda-forge]: https://conda-forge.org/
[feedstock]: https://github.com/conda-forge/fastavro-feedstock
[pypi]: https://pypi.python.org/pypi
[twine]: https://pypi.python.org/pypi/twine


# Changes

See the [ChangeLog]

[ChangeLog]: https://github.com/fastavro/fastavro/blob/master/ChangeLog

# Contact

[Project Home](https://github.com/fastavro/fastavro)
