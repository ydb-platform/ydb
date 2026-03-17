# OpenGrm Pynini

This is a a Python extension module for compiling, optimizing and applying
grammar rules. Rules can be compiled into weighted finite state transducers,
pushdown transducers, or multi-pushdown transducers. It uses OpenFst
finite-state transducers (FSTs) and FST archives (FArs) as inputs and outputs.

This library is primarily developed by [Kyle Gorman](mailto:kbg@google.com).

If you use Pynini in your research, we would appreciate if you cite the
following paper:

> K. Gorman. 2016.
> [Pynini: A Python library for weighted finite-state grammar compilation](http://openfst.cs.nyu.edu/twiki/pub/GRM/Pynini/pynini-paper.pdf).
> In *Proc. ACL Workshop on Statistical NLP and Weighted Automata*, 75-80.

(Note that some of the code samples in the paper are now out of date and not
expected to work.)

## Dependencies

-   A standards-compliant C++17 compiler (GCC \>= 7 or Clang \>= 700)
-   The compatible recent version of [OpenFst](http://openfst.org) (see
    [`NEWS`](NEWS) for this) built with the `grm` extensions (i.e., built with
    `./configure --enable-grm`) and headers
-   [Python 3.6+](https://www.python.org) and headers

## Installation instructions

There are various ways to install Pynini depending on your platform.

### Windows

While Pynini is neither designed for nor tested on Windows, it can be installed
using the
[Windows Subsystem for Linux](https://docs.microsoft.com/en-us/windows/wsl/install-win10)
(WSL). Simply enter the WSL environment and follow the Linux instructions below.

### MacOS

The pre-compiled library can be installed from
[`conda-forge`](https://conda-forge.org/) by running `conda install -c
conda-forge pynini`.

Alternatively, one can build from source from [PyPI](https://pypi.org/) by
running `pip install pynini`.

Finally, one can use [Bazel](https://bazel.build) to build from source by
running `bazel build //:all` anywhere in the source tree.

### Linux

The pre-compiled library can be installed from
[`conda-forge`](https://conda-forge.org/) by running `conda install -c
conda-forge pynini`.

Alternatively, one can install a pre-compiled
[`manylinux`](https://github.com/pypa/manylinux) wheel from
[PyPI](https://pypi.org/) by running `pip install pynini`. This will install the
pre-compiled `manylinux` wheel (if available for the release and compatible with
your platform), and build and install from source if not. Unlike the
`conda-forge` option above, which also installs [OpenFst](http://openfst.org/)
and [Graphviz](https://graphviz.org/), this does not install the OpenFst or
Graphviz command-line tools. See the enclosed
[`Dockerfile`](third_party/Dockerfile) for instructions for building and
deploying `manylinux` wheels.

Finally, one can use [Bazel](https://bazel.build) to build from source by
running `bazel build //:all` anywhere in the source tree.

## Testing

To confirm successful installation, run `pip install -r requirements`, then
`python tests/pynini_test.py`. If all tests pass, the final line will read `OK`.

## Python version support

Pynini 2.0.0 and onward support Python 3. Pynini 2.1 versions (onward) drop
Python 2 support.

# License

Pynini is released under the Apache license. See [`LICENSE`](LICENSE) for more
information.

# Interested in contributing?

See [`CONTRIBUTING`](CONTRIBUTING) for more information.

# Mandatory disclaimer

This is not an official Google product.
