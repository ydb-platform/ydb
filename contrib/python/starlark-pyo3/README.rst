Starlark-PyO3: Python bindings for starlark-rust
================================================

.. image:: https://github.com/inducer/starlark-pyo3/actions/workflows/CI.yml/badge.svg
    :alt: Github Build Status
    :target: https://github.com/inducer/starlark-pyo3/actions/workflows/CI.yml
.. image:: https://badge.fury.io/py/starlark-pyo3.png
    :alt: Python Package Index Release Page
    :target: https://pypi.org/project/starlark-pyo3/

This package provides a sandboxed/restricted Python-like environment 
by exposing the
`starlark-rust <https://github.com/facebookexperimental/starlark-rust/>`__
interpreter for the
`Starlark <https://github.com/bazelbuild/starlark/blob/master/spec.md>`__
Python-like language to Python via `PyO3 <https://pyo3.rs>`__.

`Starlark <https://github.com/bazelbuild/starlark>`__ claims the following
*design principles*:

-   **Deterministic evaluation**. Executing the same code twice will give the
    same results.
-   **Hermetic execution**. Execution cannot access the file system, network,
    system clock. It is safe to execute untrusted code.
-   **Parallel evaluation**. Modules can be loaded in parallel. To guarantee a
    thread-safe execution, shared data becomes immutable.
-   **Simplicity**. We try to limit the number of concepts needed to understand
    the code. Users should be able to quickly read and write code, even if they
    are not expert. The language should avoid pitfalls as much as possible.
-   **Focus on tooling**. We recognize that the source code will be read,
    analyzed, modified, by both humans and tools.
-   **Python-like**. Python is a widely used language. Keeping the language
    similar to Python can reduce the learning curve and make the semantics more
    obvious to users.

*Status:* This is reasonably complete and usable.

*Alternatives:* Other packages with similar goals but a different twist
include `xingque <https://github.com/xen0n/xingque>`_ and the older
`starlark-go <https://github.com/caketop/python-starlark-go>`__.

Links
-----

-  `Documentation <https://documen.tician.de/starlark-pyo3/>`__
-  `Github <https://github.com/inducer/starlark-pyo3>`__ (issues etc.)
-  `Package index <https://pypi.org/project/starlark-pyo3>`__

Installation 
------------
To install, say::

    pip install starlark-pyo3

Binary wheels are available for all major platforms.  The module is importable
as ``starlark``.

Installation for Development
----------------------------

To use this, make sure you have nightly rust available::

    curl –proto ‘=https’ –tlsv1.2 -sSf https://sh.rustup.rs \| sh rustup
    default nightly

Then, to install into the current Python virtual environment::

    pip install maturin
    maturin develop
