============
pytest-xdist
============

.. image:: http://img.shields.io/pypi/v/pytest-xdist.svg
    :alt: PyPI version
    :target: https://pypi.python.org/pypi/pytest-xdist

.. image:: https://img.shields.io/conda/vn/conda-forge/pytest-xdist.svg
    :target: https://anaconda.org/conda-forge/pytest-xdist

.. image:: https://img.shields.io/pypi/pyversions/pytest-xdist.svg
    :alt: Python versions
    :target: https://pypi.python.org/pypi/pytest-xdist

.. image:: https://github.com/pytest-dev/pytest-xdist/workflows/test/badge.svg
    :target: https://github.com/pytest-dev/pytest-xdist/actions

.. image:: https://img.shields.io/badge/code%20style-black-000000.svg
    :target: https://github.com/ambv/black

The `pytest-xdist`_ plugin extends pytest with new test execution modes, the most used being distributing
tests across multiple CPUs to speed up test execution::

    pytest -n auto

With this call, pytest will spawn a number of workers processes equal to the number of available CPUs, and distribute
the tests randomly across them.

Documentation
=============

Documentation is available at `Read The Docs <https://pytest-xdist.readthedocs.io>`__.
