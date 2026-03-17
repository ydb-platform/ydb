propcache
=========

The module provides a fast implementation of cached properties for Python 3.9+.

.. image:: https://github.com/aio-libs/propcache/actions/workflows/ci-cd.yml/badge.svg
  :target: https://github.com/aio-libs/propcache/actions?query=workflow%3ACI
  :align: right

.. image:: https://codecov.io/gh/aio-libs/propcache/branch/master/graph/badge.svg
  :target: https://codecov.io/gh/aio-libs/propcache

.. image:: https://badge.fury.io/py/propcache.svg
    :target: https://badge.fury.io/py/propcache


.. image:: https://readthedocs.org/projects/propcache/badge/?version=latest
    :target: https://propcache.readthedocs.io


.. image:: https://img.shields.io/pypi/pyversions/propcache.svg
    :target: https://pypi.python.org/pypi/propcache

.. image:: https://img.shields.io/matrix/aio-libs:matrix.org?label=Discuss%20on%20Matrix%20at%20%23aio-libs%3Amatrix.org&logo=matrix&server_fqdn=matrix.org&style=flat
   :target: https://matrix.to/#/%23aio-libs:matrix.org
   :alt: Matrix Room — #aio-libs:matrix.org

.. image:: https://img.shields.io/matrix/aio-libs-space:matrix.org?label=Discuss%20on%20Matrix%20at%20%23aio-libs-space%3Amatrix.org&logo=matrix&server_fqdn=matrix.org&style=flat
   :target: https://matrix.to/#/%23aio-libs-space:matrix.org
   :alt: Matrix Space — #aio-libs-space:matrix.org

Introduction
------------

The API is designed to be nearly identical to the built-in ``functools.cached_property`` class,
except for the additional ``under_cached_property`` class which uses ``self._cache``
instead of ``self.__dict__`` to store the cached values and prevents ``__set__`` from being called.

For full documentation please read https://propcache.readthedocs.io.

Installation
------------

::

   $ pip install propcache

The library is Python 3 only!

PyPI contains binary wheels for Linux, Windows and MacOS.  If you want to install
``propcache`` on another operating system where wheels are not provided,
the the tarball will be used to compile the library from
the source code. It requires a C compiler and and Python headers installed.

To skip the compilation you must explicitly opt-in by using a PEP 517
configuration setting ``pure-python``, or setting the ``PROPCACHE_NO_EXTENSIONS``
environment variable to a non-empty value, e.g.:

.. code-block:: console

   $ pip install propcache --config-settings=pure-python=false

Please note that the pure-Python (uncompiled) version is much slower. However,
PyPy always uses a pure-Python implementation, and, as such, it is unaffected
by this variable.


API documentation
------------------

The documentation is located at https://propcache.readthedocs.io.

Source code
-----------

The project is hosted on GitHub_

Please file an issue on the `bug tracker
<https://github.com/aio-libs/propcache/issues>`_ if you have found a bug
or have some suggestion in order to improve the library.

Discussion list
---------------

*aio-libs* google group: https://groups.google.com/forum/#!forum/aio-libs

Feel free to post your questions and ideas here.


Authors and License
-------------------

The ``propcache`` package is derived from ``yarl`` which is written by Andrew Svetlov.

It's *Apache 2* licensed and freely available.


.. _GitHub: https://github.com/aio-libs/propcache
