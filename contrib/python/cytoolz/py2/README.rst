CyToolz
=======

|Build Status| |Version Status|

Cython implementation of the
|literal toolz|_ `package, <https://pypi.python.org/pypi/toolz/>`__ which
provides high performance utility functions for iterables, functions,
and dictionaries.

.. |literal toolz| replace:: ``toolz``
.. _literal toolz: https://github.com/pytoolz/toolz

``toolz`` is a pure Python package that borrows heavily from contemporary
functional languanges.  It is designed to interoperate seamlessly with other
libraries including ``itertools``, ``functools``, and third party libraries.
High performance functional data analysis is possible with builtin types
like ``list`` and ``dict``, and user-defined data structures; and low memory
usage is achieved by using the iterator protocol and returning iterators
whenever possible.

``cytoolz`` implements the same API as ``toolz``.  The main differences are
that ``cytoolz`` is faster (typically 2-5x faster with a few spectacular
exceptions) and ``cytoolz`` offers a C API that is accessible to other
projects developed in Cython.  Since ``toolz`` is able to process very
large (potentially infinite) data sets, the performance increase gained by
using ``cytoolz`` can be significant.

See the PyToolz documentation at https://toolz.readthedocs.io and the full
`API Documentation <https://toolz.readthedocs.io/en/latest/api.html>`__
for more details.

LICENSE
-------

New BSD. See `License File <https://github.com/pytoolz/cytoolz/blob/master/LICENSE.txt>`__.


Install
-------

``cytoolz`` is on the Python Package Index (PyPI):

::

    pip install cytoolz

Dependencies
------------

``cytoolz`` supports Python 2.7+ and Python 3.4+ with a common codebase.
It is developed in Cython, but requires no dependecies other than CPython
and a C compiler.  Like ``toolz``, it is a light weight dependency.

Contributions Welcome
---------------------

``toolz`` (and ``cytoolz``) aims to be a repository for utility functions,
particularly those that come from the functional programming and list
processing traditions. We welcome contributions that fall within this scope
and encourage users to scrape their ``util.py`` files for functions that are
broadly useful.

Please take a look at our issue pages for
`toolz <https://github.com/pytoolz/toolz/issues>`__ and
`cytoolz <https://github.com/pytoolz/cytoolz/issues>`__
for contribution ideas.

Community
---------

See our `mailing list <https://groups.google.com/forum/#!forum/pytoolz>`__.
We're friendly.

.. |Build Status| image:: https://travis-ci.org/pytoolz/cytoolz.svg?branch=master
   :target: https://travis-ci.org/pytoolz/cytoolz
.. |Version Status| image:: https://badge.fury.io/py/cytoolz.svg
   :target: http://badge.fury.io/py/cytoolz
