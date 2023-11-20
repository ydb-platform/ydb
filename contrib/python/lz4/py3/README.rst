==========
python-lz4
==========

Status
======

.. image:: https://github.com/python-lz4/python-lz4/actions/workflows/build_dist.yml/badge.svg
   :target: https://github.com/python-lz4/python-lz4/actions/workflows/build_dist.yml
   :alt: Build Status

.. image:: https://readthedocs.org/projects/python-lz4/badge/?version=stable
   :target: https://readthedocs.org/projects/python-lz4/
   :alt: Documentation

.. image:: https://codecov.io/gh/python-lz4/python-lz4/branch/codecov/graph/badge.svg
   :target: https://codecov.io/gh/python-lz4/python-lz4
   :alt: CodeCov


Introduction
============
This package provides python bindings for the `LZ4 compression library
<https://lz4.github.io/lz4/>`_.

The production ready bindings provided in this package cover the `frame format
<https://github.com/lz4/lz4/blob/master/doc/lz4_Frame_format.md>`_, and the
`block format <https://github.com/lz4/lz4/blob/dev/doc/lz4_Block_format.md>`_
specifications. The frame format bindings are the recommended ones to use, as
this guarantees interoperability with other implementations and language
bindings.

Experimental bindings for the the `streaming format
<https://github.com/lz4/lz4/blob/master/examples/streaming_api_basics.md>`_
specification are also included, but further work on those is required.

The API provided by the frame format bindings follows that of the LZMA, zlib,
gzip and bzip2 compression libraries which are provided with the Python standard
library. As such, these LZ4 bindings should provide a drop-in alternative to the
compression libraries shipped with Python. The package provides context managers
and file handler support.

The bindings drop the GIL when calling in to the underlying LZ4 library, and is
thread safe. An extensive test suite is included.

Documentation
=============

.. image:: https://readthedocs.org/projects/python-lz4/badge/?version=stable
   :target: https://readthedocs.org/projects/python-lz4/
   :alt: Documentation

Full documentation is included with the project. The documentation is
generated using Sphinx. Documentation is also hosted on readthedocs.

:master: http://python-lz4.readthedocs.io/en/stable/
:development: http://python-lz4.readthedocs.io/en/latest/

Homepage
========

The `project homepage <https://www.github.com/python-lz4/python-lz4>`_ is hosted
on Github. Please report any issues you find using the `issue tracker
<https://github.com/python-lz4/python-lz4/issues>`_.

Licensing
=========
Code specific to this project is covered by the `BSD 3-Clause License
<http://opensource.org/licenses/BSD-3-Clause>`_

