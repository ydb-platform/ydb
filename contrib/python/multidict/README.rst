=========
multidict
=========

.. image:: https://github.com/aio-libs/multidict/actions/workflows/ci-cd.yml/badge.svg
   :target: https://github.com/aio-libs/multidict/actions
   :alt: GitHub status for master branch

.. image:: https://codecov.io/gh/aio-libs/multidict/branch/master/graph/badge.svg
   :target: https://codecov.io/gh/aio-libs/multidict
   :alt: Coverage metrics

.. image:: https://img.shields.io/pypi/v/multidict.svg
   :target: https://pypi.org/project/multidict
   :alt: PyPI

.. image:: https://readthedocs.org/projects/multidict/badge/?version=latest
   :target: https://multidict.aio-libs.org
   :alt: Read The Docs build status badge

.. image:: https://img.shields.io/pypi/pyversions/multidict.svg
   :target: https://pypi.org/project/multidict
   :alt: Python versions

.. image:: https://img.shields.io/matrix/aio-libs:matrix.org?label=Discuss%20on%20Matrix%20at%20%23aio-libs%3Amatrix.org&logo=matrix&server_fqdn=matrix.org&style=flat
   :target: https://matrix.to/#/%23aio-libs:matrix.org
   :alt: Matrix Room — #aio-libs:matrix.org

.. image:: https://img.shields.io/matrix/aio-libs-space:matrix.org?label=Discuss%20on%20Matrix%20at%20%23aio-libs-space%3Amatrix.org&logo=matrix&server_fqdn=matrix.org&style=flat
   :target: https://matrix.to/#/%23aio-libs-space:matrix.org
   :alt: Matrix Space — #aio-libs-space:matrix.org

Multidict is dict-like collection of *key-value pairs* where key
might occur more than once in the container.

Introduction
------------

*HTTP Headers* and *URL query string* require specific data structure:
*multidict*. It behaves mostly like a regular ``dict`` but it may have
several *values* for the same *key* and *preserves insertion ordering*.

The *key* is ``str`` (or ``istr`` for case-insensitive dictionaries).

``multidict`` has four multidict classes:
``MultiDict``, ``MultiDictProxy``, ``CIMultiDict``
and ``CIMultiDictProxy``.

Immutable proxies (``MultiDictProxy`` and
``CIMultiDictProxy``) provide a dynamic view for the
proxied multidict, the view reflects underlying collection changes. They
implement the ``collections.abc.Mapping`` interface.

Regular mutable (``MultiDict`` and ``CIMultiDict``) classes
implement ``collections.abc.MutableMapping`` and allows them to change
their own content.


*Case insensitive* (``CIMultiDict`` and
``CIMultiDictProxy``) assume the *keys* are case
insensitive, e.g.::

   >>> dct = CIMultiDict(key='val')
   >>> 'Key' in dct
   True
   >>> dct['Key']
   'val'

*Keys* should be ``str`` or ``istr`` instances.

The library has optional C Extensions for speed.


License
-------

Apache 2

Library Installation
--------------------

.. code-block:: bash

   $ pip install multidict

The library is Python 3 only!

PyPI contains binary wheels for Linux, Windows and MacOS.  If you want to install
``multidict`` on another operating system (or *Alpine Linux* inside a Docker) the
tarball will be used to compile the library from source.  It requires a C compiler and
Python headers to be installed.

To skip the compilation, please use the `MULTIDICT_NO_EXTENSIONS` environment variable,
e.g.:

.. code-block:: bash

   $ MULTIDICT_NO_EXTENSIONS=1 pip install multidict

Please note, the pure Python (uncompiled) version is about 20-50 times slower depending on
the usage scenario!!!



Changelog
---------
See `RTD page <http://multidict.aio-libs.org/en/latest/changes>`_.
