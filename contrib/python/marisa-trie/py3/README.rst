MARISA Trie
===========

|PyPI Version|
|PyPI Status|
|PyPI Python Versions|
|Github Build Status|

.. tip::

    Become **my boss** to help me work on this awesome software, and make the world better:

   |Patreon|

Static memory-efficient Trie-like structures for Python (3.8+)
based on `marisa-trie`_ C++ library.

String data in a MARISA-trie may take up to 50x-100x less memory than
in a standard Python dict; the raw lookup speed is comparable; trie also
provides fast advanced methods like prefix search.

.. note::

    There are official SWIG-based Python bindings included
    in C++ library distribution; this package provides alternative
    Cython-based pip-installable Python bindings.

.. _marisa-trie: https://github.com/s-yata/marisa-trie

Installation
============

::

    python -m pip install -U marisa-trie

Usage
=====

See `tutorial`_ and `API`_ for details.

.. _tutorial: https://marisa-trie.readthedocs.io/en/latest/tutorial.html
.. _API: https://marisa-trie.readthedocs.io/en/latest/api.html

Current limitations
===================

* The library is not tested with mingw32 compiler;
* ``.prefixes()`` method of ``BytesTrie`` and ``RecordTrie`` is quite slow
  and doesn't have iterator counterpart;
* ``read()`` and ``write()`` methods don't work with file-like objects
  (they work only with real files; pickling works fine for file-like objects);
* there are ``keys()`` and ``items()`` methods but no ``values()`` method.

License
=======

Wrapper code is licensed under MIT License.

Bundled `marisa-trie`_ C++ library is dual-licensed under
LGPL or BSD 2-clause license.

.. |PyPI Version| image:: https://img.shields.io/pypi/v/marisa-trie.svg
   :target: https://pypi.python.org/pypi/marisa-trie/
.. |PyPI Status| image:: https://img.shields.io/pypi/status/marisa-trie.svg
   :target: https://pypi.python.org/pypi/marisa-trie/
.. |PyPI Python Versions| image:: https://img.shields.io/pypi/pyversions/marisa-trie.svg
   :target: https://pypi.python.org/pypi/marisa-trie/
.. |Github Build Status| image:: https://github.com/pytries/marisa-trie/actions/workflows/tests.yml/badge.svg
   :target: https://github.com/pytries/marisa-trie/actions/workflows/tests.yml
.. |Patreon| image:: https://img.shields.io/badge/Patreon-F96854?style=for-the-badge&logo=patreon&logoColor=white
   :target: https://www.patreon.com/mschoentgen
