===========
pathlib-abc
===========

|pypi| |docs|

Base classes for ``pathlib.Path``-ish objects. Requires Python 3.8+.

This package is a preview of ``pathlib`` functionality planned for a future
release of Python; specifically, it provides two ABCs that can be used to
implement path classes for non-local filesystems, such as archive files and
storage servers:

``PurePathBase``
  Abstract base class for paths that do not perform I/O.
``PathBase``
  Abstract base class for paths that perform I/O.

These base classes are under active development. Once the base classes reach
maturity, they will be made part of the Python standard library, and this
package will continue to provide a backport for older Python releases.


Contributing
------------

Changes to ``pathlib_abc.py`` and ``test_pathlib_abc.py`` must be made in the
upstream CPython project, and undergo their usual CLA + code review process.
Once a change lands in CPython, it can be back-ported here.

Other changes (such as CI improvements) can be made as pull requests to this
project.



.. |pypi| image:: https://img.shields.io/pypi/v/pathlib-abc.svg
    :target: https://pypi.python.org/pypi/pathlib-abc
    :alt: Latest version released on PyPi

.. |docs| image:: https://readthedocs.org/projects/pathlib-abc/badge
    :target: http://pathlib-abc.readthedocs.io/en/latest
    :alt: Documentation
