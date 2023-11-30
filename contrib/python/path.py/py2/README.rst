.. image:: https://img.shields.io/pypi/v/path.py.svg
   :target: https://pypi.org/project/path.py

.. image:: https://img.shields.io/pypi/pyversions/path.py.svg

.. image:: https://img.shields.io/travis/jaraco/path.py/master.svg
   :target: https://travis-ci.org/jaraco/path.py

.. image:: https://img.shields.io/appveyor/ci/jaraco/path-py/master.svg
   :target: https://ci.appveyor.com/project/jaraco/path-py/branch/master

.. image:: https://readthedocs.org/projects/pathpy/badge/?version=latest
   :target: https://pathpy.readthedocs.io/en/latest/?badge=latest

``path.py`` implements path objects as first-class entities, allowing
common operations on files to be invoked on those path objects directly. For
example:

.. code-block:: python

    from path import Path
    d = Path('/home/guido/bin')
    for f in d.files('*.py'):
        f.chmod(0o755)

    # Globbing
    for f in d.files('*.py'):
        f.chmod(0o755)

    # Changing the working directory:
    with Path("somewhere"):
        # cwd in now `somewhere`
        ...

    # Concatenate paths with /
    foo_txt = Path("bar") / "foo.txt"

``path.py`` is `hosted at Github <https://github.com/jaraco/path.py>`_.

Find `the documentation here <https://pathpy.readthedocs.io>`_.

Guides and Testimonials
=======================

Yasoob wrote the Python 101 `Writing a Cleanup Script
<http://freepythontips.wordpress.com/2014/01/23/python-101-writing-a-cleanup-script/>`_
based on ``path.py``.

Installing
==========

Path.py may be installed using ``setuptools``, ``distribute``, or ``pip``::

    pip install path.py

The latest release is always updated to the `Python Package Index
<http://pypi.python.org/pypi/path.py>`_.

You may also always download the source distribution (zip/tarball), extract
it, and run ``python setup.py`` to install it.

Advantages
==========

Python 3.4 introduced
`pathlib <https://docs.python.org/3/library/pathlib.html>`_,
which shares many characteristics with ``path.py``. In particular,
it provides an object encapsulation for representing filesystem paths.
One may have imagined ``pathlib`` would supersede ``path.py``.

But the implementation and the usage quickly diverge, and ``path.py``
has several advantages over ``pathlib``:

- ``path.py`` implements ``Path`` objects as a subclass of
  ``str`` (unicode on Python 2), and as a result these ``Path``
  objects may be passed directly to other APIs that expect simple
  text representations of paths, whereas with ``pathlib``, one
  must first cast values to strings before passing them to
  APIs unaware of ``pathlib``. This shortcoming was `addressed
  by PEP 519 <https://www.python.org/dev/peps/pep-0519/>`_,
  in Python 3.6.
- ``path.py`` goes beyond exposing basic functionality of a path
  and exposes commonly-used behaviors on a path, providing
  methods like ``rmtree`` (from shlib) and ``remove_p`` (remove
  a file if it exists).
- As a PyPI-hosted package, ``path.py`` is free to iterate
  faster than a stdlib package. Contributions are welcome
  and encouraged.
- ``path.py`` provides a uniform abstraction over its Path object,
  freeing the implementer to subclass it readily. One cannot
  subclass a ``pathlib.Path`` to add functionality, but must
  subclass ``Path``, ``PosixPath``, and ``WindowsPath``, even
  if one only wishes to add a ``__dict__`` to the subclass
  instances.  ``path.py`` instead allows the ``Path.module``
  object to be overridden by subclasses, defaulting to the
  ``os.path``. Even advanced uses of ``path.Path`` that
  subclass the model do not need to be concerned with
  OS-specific nuances.

Alternatives
============

In addition to
`pathlib <https://docs.python.org/3/library/pathlib.html>`_, the
`pylib project <https://pypi.org/project/py/>`_ implements a
`LocalPath <https://github.com/pytest-dev/py/blob/72601dc8bbb5e11298bf9775bb23b0a395deb09b/py/_path/local.py#L106>`_
class, which shares some behaviors and interfaces with ``path.py``.

Development
===========

To install a development version, use the Github links to clone or
download a snapshot of the latest code. Alternatively, if you have git
installed, you may be able to use ``pip`` to install directly from
the repository::

    pip install git+https://github.com/jaraco/path.py.git

Testing
=======

Tests are continuously run by Travis-CI: |BuildStatus|_

.. |BuildStatus| image:: https://secure.travis-ci.org/jaraco/path.py.png
.. _BuildStatus: http://travis-ci.org/jaraco/path.py

To run the tests, refer to the ``.travis.yml`` file for the steps run on the
Travis-CI hosts.

Releasing
=========

Tagged releases are automatically published to PyPI by Travis-CI, assuming
the tests pass.
