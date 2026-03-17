.. image:: https://img.shields.io/pypi/v/path.svg
   :target: https://pypi.org/project/path

.. image:: https://img.shields.io/pypi/pyversions/path.svg

.. image:: https://github.com/jaraco/path/actions/workflows/main.yml/badge.svg
   :target: https://github.com/jaraco/path/actions?query=workflow%3A%22tests%22
   :alt: tests

.. image:: https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/charliermarsh/ruff/main/assets/badge/v2.json
    :target: https://github.com/astral-sh/ruff
    :alt: Ruff

.. image:: https://readthedocs.org/projects/path/badge/?version=latest
   :target: https://path.readthedocs.io/en/latest/?badge=latest

.. image:: https://img.shields.io/badge/skeleton-2024-informational
   :target: https://blog.jaraco.com/skeleton

.. image:: https://tidelift.com/badges/package/pypi/path
   :target: https://tidelift.com/subscription/pkg/pypi-path?utm_source=pypi-path&utm_medium=readme


``path`` (aka path pie, formerly ``path.py``) implements path
objects as first-class entities, allowing common operations on
files to be invoked on those path objects directly. For example:

.. code-block:: python

    from path import Path

    d = Path("/home/guido/bin")
    for f in d.files("*.py"):
        f.chmod(0o755)

    # Globbing
    for f in d.files("*.py"):
        f.chmod("u+rwx")

    # Changing the working directory:
    with Path("somewhere"):
        # cwd in now `somewhere`
        ...

    # Concatenate paths with /
    foo_txt = Path("bar") / "foo.txt"

Path pie is `hosted at Github <https://github.com/jaraco/path>`_.

Find `the documentation here <https://path.readthedocs.io>`_.

Guides and Testimonials
=======================

Yasoob wrote the Python 101 `Writing a Cleanup Script
<http://freepythontips.wordpress.com/2014/01/23/python-101-writing-a-cleanup-script/>`_
based on ``path``.

Advantages
==========

Path pie provides a superior experience to similar offerings.

Python 3.4 introduced
`pathlib <https://docs.python.org/3/library/pathlib.html>`_,
which shares many characteristics with ``path``. In particular,
it provides an object encapsulation for representing filesystem paths.
One may have imagined ``pathlib`` would supersede ``path``.

But the implementation and the usage quickly diverge, and ``path``
has several advantages over ``pathlib``:

- ``path`` implements ``Path`` objects as a subclass of ``str``, and as a
  result these ``Path`` objects may be passed directly to other APIs that
  expect simple text representations of paths, whereas with ``pathlib``, one
  must first cast values to strings before passing them to APIs that do
  not honor `PEP 519 <https://www.python.org/dev/peps/pep-0519/>`_
  ``PathLike`` interface.
- ``path`` give quality of life features beyond exposing basic functionality
  of a path. ``path`` provides methods like ``rmtree`` (from shlib) and
  ``remove_p`` (remove a file if it exists), properties like ``.permissions``,
  and sophisticated ``walk``, ``TempDir``, and ``chmod`` behaviors.
- As a PyPI-hosted package, ``path`` is free to iterate
  faster than a stdlib package. Contributions are welcome
  and encouraged.
- ``path`` provides superior portability using a uniform abstraction
  over its single Path object,
  freeing the implementer to subclass it readily. One cannot
  subclass a ``pathlib.Path`` to add functionality, but must
  subclass ``Path``, ``PosixPath``, and ``WindowsPath``, even
  to do something as simple as to add a ``__dict__`` to the subclass
  instances.  ``path`` instead allows the ``Path.module``
  object to be overridden by subclasses, defaulting to the
  ``os.path``. Even advanced uses of ``path.Path`` that
  subclass the model do not need to be concerned with
  OS-specific nuances. ``path.Path`` objects are inherently "pure",
  not requiring the author to distinguish between pure and non-pure
  variants.

This path project has the explicit aim to provide compatibility
with ``pathlib`` objects where possible, such that a ``path.Path``
object is a drop-in replacement for ``pathlib.Path*`` objects.
This project welcomes contributions to improve that compatibility
where it's lacking.


Origins
=======

The ``path.py`` project was initially released in 2003 by Jason Orendorff
and has been continuously developed and supported by several maintainers
over the years.


For Enterprise
==============

Available as part of the Tidelift Subscription.

This project and the maintainers of thousands of other packages are working with Tidelift to deliver one enterprise subscription that covers all of the open source you use.

`Learn more <https://tidelift.com/subscription/pkg/pypi-path?utm_source=pypi-path&utm_medium=referral&utm_campaign=github>`_.
