++++++++++++++++++++++++++
Python C API compatibility
++++++++++++++++++++++++++

.. image:: https://github.com/python/pythoncapi-compat/actions/workflows/build.yml/badge.svg
   :alt: Build status of pythoncapi-compat on GitHub Actions
   :target: https://github.com/python/pythoncapi-compat/actions

The ``pythoncapi-compat`` project can be used to write a C or C++ extension
supporting a wide range of Python versions with a single code base.  It is made
of the ``pythoncapi_compat.h`` header file and the ``upgrade_pythoncapi.py``
script.

``upgrade_pythoncapi.py`` requires Python 3.6 or newer.

See the `documentation at ReadTheDocs
<https://pythoncapi-compat.readthedocs.io/en/latest/>`_
for more details.

Getting started
===============

To upgrade a specific file::

    python3 upgrade_pythoncapi.py module.c

To upgrade all C/C++ files in a directory::

    python3 upgrade_pythoncapi.py src/

Select operations
-----------------

To only replace ``op->ob_type`` with ``Py_TYPE(op)``, select the ``Py_TYPE``
operation with::

    python3 upgrade_pythoncapi.py -o Py_TYPE module.c

Or the opposite, to apply all operations but leave ``op->ob_type`` unchanged,
deselect the ``Py_TYPE`` operation with::

    python3 upgrade_pythoncapi.py -o all,-Py_TYPE module.c

Download pythoncapi_compat.h
----------------------------

If you want to ``pythoncapi_compat.h`` to your code base, use the
``upgrade_pythoncapi.py`` tool to fetch it::

    python3 upgrade_pythoncapi.py --download PATH


This project is distributed under the `Zero Clause BSD (0BSD) license
<https://opensource.org/licenses/0BSD>`_ and is covered by the `PSF Code of
Conduct <https://www.python.org/psf/codeofconduct/>`_.
