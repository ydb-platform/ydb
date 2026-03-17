.. image:: https://img.shields.io/pypi/v/eccodes.svg
   :target: https://pypi.python.org/pypi/eccodes/

Python 3 interface to decode and encode GRIB and BUFR files via the
`ECMWF ecCodes library <https://confluence.ecmwf.int/display/ECC/>`_.

Features:

- reads and writes GRIB 1 and 2 files,
- reads and writes BUFR 3 and 4 files,
- supports all modern versions of Python and PyPy3,
- works on most *Linux* distributions and *MacOS*, the *ecCodes* C-library
  is the only system dependency,
- PyPI package can be installed without compiling,
  at the cost of being twice as slow as the original *ecCodes* module,
- an optional compile step makes the code as fast as the original module
  but it needs the recommended (the most up-to-date) version of *ecCodes*.

Limitations:

- Microsoft Windows support is untested.


Installation
============

**From version 2.43.0, the ecCodes Python bindings on PyPi will depend
on the PyPi package 'eccodeslib' on Linux and MacOS. This package provides
the binary ecCodes library. On Windows, the ecCodes Python bindings will
continue to directly provide the ecCodes binary library without a dependency
on eccodeslib. See below for details.**

Installation from PyPI
----------------------

The package can be installed from PyPI with::

    $ pip install eccodes

This installation will, by default, include the ecCodes binary library (either
supplied by the 'eccodes' package on Windows, or via the 'eccodeslib' package
on Linux and MacOS), meaning that no external ecCodes binary library is
required.


Bypassing the provided binary library
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Linux and MacOS
+++++++++++++++

If you have an external ecCodes binary library that you wish to use, consult the documentation
of the `findlibs <https://github.com/ecmwf/findlibs>`_ package, which is used by the ecCodes Python bindings to
locate the binary library. This allows the use of environment variables
to refine the search algorithm.


Windows
+++++++

If you have an external ecCodes binary library that you wish to use, set the
following environment variable before you import eccodes::

    $ export ECCODES_PYTHON_USE_FINDLIBS=1

If this is set, the ecCodes' Python bindings will use the `findlibs <https://github.com/ecmwf/findlibs>`_ package
to locate the binary library.

You may also install a version of ecCodes' Python interface that does not
include a binary library at all, in which case the findlibs mechanism will
be used as before::

    $ pip install eccodes --no-binary eccodes

See also 'Debugging the library search', below.


Installation from conda
-----------------------

ecCodes' Python bindings can be installed from the `conda-forge <https://conda-forge.org/>`_ channel with::

    $ conda install -c conda-forge python-eccodes

This will install the Python bindings (`python-eccodes`) and also the ecCodes
binary library (`eccodes`) on which they depend.


System dependencies
-------------------

The Python module depends on the ECMWF *ecCodes* binary library.
From version 2.37.0, this library is supplied with the Python
module on both PyPi and conda, as described above. If you wish
to install and use a separate binary library (see above), it must
be installed on the system and accessible as a shared library.

On a MacOS with HomeBrew use::

    $ brew install eccodes

Or if you manage binary packages with *Conda* but use Python bindings from elsewhere, use::

    $ conda install -c conda-forge eccodes

As an alternative you may install the official source distribution
by following the instructions at
https://confluence.ecmwf.int/display/ECC/ecCodes+installation

You may run a simple selfcheck command to ensure that your system is set
up correctly::

    $ python -m eccodes selfcheck
    Found: ecCodes v2.39.0.
    Your system is ready.


Debugging the library search
----------------------------

In order to gain insights into the search for the binary library, set
the following environment variable before importing eccodes::

    $ export ECCODES_PYTHON_TRACE_LIB_SEARCH=1


Usage
-----

Refer to the *ecCodes* `documentation pages <https://confluence.ecmwf.int/display/ECC/Documentation>`_
for usage.


Experimental features
=====================

Fast bindings
-------------

To test the much faster *CFFI* API level, out-of-line mode you need the
*ecCodes* header files.
Then you need to clone the repo in the same folder as your *ecCodes*
source tree, make a ``pip`` development install and custom compile
the binary bindings::

    $ git clone https://github.com/ecmwf/eccodes-python
    $ cd eccodes-python
    $ pip install -e .
    $ python builder.py

To revert back to ABI level, in-line mode just remove the compiled bindings::

    $ rm gribapi/_bindings.*


Project resources
=================

============= =========================================================
Development   https://github.com/ecmwf/eccodes-python
Download      https://pypi.org/project/eccodes
============= =========================================================


Contributing
============

The main repository is hosted on GitHub,
testing, bug reports and contributions are highly welcomed and appreciated:

https://github.com/ecmwf/eccodes-python

Please see the CONTRIBUTING.rst document for the best way to help.

Maintainers:

- `Shahram Najm <https://github.com/shahramn>`_ - `ECMWF <https://ecmwf.int>`_
- `Eugen Betke <https://github.com/joobog>`_ - `ECMWF <https://ecmwf.int>`_

Contributors:

- `Iain Russell <https://github.com/iainrussell>`_ - `ECMWF <https://ecmwf.int>`_
- `Alessandro Amici <https://github.com/alexamici>`_ - `B-Open <https://bopen.eu>`_

See also the list of other `contributors <https://github.com/ecmwf/eccodes-python/contributors>`_
who participated in this project.

.. |copy|   unicode:: U+000A9 .. COPYRIGHT SIGN

License
=======

|copy| Copyright 2017- ECMWF.

This software is licensed under the terms of the Apache Licence Version 2.0
which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.

In applying this licence, ECMWF does not waive the privileges and immunities
granted to it by virtue of its status as an intergovernmental organisation nor
does it submit to any jurisdiction.
