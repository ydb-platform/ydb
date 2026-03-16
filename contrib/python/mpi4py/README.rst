==============
MPI for Python
==============

.. image::  https://github.com/mpi4py/mpi4py/workflows/ci/badge.svg?branch=master
   :target: https://github.com/mpi4py/mpi4py/actions/
.. image::  https://readthedocs.org/projects/mpi4py/badge/?version=latest
   :target: https://mpi4py.readthedocs.io/en/latest/
.. image::  https://dev.azure.com/mpi4py/mpi4py/_apis/build/status/ci?branchName=master
   :target: https://dev.azure.com/mpi4py/mpi4py/_build
.. image::  https://ci.appveyor.com/api/projects/status/whh5xovp217h0f7n?svg=true
   :target: https://ci.appveyor.com/project/mpi4py/mpi4py
.. image::  https://circleci.com/gh/mpi4py/mpi4py.svg?style=shield
   :target: https://circleci.com/gh/mpi4py/mpi4py
.. image::  https://app.travis-ci.com/mpi4py/mpi4py.svg?branch=master
   :target: https://app.travis-ci.com/mpi4py/mpi4py
.. image::  https://codecov.io/gh/mpi4py/mpi4py/branch/master/graph/badge.svg
   :target: https://codecov.io/gh/mpi4py/mpi4py
.. image::  https://scan.coverity.com/projects/mpi4py-mpi4py/badge.svg
   :target: https://scan.coverity.com/projects/mpi4py-mpi4py

Overview
--------

This package provides Python bindings for the *Message Passing
Interface* (`MPI <https://www.mpi-forum.org/>`_) standard. It is
implemented on top of the MPI specification and exposes an API which
grounds on the standard MPI-2 C++ bindings.

Dependencies
------------

* `Python <https://www.python.org/>`_ 2.7, 3.5 or above,
  or `PyPy <https://www.pypy.org/>`_ 2.0 or above.

* An MPI implementation like `MPICH <https://www.mpich.org/>`_ or
  `Open MPI <https://www.open-mpi.org/>`_ built with shared/dynamic
  libraries.

* To work with the in-development version, you need to install `Cython
  <https://cython.org/>`_.

Documentation
-------------

* Read the Docs: https://mpi4py.readthedocs.io/
* GitHub Pages:  https://mpi4py.github.io/

Support
-------

* Mailing List:       mpi4py@googlegroups.com
* Google Groups:      https://groups.google.com/g/mpi4py
* GitHub Discussions: https://github.com/mpi4py/mpi4py/discussions

Testsuite
---------

The testsuite is run periodically on

* `GitHub Actions <https://github.com/mpi4py/mpi4py/actions/>`_

* `Read the Docs <https://readthedocs.org/projects/mpi4py/builds/>`_

* `Azure Pipelines <https://dev.azure.com/mpi4py/mpi4py>`_

* `AppVeyor <https://ci.appveyor.com/project/mpi4py/mpi4py>`_

* `Circle CI <https://circleci.com/gh/mpi4py/mpi4py>`_

* `Travis CI <https://app.travis-ci.com/mpi4py/mpi4py>`_

* `Codecov <https://app.codecov.io/gh/mpi4py/mpi4py>`_

Citation
--------

+ L. Dalcin and Y.-L. L. Fang,
  *mpi4py: Status Update After 12 Years of Development*,
  Computing in Science & Engineering, 23(4):47-54, 2021.
  https://doi.org/10.1109/MCSE.2021.3083216
