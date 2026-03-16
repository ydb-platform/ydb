execnet: distributed Python deployment and communication
========================================================

.. image:: https://img.shields.io/pypi/v/execnet.svg
    :target: https://pypi.org/project/execnet/

.. image:: https://anaconda.org/conda-forge/execnet/badges/version.svg
    :target: https://anaconda.org/conda-forge/execnet

.. image:: https://img.shields.io/pypi/pyversions/execnet.svg
    :target: https://pypi.org/project/execnet/

.. image:: https://github.com/pytest-dev/execnet/workflows/test/badge.svg
    :target: https://github.com/pytest-dev/execnet/actions?query=workflow%3Atest

.. image:: https://img.shields.io/badge/code%20style-black-000000.svg
    :target: https://github.com/python/black

.. _execnet: https://execnet.readthedocs.io

execnet_ provides carefully tested means to ad-hoc interact with Python
interpreters across version, platform and network barriers.  It provides
a minimal and fast API targeting the following uses:

* distribute tasks to local or remote processes
* write and deploy hybrid multi-process applications
* write scripts to administer multiple hosts

Features
--------

* zero-install bootstrapping: no remote installation required!

* flexible communication: send/receive as well as
  callback/queue mechanisms supported

* simple serialization of python builtin types (no pickling)

* grouped creation and robust termination of processes

* interoperable between Windows and Unix-ish systems.

* integrates with different threading models, including standard
  os threads, eventlet and gevent based systems.
