PyVISA-py
=========

.. image:: https://github.com/pyvisa/pyvisa-py/workflows/Continuous%20Integration/badge.svg
    :target: https://github.com/pyvisa/pyvisa-py/actions
    :alt: Continuous integration
.. image:: https://github.com/pyvisa/pyvisa-py/workflows/Documentation%20building/badge.svg
    :target: https://github.com/pyvisa/pyvisa-py/actions
    :alt: Documentation building
.. image:: https://dev.azure.com/pyvisa/pyvisa-py/_apis/build/status/pyvisa.pyvisa-py.keysight-assisted?branchName=main
    :target: https://dev.azure.com/pyvisa/pyvisa-py/_build
    :alt: Keysight assisted testing
.. image:: https://codecov.io/gh/pyvisa/pyvisa-py/branch/main/graph/badge.svg
    :target: https://codecov.io/gh/pyvisa/pyvisa-py
    :alt: Code Coverage
.. image:: https://readthedocs.org/projects/pyvisa-py/badge/?version=latest
    :target: https://pyvisa.readthedocs.io/en/latest/?badge=latest
    :alt: Documentation Status
.. image:: https://img.shields.io/pypi/l/PyVISA-py
    :target: https://pypi.python.org/pypi/pyvisa-py
    :alt: PyPI - License
.. image:: https://img.shields.io/pypi/v/PyVISA-py
    :target: https://pypi.python.org/pypi/pyvisa-py
    :alt: PyPI

A PyVISA backend that implements a large part of the "Virtual Instrument Software
Architecture" (VISA_) in pure Python (with the help of some nice cross platform
libraries python packages!).

Description
-----------

PyVISA started as wrapper for the IVI-VISA library and therefore you need to install
a VISA library in your system (National Instruments, Keysight, etc). This works
most of the time, for most people. But IVI-VISA implementations are proprietary
libraries that only works on certain systems. That is when PyVISA-py jumps in.

Starting from version 1.6, PyVISA allows to use different backends. These
backends can be dynamically loaded. PyVISA-py is one of such backends. It
implements most of the methods for Message Based communication
(Serial/USB/GPIB/Ethernet) using Python and some well developed, easy to deploy
and cross platform libraries

.. _VISA: https://www.ivifoundation.org/specifications/default.html#visa-specifications


VISA and Python
---------------

Python has a couple of features that make it very interesting for measurement
controlling:

- Python is an easy-to-learn scripting language with short development cycles.
- It represents a high abstraction level, which perfectly blends with the
  abstraction level of measurement programs.
- It has a very rich set of native libraries, including numerical and plotting
  modules for data analysis and visualisation.
- A large set of books (in many languages) and on-line publications is available.


Requirements
------------

- Python 3
- PyVISA

Optionally:

- PySerial (to interface with Serial instruments)
- PyUSB (to interface with USB instruments)
- linux-gpib (to interface with gpib instruments, only on linux)
- gpib-ctypes (to interface with GPIB instruments on Windows and Linux)
- psutil (to discover TCPIP devices across multiple interfaces)
- zeroconf (for HiSLIP and VICP devices discovery)
- pyvicp (to enable the Teledyne LeCroy proprietary VICP protocol)

Please refer to `pyproject.toml <./pyproject.toml>`_ for the specific version
requirements.

Installation
--------------

Using pip::

    $ pip install pyvisa-py


Documentation
--------------

The documentation can be read online at https://pyvisa-py.readthedocs.org
