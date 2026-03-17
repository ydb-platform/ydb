PyVISA
======


.. image:: https://github.com/pyvisa/pyvisa/workflows/Continuous%20Integration/badge.svg
    :target: https://github.com/pyvisa/pyvisa/actions
    :alt: Continuous integration
.. image:: https://github.com/pyvisa/pyvisa/workflows/Documentation%20building/badge.svg
    :target: https://github.com/pyvisa/pyvisa/actions
    :alt: Documentation building
.. image:: https://dev.azure.com/pyvisa/pyvisa/_apis/build/status/pyvisa.keysight-assisted?branchName=main
    :target: https://dev.azure.com/pyvisa/pyvisa/_build
    :alt: Keysight assisted testing
.. image:: https://codecov.io/gh/pyvisa/pyvisa/branch/main/graph/badge.svg
    :target: https://codecov.io/gh/pyvisa/pyvisa
    :alt: Code Coverage
.. image:: https://readthedocs.org/projects/pyvisa/badge/?version=latest
    :target: https://pyvisa.readthedocs.io/en/latest/?badge=latest
    :alt: Documentation Status
.. image:: https://img.shields.io/pypi/l/PyVISA
    :target: https://pypi.python.org/pypi/pyvisa
    :alt: PyPI - License
.. image:: https://img.shields.io/pypi/v/PyVISA
    :target: https://pypi.python.org/pypi/pyvisa
    :alt: PyPI
.. image:: https://joss.theoj.org/papers/10.21105/joss.05304/status.svg
   :target: https://doi.org/10.21105/joss.05304


A Python package for support of the "Virtual Instrument Software
Architecture" (VISA), in order to control measurement devices and
test equipment via GPIB, RS232, Ethernet or USB.

Description
-----------

The programming of measurement instruments can be real pain. There are many
different protocols, sent over many different interfaces and bus systems
(GPIB, RS232, USB). For every programming language you want to use, you have to
find libraries that support both your device and its bus system.

In order to ease this unfortunate situation, the Virtual Instrument Software
Architecture (VISA_) specification was defined in the middle of the 90's. Today
VISA is implemented on all significant operating systems. A couple of vendors
offer VISA libraries, partly with free download. These libraries work together
with arbitrary peripheral devices, although they may be limited to certain
interface devices, such as the vendor’s GPIB card.

The VISA specification has explicit bindings to Visual Basic, C, and G
(LabVIEW’s graphical language). Python can be used to call functions from a
VISA shared library (`.dll`, `.so`, `.dylib`) allowing to directly leverage the
standard implementations. In addition, Python can be used to directly access
most bus systems used by instruments which is why one can envision to implement
the VISA standard directly in Python (see the `PyVISA-Py`_ project for more
details). PyVISA is both a Python wrapper for VISA shared libraries but
can also serve as a front-end for other VISA implementation such as
`PyVISA-Py`_.


.. _VISA: http://www.ivifoundation.org/specifications/default.aspx
.. _`PyVISA-Py`: http://pyvisa-py.readthedocs.io/en/latest/


VISA and Python
---------------

Python has a couple of features that make it very interesting for controlling
instruments:

- Python is an easy-to-learn scripting language with short development cycles.
- It represents a high abstraction level [2], which perfectly blends with the
  abstraction level of measurement programs.
- It has a rich set of native libraries, including numerical and plotting
  modules for data analysis and visualisation.
- A large set of books (in many languages) and on-line publications is
  available.


Requirements
------------

- Python (tested with 3.10+)
- VISA (tested with NI-VISA 17.5, Win7, from www.ni.com/visa and Keysight-VISA )

Installation
--------------

Using pip:

    $ pip install pyvisa

or easy_install:

    $ easy_install pyvisa

or download and unzip the source distribution file and:

    $ python setup.py install


Documentation
--------------

The documentation can be read online at https://pyvisa.readthedocs.org


Citing
------

If you are using this package, you can cite the `PyVISA publication`_

Grecco et al., (2023). PyVISA: the Python instrumentation package. Journal of Open Source
Software, 8(84), 5304, https://doi.org/10.21105/joss.05304

.. _`PyVISA publication`: https://joss.theoj.org/papers/10.21105/joss.05304#
