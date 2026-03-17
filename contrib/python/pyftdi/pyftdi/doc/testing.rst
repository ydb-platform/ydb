Testing
-------

.. include:: defs.rst

Overview
~~~~~~~~

Testing PyFTDI is challenging because it relies on several pieces of hardware:

* one or more FTDI device
* |I2C|, SPI, JTAG bus slaves or communication equipment for UART

The ``tests`` directory contain several tests files, which are primarily aimed
at demonstrating usage of PyFTDI in common use cases.

Most unit tests are disabled, as they require specific slaves, with a dedicated
HW wiring. Reproducing such test environments can be challenging, as it
requires dedicated test benchs.

This is a growing concern as PyFTDI keeps evolving, and up to now, regression
tests were hard to run.

Hardware tests
~~~~~~~~~~~~~~

Please refer to the ``pyftdi/tests`` directory. There is one file dedicated to
each feature to test. Note that you need to read and edit these tests files to
fit your actual test environment, and enable the proper unit test cases, as
most are actually disabled by default.

You need specific bus slaves to perform most of these tests.

.. _virtual_framework:

Virtual test framework
~~~~~~~~~~~~~~~~~~~~~~

With PyFTDI v0.45, a new test module enables PyFTDI API partial testing using a
pure software environment with no hardware. This also eases automatic testing
within a continuous integration environment.

This new module implements a virtual USB backend for PyUSB, which creates some
kind of virtual, limited USB stack. The PyUSB can be told to substitute the
native platform's libusb with this module.

This module, ``usbvirt`` can be dynamically confifured with the help of YaML
definition files to create one or more virtual FTDI devices on a virtual USB
bus topology. This enables to test ``usbtools`` module to enumerate, detect,
report and access FTDI devices using the regular :doc:`urlscheme` syntax.

``usbvirt`` also routes all vendor-specific USB API calls to a secondary
``ftdivirt`` module, which is in charge of handling all FTDI USB requests.

This module enables testing PyFtdi_ APIs. It also re-uses the MPSSE tracker
engine to decode and verify MPSSE requests used to support |I2C|, SPI and UART
features.

For now, it is able to emulate most of GPIO requests (async, sync and MPSSE)
and UART input/output. It also manages the frequency and baudrate settings.

It is not able to emulate the MPSSE commands (with the exception of set and get
GPIO values), as it represents a massive workload...

Beware: WIP
```````````

This is an experimental work in progress, which is its early inception stage.

It has nevertheless already revealed a couple of bugs that had been hiding
within PyFtdi_ for years.

There is a large work effort ahead to be able to support more use cases and
tests more APIs, and many unit tests to write.

It cannot replace hardware tests with actual boards and slaves, but should
simplify test setup and help avoiding regression issues.


Usage
`````

No hardware is required to run these tests, to even a single FTDI device.

The test configuration files are described in YaML file format, therefore the
ruamel.yaml_ package is required.

.. code-block:: python

    pip3 install ruamel.yaml
    PYTHONPATH=. FTDI_LOGLEVEL=info pyftdi/tests/mockusb.py

Configuration
`````````````

The ``pyftdi/tests/resources`` directory contains definition files which are
loaded by the mock unit tests.

Although it is possible to create fine grained USB device definitions, the
configuration loader tries to automatically define missing parts to match the
USB device topology of FTDI devices.

This enables to create simple definition files without having to mess with low
level USB definitions whenever possible.

EEPROM content
..............

The :ref:`ftconf` tool can be used to load, modify and generate the content of
a virtual EEPROM, see :doc:`eeprom`.

Examples
........

 * An example of a nearly comprehensive syntax can be found in ``ft232h.yaml``.
 * Another, much more simple example with only mandatory settings can be found
   in ``ft230x.yaml``.
 * An example of multiple FTDI device definitions can be found in
   ``ftmany.yaml``


Availability
~~~~~~~~~~~~

Note that unit tests and the virtual infrastructure are not included in the
distributed Python packages, they are only available from the git repository.
