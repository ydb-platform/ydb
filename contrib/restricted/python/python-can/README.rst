python-can
==========

|pypi| |conda| |python_implementation| |downloads| |downloads_monthly|

|docs| |github-actions| |coverage| |formatter|

.. |pypi| image:: https://img.shields.io/pypi/v/python-can.svg
   :target: https://pypi.python.org/pypi/python-can/
   :alt: Latest Version on PyPi

.. |conda| image:: https://img.shields.io/conda/v/conda-forge/python-can
   :target: https://github.com/conda-forge/python-can-feedstock
   :alt: Latest Version on conda-forge

.. |python_implementation| image:: https://img.shields.io/pypi/implementation/python-can
   :target: https://pypi.python.org/pypi/python-can/
   :alt: Supported Python implementations

.. |downloads| image:: https://static.pepy.tech/badge/python-can
   :target: https://pepy.tech/project/python-can
   :alt: Downloads on PePy

.. |downloads_monthly| image:: https://static.pepy.tech/badge/python-can/month
   :target: https://pepy.tech/project/python-can
   :alt: Monthly downloads on PePy

.. |formatter| image:: https://img.shields.io/badge/code%20style-black-000000.svg
   :target: https://github.com/python/black
   :alt: This project uses the black formatter.

.. |docs| image:: https://readthedocs.org/projects/python-can/badge/?version=stable
   :target: https://python-can.readthedocs.io/en/stable/
   :alt: Documentation

.. |github-actions| image:: https://github.com/hardbyte/python-can/actions/workflows/ci.yml/badge.svg
   :target: https://github.com/hardbyte/python-can/actions/workflows/ci.yml
   :alt: Github Actions workflow status

.. |coverage| image:: https://coveralls.io/repos/github/hardbyte/python-can/badge.svg?branch=main
   :target: https://coveralls.io/github/hardbyte/python-can?branch=main
   :alt: Test coverage reports on Coveralls.io

The **C**\ ontroller **A**\ rea **N**\ etwork is a bus standard designed
to allow microcontrollers and devices to communicate with each other. It
has priority based bus arbitration and reliable deterministic
communication. It is used in cars, trucks, boats, wheelchairs and more.

The ``can`` package provides controller area network support for
Python developers; providing common abstractions to
different hardware devices, and a suite of utilities for sending and receiving
messages on a can bus.

The library currently supports CPython as well as PyPy and runs on Mac, Linux and Windows.

==============================  ===========
Library Version                 Python
------------------------------  -----------
  2.x                           2.6+, 3.4+
  3.x                           2.7+, 3.5+
  4.0+                          3.7+
  4.3+                          3.8+
  4.6+                          3.9+
==============================  ===========


Features
--------

- common abstractions for CAN communication
- support for many different backends (see the `docs <https://python-can.readthedocs.io/en/stable/interfaces.html>`__)
- receiving, sending, and periodically sending messages
- normal and extended arbitration IDs
- `CAN FD <https://en.wikipedia.org/wiki/CAN_FD>`__ support
- many different loggers and readers supporting playback: ASC (CANalyzer format), BLF (Binary Logging Format by Vector), MF4 (Measurement Data Format v4 by ASAM), TRC, CSV, SQLite, and Canutils log
- efficient in-kernel or in-hardware filtering of messages on supported interfaces
- bus configuration reading from a file or from environment variables
- command line tools for working with CAN buses (see the `docs <https://python-can.readthedocs.io/en/stable/scripts.html>`__)
- more


Example usage
-------------

``pip install python-can``

.. code:: python

    # import the library
    import can

    # create a bus instance using 'with' statement,
    # this will cause bus.shutdown() to be called on the block exit;
    # many other interfaces are supported as well (see documentation)
    with can.Bus(interface='socketcan',
                  channel='vcan0',
                  receive_own_messages=True) as bus:

       # send a message
       message = can.Message(arbitration_id=123, is_extended_id=True,
                             data=[0x11, 0x22, 0x33])
       bus.send(message, timeout=0.2)

       # iterate over received messages
       for msg in bus:
           print(f"{msg.arbitration_id:X}: {msg.data}")

       # or use an asynchronous notifier
       notifier = can.Notifier(bus, [can.Logger("recorded.log"), can.Printer()])

You can find more information in the documentation, online at
`python-can.readthedocs.org <https://python-can.readthedocs.org/en/stable/>`__.


Discussion
----------

If you run into bugs, you can file them in our
`issue tracker <https://github.com/hardbyte/python-can/issues>`__ on GitHub.

`Stackoverflow <https://stackoverflow.com/questions/tagged/can+python>`__ has several
questions and answers tagged with ``python+can``.

Wherever we interact, we strive to follow the
`Python Community Code of Conduct <https://www.python.org/psf/codeofconduct/>`__.


Contributing
------------

See `doc/development.rst <doc/development.rst>`__ for getting started.
