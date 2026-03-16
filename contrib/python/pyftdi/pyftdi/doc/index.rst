PyFtdi
======

.. cannot use defs.rst here, as PyPi wants a standalone file.
.. |I2C| replace:: I\ :sup:`2`\ C

Documentation
-------------

The latest PyFtdi online documentation is always available from
`here <https://eblot.github.io/pyftdi>`_.

Beware the online version may be more recent than the PyPI hosted version, as
intermediate development versions are not published to
`PyPi <https://pypi.org/project/pyftdi>`_.

PyFtdi documentation can be locally build with Sphinx, see the installation
instructions.

Source code
-----------

PyFtdi releases are available from the Python Package Index from
`PyPi <https://pypi.org/project/pyftdi>`_.

PyFtdi development code is available from
`GitHub <https://github.com/eblot/pyftdi>`_.

Overview
--------

PyFtdi aims at providing a user-space driver for popular FTDI devices,
implemented in pure Python language.

Supported FTDI devices include:

* UART and GPIO bridges

  * FT232R (single port, 3Mbps)
  * FT230X/FT231X/FT234X (single port, 3Mbps)

* UART and multi-serial protocols (SPI, |I2C|, JTAG) bridges

  * FT2232C/D (dual port, clock up to 6 MHz)
  * FT232H (single port, clock up to 30 MHz)
  * FT2232H (dual port, clock up to 30 MHz)
  * FT4232H (quad port, clock up to 30 MHz)
  * FT4232HA (quad port, clock up to 30 MHz)

Features
--------

PyFtdi currently supports the following features:

* UART/Serial USB converter, up to 12Mbps (depending on the FTDI device
  capability)
* GPIO/Bitbang support, with 8-bit asynchronous, 8-bit synchronous and
  8-/16-bit MPSSE variants
* SPI master, with simultanous GPIO support, up to 12 pins per port,
  with support for non-byte sized transfer
* |I2C| master, with simultanous GPIO support, up to 14 pins per port
* Basic JTAG master capabilities
* EEPROM support (some parameters cannot yet be modified, only retrieved)
* Experimental CBUS support on selected devices, 4 pins per port

Supported host OSes
-------------------

* macOS
* Linux
* FreeBSD
* Windows, although not officially supported

.. EOT

Warning
-------

Starting with version *v0.40.0*, several API changes are being introduced.
While PyFtdi tries to maintain backward compatibility with previous versions,
some of these changes may require existing clients to update calls to PyFtdi.

Do not upgrade to *v0.40.0* or above without testing your client against the
new PyFtdi releases. PyFtdi versions up to *v0.39.9* keep a stable API
with *v0.22+* series.

See the *Major Changes* section on the online documentation for details about
potential API breaks.


Major changes
~~~~~~~~~~~~~

 * *read* methods now return ``bytearray`` instead of `Array('B')` so that
   pyserial ``readline()`` may be used. It also brings some performance
   improvements.
 * PyFtdi URLs now supports ``bus:address`` alternative specifiers, which
   required to augment the ``open_*()`` methods with new, optional parameters.
 * ``SpiController`` reserves only one slave line (*/CS*) where it used to
   reserve 4 slave lines in previous releases. This frees more GPIOs when
   default value is used - it is nevertheless still possible to reserve up to 5
   slave lines.
 * type hinting is used for most, if not all, public methods.
 * simplified baudrate divider calculation.

PyFTDI in details
-----------------

.. toctree::
   :maxdepth: 1
   :glob:

   features
   requirements
   installation
   urlscheme
   tools
   api/index
   pinout
   gpio
   eeprom
   testing
   troubleshooting
   authors
   license
