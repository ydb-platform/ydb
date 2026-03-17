.. include:: defs.rst

Features
--------

Devices
~~~~~~~

* All FTDI device ports (UART, MPSSE) can be used simultaneously.

  * SPI and |I2C| SPI support simultaneous GPIO R/W access for all pins that
    are not used for SPI/|I2C| feature.

* Several FTDI adapters can be accessed simultaneously from the same Python
  runtime instance.

Supported features
~~~~~~~~~~~~~~~~~~

UART
````

Serial port, up to 6 Mbps. PyFtdi_ includes a pyserial_ emulation layer that
offers transparent access to the FTDI serial ports through a pyserial_-
compliant API. The ``serialext`` directory contains a minimal serial terminal
demonstrating the use of this extension, and a dispatcher automatically
selecting the serial backend (pyserial_, PyFtdi_), based on the serial port
name.

See also :ref:`uart-limitations`.

SPI master
``````````

Supported devices:

=====  ===== ====== ============================================================
Mode   CPol   CPha  Status
=====  ===== ====== ============================================================
  0      0      0   Supported on all MPSSE devices
  1      0      1   Workaround available for on -H series
  2      1      0   Supported on -H series (FT232H_/FT2232H_/FT4232H_/FT4232HA_)
  3      1      1   Workaround available for on -H series
=====  ===== ====== ============================================================

PyFtdi_ can be used with pyspiflash_ module that demonstrates how to
use the FTDI SPI master with a pure-Python serial flash device driver for
several common devices.

Both Half-duplex (write or read) and full-duplex (synchronous write and read)
communication modes are supported.

Experimental support for non-byte aligned access, where up to 7 trailing bits
can be discarded: no clock pulse is generated for those bits, so that SPI
transfer of non byte-sized can be performed.

See :ref:`spi_wiring` and :ref:`spi_limitations`.

Note: FTDI*232* devices cannot be used as an SPI slave.

|I2C| master
````````````

Supported devices: FT232H_, FT2232H_, FT4232H_, FT4232HA_

For now, only 7-bit addresses are supported.

GPIOs can be used while |I2C| mode is enabled.

The ``i2cscan.py`` script helps to discover which I2C devices are connected to
the FTDI I2C bus. See the :ref:`tools` chapter to locate this tool.

The pyi2cflash_ module demonstrates how to use the FTDI |I2C| master to access
serial EEPROMS.

See :ref:`i2c_wiring` and :ref:`i2c_limitations`.

Note: FTDI*232* devices cannot be used as an |I2C| slave.

JTAG
````

JTAG API is limited to low-level access. It is not intented to be used for
any flashing or debugging purpose, but may be used as a base to perform SoC
tests and boundary scans.

It requires the PyJtagTools_ Python module which integrates a JTAG engine, while
PyFtdi_ implements the FTDI JTAG backend.

EEPROM
``````

The ``pyftdi/bin/ftconf.py`` script helps to manage the content of the FTDI
companion EEPROM.


Status
~~~~~~

This project is still in beta development stage. PyFtdi_ is developed as an
open-source solution.
