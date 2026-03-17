[![SWUbanner](https://raw.githubusercontent.com/vshymanskyy/StandWithUkraine/main/banner2-direct.svg)](https://vshymanskyy.github.io/StandWithUkraine)

# PyFtdi

![Python package](https://github.com/eblot/pyftdi/actions/workflows/pythonpackage.yml/badge.svg)
![Mock tests](https://github.com/eblot/pyftdi/actions/workflows/pythonmocktests.yml/badge.svg)
![Syntax tests](https://github.com/eblot/pyftdi/actions/workflows/pythonchecksyntax.yml/badge.svg)
[![StandWithUkraine](https://raw.githubusercontent.com/vshymanskyy/StandWithUkraine/main/badges/StandWithUkraine.svg)](https://vshymanskyy.github.io/StandWithUkraine)

[![PyPI](https://img.shields.io/pypi/v/pyftdi.svg?maxAge=2592000)](https://pypi.org/project/pyftdi/)
[![Python Versions](https://img.shields.io/pypi/pyversions/pyftdi.svg)](https://pypi.org/project/pyftdi/)
[![Downloads](https://img.shields.io/pypi/dm/pyftdi.svg)](https://pypi.org/project/pyftdi/)

## Documentation

PyFtdi documentation is available from https://eblot.github.io/pyftdi/

## Overview

PyFtdi aims at providing a user-space driver for popular FTDI devices,
implemented in pure Python language.

Suported FTDI devices include:

* UART and GPIO bridges

  * FT232R (single port, 3Mbps)
  * FT230X/FT231X/FT234X (single port, 3Mbps)

* UART, GPIO and multi-serial protocols (SPI, I2C, JTAG) bridges

  * FT2232C/D (dual port, clock up to 6 MHz)
  * FT232H (single port, clock up to 30 MHz)
  * FT2232H (dual port, clock up to 30 MHz)
  * FT4232H (quad port, clock up to 30 MHz)
  * FT4232HA (quad port, clock up to 30 MHz)

## Features

PyFtdi currently supports the following features:

* UART/Serial USB converter, up to 12Mbps (depending on the FTDI device
  capability)
* GPIO/Bitbang support, with 8-bit asynchronous, 8-bit synchronous and
  8-/16-bit MPSSE variants
* SPI master, with simultanous GPIO support, up to 12 pins per port,
  with support for non-byte sized transfer
* I2C master, with simultanous GPIO support, up to 14 pins per port
* Basic JTAG master capabilities
* EEPROM support (some parameters cannot yet be modified, only retrieved)
* Experimental CBUS support on selected devices, 4 pins per port

## Supported host OSes

* macOS
* Linux
* FreeBSD
* Windows, although not officially supported

## License

`SPDX-License-Identifier: BSD-3-Clause`

## Warnings

### Python support

PyFtdi requires Python 3.9+.

See `pyftdi/doc/requirements.rst` for more details.
