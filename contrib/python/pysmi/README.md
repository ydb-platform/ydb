# SNMP MIB Compiler

[![Become a Sponsor](https://img.shields.io/badge/Become%20a%20Sponsor-lextudio-orange.svg?style=for-readme)](https://github.com/sponsors/lextudio)
[![PyPI](https://img.shields.io/pypi/v/pysmi.svg)](https://pypi.org/project/pysmi)
[![PyPI Downloads](https://img.shields.io/pypi/dd/pysmi)](https://pypi.python.org/pypi/pysmi/)
[![Python Versions](https://img.shields.io/pypi/pyversions/pysmi.svg)](https://pypi.org/project/pysmi/)
[![GitHub license](https://img.shields.io/badge/license-BSD-blue.svg)](https://raw.githubusercontent.com/lextudio/pysmi/master/LICENSE.rst)

PySMI is a pure-Python implementation of
[SNMP SMI](https://en.wikipedia.org/wiki/Management_information_base) MIB parser.
This tool is designed to turn ASN.1 MIBs into various formats. As of this moment,
JSON and [PySNMP](https://github.com/lextudio/pysnmp) modules can be generated
from ASN.1 MIBs.

## Features

* Understands SMIv1, SMIv2 and de-facto SMI dialects
* Turns MIBs into PySNMP classes and JSON documents
* Maintains an index of MIB objects over many MIB modules
* Automatically pulls ASN.1 MIBs from local directories, ZIP archives,
  and HTTP servers
* 100% Python, works with Python 3.9+

PySMI documentation can be found at [PySMI site](https://www.pysnmp.com/pysmi).

## How to get PySMI

The pysmi package is distributed under terms and conditions of 2-clause
BSD [license](https://www.pysnmp.com/pysmi/license.html). Source code is freely
available as a GitHub [repo](https://github.com/lextudio/pysmi).

You could `pip install pysmi` or download it from [PyPI](https://pypi.org/project/pysmi/).

If something does not work as expected,
[open an issue](https://github.com/lextudio/pysnmp/issues) at GitHub.

Copyright (c) 2015-2020, [Ilya Etingof](mailto:etingof@gmail.com).
Copyright (c) 2022-2026, [LeXtudio Inc.](mailto:support@lextudio.com).
All rights reserved.
