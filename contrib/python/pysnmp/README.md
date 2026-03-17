
SNMP Library for Python
=======================

[![Become a Sponsor](https://img.shields.io/badge/Become%20a%20Sponsor-lextudio-orange.svg?style=for-readme)](https://github.com/sponsors/lextudio)
[![PyPI](https://img.shields.io/pypi/v/pysnmp.svg)](https://pypi.python.org/pypi/pysnmp)
[![PyPI Downloads](https://img.shields.io/pypi/dd/pysnmp)](https://pypi.python.org/pypi/pysnmp/)
[![Python Versions](https://img.shields.io/pypi/pyversions/pysnmp.svg)](https://pypi.python.org/pypi/pysnmp/)
[![GitHub license](https://img.shields.io/badge/license-BSD-blue.svg)](https://raw.githubusercontent.com/lextudio/pysnmp/master/LICENSE.rst)

This is a pure-Python, open source and free implementation of v1/v2c/v3
SNMP engine distributed under 2-clause
[BSD license](https://www.pysnmp.com/pysnmp/license.html).

Features
--------

* Complete SNMPv1/v2c and SNMPv3 support based on IETF RFC standards.
* SMI framework for resolving MIB information and implementing SMI
  Managed Objects
* Complete SNMP entity implementation
* USM Extended Security Options support (3DES, 192/256-bit AES encryption)
  based on draft standards and vendor implementations.
* Extensible network transports framework (UDP/IPv4, UDP/IPv6)
* Asynchronous socket-based IO API support
  via [asyncio](https://docs.python.org/3/library/asyncio.html) integration
* [PySMI](https://www.pysnmp.com/pysmi/) integration for dynamic MIB
  compilation
* Built-in instrumentation exposing protocol engine operations
* Python eggs and py2exe friendly
* 100% Python, works with Python 3.10+ (tested on 3.10–3.14)
* MT-safe (if SnmpEngine is thread-local)

Features, specific to SNMPv3 model include:

* USM authentication (MD5/SHA-1/SHA-2) and privacy (DES/AES) protocols
  (RFC3414, RFC7860)
* View-based access control to use with any SNMP model (RFC3415)
* Built-in SNMP proxy PDU converter for building multi-lingual
  SNMP entities (RFC2576)
* Remote SNMP engine configuration
* Optional SNMP engine discovery
* Shipped with standard SNMP applications (RFC3413)

Download & Install
------------------

The PySNMP package is freely available for download from
[PyPI](https://pypi.python.org/pypi/pysnmp) and
[GitHub](https://github.com/lextudio/pysnmp.git).

Just run:

```bash
pip install pysnmp
```

To download and install PySNMP along with its dependencies:

* `pyasn1` package from [PyASN1](https://pyasn1.readthedocs.io)
* If `pysmi` package from [PySMI](https://www.pysnmp.com/pysmi/) presents,
  MIB services are enabled.
* If `cryptography` package (43.0.x and above) presents, strong SNMPv3 encryption is enabled.

Make sure you check out other sibling projects of PySNMP from
[the home page](https://www.pysnmp.com/).

Documentation
-------------

PySNMP documentation is hosted at the [docs site](https://www.pysnmp.com/pysnmp/).

* Copyright (c) 1999-2020, [Ilya Etingof](https://lists.openstack.org/pipermail/openstack-discuss/2022-August/030062.html)
* Copyright (c) 2022-2025, [LeXtudio Inc.](mailto:support@lextudio.com)
* Copyright (c) 1999-2025, [Other PySNMP contributors](https://github.com/lextudio/pysnmp/THANKS.txt)

All rights reserved.
