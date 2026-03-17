# Zeep: Python SOAP client
[![Documentation Status](https://readthedocs.org/projects/python-zeep/badge/?version=latest)](https://readthedocs.org/projects/python-zeep/)
[![Python Tests](https://github.com/mvantellingen/python-zeep/workflows/Python%20Tests/badge.svg)](https://github.com/mvantellingen/python-zeep/actions?query=workflow%3A%22Python+Tests%22)
[![Coverage](https://codecov.io/gh/mvantellingen/python-zeep/graph/badge.svg?token=zwew4hc8ih)](https://codecov.io/gh/mvantellingen/python-zeep)
[![PyPI version](https://img.shields.io/pypi/v/zeep.svg)](https://pypi.python.org/pypi/zeep/)

A Python SOAP client

## Highlights:
- Compatible with Python 3.9, 3.10, 3.11, 3.12, 3.13 and PyPy3
- Built on top of lxml, requests, and httpx
- Support for Soap 1.1, Soap 1.2, and HTTP bindings
- Support for WS-Addressing headers
- Support for WSSE (UserNameToken / x.509 signing)
- Support for asyncio using the httpx module
- Experimental support for XOP messages

Please see the [documentation](http://docs.python-zeep.org/) for more information.

## Status

> [!NOTE]
> I consider this library to be stable. Since no new developments happen around the SOAP specification, it won't be updated that much. Good PRs which fix bugs are always welcome, however.


## Installation

```bash
pip install zeep
```

Zeep uses the lxml library for parsing XML. See [lxml installation requirements](https://lxml.de/installation.html).

## Usage

```python
from zeep import Client

client = Client('tests/wsdl_files/example.rst')
client.service.ping()
```

To quickly inspect a WSDL file, use:

```bash
python -m zeep <url-to-wsdl>
```

Please see the [documentation](http://docs.python-zeep.org) for more information.


# Sponsors
[![Kraken Tech](https://camo.githubusercontent.com/ecc2b8426b961f8895e4f42741c006839e4488fbe9ba8e92cfa02d48c7fdb3f1/68747470733a2f2f7374617469632e6f63746f70757363646e2e636f6d2f6b746c2f6b72616b656e2d6c6f676f2d772d67726f75702d7365636f6e646172792d323032322e706e67)](https://github.com/octoenergy)

# Support

If you want to report a bug, please first read [the bug reporting guidelines](http://docs.python-zeep.org/en/master/reporting_bugs.html).

Please only report bugs, not support requests, to the GitHub issue tracker.
