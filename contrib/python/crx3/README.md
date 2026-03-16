# CRX3

[![PyPI](https://img.shields.io/pypi/v/crx3)](https://pypi.org/project/crx3/)
[![PyPI Supported Python Versions](https://img.shields.io/pypi/pyversions/crx3.svg)](https://pypi.python.org/pypi/crx3/)
[![Downloads](https://static.pepy.tech/badge/crx3)](https://pepy.tech/project/crx3)
[![GitHub Actions (Tests)](https://github.com/liying2008/python-crx3/actions/workflows/tests.yml/badge.svg)](https://github.com/liying2008/python-crx3/actions/workflows/tests.yml)

**crx3** is a python library for packaging and parsing crx files.

## Installation

`crx3` is available on PyPI:

```console
$ python -m pip install crx3
```

crx3 officially supports Python 3.7+.

## Functions

- Create a private key for signing the crx file.

```python
from crx3 import creator

creator.create_private_key_file('output/example-extension.pem')
```

- Packaging a zip file or extension code directory to a crx file.

```python
from crx3 import creator

creator.create_crx_file('example/example-extension', 'example/example-extension.pem', 'output/example-extension.crx')
```

- Verify if a file is a valid crx version 3 file.

```python
from crx3 import verifier

verifier_result, header_info = verifier.verify('example/example-extension.crx')

assert verifier_result == verifier.VerifierResult.OK_FULL
assert header_info.crx_id == 'jjomgndeajdmncfenopimafofpnflcfo'
assert header_info.public_key == 'MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMI...FkbU7H8sDQIDAQAB'
```

## Commands

- **Create a crx file.**

`crx3 create [-pk PRIVATE_KEY_FILE] [-o OUTPUT_FILE] [-v] source`

```
usage: crx3 create [-h] [-pk PRIVATE_KEY_FILE] [-o OUTPUT_FILE] [-v] source

positional arguments:
  source                zip file or directory to be packed

options:
  -h, --help            show this help message and exit
  -pk PRIVATE_KEY_FILE, --private-key PRIVATE_KEY_FILE
                        private key file to be used for signing. If not specified, the program automatically creates a new one and saves it to the same directory as the crx file
  -o OUTPUT_FILE, --output OUTPUT_FILE
                        path to the output crx file
  -v, --verbose         print more information
```

- **Verify that a crx file is valid.**

`crx3 verify [-v] crx_file`

```
usage: crx3 verify [-h] [-v] crx_file
positional arguments:
  crx_file       crx file

options:
  -h, --help     show this help message and exit
  -v, --verbose  print more information
```
