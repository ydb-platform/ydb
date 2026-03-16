
[![os: linux](https://img.shields.io/badge/os-linux-blue)](https://docs.python.org/3.10/)
[![python: 3.10+](https://img.shields.io/badge/python-3.10_|_3.11-blue)](https://devguide.python.org/versions)
[![python style: google](https://img.shields.io/badge/python%20style-google-blue)](https://google.github.io/styleguide/pyguide.html)
[![imports: isort](https://img.shields.io/badge/%20imports-isort-%231674b1?style=flat&labelColor=ef8336)](https://github.com/PyCQA/isort)
[![code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![code style: pycodestyle](https://img.shields.io/badge/code%20style-pycodestyle-green)](https://github.com/PyCQA/pycodestyle)
[![doc style: pydocstyle](https://img.shields.io/badge/doc%20style-pydocstyle-green)](https://github.com/PyCQA/pydocstyle)
[![static typing: mypy](https://img.shields.io/badge/static_typing-mypy-green)](https://github.com/python/mypy)
[![linting: pylint](https://img.shields.io/badge/linting-pylint-yellowgreen)](https://github.com/PyCQA/pylint)
[![testing: pytest](https://img.shields.io/badge/testing-pytest-yellowgreen)](https://github.com/pytest-dev/pytest)
[![security: bandit](https://img.shields.io/badge/security-bandit-black)](https://github.com/PyCQA/bandit)
[![license: MIT](https://img.shields.io/badge/license-MIT-lightgrey)](LICENSE)


# Atoparser

Atoparser is a zero dependency Atop log processing library written in Python. The library supports reading binary C data
directly from compressed or uncompressed Atop log files, without the need to install Atop or call a subprocess.
The converted data contains structured Python objects, that can then be used for JSON, CSV, or other types of output,
storage, and analysis.

For full information on the amazing performance monitoring software that creates these files, known as "Atop", refer to:  
[Atop - The one stop shop for all your tops](https://www.atoptool.nl/)


## Table Of Contents

  * [Compatibility](#compatibility)
  * [Getting Started](#getting-started)
    * [Installation](#installation)
  * [How Tos](#how-tos)
    * [Read an Atop log with the example JSON command](#read-an-atop-log-with-the-example-json-command)
    * [Iterate over the C structs as Python objects](#iterate-over-the-c-structs-as-python-objects)
    * [Convert the C structs into JSON compatible objects](#convert-the-c-structs-into-json-compatible-objects)
    * [Contribute](#contribute)
    * [Advanced Guides](#advanced-guides)


## Compatibility

- Supports Python 3.10+
- Supports Atop 1.26 and 2.3 through 2.12.


## Getting Started

### Installation

Install Atoparser via pip:
```shell
pip install atoparser
```

Or via git clone:
```shell
git clone <path to fork>
cd atoparser
pip install .
```

Or build and install from wheel:
```shell
# Build locally.
git clone <path to fork>
cd atoparser
make wheel

# Push dist/atoparser*.tar.gz to environment where it will be installed.
pip install dist/atoparser*.tar.gz
```


## How Tos

### Read an Atop log with the example JSON command:
```shell
atoparser ~/atop.log -P CPU --pretty
```

### Iterate over the C structs as Python objects:  
```python
import atoparser

with open(file, 'rb') as raw_file:
    header = atoparser.get_header(raw_file)
    for record, sstat, tstats, cgroups in atoparser.generate_statistics(raw_file, header):
        total_cycles = record.interval * sstat.cpu.nrcpu * header.hertz
        usage = 1 - sstat.cpu.all.itime / total_cycles
        print(f'CPU usage was {usage:.02%}')
```

### Convert the C structs into JSON compatible objects:  
```python
import json
import atoparser

with open(file, 'rb') as raw_file:
    header = atoparser.get_header(raw_file)
    print(json.dumps(atoparser.struct_to_dict(header), indent=2))
```

### Contribute

Refer to the [Contributing Guide](CONTRIBUTING.md) for information on how to contribute to this project.

### Advanced Guides

Refer to [Advanced How Tos](docs/HOW_TO.md) for more advanced topics, such as adding a new Atop version.
