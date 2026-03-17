# hvac

![Header image](https://raw.githubusercontent.com/hvac/hvac/main/docs/_static/hvac_logo_800px.png)

[HashiCorp](https://hashicorp.com/) [Vault](https://www.vaultproject.io) API client for Python 3.x

[![Build](https://github.com/hvac/hvac/actions/workflows/build-test.yml/badge.svg)](https://github.com/hvac/hvac/actions/workflows/build-test.yml) 
[![Lint](https://github.com/hvac/hvac/actions/workflows/lint-and-test.yml/badge.svg)](https://github.com/hvac/hvac/actions/workflows/lint-and-test.yml)
[![codecov](https://codecov.io/gh/hvac/hvac/branch/main/graph/badge.svg)](https://codecov.io/gh/hvac/hvac)
[![Documentation Status](https://readthedocs.org/projects/hvac/badge/)](https://hvac.readthedocs.io/en/latest/?badge=latest)
[![PyPI version](https://badge.fury.io/py/hvac.svg)](https://badge.fury.io/py/hvac)
[![Twitter - @python_hvac](https://img.shields.io/twitter/follow/python_hvac.svg?label=Twitter%20-%20@python_hvac&style=social?style=plastic)](https://twitter.com/python_hvac)
[![Gitter chat](https://badges.gitter.im/hvac/community.png)](https://gitter.im/hvac/community)

Tested against the latest release, HEAD ref, and 3 previous minor versions (counting back from the latest release) of Vault.
Current official support covers Vault v1.4.7 or later.

> **NOTE:**  Support for EOL Python versions will be dropped at the end of 2022.  Starting in 2023, hvac will track
> with the CPython EOL dates.

## Installation

```console
pip install hvac
```

If you would like to be able to return parsed HCL data as a Python dict for methods that support it:

```console
pip install "hvac[parser]"
```

## Documentation

Additional documentation for this module available at: [hvac.readthedocs.io](https://hvac.readthedocs.io/en/stable/usage/index.html):

* [Getting Started](https://hvac.readthedocs.io/en/stable/overview.html#getting-started)
* [Usage](https://hvac.readthedocs.io/en/stable/usage/index.html)
* [Advanced Usage](https://hvac.readthedocs.io/en/stable/advanced_usage.html)
* [Source Reference / Autodoc](https://hvac.readthedocs.io/en/stable/source/index.html)
* [Contributing](https://hvac.readthedocs.io/en/stable/contributing.html)
* [Changelog](https://hvac.readthedocs.io/en/stable/changelog.html)
