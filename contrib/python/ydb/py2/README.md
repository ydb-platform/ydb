YDB Python SDK
---
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/ydb-platform/ydb/blob/main/LICENSE)
[![PyPI version](https://badge.fury.io/py/ydb.svg)](https://badge.fury.io/py/ydb)
[![Functional tests](https://github.com/ydb-platform/ydb-python-sdk/actions/workflows/tests.yaml/badge.svg)](https://github.com/ydb-platform/ydb-python-sdk/actions/workflows/tests.yaml)
[![Style checks](https://github.com/ydb-platform/ydb-python-sdk/actions/workflows/style.yaml/badge.svg)](https://github.com/ydb-platform/ydb-python-sdk/actions/workflows/style.yaml)

Officially supported Python client for YDB.

## Quickstart

### Prerequisites

- Python 3.8 or higher
- `pip` version 9.0.1 or higher

If necessary, upgrade your version of `pip`:

```sh
$ python -m pip install --upgrade pip
```

If you cannot upgrade `pip` due to a system-owned installation, you can
run the example in a virtualenv:

```sh
$ python -m pip install virtualenv
$ virtualenv venv
$ source venv/bin/activate
$ python -m pip install --upgrade pip
```

Install YDB python sdk:

```sh
$ python -m pip install ydb
```
