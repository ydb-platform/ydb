# YDB Dialect for SQLAlchemy
---
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/ydb-platform/ydb-sqlalchemy/blob/main/LICENSE)
[![PyPI version](https://badge.fury.io/py/ydb-sqlalchemy.svg)](https://badge.fury.io/py/ydb-sqlalchemy)
[![API Reference](https://img.shields.io/badge/API-Reference-lightgreen.svg)](https://ydb-platform.github.io/ydb-sqlalchemy/api/index.html)
[![Functional tests](https://github.com/ydb-platform/ydb-sqlalchemy/actions/workflows/tests.yml/badge.svg)](https://github.com/ydb-platform/ydb-sqlalchemy/actions/workflows/tests.yml)
[![Style checks](https://github.com/ydb-platform/ydb-sqlalchemy/actions/workflows/style.yml/badge.svg)](https://github.com/ydb-platform/ydb-sqlalchemy/actions/workflows/style.yml)

This repository contains YQL dialect for SqlAlchemy 2.0.

---

**Documentation**: <a href="https://ydb-platform.github.io/ydb-sqlalchemy" target="_blank">https://ydb-platform.github.io/ydb-sqlalchemy</a>

---

**Note**: Dialect also works with SqlAlchemy 1.4, but it is not fully tested.


## Installation

### Via PyPI
To install ydb-sqlalchemy from PyPI:

```bash
$ pip install ydb-sqlalchemy
```

### Installation from source code
To work with current ydb-sqlalchemy version clone this repo and run from source root:

```bash
$ pip install -U .
```

## Getting started

Connect to local YDB using SqlAlchemy:

```python3
import sqlalchemy as sa


engine = sa.create_engine("yql+ydb://localhost:2136/local")

with engine.connect() as conn:
  rs = conn.execute(sa.text("SELECT 1 AS value"))
  print(rs.fetchone())

```

## Authentication

To specify credentials, you should pass `credentials` object to `connect_args` argument of `create_engine` method.

### Static Credentials

To use static credentials you should specify `username` and `password` as follows:

```python3
engine = sa.create_engine(
    "yql+ydb://localhost:2136/local",
    connect_args = {
        "credentials": {
            "username": "...",
            "password": "..."
        }
    }
)
```

### Token Credentials

To use access token credentials you should specify `token` as follows:

```python3
engine = sa.create_engine(
    "yql+ydb://localhost:2136/local",
    connect_args = {
        "credentials": {
            "token": "..."
        }
    }
)
```

### Service Account Credentials

To use service account credentials you should specify `service_account_json` as follows:

```python3
engine = sa.create_engine(
    "yql+ydb://localhost:2136/local",
    connect_args = {
        "credentials": {
            "service_account_json": {
                "id": "...",
                "service_account_id": "...",
                "created_at": "...",
                "key_algorithm": "...",
                "public_key": "...",
                "private_key": "..."
            }
        }
    }
)
```

### Credentials from YDB SDK

To use any credentials that comes with `ydb` package, just pass credentials object as follows:

```python3
import ydb.iam

engine = sa.create_engine(
    "yql+ydb://localhost:2136/local",
    connect_args = {
        "credentials": ydb.iam.MetadataUrlCredentials()
    }
)

```


## Migrations

To setup `alembic` to work with `YDB` please check [this example](https://github.com/ydb-platform/ydb-sqlalchemy/tree/main/examples/alembic).

## Development

### Run Tests:

Run the command from the root directory of the repository to start YDB in a local docker container.
```bash
$ docker-compose up
```

To run all tests execute the command from the root directory of the repository:
```bash
$ tox -e test-all
```

Run specific test:
```bash
$ tox -e test -- test/test_core.py
```

Check code style:
```bash
$ tox -e style
```

Reformat code:
```bash
$ tox -e isort
$ tox -e black-format
```

Run example (needs running local YDB):
```bash
$ python -m pip install virtualenv
$ virtualenv venv
$ source venv/bin/activate
$ pip install -r requirements.txt
$ python examples/example.py
```

## Additional Notes

### Pandas
It is possible to use YDB SA engine with `pandas` fuctions [to_sql()](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.to_sql.html) and [read_sql](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_sql.html). However, there are some limitations:

* `to_sql` method can not be used with column tables, since it is impossible to specify `NOT NULL` columns with current `to_sql` arguments. YDB requires column tables to have `NOT NULL` attribute on `PK` columns.

* `to_sql` is not fully optimized to load huge datasets. It is recommended to use `method="multi"` and avoid setting a very large `chunksize`.

* `read_sql` is not fully optimized to load huge datasets and could lead to significant memory consumptions.
