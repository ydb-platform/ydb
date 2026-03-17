# Common Table Expressions with Django

[![Build Status](https://github.com/dimagi/django-cte/actions/workflows/tests.yml/badge.svg)](https://github.com/dimagi/django-cte/actions/workflows/tests.yml)
[![PyPI version](https://badge.fury.io/py/django-cte.svg)](https://badge.fury.io/py/django-cte)

## Installation
```
pip install django-cte
```


## Documentation

The [django-cte documentation](https://dimagi.github.io/django-cte/) shows how
to use Common Table Expressions with the Django ORM.


## Running tests

```
cd django-cte
uv sync

pytest
ruff check

# To run tests against postgres
psql -U username -h localhost -p 5432 -c 'create database django_cte;'
export PG_DB_SETTINGS='{
    "ENGINE":"django.db.backends.postgresql_psycopg2",
    "NAME":"django_cte",
    "USER":"username",
    "PASSWORD":"password",
    "HOST":"localhost",
    "PORT":"5432"}'

# WARNING pytest will delete the test_django_cte database if it exists!
DB_SETTINGS="$PG_DB_SETTINGS" pytest
```

All feature and bug contributions are expected to be covered by tests.


## Publishing a new verison to PyPI

Push a new tag to Github using the format vX.Y.Z where X.Y.Z matches the version
in [`__init__.py`](django_cte/__init__.py).

A new version is published to https://test.pypi.org/p/django-cte on every
push to the *main* branch.

Publishing is automated with [Github Actions](.github/workflows/pypi.yml).
