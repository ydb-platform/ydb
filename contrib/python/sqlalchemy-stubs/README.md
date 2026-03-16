<img src="http://mypy-lang.org/static/mypy_light.svg" alt="mypy logo" width="300px"/>

Mypy plugin and stubs for SQLAlchemy
====================================

[![Build Status](https://travis-ci.org/dropbox/sqlalchemy-stubs.svg?branch=master)](https://travis-ci.org/dropbox/sqlalchemy-stubs)
[![Checked with mypy](http://www.mypy-lang.org/static/mypy_badge.svg)](http://mypy-lang.org/)

This package contains [type stubs](https://www.python.org/dev/peps/pep-0561/) and a
[mypy plugin](https://mypy.readthedocs.io/en/latest/extending_mypy.html#extending-mypy-using-plugins)
to provide more precise static types and type inference for
[SQLAlchemy framework](http://docs.sqlalchemy.org/en/latest/). SQLAlchemy uses some
Python "magic" that makes having precise types for some code patterns problematic.
This is why we need to accompany the stubs with mypy plugins. The final goal is to
be able to get precise types for most common patterns. Currently, basic operations
with models are supported. A simple example:
```python
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String

Base = declarative_base()

class User(Base):
    __tablename__ = 'users'
    id = Column(Integer, primary_key=True)
    name = Column(String)

user = User(id=42, name=42)  # Error: Incompatible type for "name" of "User"
                             # (got "int", expected "Optional[str]")
user.id  # Inferred type is "int"
User.name  # Inferred type is "Column[Optional[str]]"
```

Some auto-generated attributes are added to models. Simple relationships
are supported but require models to be imported:
```python
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from models.address import Address

...

class User(Base):
    __tablename__ = 'users'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    address = relationship('Address')  # OK, mypy understands string references.
```

The next step is to support precise types for table definitions (e.g.
inferring `Column[Optional[str]]` for `users.c.name`, currently it is just
`Column[Any]`), and precise types for results of queries made using `query()`
and `select()`.

## Installation
Install latest published version as:
```
pip install -U sqlalchemy-stubs
```

*Important*: you need to enable the plugin in your mypy config file:
```
[mypy]
plugins = sqlmypy
```

To install the development version of the package:
```
git clone https://github.com/dropbox/sqlalchemy-stubs
cd sqlalchemy-stubs
pip install -U .
```

## Development Setup

First, clone the repo and cd into it, like in _Installation_, then:
```
git submodule update --init --recursive
pip install -r dev-requirements.txt
```

Then, to run the tests, simply:
```
pytest
```

## Development status

The package is currently in alpha stage. See [issue tracker](https://github.com/dropbox/sqlalchemy-stubs/issues)
for bugs and missing features. If you want to contribute, a good place to start is
[`help-wanted` label](https://github.com/dropbox/sqlalchemy-stubs/issues?q=is%3Aissue+is%3Aopen+label%3A%22help+wanted%22).

Currently, some basic use cases like inferring model field types are supported.
The long term goal is to be able to infer types for more complex situations
like correctly inferring columns in most compound queries.

External contributions to the project should be subject to
[Dropbox Contributor License Agreement (CLA)](https://opensource.dropbox.com/cla/).

--------------------------------
Copyright (c) 2018 Dropbox, Inc.
