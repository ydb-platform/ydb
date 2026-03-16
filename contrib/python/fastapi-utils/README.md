<p align="center">
    <em>Quicker FastApi developing tools</em>
</p>
<p align="center">
<a href="https://github.com/dmontagu/fastapi-utils" target="_blank">
 <img src="https://img.shields.io/github/last-commit/dmontagu/fastapi-utils.svg">
 <img src="https://github.com/dmontagu/fastapi-utils/workflows/build/badge.svg" alt="Build CI">
</a>
<a href="https://codecov.io/gh/dmontagu/fastapi-utils" target="_blank">
    <img src="https://codecov.io/gh/dmontagu/fastapi-utils/branch/master/graph/badge.svg" alt="Coverage">
</a>
<br />
<a href="https://pypi.org/project/fastapi-utils" target="_blank">
    <img src="https://badge.fury.io/py/fastapi-utils.svg" alt="Package version">
</a>
<a href="https://github.com/dmontagu/fastapi-utils" target="_blank">
    <img src="https://img.shields.io/pypi/pyversions/fastapi-utils.svg" alt="Python versions">
    <img src="https://img.shields.io/github/license/dmontagu/fastapi-utils.svg" alt="License">
</a>
</p>

---
**Documentation**: <a href="https://fastapiutils.github.io/fastapi-utils/" target="_blank">https://fastapiutils.github.io/fastapi-utils/</a>

**Source Code**: <a href="https://github.com/dmontagu/fastapi-utils" target="_blank">https://github.com/dmontagu/fastapi-utils</a>

---

<a href="https://fastapi.tiangolo.com">FastAPI</a> is a modern, fast web framework for building APIs with Python 3.8+.

But if you're here, you probably already knew that!

---

## Features

This package includes a number of utilities to help reduce boilerplate and reuse common functionality across projects:

* **Resource Class**: Create CRUD with ease the OOP way with `Resource` base class that lets you implement methods quick.
* **Class Based Views**: Stop repeating the same dependencies over and over in the signature of related endpoints.
* **Repeated Tasks**: Easily trigger periodic tasks on server startup
* **Timing Middleware**: Log basic timing information for every request
* **OpenAPI Spec Simplification**: Simplify your OpenAPI Operation IDs for cleaner output from OpenAPI Generator
* **SQLAlchemy Sessions**: The `FastAPISessionMaker` class provides an easily-customized SQLAlchemy Session dependency

---

It also adds a variety of more basic utilities that are useful across a wide variety of projects:

* **APIModel**: A reusable `pydantic.BaseModel`-derived base class with useful defaults
* **APISettings**: A subclass of `pydantic.BaseSettings` that makes it easy to configure FastAPI through environment variables
* **String-Valued Enums**: The `StrEnum` and `CamelStrEnum` classes make string-valued enums easier to maintain
* **CamelCase Conversions**: Convenience functions for converting strings from `snake_case` to `camelCase` or `PascalCase` and back
* **GUID Type**: The provided GUID type makes it easy to use UUIDs as the primary keys for your database tables

See the [docs](https://fastapiutils.github.io/fastapi-utils//) for more details and examples.

## Requirements

This package is intended for use with any recent version of FastAPI (depending on `pydantic>=1.0`), and Python 3.8+.

## Installation

```bash
pip install fastapi-utils  # For basic slim package :)

pip install fastapi-utils[session]  # To add sqlalchemy session maker

pip install fastapi-utils[all]  # For all the packages
```

## License

This project is licensed under the terms of the MIT license.
