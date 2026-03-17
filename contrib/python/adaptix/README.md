<div align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://github.com/reagento/adaptix/blob/v3.0.0b6/docs/logo/adaptix-with-title-dark.png?raw=true">
    <source media="(prefers-color-scheme: light)" srcset="https://github.com/reagento/adaptix/blob/v3.0.0b6/docs/logo/adaptix-with-title-light.png?raw=true">
    <img alt="adaptix logo" src="https://raw.githubusercontent.com/reagento/adaptix/v3.0.0b2/docs/logo/adaptix-with-title-light.png?raw=true">
  </picture>

  <hr>

  [![PyPI version](https://img.shields.io/pypi/v/adaptix.svg?color=blue)](https://pypi.org/project/adaptix/)
  [![downloads](https://img.shields.io/pypi/dm/adaptix.svg)](https://pypistats.org/packages/adaptix)
  [![versions](https://img.shields.io/pypi/pyversions/adaptix.svg)](https://github.com/reagento/adaptix)
  [![license](https://img.shields.io/github/license/reagento/dataclass_factory.svg)](https://github.com/reagento/adaptix/blob/master/LICENSE)
</div>

An extremely flexible and configurable data model conversion library.

> [!IMPORTANT]
> Adaptix is ready for production!
> The beta version only means there may be some backward incompatible changes, so you need to pin a specific version.

📚 [Documentation](https://adaptix.readthedocs.io/)

## TL;DR

Install
```bash
pip install adaptix==3.0.0b11
```

Use for model loading and dumping.

```python
from dataclasses import dataclass

from adaptix import Retort


@dataclass
class Book:
    title: str
    price: int


data = {
    "title": "Fahrenheit 451",
    "price": 100,
}

# Retort is meant to be global constant or just one-time created
retort = Retort()

book = retort.load(data, Book)
assert book == Book(title="Fahrenheit 451", price=100)
assert retort.dump(book) == data
```

Use for converting one model to another.

```python
from dataclasses import dataclass

from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from adaptix.conversion import get_converter


class Base(DeclarativeBase):
    pass


class Book(Base):
    __tablename__ = 'books'

    id: Mapped[int] = mapped_column(primary_key=True)
    title: Mapped[str]
    price: Mapped[int]


@dataclass
class BookDTO:
    id: int
    title: str
    price: int


convert_book_to_dto = get_converter(Book, BookDTO)

assert (
    convert_book_to_dto(Book(id=183, title="Fahrenheit 451", price=100))
    ==
    BookDTO(id=183, title="Fahrenheit 451", price=100)
)
```

## Use cases

* Validation and transformation of received data for your API.
* Conversion between data models and DTOs.
* Config loading/dumping via codec that produces/takes dict.
* Storing JSON in a database and representing it as a model inside the application code.
* Creating API clients that convert a model to JSON sending to the server.
* Persisting entities at cache storage.
* Implementing fast and primitive ORM.

## Advantages

* Sane defaults for JSON processing, no configuration is needed for simple cases.
* Separated model definition and rules of conversion
  that allow preserving [SRP](https://blog.cleancoder.com/uncle-bob/2014/05/08/SingleReponsibilityPrinciple.html)
  and have different representations for one model.
* Speed. It is one of the fastest data parsing and serialization libraries.
* There is no forced model representation, adaptix can adjust to your needs.
* Support [dozens](https://adaptix.readthedocs.io/en/latest/loading-and-dumping/specific-types-behavior.html) of types,
  including different model kinds:
  ``@dataclass``, ``TypedDict``, ``NamedTuple``,
  [``attrs``](https://www.attrs.org/en/stable/), [``sqlalchemy``](https://docs.sqlalchemy.org/en/20/),
  [``pydantic``](https://docs.pydantic.dev/latest/) and [``msgspec``](https://jcristharif.com/msgspec/).
* Working with self-referenced data types (such as linked lists or trees).
* Saving [path](https://adaptix.readthedocs.io/en/latest/loading-and-dumping/tutorial.html#error-handling)
  where an exception is raised (including unexpected errors).
* Machine-readable [errors](https://adaptix.readthedocs.io/en/latest/loading-and-dumping/tutorial.html#error-handling)
  that could be dumped.
* Support for user-defined generic models.
* Automatic name style conversion (e.g. `snake_case` to `camelCase`).
* [Predicate system](https://adaptix.readthedocs.io/en/latest/loading-and-dumping/tutorial.html#predicate-system)
  that allows to concisely and precisely override some behavior.
* Disabling additional checks to speed up data loading from trusted sources.
* No auto casting by default. The loader does not try to guess value from plenty of input formats.
