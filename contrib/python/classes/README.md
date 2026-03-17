# classes

[![classes logo](https://raw.githubusercontent.com/dry-python/brand/master/logo/classes.png)](https://github.com/dry-python/classes)

-----

[![Build Status](https://travis-ci.org/dry-python/classes.svg?branch=master)](https://travis-ci.org/dry-python/classes)
[![codecov](https://codecov.io/gh/dry-python/classes/branch/master/graph/badge.svg)](https://codecov.io/gh/dry-python/classes)
[![Documentation Status](https://readthedocs.org/projects/classes/badge/?version=latest)](https://classes.readthedocs.io/en/latest/?badge=latest)
[![Python Version](https://img.shields.io/pypi/pyversions/classes.svg)](https://pypi.org/project/classes/)
[![wemake-python-styleguide](https://img.shields.io/badge/style-wemake-000000.svg)](https://github.com/wemake-services/wemake-python-styleguide)
[![Telegram chat](https://img.shields.io/badge/chat-join-blue?logo=telegram)](https://t.me/drypython)

-----

Smart, pythonic, ad-hoc, typed polymorphism for Python.


## Features

- Provides a bunch of primitives to write declarative business logic
- Enforces better architecture
- Fully typed with annotations and checked with `mypy`, [PEP561 compatible](https://www.python.org/dev/peps/pep-0561/)
- Allows to write a lot of simple code without inheritance or interfaces
- Pythonic and pleasant to write and to read (!)
- Easy to start: has lots of docs, tests, and tutorials


## Installation

```bash
pip install classes
```

You also need to [configure](https://classes.readthedocs.io/en/latest/pages/container.html#type-safety)
`mypy` correctly and install our plugin:

```ini
# In setup.cfg or mypy.ini:
[mypy]
plugins =
  classes.contrib.mypy.classes_plugin
```

**Without this step**, your project will report type-violations here and there.

We also recommend to use the same `mypy` settings [we use](https://github.com/wemake-services/wemake-python-styleguide/blob/master/styles/mypy.toml).

Make sure you know how to get started, [check out our docs](https://classes.readthedocs.io/en/latest/)!


## Example

Imagine, that you want to bound implementation to some particular type.
Like, strings behave like this, numbers behave like that, and so on.

The good realworld example is `djangorestframework`.
It is build around the idea that different
data types should be converted differently to and from `json` format.

What is the "traditional" (or outdated if you will!) approach?
To create tons of classes for different data types and use them.

That's how we end up with classes like so:

```python
class IntField(Field):
    def from_json(self, value):
        return value

    def to_json(self, value):
        return value
```

It literally has a lot of problems:

- It is hard to type this code. How can I be sure that my `json` is parseable by the given schema?
- It produces a lot of boilerplate
- It has complex API: there are usually several methods to override, some fields to adjust. Moreover, we use a class, not a simple function
- It is hard to extend the default library for new custom types you will have in your own project
- It is hard to override

There should be a better way of solving this problem!
And typeclasses are a better way!

How would new API look like with this concept?

```python
>>> from typing import Union
>>> from classes import typeclass

>>> @typeclass
... def to_json(instance) -> str:
...     """This is a typeclass definition to convert things to json."""

>>> @to_json.instance(int)
... @to_json.instance(float)
... def _to_json_int(instance: Union[int, float]) -> str:
...     return str(instance)

>>> @to_json.instance(bool)
... def _to_json_bool(instance: bool) -> str:
...     return 'true' if instance else 'false'

>>> @to_json.instance(list)
... def _to_json_list(instance: list) -> str:
...     return '[{0}]'.format(
...         ', '.join(to_json(list_item) for list_item in instance),
...     )

```

See how easy it is to works with types and implementation?

Typeclass is represented as a regular function, so you can use it like one:

```python
>>> to_json(True)
'true'
>>> to_json(1)
'1'
>>> to_json([False, 1, 2.5])
'[false, 1, 2.5]'

```

And it easy to extend this typeclass with your own classes as well:

```python
# Pretending to import the existing library from somewhere:
# from to_json import to_json

>>> import datetime as dt

>>> @to_json.instance(dt.datetime)
... def _to_json_datetime(instance: dt.datetime) -> str:
...     return instance.isoformat()

>>> to_json(dt.datetime(2019, 10, 31, 12, 28, 00))
'2019-10-31T12:28:00'

```

That's how simple, safe, and powerful typeclasses are!
Make sure to [check out our full docs](https://github.com/dry-python/classes) to learn more.


## More!

Want more? [Go to the docs!](https://classes.readthedocs.io) Or read these articles:
- [Typeclasses in Python](https://sobolevn.me/2021/06/typeclasses-in-python)


<p align="center">&mdash; ⭐️ &mdash;</p>
<p align="center"><i>Drylabs maintains dry-python and helps those who want to use it inside their organizations.</i></p>
<p align="center"><i>Read more at <a href="https://drylabs.io">drylabs.io</a></i></p>
