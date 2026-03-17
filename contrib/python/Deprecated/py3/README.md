# Deprecated Decorator

Python ``@deprecated`` decorator to deprecate old python classes, functions or methods.


[![license](https://img.shields.io/badge/license-MIT-blue?logo=opensourceinitiative&logoColor=white)](https://raw.githubusercontent.com/laurent-laporte-pro/deprecated/master/LICENSE.rst)
[![GitHub release](https://img.shields.io/github/v/release/laurent-laporte-pro/deprecated?logo=github&logoColor=white)](https://github.com/laurent-laporte-pro/deprecated/releases/latest)
[![PyPI](https://img.shields.io/pypi/v/deprecated?logo=pypi&logoColor=white)](https://pypi.org/project/Deprecated/)
[![GitHub Workflow Status](https://img.shields.io/github/actions/workflow/status/laurent-laporte-pro/deprecated/python-package.yml?logo=github&logoColor=white)](https://github.com/laurent-laporte-pro/deprecated/actions/workflows/python-package.yml)
[![Coveralls branch](https://img.shields.io/coverallsCoverage/github/laurent-laporte-pro/deprecated?logo=coveralls&logoColor=white)](https://coveralls.io/github/laurent-laporte-pro/deprecated?branch=master)
[![Read the Docs (version)](https://img.shields.io/readthedocs/deprecated/latest?logo=readthedocs&logoColor=white)
](http://deprecated.readthedocs.io/en/latest/?badge=latest)

## Installation

```shell
pip install Deprecated
```

## Usage

To use this, decorate your deprecated function with **@deprecated** decorator:

```python
from deprecated import deprecated


@deprecated
def some_old_function(x, y):
    return x + y
```

You can also decorate a class or a method:

```python
from deprecated import deprecated


class SomeClass(object):
    @deprecated
    def some_old_method(self, x, y):
        return x + y


@deprecated
class SomeOldClass(object):
    pass
```

You can give a "reason" message to help the developer to choose another function/class:

```python
from deprecated import deprecated


@deprecated(reason="use another function")
def some_old_function(x, y):
    return x + y
```

## Authors

The authors of this library are:
[Marcos CARDOSO](https://github.com/vrcmarcos), and
[Laurent LAPORTE](https://github.com/laurent-laporte-pro).

The original code was made in [this StackOverflow post](https://stackoverflow.com/questions/2536307) by
[Leandro REGUEIRO](https://stackoverflow.com/users/1336250/leandro-regueiro),
[Patrizio BERTONI](https://stackoverflow.com/users/1315480/patrizio-bertoni), and
[Eric WIESER](https://stackoverflow.com/users/102441/eric).
