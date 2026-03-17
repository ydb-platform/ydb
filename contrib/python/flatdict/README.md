# FlatDict

[![Version](https://img.shields.io/pypi/v/flatdict.svg?)](https://pypi.python.org/pypi/flatdict)
[![Status](https://github.com/gmr/flatdict/workflows/Testing/badge.svg)](https://github.com/gmr/flatdict/actions)
[![Coverage](https://img.shields.io/codecov/c/github/gmr/flatdict.svg?)](https://codecov.io/github/gmr/flatdict?branch=master)
[![License](https://img.shields.io/pypi/l/flatdict.svg?)](https://flatdict.readthedocs.org)

`FlatDict` and `FlatterDict` are dict classes that allow for single level,
delimited key/value pair mapping of nested dictionaries. You can interact with
`FlatDict` and `FlatterDict` like a normal dictionary and access child
dictionaries as you normally would or with the composite key.

*For example:*

```python
value = flatdict.FlatDict({'foo': {'bar': 'baz', 'qux': 'corge'}})
```

*would be the same as:*

```python
value == {'foo:bar': 'baz', 'foo:qux': 'corge'}
```

*values can be accessed as:*

```python
print(foo['foo:bar'])

# or

print(foo['foo']['bar'])
```

Additionally, lists and tuples are also converted into dicts using `enumerate()`,
using the `FlatterDict` class.

*For example:*

```python
value = flatdict.FlatterDict({'list': ['a', 'b', 'c']})
```

*will be the same as:*

```python
value == {'list:0': 'a', 'list:1': 'b', 'list:2': 'c'}
```

## API

Documentation is available at https://flatdict.readthedocs.io

## Versioning

This package attempts to use semantic versioning. API changes are indicated
by the major version, non-breaking improvements by the minor, and bug fixes
in the revision.

It is recommended that you pin your targets to greater or equal to the current
version and less than the next major version.

## Installation

```bash
pip install flatdict
```
