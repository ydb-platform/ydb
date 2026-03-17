<p align="center">
  <a href="https://dirty-equals.helpmanual.io">
    <img src="https://dirty-equals.helpmanual.io/latest/img/logo-text.svg" alt="dirty-equals">
  </a>
</p>
<p align="center">
  <em>Doing dirty (but extremely useful) things with equals.</em>
</p>
<p align="center">
  <a href="https://github.com/samuelcolvin/dirty-equals/actions/workflows/ci.yml?query=event%3Apush+branch%3Amain+workflow%3ACI"><img src="https://github.com/samuelcolvin/dirty-equals/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://codecov.io/gh/samuelcolvin/dirty-equals"><img src="https://codecov.io/gh/samuelcolvin/dirty-equals/branch/main/graph/badge.svg" alt="Coverage"></a>
  <a href="https://pypi.python.org/pypi/dirty-equals"><img src="https://img.shields.io/pypi/v/dirty-equals.svg" alt="pypi"></a>
  <a href="https://github.com/samuelcolvin/dirty-equals"><img src="https://img.shields.io/pypi/pyversions/dirty-equals.svg" alt="versions"></a>
  <a href="https://github.com/samuelcolvin/dirty-equals/blob/main/LICENSE"><img src="https://img.shields.io/github/license/samuelcolvin/dirty-equals.svg" alt="license"></a>
</p>

---

**Documentation**: [dirty-equals.helpmanual.io](https://dirty-equals.helpmanual.io)

**Source Code**: [github.com/samuelcolvin/dirty-equals](https://github.com/samuelcolvin/dirty-equals)

---

**dirty-equals** is a python library that (mis)uses the `__eq__` method to make python code (generally unit tests)
more declarative and therefore easier to read and write.

*dirty-equals* can be used in whatever context you like, but it comes into its own when writing unit tests for
applications where you're commonly checking the response to API calls and the contents of a database.

## Usage

Here's a trivial example of what *dirty-equals* can do:

```py
from dirty_equals import IsPositive

assert 1 == IsPositive
assert -2 == IsPositive  # this will fail!
```

**That doesn't look very useful yet!**, but consider the following unit test code using *dirty-equals*:

```py title="More Powerful Usage"
from dirty_equals import IsJson, IsNow, IsPositiveInt, IsStr

...

# user_data is a dict returned from a database or API which we want to test
assert user_data == {
    # we want to check that id is a positive int
    'id': IsPositiveInt,
    # we know avatar_file should be a string, but we need a regex as we don't know whole value
    'avatar_file': IsStr(regex=r'/[a-z0-9\-]{10}/example\.png'),
    # settings_json is JSON, but it's more robust to compare the value it encodes, not strings
    'settings_json': IsJson({'theme': 'dark', 'language': 'en'}),
    # created_ts is datetime, we don't know the exact value, but we know it should be close to now
    'created_ts': IsNow(delta=3),
}
```

Without *dirty-equals*, you'd have to compare individual fields and/or modify some fields before comparison -
the test would not be declarative or as clear.

*dirty-equals* can do so much more than that, for example:

* [`IsPartialDict`](https://dirty-equals.helpmanual.io/types/dict/#dirty_equals.IsPartialDict)
  lets you compare a subset of a dictionary
* [`IsStrictDict`](https://dirty-equals.helpmanual.io/types/dict/#dirty_equals.IsStrictDict)
  lets you confirm order in a dictionary
* [`IsList`](https://dirty-equals.helpmanual.io/types/sequence/#dirty_equals.IsList) and
  [`IsTuple`](https://dirty-equals.helpmanual.io/types/sequence/#dirty_equals.IsTuple)
  lets you compare partial lists and tuples, with or without order constraints
* nesting any of these types inside any others
* [`IsInstance`](https://dirty-equals.helpmanual.io/types/other/#dirty_equals.IsInstance)
  lets you simply confirm the type of an object
* You can even use [boolean operators](https://dirty-equals.helpmanual.io/usage/#boolean-logic)
  `|` and `&` to combine multiple conditions
* and much more...

## Installation

Simply:

```bash
pip install dirty-equals
```

**dirty-equals** requires **Python 3.9+**.
