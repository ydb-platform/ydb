# jsonobject

[![Build Status](https://github.com/dimagi/jsonobject/actions/workflows/tests.yml/badge.svg)](https://github.com/dimagi/jsonobject/actions/workflows/tests.yml)
[![Downloads](https://pepy.tech/badge/jsonobject/month)](https://pepy.tech/project/jsonobject)
[![Supported Versions](https://img.shields.io/pypi/pyversions/jsonobject.svg)](https://pypi.org/project/jsonobject)
[![Contributors](https://img.shields.io/github/contributors/dimagi/jsonobject.svg)](https://github.com/dimagi/jsonobject/graphs/contributors)

jsonobject is a python library for handling deeply nested JSON objects
as well-schema'd python objects.

jsonobject is made by [Dimagi](https://www.dimagi.com/), where we build, use, and contribute to OSS in our mission to reduce inequity in the world.

jsonobject is inspired by and largely API compatible with
the `Document`/`DocumentSchema` portion of `couchdbkit`.
Because jsonobject is not only simpler and standalone, but also faster,
we also maintain a fork of `couchdbkit`, [jsonobject-couchdbkit](https://pypi.python.org/pypi/jsonobject-couchdbkit),
that is backed by `jsonobject` and works seamlessly as a swap-in replacement
for the main library.

It is used heavily in [CommCare HQ](https://www.commcarehq.org/) ([source](https://github.com/dimagi/commcare-hq)),
and the API is largely stable,
but more advanced features may change in the future.

## Getting Started

To install using pip, simply run

```
pip install jsonobject
```


### Example

The code below defines a simple user model, and its natural mapping to JSON.

```python
from jsonobject import *

class User(JsonObject):
    username = StringProperty()
    name = StringProperty()
    active = BooleanProperty(default=False)
    date_joined = DateTimeProperty()
    tags = ListProperty(unicode)

```

Once it is defined, it can be used to wrap or produce deserialized JSON.

```python
>>> user1 = User(
    name='John Doe',
    username='jdoe',
    date_joined=datetime.datetime.utcnow(),
    tags=['generic', 'anonymous']
)
>>> user1.to_json()
{
    'name': u'John Doe',
    'username': u'jdoe',
    'active': False,
    'date_joined': '2013-08-05T02:46:58Z',
    'tags': [u'generic', u'anonymous']
}
```

Notice that the datetime is converted to an ISO format string in JSON, but is a real datetime on the object:

```python
>>> user1.date_joined
datetime.datetime(2013, 8, 5, 2, 46, 58)
```

### The jsonobject Constructor

A JsonObject subclass that has been defined as `User` above
comes with a lot of built-in functionality.
The basic operations are

1. Make a new object from deserialized JSON (e.g. the output of `json.loads`)
2. Construct a new object with given values
3. Modify an object
4. Dump to deserialized json (e.g. the input of `json.dumps`)

1 & 2 are accomplished with the constructor. There are two main ways to call
the constructor:

```python
User(
    name='John Doe',
    username='jdoe',
    date_joined=datetime.datetime.utcnow(),
    tags=['generic', 'anonymous']
)
```

as above (satisfies #2) and

```python
User({
    'name': u'John Doe',
    'username': u'jdoe',
    'active': False,
    'date_joined': '2013-08-05T02:46:58Z',
    'tags': [u'generic', u'anonymous']
})
```

(satisfies #1). These two styles can also be mixed and matched:

```python
User({
    'name': u'John Doe',
    'username': u'jdoe',
    'active': False,
    'tags': [u'generic', u'anonymous']
}, date_joined=datetime.datetime.utcnow())
```

Notice how datetimes are stored as strings in the deserialized JSON, but as
`datetime.datetime`s in the nice python object—we will refer to these as the
"json" representation and the "python" representation, or alternatively the
"unwrapped" representation and the "wrapped" representation.

**Gotcha**.
When calling the constructor, remember that the keyword argument style
requires you to pass in the "python" representation (e.g. a `datetime`)
while the json-wrapping style of passing in a `dict` requires you to give it
in the "json" representation (e.g. a datetime-formatted string).

## Property Types

There are two main kinds of property types:
scalar types (like string, bool, int, datetime, etc.)
and container types (list, dict, set).
They are dealt with separately below.

### Scalar Types

All scalar properties can take the value `None` in addition to
the values particular to their type (strings, bools, etc).
If set to the wrong type,
properties raise a `jsonobject.exceptions.BadValueError`:

```python
class Foo(jsonobject.JsonObject):
    b = jsonobject.BooleanProperty()
```

```python
>>> Foo(b=0)
Traceback (most recent call last):
  [...]
jsonobject.exceptions.BadValueError: 0 not of type <type 'bool'>
```

#### `jsonobject.StringProperty`

Maps to a `unicode`. Usage:

```python
class Foo(jsonobject.JsonObject):
    s = jsonobject.StringProperty()
```

If you set it to an ascii `str` it will implicitly convert to `unicode`:

```python
>>> Foo(s='hi')  # converts to unicode
Foo(s=u'hi')
```

If you set it to a non-ascii `str`, it will fail with a `UnicodeDecodeError`:

```python
>>> Foo(s='\xff')
Traceback (most recent call last):
  [...]
UnicodeDecodeError: 'ascii' codec can't decode byte 0xff in position 0: ordinal not in range(128)
```

#### `jsonobject.BooleanProperty`

Maps to a `bool`.


#### `jsonobject.IntegerProperty`

Maps to an `int` or `long`.

#### `jsonobject.FloatProperty`

Maps to a `float`.

#### `jsonobject.DecimalProperty`

Maps to a `decimal.Decimal` and stored as a JSON string.
This type, unlike `FloatProperty`,
stores the "human" representation of the digits. Usage:

```python
class Foo(jsonobject.JsonObject):
    number = jsonobject.DecimalProperty()
```

If you set it to an `int` or `float`, it will implicitly convert to `Decimal`:

```python
>>> Foo(number=1)
Foo(number=Decimal('1'))
>>> Foo(number=1.2)
Foo(number=Decimal('1.2'))
```

If you set it to a `str` or `unicode`, however, it raises an `AssertionError`:

```python
>>> Foo(number='1.0')
Traceback (most recent call last):
  [...]
AssertionError
```

Todo: this should really raise a `BadValueError`.

If you pass in json in which the Decimal value is a `str` or `unicode`,
but it is malformed, it throws the same errors as `decimal.Decimal`.

```python
>>> Foo({'number': '1.0'})
Foo(number=Decimal('1.0'))
>>> Foo({'number': '1.0.0'})
Traceback (most recent call last):
  [...]
decimal.InvalidOperation: Invalid literal for Decimal: '1.0.0'
```

#### `jsonobject.DateProperty`

Maps to a `datetime.date` and stored as a JSON string of the format
`'%Y-%m-%d'`. Usage:

```python
class Foo(jsonobject.JsonObject):
    date = jsonobject.DateProperty()
```

Wrapping a badly formatted string raises a `BadValueError`:

```python
>>> Foo({'date': 'foo'})
Traceback (most recent call last):
  [...]
jsonobject.exceptions.BadValueError: 'foo' is not a date-formatted string
```

#### `jsonobject.DateTimeProperty`

Maps to a timezone-unaware `datetime.datetime`
and stored as a JSON string of the format
`'%Y-%m-%dT%H:%M:%SZ'`.

While it works perfectly with good inputs, it is extremely sloppy when it comes
to dealing with inputs that don't match the exact specified format.
Rather than matching stricty, it simply truncates the string
to the first 19 characters and tries to parse that as `'%Y-%m-%dT%H:%M:%S'`.
This ignores both microseconds and, even worse, *the timezone*.
This is a holdover from `couchdbkit`.

In newer versions of jsonboject, you may optionally specify
a `DateTimeProperty` as `exact`:

```python
class Foo(jsonobject.JsonObject):
    date = jsonobject.DateTimeProperty(exact=True)
```

This provides a much cleaner conversion model
that has the following properties:

1. It preserves microseconds
2. The incoming JSON representation **must** match `'%Y-%m-%dT%H:%M:%S.%fZ'`
   exactly. (This is similar to the default output,
   except for the mandatory 6 decimal places, i.e. milliseconds.)
3. Representations that don't match exactly will be rejected with a
   `BadValueError`.

**Recommendation**:
If you are not locked into `couchdbkit`'s earlier bad behavior,
you should **always** use the `exact=True` flag on `DateTimeProperty`s
and `TimeProperty`s (below).

#### `jsonobject.TimeProperty`

Maps to a `datetime.time`, stored as a JSON string of the format
`'%H:%M:%S'`.

To get access to milliseconds and strict behavior, use the `exact=True` setting
which strictly accepts the format `'%H:%M:%S.%f'`. This is always recommended.
For more information please read the previous section on `DateTimeProperty`.

### Container Types

Container types generally take a first argument, `item_type`,
specifying the type of the contained objects.


#### `jsonobject.ObjectProperty(item_type)`

Maps to a `dict` that has a schema specified by `item_type`,
which must be itself a subclass of `JsonObject`. Usage:

```python
class Bar(jsonobject.JsonObject):
    name = jsonobject.StringProperty()


class Foo(jsonobject.JsonObject):
    bar = jsonobject.ObjectProperty(Bar)
```

If not specified, it will be set to a new object with default values:

```python
>>> Foo()
Foo(bar=Bar(name=None))
```

If you want it set to `None` you must do so explicitly.

#### `jsonobject.ListProperty(item_type)`

Maps to a `list` with items of type `item_type`,
which can be any of the following:

- an _instance_ of a property class. This is the most flexible option,
  and all validation (`required`, etc.) will be done as as specified by the property instance.
- a property class, which will be instantiated with `required=True`
- one of their corresponding python types (i.e. `int` for `IntegerProperty`, etc.)
- a `JsonObject` subclass

Note that a property _class_ (as well as the related python type syntax)
will be instantiated with `required=True`,
so `ListProperty(IntegerProperty)` and `ListProperty(int)` do not allow `None`, and
`ListProperty(IntegerProperty())` _does_ allow `None`.

The serialization behavior of whatever item type is given is recursively
applied to each member of the list.

If not specified, it will be set to an empty list.

#### `jsonobject.SetProperty(item_type)`

Maps to a `set` and stored as a list (with only unique elements).
Otherwise its behavior is very much like `ListProperty`'s.

#### `jsonobject.DictProperty(item_type)`

Maps to a `dict` with string keys and values specified by `item_type`.
Otherwise its behavior is very much like `ListProperty`'s.

If not specified, it will be set to an empty dict.

### Other

#### `jsonobject.DefaultProperty()`

This flexibly wraps any valid JSON, including all scalar and container types,
dynamically detecting the value's type and treating it
with the corresponding property.

## Property options

Certain parameters may be passed in to any property.

For example, `required` is one such parameter in the example below:

```python

class User(JsonObject):
    username = StringProperty(required=True)

```

Here is a complete list of properties:

- `default`

  Specifies a default value for the property

- `name`

  The name of the property within the JSON representation\*.
  This defaults to the name of the python property, but you can override it
  if you wish. This can be useful, for example, to get around conflicting
  with python keywords:
  ```python
  >>> class Route(JsonObject):
  ...     from_ = StringProperty(name='from')
  ...     to = StringProperty()  # name='to' by default
  >>> Route(from_='me', to='you').to_json()
  {'from': u'me', 'to': u'you'}
  ```
  Notice how an underscore is present in the python property name ('from_'),
  but absent in the JSON property name ('from').


  <small>
  \*If you're wondering how `StringProperty`'s `name` parameter
  could possibly default to `to` in the example above,
  when it doesn't have access to the `Route` class's properties at init time,
  you're completely right.
  The behavior described is implemented in `JsonObject`'s `__metaclass__`,
  which *does* have access to the `Route` class's properties.
  </small>

- `choices`

  A list of allowed values for the property.
  (Unless otherwise specified, `None` is also an allowed value.)

- `required`

  Defaults to `False`.
  For scalar properties `requires` means that the value `None` may not be used.
  For container properties it means they may not be empty
  or take the value `None`.

- `exclude_if_none`

  Defaults to `False`. When set to true, this property will be excluded
  from the JSON output when its value is falsey.
  (Note that currently this is at odds with the parameter's name,
  since the condition is that it is falsey, not that it is `None`).

- `validators`

  A single validator function or list of validator functions.
  Each validator function should raise an exception on invalid input
  and do nothing otherwise.

- `verbose_name`

  This property does nothing and was added to match couchdbkit's API.


## Performance Comparison with Couchdbkit

In order to do a direct comparison with couchdbkit, the test suite includes a large sample schema originally written with couchdbkit. It is easy to swap in jsonobject for couchdbkit and run the tests with each. Here are the results:

```
$ python -m unittest test.test_couchdbkit
....
----------------------------------------------------------------------
Ran 4 tests in 1.403s

OK
$ python -m unittest test.test_couchdbkit
....
----------------------------------------------------------------------
Ran 4 tests in 0.153s

OK
```

# Development Lifecycle
`jsonobject` versions follow [semantic versioning](https://semver.org/).
Version information can be found in [CHANGES.md](CHANGES.md).

Information for developers and maintainers, such as how to run tests and release new versions,
can be found in [LIFECYCLE.md](LIFECYCLE.md).
