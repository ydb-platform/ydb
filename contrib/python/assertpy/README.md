# assertpy

Simple assertions library for unit testing in Python with a nice fluent API.  Supports both Python 2 and 3.

[![Build Status](https://travis-ci.org/assertpy/assertpy.svg?branch=master)](https://travis-ci.org/assertpy/assertpy)
[![Coverage Status](https://coveralls.io/repos/github/assertpy/assertpy/badge.svg?branch=master)](https://coveralls.io/github/assertpy/assertpy?branch=master)


## Usage

Just import the `assert_that` function, and away you go...

```py
from assertpy import assert_that

def test_something():
    assert_that(1 + 2).is_equal_to(3)
    assert_that('foobar').is_length(6).starts_with('foo').ends_with('bar')
    assert_that(['a', 'b', 'c']).contains('a').does_not_contain('x')
```

Of course, `assertpy` works best with a python test runner like [pytest](http://pytest.org/) (our favorite) or [Nose](http://nose.readthedocs.org/).


## Installation

### Install via pip
[![PyPI Badge](https://badge.fury.io/py/assertpy.svg)](https://pypi.org/project/assertpy/)

The `assertpy` library is available via [PyPI](https://pypi.org/project/assertpy/).
Just install with:

```
pip install assertpy
```

### Install via conda

[![Conda Version](https://img.shields.io/conda/vn/conda-forge/assertpy.svg)](https://anaconda.org/conda-forge/assertpy)
[![Conda Platforms](https://img.shields.io/conda/pn/conda-forge/assertpy.svg)](https://anaconda.org/conda-forge/assertpy)

Or, if you are a big fan of [conda](https://conda.io/) like we are, there is an [assertpy-feedstock](https://github.com/conda-forge/assertpy-feedstock) for [Conda-Forge](https://conda-forge.org/) that you can use:

```
conda install assertpy --channel conda-forge
```


## Docs

The fluent API of `assertpy` is designed to create compact, yet readable tests.
The API has been modeled after other fluent testing APIs, especially the awesome
[AssertJ](http://joel-costigliola.github.io/assertj/) assertion library for Java.  Of course, in the `assertpy` library everything is fully pythonic and designed to take full advantage of the dynamism in the Python runtime.

All assertions, with usage examples, are documented here:  
https://assertpy.github.io/docs.html

And there are hundreds of examples below.  Read on...

### Strings

Matching strings:

```py
assert_that('').is_not_none()
assert_that('').is_empty()
assert_that('').is_false()
assert_that('').is_type_of(str)
assert_that('').is_instance_of(str)

assert_that('foo').is_length(3)
assert_that('foo').is_not_empty()
assert_that('foo').is_true()
assert_that('foo').is_alpha()
assert_that('123').is_digit()
assert_that('foo').is_lower()
assert_that('FOO').is_upper()
assert_that('foo').is_iterable()
assert_that('foo').is_equal_to('foo')
assert_that('foo').is_not_equal_to('bar')
assert_that('foo').is_equal_to_ignoring_case('FOO')

assert_that(u'foo').is_unicode()  # on python 2
assert_that('foo').is_unicode()   # on python 3

assert_that('foo').contains('f')
assert_that('foo').contains('f','oo')
assert_that('foo').contains_ignoring_case('F','oO')
assert_that('foo').does_not_contain('x')
assert_that('foo').contains_only('f','o')
assert_that('foo').contains_sequence('o','o')

assert_that('foo').contains_duplicates()
assert_that('fox').does_not_contain_duplicates()

assert_that('foo').is_in('foo','bar','baz')
assert_that('foo').is_not_in('boo','bar','baz')
assert_that('foo').is_subset_of('abcdefghijklmnopqrstuvwxyz')

assert_that('foo').starts_with('f')
assert_that('foo').ends_with('oo')

assert_that('foo').matches(r'\w')
assert_that('123-456-7890').matches(r'\d{3}-\d{3}-\d{4}')
assert_that('foo').does_not_match(r'\d+')
```

Regular expressions can be tricky.  Be sure to use raw strings (prefix the pattern string with `r`) for the regex pattern to be matched.  Also, note that the `matches()` function passes for partial matches (as does the [re.match](https://docs.python.org/3/library/re.html#re.match) function that underlies it). If you want to match the entire string, just include anchors in the regex pattern.

```py
# partial matches, these all pass
assert_that('foo').matches(r'\w')
assert_that('foo').matches(r'oo')
assert_that('foo').matches(r'\w{2}')

# match the entire string with an anchored regex pattern, passes
assert_that('foo').matches(r'^\w{3}$')

# fails
assert_that('foo').matches(r'^\w{2}$')
```

Additionally, while `assertpy` `matches()` assertion does not have support for [re.match](https://docs.python.org/3/library/re.html#re.match) flags such as `re.MULTILINE` or `re.DOTALL`, it works as expected with _inline flags_ in the pattern.

```py
s = """bar
foo
baz"""

# use multiline inline flag (?m)
assert_that(s).matches(r'(?m)^foo$')

# use dotall inline flag (?s)
assert_that(s).matches(r'(?s)b(.*)z')
```

### Numbers

Matching integers:

```py
assert_that(0).is_not_none()
assert_that(0).is_false()
assert_that(0).is_type_of(int)
assert_that(0).is_instance_of(int)

assert_that(0).is_zero()
assert_that(1).is_not_zero()
assert_that(1).is_positive()
assert_that(-1).is_negative()

assert_that(123).is_equal_to(123)
assert_that(123).is_not_equal_to(456)

assert_that(123).is_greater_than(100)
assert_that(123).is_greater_than_or_equal_to(123)
assert_that(123).is_less_than(200)
assert_that(123).is_less_than_or_equal_to(200)
assert_that(123).is_between(100, 200)
assert_that(123).is_close_to(100, 25)

assert_that(1).is_in(0,1,2,3)
assert_that(1).is_not_in(-1,-2,-3)
```

Matching floats:

```py
assert_that(0.0).is_not_none()
assert_that(0.0).is_false()
assert_that(0.0).is_type_of(float)
assert_that(0.0).is_instance_of(float)

assert_that(123.4).is_equal_to(123.4)
assert_that(123.4).is_not_equal_to(456.7)

assert_that(123.4).is_greater_than(100.1)
assert_that(123.4).is_greater_than_or_equal_to(123.4)
assert_that(123.4).is_less_than(200.2)
assert_that(123.4).is_less_than_or_equal_to(123.4)
assert_that(123.4).is_between(100.1, 200.2)
assert_that(123.4).is_close_to(123, 0.5)

assert_that(float('NaN')).is_nan()
assert_that(123.4).is_not_nan()
assert_that(float('Inf')).is_inf()
assert_that(123.4).is_not_inf()
```

Of course, using `is_equal_to()` with a `float` value is just asking for trouble. You'll always want to use the assertions methods like `is_close_to()` and `is_between()`.


### Lists

Matching lists:

```py
assert_that([]).is_not_none()
assert_that([]).is_empty()
assert_that([]).is_false()
assert_that([]).is_type_of(list)
assert_that([]).is_instance_of(list)
assert_that([]).is_iterable()

assert_that(['a','b']).is_length(2)
assert_that(['a','b']).is_not_empty()
assert_that(['a','b']).is_equal_to(['a','b'])
assert_that(['a','b']).is_not_equal_to(['b','a'])

assert_that(['a','b']).contains('a')
assert_that(['a','b']).contains('b','a')
assert_that(['a','b']).does_not_contain('x','y')
assert_that(['a','b']).contains_only('a','b')
assert_that(['a','a']).contains_only('a')
assert_that(['a','b','c']).contains_sequence('b','c')
assert_that(['a','b']).is_subset_of(['a','b','c'])
assert_that(['a','b','c']).is_sorted()
assert_that(['c','b','a']).is_sorted(reverse=True)

assert_that(['a','x','x']).contains_duplicates()
assert_that(['a','b','c']).does_not_contain_duplicates()

assert_that(['a','b','c']).starts_with('a')
assert_that(['a','b','c']).ends_with('c')
```

#### List Flattening

Lists of lists can be flattened on any item (by index) using the `extracting` helper (see [dict flattening](#dict-flattening)):

```py
people = [['Fred', 'Smith'], ['Bob', 'Barr']]
assert_that(people).extracting(0).is_equal_to(['Fred','Bob'])
assert_that(people).extracting(-1).is_equal_to(['Smith','Barr'])
```

### Tuples

Matching tuples:

```py
assert_that(()).is_not_none()
assert_that(()).is_empty()
assert_that(()).is_false()
assert_that(()).is_type_of(tuple)
assert_that(()).is_instance_of(tuple)
assert_that(()).is_iterable()

assert_that((1,2,3)).is_length(3)
assert_that((1,2,3)).is_not_empty()
assert_that((1,2,3)).is_equal_to((1,2,3))
assert_that((1,2,3)).is_not_equal_to((1,2,4))

assert_that((1,2,3)).contains(1)
assert_that((1,2,3)).contains(3,2,1)
assert_that((1,2,3)).does_not_contain(4,5,6)
assert_that((1,2,3)).contains_only(1,2,3)
assert_that((1,1,1)).contains_only(1)
assert_that((1,2,3)).contains_sequence(2,3)
assert_that((1,2,3)).is_subset_of((1,2,3,4))
assert_that((1,2,3)).is_sorted()
assert_that((3,2,1)).is_sorted(reverse=True)

assert_that((1,2,2)).contains_duplicates()
assert_that((1,2,3)).does_not_contain_duplicates()

assert_that((1,2,3)).starts_with(1)
assert_that((1,2,3)).ends_with(3)
```

#### Tuple Flattening

Tuples of tuples can be flattened on any item (by index) using the `extracting` helper (see [dict flattening](#dict-flattening)):

```py
points = ((1,2,3),(4,5,6))
assert_that(points).extracting(0).is_equal_to([1, 4])
assert_that(points).extracting(-1).is_equal_to([3, 6])
```

### Dicts

Matching dicts:

```py
assert_that({}).is_not_none()
assert_that({}).is_empty()
assert_that({}).is_false()
assert_that({}).is_type_of(dict)
assert_that({}).is_instance_of(dict)

assert_that({'a':1,'b':2}).is_length(2)
assert_that({'a':1,'b':2}).is_not_empty()
assert_that({'a':1,'b':2}).is_equal_to({'a':1,'b':2})
assert_that({'a':1,'b':2}).is_equal_to({'b':2,'a':1})
assert_that({'a':1,'b':2}).is_not_equal_to({'a':1,'b':3})

assert_that({'a':1,'b':2}).contains('a')
assert_that({'a':1,'b':2}).contains('b','a')
assert_that({'a':1,'b':2}).does_not_contain('x')
assert_that({'a':1,'b':2}).does_not_contain('x','y')
assert_that({'a':1,'b':2}).contains_only('a','b')
assert_that({'a':1,'b':2}).is_subset_of({'a':1,'b':2,'c':3})

# contains_key() is just an alias for contains()
assert_that({'a':1,'b':2}).contains_key('a')
assert_that({'a':1,'b':2}).contains_key('b','a')

# does_not_contain_key() is just an alias for does_not_contain()
assert_that({'a':1,'b':2}).does_not_contain_key('x')
assert_that({'a':1,'b':2}).does_not_contain_key('x','y')

assert_that({'a':1,'b':2}).contains_value(1)
assert_that({'a':1,'b':2}).contains_value(2,1)
assert_that({'a':1,'b':2}).does_not_contain_value(3)
assert_that({'a':1,'b':2}).does_not_contain_value(3,4)

assert_that({'a':1,'b':2}).contains_entry({'a':1})
assert_that({'a':1,'b':2}).contains_entry({'a':1},{'b':2})
assert_that({'a':1,'b':2}).does_not_contain_entry({'a':2})
assert_that({'a':1,'b':2}).does_not_contain_entry({'a':2},{'b':1})
```

#### Dict Comparison

Dict keys can optionally be ignored or included when using the `is_equal_to()` assertion.

Ignore dict keys with the `ignore` keyword argument:

```py
# ignore a single key
assert_that({'a':1,'b':2}).is_equal_to({'a':1}, ignore='b')

# ignore multiple keys using a list
assert_that({'a':1,'b':2,'c':3}).is_equal_to({'a':1}, ignore=['b','c'])

# ignore nested keys using a tuple
assert_that({'a':1,'b':{'c':2,'d':3}}).is_equal_to({'a':1,'b':{'c':2}}, ignore=('b','d'))
```

Or include dict keys with the `include` keyword argument:

```py
# include a single key
assert_that({'a':1,'b':2}).is_equal_to({'a':1}, include='a')

# include multiple keys using a list
assert_that({'a':1,'b':2,'c':3}).is_equal_to({'a':1,'b':2}, include=['a','b'])

# include nested keys using a tuple
assert_that({'a':1,'b':{'c':2,'d':3}}).is_equal_to({'b':{'d':3}}, include=('b','d'))
```

Or do both:

```py
assert_that({'a':1,'b':{'c':2,'d':3,'e':4,'f':5}}).is_equal_to(
    {'b':{'d':3,'f':5}},
    ignore=[('b','c'),('b','e')],
    include='b'
)
```

#### Dict Flattening

Lists of dicts can be flattened on key using the `extracting` helper (see [extracting attributes](#extracting-attributes-from-objects)):

```py
fred = {'first_name': 'Fred', 'last_name': 'Smith'}
bob = {'first_name': 'Bob', 'last_name': 'Barr'}
people = [fred, bob]

assert_that(people).extracting('first_name').is_equal_to(['Fred','Bob'])
assert_that(people).extracting('first_name').contains('Fred','Bob')
```

#### Dict Key Assertions

Fluent assertions against the value of a given key can be done by prepending `has_` to the key name (see [dynamic assertions](#dynamic-assertions-on-objects)):

```py
fred = {'first_name': 'Fred', 'last_name': 'Smith', 'shoe_size': 12}

assert_that(fred).has_first_name('Fred')
assert_that(fred).has_last_name('Smith')
assert_that(fred).has_shoe_size(12)
```


### Sets

Matching sets:

```py
assert_that(set([])).is_not_none()
assert_that(set([])).is_empty()
assert_that(set([])).is_false()
assert_that(set([])).is_type_of(set)
assert_that(set([])).is_instance_of(set)

assert_that(set(['a','b'])).is_length(2)
assert_that(set(['a','b'])).is_not_empty()
assert_that(set(['a','b'])).is_equal_to(set(['a','b']))
assert_that(set(['a','b'])).is_equal_to(set(['b','a']))
assert_that(set(['a','b'])).is_not_equal_to(set(['a','x']))

assert_that(set(['a','b'])).contains('a')
assert_that(set(['a','b'])).contains('b','a')
assert_that(set(['a','b'])).does_not_contain('x','y')
assert_that(set(['a','b'])).contains_only('a','b')
assert_that(set(['a','b'])).is_subset_of(set(['a','b','c']))
assert_that(set(['a','b'])).is_subset_of(set(['a']), set(['b']))
```


### Booleans

Matching booleans:

```py
assert_that(True).is_true()
assert_that(False).is_false()
assert_that(True).is_type_of(bool)
```


### None

Matching `None`:

```py
assert_that(None).is_none()
assert_that('').is_not_none()
assert_that(None).is_type_of(type(None))
```


### Dates

Matching dates:

```py
import datetime

today = datetime.datetime.today()
yesterday = today - datetime.timedelta(days=1)

assert_that(yesterday).is_before(today)
assert_that(today).is_after(yesterday)
```

You can also make assertions about date equality (ignoring various units of time) like this:

```py
today_0us = today - datetime.timedelta(microseconds=today.microsecond)
today_0s = today - datetime.timedelta(seconds=today.second)
today_0h = today - datetime.timedelta(hours=today.hour)

assert_that(today).is_equal_to_ignoring_milliseconds(today_0us)
assert_that(today).is_equal_to_ignoring_seconds(today_0s)
assert_that(today).is_equal_to_ignoring_time(today_0h)
assert_that(today).is_equal_to(today)
```

You can use these numeric assertions on dates:

```py
middle = today - datetime.timedelta(hours=12)
hours_24 = datetime.timedelta(hours=24)

assert_that(today).is_greater_than(yesterday)
assert_that(yesterday).is_less_than(today)
assert_that(middle).is_between(yesterday, today)

#note that the tolerance must be a datetime.timedelta object
assert_that(yesterday).is_close_to(today, hours_24)
```

Lastly, because datetime is an object we can easily test the properties of a given date by prepending `has_` to the property name (see [dynamic assertions](#dynamic-assertions-on-objects)):

```py
# 1980-01-02 03:04:05.000006
x = datetime.datetime(1980, 1, 2, 3, 4, 5, 6)

assert_that(x).has_year(1980)
assert_that(x).has_month(1)
assert_that(x).has_day(2)
assert_that(x).has_hour(3)
assert_that(x).has_minute(4)
assert_that(x).has_second(5)
assert_that(x).has_microsecond(6)
```

Currently, `assertpy` only supports dates via the `datetime` type.


### Files

Matching files:

```py
assert_that('foo.txt').exists()
assert_that('missing.txt').does_not_exist()
assert_that('foo.txt').is_file()

assert_that('mydir').exists()
assert_that('missing_dir').does_not_exist()
assert_that('mydir').is_directory()

assert_that('foo.txt').is_named('foo.txt')
assert_that('foo.txt').is_child_of('mydir')
```

Matching file contents is done using the `contents_of()` helper to read the file into a string with the given encoding (if no encoding is given it defaults to `utf-8`).  Once the file is read into a string, you can make quick work of it using the `assertpy` string assertions like this:

```py
from assertpy import assert_that, contents_of

contents = contents_of('foo.txt', 'ascii')
assert_that(contents).starts_with('foo').ends_with('bar').contains('oob')
```


### Objects

Matching an object:

```py
fred = Person('Fred','Smith')

assert_that(fred).is_not_none()
assert_that(fred).is_true()
assert_that(fred).is_type_of(Person)
assert_that(fred).is_instance_of(object)
assert_that(fred).is_same_as(fred)
```

Matching an attribute, a property, and a method:

```py
assert_that(fred.first_name).is_equal_to('Fred')
assert_that(fred.name).is_equal_to('Fred Smith')
assert_that(fred.say_hello()).is_equal_to('Hello, Fred!')
```

Given `fred` is an instance of the following `Person` class:

```py
class Person(object):
    def __init__(self, first_name, last_name):
        self.first_name = first_name
        self.last_name = last_name

    @property
    def name(self):
        return '%s %s' % (self.first_name, self.last_name)

    def say_hello(self):
        return 'Hello, %s!' % self.first_name
```


#### Extracting Attributes from Objects

It is frequently necessary to test collections of objects.  The `assertpy` library includes an `extracting` helper to flatten the collection on a given attribute, like this:

```py
fred = Person('Fred','Smith')
bob = Person('Bob','Barr')
people = [fred, bob]

assert_that(people).extracting('first_name').is_equal_to(['Fred','Bob'])
assert_that(people).extracting('first_name').contains('Fred','Bob')
assert_that(people).extracting('first_name').does_not_contain('Charlie')
```

Of course `extracting` works with subclasses too...suppose we create a simple class hierarchy by creating a `Developer` subclass of `Person`, like this:

```py
class Developer(Person):
    def say_hello(self):
        return '%s writes code.' % self.first_name
```

Testing a mixed collection of parent and child objects works as expected:

```py
fred = Person('Fred','Smith')
joe = Developer('Joe','Coder')
people = [fred, joe]

assert_that(people).extracting('first_name').contains('Fred','Joe')
```

Additionally, the `extracting` helper can accept a list of attributes to be extracted, and will flatten them into a list of tuples:

```py
assert_that(people).extracting('first_name', 'last_name').contains(('Fred','Smith'), ('Joe','Coder'))
```

Lastly, `extracting` works on not just class attributes, but also properties, and even zero-argument methods:

```py
assert_that(people).extracting('name').contains('Fred Smith', 'Joe Coder')
assert_that(people).extracting('say_hello').contains('Hello, Fred!', 'Joe writes code.')
```

As noted above, the `extracting` helper also works on a collection of dicts:

```py
fred = {'first_name': 'Fred', 'last_name': 'Smith'}
bob = {'first_name': 'Bob', 'last_name': 'Barr'}
people = [fred, bob]

assert_that(people).extracting('first_name').contains('Fred','Bob')
```

##### Extracting and Filtering

The `extracting` helper can include a `filter` to keep only those items for which the given `filter` is truthy.  For example, suppose we have the following list of dicts we wish to test:

```py
users = [
    {'user': 'Fred', 'age': 36, 'active': True},
    {'user': 'Bob', 'age': 40, 'active': False},
    {'user': 'Johnny', 'age': 13, 'active': True}
]
```

The `filter` can be the name of a key (or attribute, or property, or zero-argument method) and the extracted items are kept if the corresponding value is truthy:

```py
assert_that(users).extracting('user', filter='active')\
    .is_equal_to(['Fred','Johnny'])
```

The `filter` can be a `dict`-like object and the extracted items are kept if *all* corresponding key-value pairs are equal:

```py
assert_that(users).extracting('user', filter={'active': False})\
    .is_equal_to(['Bob'])
assert_that(users).extracting('user', filter={'age': 36, 'active': True})\
    .is_equal_to(['Fred'])
```

The `filter` can be any function (including an in-line `lambda`) that accepts as its single argument each item in the collection and the extracted items are kept if the function evaluates to `True`:

```py
assert_that(users).extracting('user', filter=lambda x: x['age'] > 20)\
    .is_equal_to(['Fred', 'Bob'])
```

##### Extracting and Sorting

The `extracting` helper can include a `sort` to enforce order on the extracted items.

The `sort` can be the name of a key (or attribute, or property, or zero-argument method) and the extracted items are ordered by the corresponding values:

```py
assert_that(users).extracting('user', sort='age').is_equal_to(['Johnny','Fred','Bob'])
```

The `sort` can be an `iterable` of names and the extracted items are ordered by corresponding value of the first name, ties are broken by the corresponding values of the second name, and so on:

```py
assert_that(users).extracting('user', sort=['active','age']).is_equal_to(['Bob','Johnny','Fred'])
```

The `sort` can be any function (including an in-line `lambda`) that accepts as its single argument each item in the collection and the extracted items are ordered by the corresponding function return values:

```py
assert_that(users).extracting('user', sort=lambda x: -x['age'])\
    .is_equal_to(['Bob','Fred','Johnny'])
```

#### Dynamic Assertions on Objects

When testing attributes of an object, the basic `assertpy` assertions can get a little verbose like this:

```py
fred = Person('Fred','Smith')

assert_that(fred.first_name).is_equal_to('Fred')
assert_that(fred.name).is_equal_to('Fred Smith')
assert_that(fred.say_hello()).is_equal_to('Hello, Fred!')
```

So, `assertpy` takes advantage of the awesome dyanmism in the Python runtime to provide dynamic assertions in the form of `has_<name>()` where `<name>` is the name of any attribute, property, or zero-argument method on the given object.

Using dynamic assertions, we can rewrite the above assertions in a more compact and readable way like this:

```py
assert_that(fred).has_first_name('Fred')
assert_that(fred).has_name('Fred Smith')
assert_that(fred).has_say_hello('Hello, Fred!')
```

Since `fred` has the attribute `first_name`, the dynamic assertion method `has_first_name()` is available.
Similarly, the property `name` can be tested via `has_name()` and the zero-argument method `say_hello()` via
the `has_say_hello()` assertion.

As noted above, dynamic assertions also work on dicts:

```py
fred = {'first_name': 'Fred', 'last_name': 'Smith'}

assert_that(fred).has_first_name('Fred')
assert_that(fred).has_last_name('Smith')
```

### Failure

The `assertpy` library includes a `fail()` method to explicitly force a test failure.  It can be used like this:

```py
from assertpy import assert_that,fail

def test_fail():
    fail('forced failure')
```

A very useful test pattern that requires the `fail()` method is to verify the exact contents of an error message. For example:

```py
from assertpy import assert_that,fail

def test_error_msg():
    try:
        some_func('foo')
        fail('should have raised error')
    except RuntimeError as e:
        assert_that(str(e)).is_equal_to('some err')
```

In the above code, we invoke `some_func()` with a bad argument which raises an exception.  The exception is then handled by the `try..except` block and the exact contents of the error message are verified.  Lastly, if an exception is *not* thrown by `some_func()` as expected, we fail the test via `fail()`.

This pattern is only used when you need to verify the contents of the error message.  If you only wish to check for an expected exception (and don't need to verify the contents of the error message itself), you're much better off using a test runner that supports expected exceptions.  [Nose](http://nose.readthedocs.org/) provides a [@raises](http://nose.readthedocs.org/en/latest/testing_tools.html#nose.tools.raises) decorator. [Pytest](http://pytest.org/latest/contents.html) has a [pytest.raises](http://pytest.org/latest/assert.html#assertions-about-expected-exceptions) method.


#### Expected Exceptions

We recommend you use your test runner to check for expected exceptions (Pytest's [pytest.raises](http://pytest.org/latest/assert.html#assertions-about-expected-exceptions) context or Nose's [@raises](http://nose.readthedocs.org/en/latest/testing_tools.html#nose.tools.raises) decorator).  In the special case of invoking a function, `assertpy` provides its own expected exception handling via a simple fluent API.

Given a function `some_func()`:

```py
def some_func(arg):
    raise RuntimeError('some err')
```

We can expect a `RuntimeError` with:

```py
assert_that(some_func).raises(RuntimeError).when_called_with('foo')
```

Additionally, the error message contents are chained, and can be further verified:

```py
assert_that(some_func).raises(RuntimeError).when_called_with('foo')\
    .is_length(8).starts_with('some').is_equal_to('some err')
```


#### Custom Error Messages

Sometimes you need a little more information in your failures.  For this case, `assertpy` includes a `described_as()` helper that will add a custom message when a failure occurs.  For example, if we had these failing assertions:

```py
assert_that(1+2).is_equal_to(2)
assert_that(1+2).described_as('adding stuff').is_equal_to(2)
```

When run (separately, of course), they would produce these errors:

```
Expected <3> to be equal to <2>, but was not.
[adding stuff] Expected <3> to be equal to <2>, but was not.
```

The `described_as()` helper causes the custom message `adding stuff` to be prepended to the front of the second error.


#### Just A Warning

There are times when you only want a warning message instead of a failing test. For example, if you are using `assertpy`
to write defensive assertions in the normal flow of your application (not in a test).  In this case, just replace
`assert_that` with `assert_warn`.

```py
assert_warn('foo').is_length(4)
assert_warn('foo').is_empty()
assert_warn('foo').is_false()
assert_warn('foo').is_digit()
assert_warn('123').is_alpha()
assert_warn('foo').is_upper()
assert_warn('FOO').is_lower()
assert_warn('foo').is_equal_to('bar')
assert_warn('foo').is_not_equal_to('foo')
assert_warn('foo').is_equal_to_ignoring_case('BAR')
```

Even though all of the above assertions fail, an `AssertionError` is never raised and execution is
not halted.  Instead, the failed assertions merely log the following warning messages to `stdout`:

```
2019-10-27 20:00:35 WARNING [test_readme.py:423]: Expected <foo> to be of length <4>, but was <3>.
2019-10-27 20:00:35 WARNING [test_readme.py:424]: Expected <foo> to be empty string, but was not.
2019-10-27 20:00:35 WARNING [test_readme.py:425]: Expected <False>, but was not.
2019-10-27 20:00:35 WARNING [test_readme.py:426]: Expected <foo> to contain only digits, but did not.
2019-10-27 20:00:35 WARNING [test_readme.py:427]: Expected <123> to contain only alphabetic chars, but did not.
2019-10-27 20:00:35 WARNING [test_readme.py:428]: Expected <foo> to contain only uppercase chars, but did not.
2019-10-27 20:00:35 WARNING [test_readme.py:429]: Expected <FOO> to contain only lowercase chars, but did not.
2019-10-27 20:00:35 WARNING [test_readme.py:430]: Expected <foo> to be equal to <bar>, but was not.
2019-10-27 20:00:35 WARNING [test_readme.py:431]: Expected <foo> to be not equal to <foo>, but was.
2019-10-27 20:00:35 WARNING [test_readme.py:432]: Expected <foo> to be case-insensitive equal to <BAR>, but was not.
```

##### Custom Warning Logger

By default, warnings are written to `stdout` with a formatter that includes timestamp, log level `WARNING`, and message,
plus some stack frame magic to find the correct filename and line number where `assert_warn()` was called and failed.
For more control or better log formatting, you can pass in your own customer logger when you call `assert_warn()`.

```py
assert_warn('foo', logger=my_logger).is_length(4)
assert_warn('foo', logger=my_logger).is_equal_to_ignoring_case('BAR')
```

### Soft Assertions

Normally, an assertion failure will halt test execution immediately by raising an error. Soft assertions are
way to collect assertion failures together, to be raise all at once at the end, without halting your test.  To use
soft assertions in `assertpy`, just use the `with soft_assertions()` context manager, like this:

```py
from assertpy import assert_that, soft_assertions

with soft_assertions():
    assert_that('foo').is_length(4)
    assert_that('foo').is_empty()
    assert_that('foo').is_false()
    assert_that('foo').is_digit()
    assert_that('123').is_alpha()
    assert_that('foo').is_upper()
    assert_that('FOO').is_lower()
    assert_that('foo').is_equal_to('bar')
    assert_that('foo').is_not_equal_to('foo')
    assert_that('foo').is_equal_to_ignoring_case('BAR')
```

At the end of the block, all assertion failures are collected together and a single `AssertionError` is raised:

```
AssertionError: soft assertion failures:
1. Expected <foo> to be of length <4>, but was <3>.
2. Expected <foo> to be empty string, but was not.
3. Expected <False>, but was not.
4. Expected <foo> to contain only digits, but did not.
5. Expected <123> to contain only alphabetic chars, but did not.
6. Expected <foo> to contain only uppercase chars, but did not.
7. Expected <FOO> to contain only lowercase chars, but did not.
8. Expected <foo> to be equal to <bar>, but was not.
9. Expected <foo> to be not equal to <foo>, but was.
10. Expected <foo> to be case-insensitive equal to <BAR>, but was not.
```

Also, note that *only* assertion failures are collected, errors such as `TypeError` or `ValueError` are raised immediately.
Triggering an explicit test failure with `fail()` will similarly halt execution immediately.  If you need more
forgiving behavior, you can use `soft_fail()` which is collected like any other failing assertion within a soft assertions block.

### Snapshot Testing

Take a snapshot of a python data structure, store it on disk in JSON format, and automatically compare the latest data to the stored data on every test run.  The snapshot testing features of `assertpy` are borrowed from [Jest](https://facebook.github.io/jest/), a well-known and powerful Javascript testing framework.  Snapshots require Python 3.

For example, snapshot the following dict:

```py
assert_that({'a':1,'b':2,'c':3}).snapshot()
```

Stored on disk as the following JSON:

```
{
  "a": 1,
  "b": 2,
  "c": 3
}
```

Additionally, the on-disk snapshot format supports most python data structures (dict, list, object, etc).  For example:

```py
assert_that(None).snapshot()
assert_that(True).snapshot()
assert_that(123).snapshot()
assert_that(-987.654).snapshot()
assert_that('foo').snapshot()
assert_that([1,2,3]).snapshot()
assert_that(set(['a','b','c'])).snapshot()
assert_that({'a':1,'b':2,'c':3}).snapshot()
assert_that(1 + 2j).snapshot()
assert_that(someobj).snapshot()
```

Snapshot artifacts (typically found in the `__snapshots` folder), should be committed to source control alongside any code changes.

On the first run (when the snapshot file doesn't yet exist), the snapshot is created, stored to disk, and the test is passed.  On all subsequent runs, the given data is compared to the on-disk snapshot, and the test fails if they don't match.  Failure means that some change occured, so either a bug or a known implementation changed.

#### Updating Snapshots

It's easy to update your snapshots...just delete them all and re-run the test suite to regenerate all snapshots.

#### Snapshot Parameters

By default, snapshots are identified by test filename plus line number.  Alternately, you can specify a custom identifier using the `id` keyword:

```py
assert_that({'a':1,'b':2,'c':3}).snapshot(id='my-custom-id')
```

By default, all snapshots (including those with custom identifiers) are stored in the `__snapshots` folder.  Alternately, you can specify a custom path using the `path` keyword:

```py
assert_that({'a':1,'b':2,'c':3}).snapshot(path='my-custom-folder')
```

#### Snapshot Blackbox

Functional testing (which snapshot testing falls under) is very much blackbox testing.  When something goes wrong, it's hard to pinpoint the issue, because functional tests provide little *isolation*.  On the plus side, snapshots can provide enormous *leverage* as a few well-placed snapshot tests can strongly verify an application is working that would otherwise require dozens if not hundreds of unit tests.

### Extension System - adding custom assertions

Sometimes you want to add your own custom assertions to `assertpy`.  This can be done using the `add_extension()` helper.

For example, we can write a custom `is_5()` assertion like this:

```py
from assertpy import add_extension

def is_5(self):
    if self.val != 5:
        self.error(f'{self.val} is NOT 5!')
    return self

add_extension(is_5)
```

Once registered with `assertpy`, we can use our new assertion as expected:

```py
assert_that(5).is_5()
assert_that(6).is_5()  # fails!
```

Of course, `is_5()` is only available in the test file where `add_extension()` is called.  If you want better control of scope of your custom extensions, such as writing extensions once and using them in any test file, you'll need to use the test setup functionality of your test runner.  With [pytest](http://pytest.org/latest/contents.html), you can just use a `conftest.py` file and a _fixture_.

For example, if your `conftest.py` is:

```py
import pytest
from assertpy import add_extension

def is_5(self):
    if self.val != 5:
        self.error(f'{self.val} is NOT 5!')
    return self

@pytest.fixture(scope='module')
def my_extensions():
    add_extension(is_5)
```

Then in any test method in any test file (like `test_foo.py` for example), you just pass in the fixture and all of your extensions are available, like this:

```py
from assertpy import assert_that

def test_foo(my_extensions):
    assert_that(5).is_5()
    assert_that(6).is_5()  # fails!
```

where the `my_extensions` parameter must be the name of your fixture function in `conftest.py`.  See the [fixture docs](https://docs.pytest.org/en/latest/fixture.html) for details.

#### Writing custom assertions

Here are some useful tips to help you write your own custom assertions:

1. Use `self` as first param (as if your function was an instance method).
2. Use `self.val` to get the _actual_ value to be tested.
3. It's better to test the negative, and then fail if true.
4. Fail by raising an `AssertionError` (the `self.error()` helper does this for you).
5. Always use the `self.error()` helper to fail (and print your failure message).
6. Always `return self` to allow for chaining.

Putting it all together, here is another custom assertion example, but annotated with comments:

```py
def is_multiple_of(self, other):
    # validate actual value - must be "integer" (aka int or long)
    if isinstance(self.val, numbers.Integral) is False or self.val <= 0:
        # bad input is error, not an assertion fail, so raise error
        raise TypeError('val must be a positive integer')

    # validate expected value
    if isinstance(other, numbers.Integral) is False or other <= 0:
        raise TypeError('given arg must be a positive integer')

    # calc remainder using divmod() built-in
    _, rem = divmod(self.val, other)

    # test the negative (is remainder non-zero?)
    if rem > 0:
        # non-zero remainder, so not multiple -> we fail!
        self.error('Expected <%s> to be multiple of <%s>, but was not.' % (self.val, other))

    # success, and return self to allow chaining
    return self
```

### Chaining

One of the nicest aspects of any fluent API is the ability to chain methods together.  In the case of `assertpy`, chaining
allows you to write assertions as single statement -- that reads like a sentence, and is easy to understand.

Here are just a few examples:

```py
assert_that('foo').is_length(3).starts_with('f').ends_with('oo')

assert_that([1,2,3]).is_type_of(list).contains(1,2).does_not_contain(4,5)

assert_that(fred).has_first_name('Fred').has_last_name('Smith').has_shoe_size(12)

assert_that(people).is_length(2).extracting('first_name').contains('Fred','Joe')
```


## Future

There are always a few new features in the works...if you'd like to help, check out the [open issues](https://github.com/assertpy/assertpy/issues) and see our [Contributing](CONTRIBUTING.md) doc.


## License

All files are licensed under the BSD 3-Clause License as follows:

> Copyright (c) 2015-2019, Activision Publishing, Inc.
> All rights reserved.
>
> Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:
>
> 1. Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
>
> 2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.
>
> 3. Neither the name of the copyright holder nor the names of its contributors may be used to endorse or promote products derived from this software without specific prior written permission.
>
> THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
