[![Build Status](https://travis-ci.org/clarete/forbiddenfruit.png?branch=master)](https://travis-ci.org/clarete/forbiddenfruit)

# Forbidden Fruit

![Forbidden Fruit](logo.png)

This project allows Python code to extend built-in types.

If that's a good idea or not, you tell me. The first need this project
attended was allowing a [Python assertion
library](https://github.com/gabrielfalcao/sure) to implement a similar
API to [RSpec
Expectations](https://github.com/rspec/rspec-expectations) and
[should.js](https://shouldjs.github.io/). But people got creative and
used it to among other things [spy on
things](https://github.com/ikamensh/flynt/blob/43a64ac1a030be79741402d8920a6da253a96670/src/flynt/file_spy.py)
or to [integrate
profiling](https://github.com/localstack/localstack/blob/e38eae0d1fe442924f4256d4bc87710a4cb6f142/localstack/utils/analytics/profiler.py).

## Tiny Example

It basically allows you to patch built-in objects, declared in C through
python. Just like this:

1. Add a new method to the `int` class:

```python
from forbiddenfruit import curse


def words_of_wisdom(self):
    return self * "blah "


curse(int, "words_of_wisdom", words_of_wisdom)

assert (2).words_of_wisdom() == "blah blah "
```

2. Add a `classmethod` to the `str` class:

```python
from forbiddenfruit import curse


def hello(self):
    return "blah"


curse(str, "hello", classmethod(hello))

assert str.hello() == "blah"
```

### Reversing a curse

If you want to free your object from a curse, you can use the `reverse()`
function. Just like this:

```python
from forbiddenfruit import curse, reverse

curse(str, "test", "blah")
assert 'test' in dir(str)

# Time to reverse the curse
reverse(str, "test")
assert 'test' not in dir(str)
```

**Beware:** `reverse()` only deletes attributes. If you `curse()`'d to replace
a pre-existing attribute, `reverse()` won't re-install the existing attribute.

### Context Manager / Decorator

`cursed()` acts as a context manager to make a `curse()`, and then `reverse()`
it on exit. It uses
[`contextlib.contextmanager()`](https://docs.python.org/3/library/contextlib.html#contextlib.contextmanager),
so on Python 3.2+ it can also be used as a function decorator. Like so:

```python
from forbiddenfruit import cursed

with cursed(str, "test", "blah"):
    assert str.test == "blah"

assert "test" not in dir(str)


@cursed(str, "test", "blah")
def function():
    assert str.test == "blah"


function()

assert "test" not in dir(str)
```

## Compatibility

Forbbiden Fruit is tested on CPython 2.7, 3.0, and 3.3-3.7.

Since Forbidden Fruit is fundamentally dependent on the C API,
this library won't work on other python implementations, such
as Jython, pypy, etc.

## License

Copyright (C) 2013,2019  Lincoln Clarete <lincoln@clarete.li>

This software is available under two different licenses at your
choice:

### GPLv3

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.

### MIT

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

### Logo by

Kimberly Chandler, from The Noun Project

### Changelog

#### 0.1.4

  * Add cursed() context manager/decorator
  * Conditionally build test C extension
  * Allow cursing dunder methods with non functions
  * Fix dual licensing issues. Distribute both GPLv3 & MIT license
    files.
