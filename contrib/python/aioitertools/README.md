aioitertools
============

Implementation of itertools, builtins, and more for AsyncIO and mixed-type iterables.

[![documentation](https://readthedocs.org/projects/aioitertools/badge/?version=latest)](https://aioitertools.omnilib.dev)
[![version](https://img.shields.io/pypi/v/aioitertools.svg)](https://pypi.org/project/aioitertools)
[![changelog](https://img.shields.io/badge/change-log-blue)](https://aioitertools.omnilib.dev/en/latest/changelog.html)
[![license](https://img.shields.io/pypi/l/aioitertools.svg)](https://github.com/omnilib/aioitertools/blob/main/LICENSE)


Install
-------

aioitertools requires Python 3.9 or newer.
You can install it from PyPI:

```sh
$ pip install aioitertools
```

Usage
-----

aioitertools shadows the standard library whenever possible to provide
asynchronous version of the modules and functions you already know.  It's
fully compatible with standard iterators and async iterators alike, giving
you one unified, familiar interface for interacting with iterable objects:

```python
from aioitertools import iter, next, map, zip

something = iter(...)
first_item = await next(something)

async for item in iter(something):
    ...


async def fetch(url):
    response = await aiohttp.request(...)
    return response.json

async for value in map(fetch, MANY_URLS):
    ...


async for a, b in zip(something, something_else):
    ...
```


aioitertools emulates the entire `itertools` module, offering the same
function signatures, but as async generators.  All functions support
standard iterables and async iterables alike, and can take functions or
coroutines:

```python
from aioitertools import chain, islice

async def generator1(...):
    yield ...

async def generator2(...):
    yield ...

async for value in chain(generator1(), generator2()):
    ...

async for value in islice(generator1(), 2, None, 2):
    ...
```


See [builtins.py][], [itertools.py][], and [more_itertools.py][] for full
documentation of functions and abilities.


License
-------

aioitertools is copyright [Amethyst Reese](https://noswap.com), and licensed under
the MIT license.  I am providing code in this repository to you under an open
source license.  This is my personal repository; the license you receive to
my code is from me and not from my employer. See the `LICENSE` file for details.


[builtins.py]: https://github.com/omnilib/aioitertools/blob/main/aioitertools/builtins.py
[itertools.py]: https://github.com/omnilib/aioitertools/blob/main/aioitertools/itertools.py
[more_itertools.py]: https://github.com/omnilib/aioitertools/blob/main/aioitertools/more_itertools.py
