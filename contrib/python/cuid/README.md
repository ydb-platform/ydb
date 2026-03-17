# cuid.py 

Implementation of https://github.com/ericelliott/cuid in Python.

A `cuid` is a portable and sequentially-ordered unique identifier designed for
horizontal scalability and speed -- this version is ported from the reference
implementation in Javascript.

**NOTE**: Particularly if you have security concerns, the [`cuid` standard has been deprecated in favor of `cuid2`](https://github.com/paralleldrive/cuid2)!

Tested on CPython 2.7, 3.6-3.11 as well as PyPy & PyPy3.

Rough benchmarks on my machine (i7-8750H CPU @ 2.20GHz) using `setup.py bench`
(which times the creation of 1 million `cuid`s):

| Version                    | ns / cuid |
| -------------------------- | --------- |
| CPython 3.7.3              | 6095.257  |
| CPython 3.6.8              | 6846.050  |
| CPython 3.5.6              | 6604.012  |
| CPython 2.7.16             | 6913.681  |
| PyPy 7.1.1 (Python 2.7.13) | 326.344   |
| PyPy3 7.1.1 (Python 3.6.1) | 562.673   |

_(Note that timing the creation of fewer IDs changes the way PyPy runs the code, because of JIT warmup --
obviously creating this many IDs takes advantage of the warmed JIT)_

For now, this has no dependencies outside the standard library -- in time this may change, 
to provide better random numbers and / or performance.
