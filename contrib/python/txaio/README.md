# txaio

[![PyPI](https://img.shields.io/pypi/v/txaio.svg)](https://pypi.python.org/pypi/txaio)
[![Python](https://img.shields.io/pypi/pyversions/txaio.svg)](https://pypi.python.org/pypi/txaio)
[![CI](https://github.com/crossbario/txaio/workflows/main/badge.svg)](https://github.com/crossbario/txaio/actions?query=workflow%3Amain)
[![Docs](https://readthedocs.org/projects/txaio/badge/?version=latest)](https://txaio.readthedocs.io/en/latest/)
[![License](https://img.shields.io/pypi/l/txaio.svg)](https://github.com/crossbario/txaio/blob/master/LICENSE)
[![Downloads](https://img.shields.io/pypi/dm/txaio.svg)](https://pypi.python.org/pypi/txaio)

------------------------------------------------------------------------

**txaio** is a helper library for writing code that runs unmodified on
both [Twisted](https://twistedmatrix.com/) and
[asyncio](https://docs.python.org/3/library/asyncio.html) /
[Trollius](http://trollius.readthedocs.org/en/latest/index.html).

This is like [six](http://pythonhosted.org/six/), but for wrapping over
differences between Twisted and asyncio so one can write code that runs
unmodified on both (aka *source code compatibility*). In other words:
your *users* can choose if they want asyncio **or** Twisted as a
dependency.

Note that, with this approach, user code **runs under the native event
loop of either Twisted or asyncio**. This is different from attaching
either one's event loop to the other using some event loop adapter.

## Platform support

**txaio** runs on CPython 3.6+ and PyPy 3, on top of *Twisted* or
*asyncio*. Specifically, **txaio** is tested on the following platforms:

-   CPython 3.6 and 3.9 on Twisted 18.7, 19.10, trunk and on asyncio
    (stdlib)
-   PyPy 3.6 an 3.7 on Twisted 18.7, 19.10, trunk and on asyncio
    (stdlib)

&gt; Note: txaio up to version 18.8.1 also supported Python 2.7 and
Python 3.4. Beginning with release v20.1.1, txaio only supports Python
3.5+. Beginning with release v20.12.1, txaio only supports Python 3.6+.

## How it works

Instead of directly importing, instantiating and using `Deferred` (for
Twisted) or `Future` (for asyncio) objects, **txaio** provides
helper-functions to do that for you, as well as associated things like
adding callbacks or errbacks.

This obviously changes the style of your code, but then you can choose
at runtime (or import time) which underlying event-loop to use. This
means you can write **one** code-base that can run on Twisted *or*
asyncio (without a Twisted dependency) as you or your users see fit.

Code like the following can then run on *either* system:

```python
import txaio
txaio.use_twisted()  # or .use_asyncio()

f0 = txaio.create_future()
f1 = txaio.as_future(some_func, 1, 2, key='word')
txaio.add_callbacks(f0, callback, errback)
txaio.add_callbacks(f1, callback, errback)
# ...
txaio.resolve(f0, "value")
txaio.reject(f1, RuntimeError("it failed"))
```

Please refer to the
[documentation](https://txaio.readthedocs.io/en/latest/) for description
and usage of the library features.

## AI Policy

> **IMPORTANT: A Note on Upcoming Policy Changes Regarding AI-Assisted Content**
>
> Up to and including release **v25.6.1**, this project contains no code
> or documentation generated with the assistance of AI tools. This version
> represents the final release under our historical contribution policy.
> Starting with future versions (*after* release v25.6.1), our contribution policy
> will change. Subsequent releases **MAY** contain code or documentation
> created with AI assistance.

We urge all users and contributors to review our [AI
Policy](https://github.com/crossbario/txaio/blob/master/AI_POLICY.md).
This document details:

-   The rules and warranties required for all future contributions.
-   The potential intellectual property implications for the project and
    its users.

This policy was established following an open community discussion,
which you can review on [GitHub issue
\#1663](https://github.com/crossbario/autobahn-python/issues/1663).

We are providing this transparent notice to enable you to make an
informed decision. If our new AI policy is incompatible with your own
(or your organization's) development practices or risk tolerance, please
take this into consideration when deciding whether to upgrade beyond
version v25.6.1.
