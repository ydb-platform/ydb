# FairAsyncRLock

[![Python](https://img.shields.io/pypi/pyversions/fair_async_rlock.svg)](https://badge.fury.io/py/fair_async_rlock)
[![PyPI](https://badge.fury.io/py/fair_async_rlock.svg)](https://badge.fury.io/py/fair_async_rlock)

Main
Status: ![Workflow name](https://github.com/JoshuaAlbert/FairAsyncRLock/actions/workflows/unittests.yml/badge.svg?branch=main)

Develop
Status: ![Workflow name](https://github.com/JoshuaAlbert/FairAsyncRLock/actions/workflows/unittests.yml/badge.svg?branch=develop)

This is a well-tested standalone implementation of a fair reentrant lock for conncurrent programming with asyncio.
This was built
because [python decided not to support RLock in asyncio](https://discuss.python.org/t/asyncio-rlock-reentrant-locks-for-async-python/21509),
their [argument](https://discuss.python.org/t/asyncio-rlock-reentrant-locks-for-async-python/21509/2) being that every
extra bit of functionality adds to maintenance cost.

Install with

```bash
pip install fair-async-rlock
```

## About Fair Reentrant Lock for AsyncIO

A reentrant lock (or recursive lock) is a particular type of lock that can be "locked" multiple times by the same task
without causing a deadlock. This is in contrast to a standard lock (sometimes called a "non-reentrant lock"), which
could cause a deadlock if a task attempts to lock it multiple times. A "Fair" Reentrant Lock respects the order of lock
granting, so that all lock acquirers get acquisition in the order they asked for it.

A reentrant lock can be useful in a number of scenarios:

* **Recursive function calls**: If you have a function that can be called recursively, and that function needs to lock a
  resource, a reentrant lock would be useful. This is because the same task will be able to acquire the lock multiple
  times, once for each recursive call.

* **Complex operations**: If you're performing a complex operation that consists of multiple steps, and each of these
  steps
  needs to lock a resource, a reentrant lock might be necessary. This is especially true if the steps are performed by
  the
  same task and if the steps can overlap in some way.

* **Improve readability**: Reentrant locks can sometimes make code easier to understand and debug. This is because you
  don't
  need to worry about a task deadlocking itself by attempting to acquire a lock it already holds.

* **Task ownership**: If your code logic depends on which task owns a lock, reentrant locks can be
  helpful. They inherently track ownership since they are tied to the task that acquired them first.

This code demonstrates the reentrant property:

```python
from fair_async_rlock import FairAsyncRLock


async def reentrant():
    lock = FairAsyncRLock()
    async with lock:
        async with lock:  # This will not block, whereas it would with asyncio.Lock
            assert True
```

In the context of Python's asyncio, FairAsyncRLock can be beneficial when used with
coroutines that might call each other or be invoked recursively. They also help when a single coroutine holds a lock and
wants to ensure fairness among other coroutines waiting for the same lock, so that no coroutine is left waiting for the
lock indefinitely while others after it acquire the lock, which can happen with standard asyncio Locks.

This code demonstrates fairness:

```python
import asyncio

from fair_async_rlock import FairAsyncRLock


async def fairness():
    lock = FairAsyncRLock()
    num_tasks = 1000
    order = []

    async def worker(n):
        async with lock:
            order.append(n)
            await asyncio.sleep(0)  # emulate work

    # Start several tasks to acquire the lock
    tasks = [asyncio.create_task(worker(i)) for i in range(num_tasks)]

    # Make sure they all start and try to acquire the lock before releasing it
    await asyncio.sleep(0)

    await asyncio.gather(*tasks)

    assert order == list(range(num_tasks))  # The tasks have run in order
```

### Potential downsides

It's also important to note the potential downsides of using a reentrant lock.
For one, they can hide bugs in code that isn't properly synchronized, since
the same task can acquire a reentrant lock multiple times without issue.

**_Non-reentrant locks are often simpler and can expose synchronization bugs more easily, so you should only use a
reentrant lock if you have a specific need for it._**

### Performance

The performance is about 50% slower than `asyncio.Lock`, i.e. overhead of sequential locks is about 3/2 of same
with `asyncio.Lock`.

### Change Log

27 Jan, 2024 - 1.0.7 released. Fixed a bug that allowed another task to get the lock before a waiter got its turn on the
event loop.