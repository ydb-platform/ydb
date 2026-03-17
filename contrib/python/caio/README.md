Python wrapper for AIO
======================

> **NOTE:** Native Linux aio implementation supports since 4.18 kernel version.

Python bindings for Linux AIO API and simple asyncio wrapper.

Example
-------

```python

import asyncio
from caio import AsyncioContext

loop = asyncio.get_event_loop()

async def main():
    # max_requests=128 by default
    ctx = AsyncioContext(max_requests=128)

    with open("test.file", "wb+") as fp:
        fd = fp.fileno()

        # Execute one write operation
        await ctx.write(b"Hello world", fd, offset=0)

        # Execute one read operation
        print(await ctx.read(32, fd, offset=0))

        # Execute one fdsync operation
        await ctx.fdsync(fd)

        op1 = ctx.write(b"Hello from ", fd, offset=0)
        op2 = ctx.write(b"async world", fd, offset=11)

        await asyncio.gather(op1, op2)

        print(await ctx.read(32, fd, offset=0))
        # Hello from async world


loop.run_until_complete(main())
```

Troubleshooting
---------------

The `linux` implementation works normal for modern linux kernel versions
and file systems. So you may have problems specific for your environment.
It's not a bug and might be resolved some ways:

1. Upgrade the kernel
2. Use compatible file system
3. Use threads based or pure python implementation.

The caio since version 0.7.0 contains some ways to do this.

1. In runtime use the environment variable `CAIO_IMPL` with possible values:
   * `linux` - use native linux kernels aio mechanism
   * `thread` - use thread based implementation written in C
   * `python` - use pure python implementation
2. File ``default_implementation`` located near ``__init__.py`` in caio
   installation path. It's useful for distros package maintainers. This file
   might contains comments (lines starts with ``#`` symbol) and the first line
   should be one of ``linux`` ``thread`` or ``python``.

Previous versions allows direct import of the target implementation.
