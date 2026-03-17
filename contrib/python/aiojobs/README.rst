=======
aiojobs
=======
.. image:: https://travis-ci.org/aio-libs/aiojobs.svg?branch=master
    :target: https://travis-ci.org/aio-libs/aiojobs
.. image:: https://codecov.io/gh/aio-libs/aiojobs/branch/master/graph/badge.svg
    :target: https://codecov.io/gh/aio-libs/aiojobs
.. image:: https://img.shields.io/pypi/v/aiojobs.svg
    :target: https://pypi.python.org/pypi/aiojobs
.. image:: https://readthedocs.org/projects/aiojobs/badge/?version=latest
    :target: http://aiojobs.readthedocs.io/en/latest/?badge=latest
    :alt: Documentation Status
.. image:: https://badges.gitter.im/Join%20Chat.svg
    :target: https://gitter.im/aio-libs/Lobby
    :alt: Chat on Gitter

Job scheduler for managing background tasks (asyncio)


The library gives a controlled way for scheduling background tasks for
asyncio applications.

Installation
============

.. code-block:: bash

   $ pip3 install aiojobs

Usage example
=============

.. code-block:: python

   import asyncio
   import aiojobs

   async def coro(timeout):
       await asyncio.sleep(timeout)

   async def main():
       async with aiojobs.Scheduler() as scheduler:
           for i in range(100):
               # spawn jobs
               await scheduler.spawn(coro(i/10))

           await asyncio.sleep(5.0)
           # not all scheduled jobs are finished at the moment
       # Exit from context will gracefully wait on tasks before closing
       # any remaining spawned jobs

   asyncio.run(main())

Shielding tasks with a scheduler
================================

It is typically recommended to use ``asyncio.shield`` to protect tasks
from cancellation. However, the inner shielded tasks can't be tracked and
are therefore at risk of being cancelled during application shutdown.

To resolve this issue aiojobs includes a ``aiojobs.Scheduler.shield``
method to shield tasks while also keeping track of them in the scheduler.
In combination with the ``aiojobs.Scheduler.wait_and_close`` method,
this allows shielded tasks the required time to complete successfully
during application shutdown.

For example:

.. code-block:: python

   import asyncio
   import aiojobs
   from contextlib import suppress

   async def important():
       print("START")
       await asyncio.sleep(5)
       print("DONE")

   async def run_something(scheduler):
       # If we use asyncio.shield() here, then the task doesn't complete and DONE is never printed.
       await scheduler.shield(important())

   async def main():
       async with aiojobs.Scheduler() as scheduler:
           t = asyncio.create_task(run_something(scheduler))
           await asyncio.sleep(0.1)
           t.cancel()
           with suppress(asyncio.CancelledError):
               await t

   asyncio.run(main())


Integration with aiohttp.web
============================

.. code-block:: python

   from aiohttp import web
   from aiojobs.aiohttp import setup, spawn

   async def handler(request):
       await spawn(request, coro())
       return web.Response()

   app = web.Application()
   app.router.add_get('/', handler)
   setup(app)

or just

.. code-block:: python

   from aiojobs.aiohttp import atomic

   @atomic
   async def handler(request):
       return web.Response()

For more information read documentation: https://aiojobs.readthedocs.io

Communication channels
======================

*aio-libs* google group: https://groups.google.com/forum/#!forum/aio-libs

Feel free to post your questions and ideas here.

*Gitter Chat* https://gitter.im/aio-libs/Lobby

We support `Stack Overflow <https://stackoverflow.com>`_.
Please add *python-asyncio* or *aiohttp* tag to your question there.


Author and License
==================

The ``aiojobs`` package is written by Andrew Svetlov.

It's *Apache 2* licensed and freely available.
