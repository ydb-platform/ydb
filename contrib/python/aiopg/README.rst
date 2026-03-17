aiopg
=====
.. image:: https://github.com/aio-libs/aiopg/workflows/CI/badge.svg
   :target: https://github.com/aio-libs/aiopg/actions?query=workflow%3ACI
.. image:: https://codecov.io/gh/aio-libs/aiopg/branch/master/graph/badge.svg
   :target: https://codecov.io/gh/aio-libs/aiopg
.. image:: https://badges.gitter.im/Join%20Chat.svg
    :target: https://gitter.im/aio-libs/Lobby
    :alt: Chat on Gitter

**aiopg** is a library for accessing a PostgreSQL_ database
from the asyncio_ (PEP-3156/tulip) framework. It wraps
asynchronous features of the Psycopg database driver.

Example
-------

.. code:: python

    import asyncio
    import aiopg

    dsn = 'dbname=aiopg user=aiopg password=passwd host=127.0.0.1'

    async def go():
        pool = await aiopg.create_pool(dsn)
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT 1")
                ret = []
                async for row in cur:
                    ret.append(row)
                assert ret == [(1,)]

    loop = asyncio.get_event_loop()
    loop.run_until_complete(go())


Example of SQLAlchemy optional integration
------------------------------------------

.. code:: python

   import asyncio
   from aiopg.sa import create_engine
   import sqlalchemy as sa

   metadata = sa.MetaData()

   tbl = sa.Table('tbl', metadata,
       sa.Column('id', sa.Integer, primary_key=True),
       sa.Column('val', sa.String(255)))

   async def create_table(engine):
       async with engine.acquire() as conn:
           await conn.execute('DROP TABLE IF EXISTS tbl')
           await conn.execute('''CREATE TABLE tbl (
                                     id serial PRIMARY KEY,
                                     val varchar(255))''')

   async def go():
       async with create_engine(user='aiopg',
                                database='aiopg',
                                host='127.0.0.1',
                                password='passwd') as engine:

           async with engine.acquire() as conn:
               await conn.execute(tbl.insert().values(val='abc'))

               async for row in conn.execute(tbl.select()):
                   print(row.id, row.val)

   loop = asyncio.get_event_loop()
   loop.run_until_complete(go())

.. _PostgreSQL: http://www.postgresql.org/
.. _asyncio: https://docs.python.org/3/library/asyncio.html

Please use::

   $ make test

for executing the project's unittests.
See https://aiopg.readthedocs.io/en/stable/contributing.html for details
on how to set up your environment to run the tests.
