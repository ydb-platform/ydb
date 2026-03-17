.. image:: https://raw.githubusercontent.com/aiokitchen/hasql/master/resources/logo.svg
   :width: 365
   :height: 265

hasql
=====

``hasql`` is a library for acquiring actual connections to masters and replicas
in high available PostgreSQL clusters.

.. image:: https://raw.githubusercontent.com/aiokitchen/hasql/master/resources/diagram.svg

Features
========

* completely asynchronous api
* automatic detection of the host role in the cluster
* health-checks for each host and automatic traffic outage for
  unavailable hosts
* autodetection of hosts role changes, in case replica
  host will be promoted to master
* different policies for load balancing
* support for ``asyncpg``, ``psycopg3``, ``aiopg``, ``sqlalchemy`` and ``asyncpgsa``


Usage
=====

Some useful examples

Creating connection pool
************************

When acquiring a connection, the connection object of the used driver is
returned (``aiopg.connection.Connection`` for **aiopg** and
``asyncpg.pool.PoolConnectionProxy`` for **asyncpg** and **asyncpgsa**)


Database URL specirication rules
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* Multiple hosts should be passed comma separated

  * multihost example:

    * ``postgresql://db1,db2,db3/``
  * split result:

    * ``postgresql://db1:5432/``
    * ``postgresql://db2:5432/``
    * ``postgresql://db3:5432/``
* The non-default port for each host might be passed after hostnames. e.g.

  * multihost example:

    * ``postgresql://db1:1234,db2:5678,db3/``
  * split result:

    * ``postgresql://db1:1234/``
    * ``postgresql://db2:5678/``
    * ``postgresql://db3:5432/``
* The special case for non-default port for all hosts

  * multihost example:

    * ``postgresql://db1,db2,db3:6432/``
  * split result:

    * ``postgresql://db1:6432/``
    * ``postgresql://db2:6432/``
    * ``postgresql://db3:6432/``


For ``aiopg`` or ``aiopg.sa``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**aiopg** must be installed as a requirement.

Code example using ``aiopg``:

.. code-block:: python

    from hasql.aiopg import PoolManager

    hosts = ",".join([
        "master-host:5432",
        "replica-host-1:5432",
        "replica-host-2:5432",
    ])

    multihost_dsn = f"postgresql://user:password@{hosts}/dbname"

    async def create_pool(dsn) -> PoolManager:
        pool = PoolManager(multihost_dsn)

        # Waiting for 1 master and 1 replica will be available
        await pool.ready(masters_count=1, replicas_count=1)
        return pool

Code example using ``aiopg.sa``:

.. code-block:: python

    from hasql.aiopg_sa import PoolManager

    hosts = ",".join([
        "master-host:5432",
        "replica-host-1:5432",
        "replica-host-2:5432",
    ])

    multihost_dsn = f"postgresql://user:password@{hosts}/dbname"

    async def create_pool(dsn) -> PoolManager:
        pool = PoolManager(multihost_dsn)

        # Waiting for 1 master and 1 replica will be available
        await pool.ready(masters_count=1, replicas_count=1)
        return pool

For ``asyncpg``
~~~~~~~~~~~~~~~

**asyncpg** must be installed as a requirement

.. code-block:: python

    from hasql.asyncpg import PoolManager

    hosts = ",".join([
        "master-host:5432",
        "replica-host-1:5432",
        "replica-host-2:5432",
    ])

    multihost_dsn = f"postgresql://user:password@{hosts}/dbname"

    async def create_pool(dsn) -> PoolManager:
        pool = PoolManager(multihost_dsn)

        # Waiting for 1 master and 1 replica will be available
        await pool.ready(masters_count=1, replicas_count=1)
        return pool

For ``sqlalchemy``
~~~~~~~~~~~~~~~~~~

**sqlalchemy[asyncio] & asyncpg** must be installed as requirements

.. code-block:: python

    from hasql.asyncsqlalchemy import PoolManager

    hosts = ",".join([
        "master-host:5432",
        "replica-host-1:5432",
        "replica-host-2:5432",
    ])

    multihost_dsn = f"postgresql://user:password@{hosts}/dbname"


    async def create_pool(dsn) -> PoolManager:
        pool = PoolManager(
            multihost_dsn,

            # Use master for acquire_replica, if no replicas available
            fallback_master=True,

            # You can pass pool-specific options
            pool_factory_kwargs=dict(
                pool_size=10,
                max_overflow=5
            )
        )

        # Waiting for 1 master and 1 replica will be available
        await pool.ready(masters_count=1, replicas_count=1)
        return pool


For ``asyncpgsa``
~~~~~~~~~~~~~~~~~

**asyncpgsa** must be installed as a requirement

.. code-block:: python

    from hasql.asyncpgsa import PoolManager

    hosts = ",".join([
        "master-host:5432",
        "replica-host-1:5432",
        "replica-host-2:5432",
    ])

    multihost_dsn = f"postgresql://user:password@{hosts}/dbname"

    async def create_pool(dsn) -> PoolManager:
        pool = PoolManager(multihost_dsn)

        # Waiting for 1 master and 1 replica will be available
        await asyncio.gather(
            pool.wait_masters_ready(1),
            pool.wait_replicas_ready(1)
        )
        return pool


For ``psycopg3``
~~~~~~~~~~~~~~~~

**psycopg3** must be installed as a requirement (package name is `psycopg`)

.. code-block:: python

    from hasql.psycopg3 import PoolManager


    hosts = ",".join([
        "master-host:5432",
        "replica-host-1:5432",
        "replica-host-2:5432",
    ])
    multihost_dsn = f"postgresql://user:password@{hosts}/dbname"

    async def create_pool(dsn) -> PoolManager:
        pool = PoolManager(multihost_dsn)

        # Waiting for 1 master and 1 replica will be available
        await pool.ready(masters_count=1, replicas_count=1)
        return pool


Acquiring connections
*********************

Connections should be acquired with async context manager:

Acquiring master connection
~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    async def do_something():
        pool = await create_pool(multihost_dsn)
        async with pool.acquire(read_only=False) as connection:
            ...

or

.. code-block:: python

    async def do_something():
        pool = await create_pool(multihost_dsn)
        async with pool.acquire_master() as connection:
            ...

Acquiring replica connection
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    async def do_something():
        pool = await create_pool(multihost_dsn)
        async with pool.acquire(read_only=True) as connection:
            ...

or

.. code-block:: python

    async def do_something():
        pool = await create_pool(multihost_dsn)
        async with pool.acquire_replica() as connection:
            ...

Without context manager (really not recommended)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    async def do_something():
        pool = await create_pool(multihost_dsn)
        connection = await pool.acquire(read_only=False)
        await pool.release(connection)

or more useful

.. code-block:: python

    async def do_something():
        pool = await create_pool(multihost_dsn)
        try:
            connection = await pool.acquire(read_only=False)
        finally:
            await pool.release(connection)

How it works?
=============

For each host from dsn string, a connection pool is created. From each pool one
connection is reserved, which is used to check the availability of the host and
its role. The minimum and maximum number of connections in the pool increases
by 1 (to reserve a system connection).

For each pool a background task is created, in which the host availability and
its role (master or replica) is checked once every `refresh_delay` second.

When switching hosts roles, hasql detects this with a slight delay.

For PostgreSQL, when switching the master, all connections to all hosts are
broken (the details of implementing PostgreSQL).

If there are no available hosts, the methods acquire(), acquire_master(), and
acquire_replica() wait until the host with the desired role startup.

Overview
========

* hasql.base.BasePoolManager
    * ``__init__(dsn, acquire_timeout, refresh_delay, refresh_timeout, fallback_master, master_as_replica_weight, balancer_policy, pool_factory_kwargs)``:

        * ``dsn: str`` - Connection string used by the connection.

        * ``acquire_timeout: Union[int, float]`` - Default timeout (in seconds)
          for connection operations. 1 sec by default.

        * ``refresh_delay: Union[int, float]`` - Delay time (in seconds)
          between host polls. 1 sec by default.

        * ``refresh_timeout: Union[int, float]`` - Timeout (in seconds) for
          trying to connect and get the host role. 1 sec by default.

        * ``fallback_master: bool`` - Use connections from master if replicas
          are missing. False by default.

        * ``master_as_replica_weight: float`` - Probability of using the master
          as a replica (from 0. to 1.; 0. - master is not used as a replica;
          1. - master can be used as a replica).

        * ``balancer_policy: type`` - Connection pool balancing policy
          (`hasql.balancer_policy.GreedyBalancerPolicy`,
          `hasql.balancer_policy.RandomWeightedBalancerPolicy` or
          `hasql.balancer_policy.RoundRobinBalancerPolicy`).

        * ``stopwatch_window_size: int`` - Window size for calculating the
          median response time of each pool.

        * ``pool_factory_kwargs: Optional[dict]`` - Connection pool creation
          parameters that are passed to pool factory.

    * ``get_pool_freesize(pool)``
      Getting the number of free connections in the connection pool. Returns
      number of free connections in the connection pool.

        * ``pool`` - Pool for which you to be getting the number of
          free connections.

    * coroutine async-with ``acquire_from_pool(pool, **kwargs)``
      Acquire a connection from pool. Returns connection to the database.

        * ``pool`` - Pool from which you to be acquiring the connection.

        * ``kwargs`` - Arguments to be passed to the pool acquire() method.

    * coroutine ``release_to_pool(connection, pool, **kwargs)``
      A coroutine that reverts connection conn to pool for future recycling.

        * ``connection`` - Connection to be released.

        * ``pool`` - Pool to which you are returning the connection.

        * ``kwargs`` - Arguments to be passed to the pool release() method.

    * ``is_connection_closed(connection)``
      Returns True if connection is closed.

    * ``get_last_response_time(pool)``
      Returns database host last response time (in seconds).

    * coroutine async-with
      ``acquire(read_only, fallback_master, timeout, **kwargs)``
      Acquire a connection from free pool.

        * ``readonly: bool`` - ``True`` if need return connection to replica,
          ``False`` - to master. False by default.

        * ``fallback_master: Optional[bool]`` - Use connections from master
          if replicas are missing. If None, then the default value is used.

        * ``master_as_replica_weight: float`` - Probability of using the master
          as a replica (from 0. to 1.; 0. - master is not used as a replica;
          1. - master can be used as a replica).

        * ``timeout: Union[int, float]`` - Timeout (in seconds) for connection
          operations.

        * ``kwargs`` - Arguments to be passed to the pool acquire() method.

    * coroutine async-with ``acquire_master(timeout, **kwargs)``
      Acquire a connection from free master pool.
      Equivalent ``acquire(read_only=False)``

        * ``timeout: Union[int, float]`` - Timeout (in seconds) for
          connection operations.

        * ``kwargs`` - Arguments to be passed to the pool acquire() method.

    * coroutine async-with
      ``acquire_replica(fallback_master, timeout, **kwargs)``
      Acquire a connection from free master pool.
      Equivalent ``acquire(read_only=True)``

        * ``fallback_master: Optional[bool]`` - Use connections from master if
          replicas are missing. If None, then the default value is used.

        * ``master_as_replica_weight: float`` - Probability of using the master
          as a replica (from 0. to 1.; 0. - master is not used as a replica;
          1. - master can be used as a replica).

        * ``timeout: Union[int, float]`` - Timeout (in seconds) for connection
          operations.

        * ``kwargs`` - Arguments to be passed to the pool acquire() method.

    * coroutine ``release(connection, **kwargs)``
      A coroutine that reverts connection conn to pool for future recycling.

        * ``connection`` - Connection to be released.
        * ``kwargs`` - Arguments to be passed to the pool release() method.

    * coroutine ``close()``
      Close pool. Mark all pool connections to be closed on getting back to
      pool. Closed pool doesn’t allow to acquire new connections.

    * coroutine ``terminate()``
      Terminate pool. Close pool with instantly closing all acquired
      connections also.

    * coroutine ``wait_next_pool_check(timeout)``
      Waiting for the next step to update host roles.

    * coroutine ``ready(masters_count, replicas_count, timeout)``
      Waiting for a connection to the database hosts. If masters_count is
      ``None`` and replicas_count is None, then connection to all hosts
      is expected.

        * ``masters_count: Optional[int]`` - Minimum number of master hosts.
          ``None`` by default.

        * ``replicas_count: Optional[int]`` - Minimum number of replica hosts.
          ``None`` by default.

        * ``timeout: Union[int, float]`` - Timeout for database connections.
          10 seconds by default.

    * coroutine ``wait_all_ready()```
      Waiting to connect to all database hosts.

    * coroutine ``wait_masters_ready(masters_count)``
      Waiting for connection to the specified number of
      database master servers.

        * ``masters_count: int`` - Minimum number of master hosts.

    * coroutine `wait_replicas_ready(replicas_count)`
      Waiting for connection to the specified number of
      database replica servers.

        * ``replicas_count: int`` - Minimum number of replica hosts.

    * coroutine ``get_pool(read_only, fallback_master)``
      Returns connection pool with the maximum number of free connections.

        * ``readonly: bool`` - True if need return replica pool,
          ``False`` - master pool.

        * ``fallback_master: Optional[bool]`` - Returns master pool if
          replicas are missing. False by default.

    * coroutine ``get_master_pools()``
      Returns a list of all master pools.

    * coroutine ``get_replica_pools(fallback_master)``
      Returns a list of all replica pools.

        * ``fallback_master: Optional[bool]`` - Returns a list of all master
          pools if replicas are missing. False by default.

    * ``pool_is_master(pool)``
      Returns True if connection is master.

    * ``pool_is_replica(pool)``
      Returns True if connection is replica.

    * ``register_connection(connection, pool)``
      Match connection with the pool from which it was taken.
      It is necessary for the release() method to work correctly.

* ``hasql.aiopg.PoolManager``

* ``hasql.aiopg_sa.PoolManager``

* ``hasql.asyncpg.PoolManager``

* ``hasql.asyncpgsa.PoolManager``

* ``hasql.psycopg3.PoolManager``

Balancer policies
=================

* ``hasql.balancer_policy.GreedyBalancerPolicy``
  Chooses pool with the most free connections. If there are several such pools,
  a random one is taken.

* ``hasql.balancer_policy.RandomWeightedBalancerPolicy``
  Chooses random pool according to their weights. The weight is inversely
  proportional to the response time of the database of the respective pool 
  (faster response - higher weight).

* ``hasql.balancer_policy.RoundRobinBalancerPolicy``
