pyscopg2
========

`pyscopg2` is an acronym of **PY**thon **S**tateful **C**ross-datacenter
**O**rganic **P**ostgreSQL **G**luten-free **D**river

Library for acquiring actual connections with masters and replicas.

Usage
=====

Some useful examples

### Creating connection pool

When acquiring a connection, the connection object of the used driver is
returned (`aiopg.connection.Connection` for **aiog** and
`asyncpg.pool.PoolConnectionProxy` for **asyncpg** and **asyncpgsa**)

#### For aiopg or aiopg.sa:

**aiopg** must be installed as a requirement

```python
from pyscopg2.aiopg import PoolManager

multihost_dsn = "postgresql://user:password@master-host:5432,replica-host-1:5432,replica-host-2:5432/dbname"
pool = PoolManager(multihost_dsn)
```

or

```python
from pyscopg2.aiopg_sa import PoolManager

multihost_dsn = "postgresql://user:password@master-host:5432,replica-host-1:5432,replica-host-2:5432/dbname"
pool = PoolManager(multihost_dsn)
```

#### For asyncpg:

**asyncpg** must be installed as a requirement

```python
from pyscopg2.asyncpg import PoolManager

multihost_dsn = "postgresql://user:password@master-host:5432,replica-host-1:5432,replica-host-2:5432/dbname"
pool = PoolManager(multihost_dsn)
```

#### For asyncpgsa:

**asyncpgsa** must be installed as a requirement

```python
from pyscopg2.asyncpgsa import PoolManager

multihost_dsn = "postgresql://user:password@master-host:5432,replica-host-1:5432,replica-host-2:5432/dbname"
pool = PoolManager(multihost_dsn)
```

### Acquiring connections

Connections should be acquired with async context manager:

#### Acquiring master connection

```python

async def do_something():
    async with pool.acquire(read_only=False) as connection:
        ...

```

or

```python
async def do_something():
    async with pool.acquire_master() as connection:
        ...
```

##### Acquiring replica connection

```python
async def do_something():
    async with pool.acquire(read_only=True) as connection:
        ...
```

or

```python
async def do_something():
    async with pool.acquire_replica() as connection:
        ...
```

##### Without context manager (not recommended)

```python
async def do_something():
    connection = await pool.acquire(read_only=False)
    await pool.release(connection)
```

or more useful

```python
async def do_something():
    try:
        connection = await pool.acquire(read_only=False)
    finally:
        await pool.release(connection)
```

How it works?
=============

For each host from dsn string, a connection pool is created. From each pool one connection is reserved, which is used to check the availability of the host and its role. The minimum and maximum number of connections in the pool increases by 1 (to reserve a system connection).

For each pool a background task is created, in which the host availability and its role (master or replica) is checked once every `refresh_delay` second.

When switching hosts roles, pyscopg2 detects this with a slight delay.

For PostgreSQL, when switching the master, all connections to all hosts are broken (the details of implementing PostgreSQL).

If there are no available hosts, the methods acquire(), acquire_master(), and acquire_replica() wait until the host with the desired role startup.

Overview
========

* pyscopg2.base.BasePoolManager
    * `__init__(dsn, acquire_timeout, refresh_delay, refresh_timeout, fallback_master, master_as_replica_weight, balancer_policy, pool_factory_kwargs)`
        * `dsn: str` - Connection string used by the connection.
        * `acquire_timeout: Union[int, float]` - Default timeout (in seconds) for connection operations. 1 sec by default.
        * `refresh_delay: Union[int, float]` - Delay time (in seconds) between host polls. 1 sec by default.
        * `refresh_timeout: Union[int, float]` - Timeout (in seconds) for trying to connect and get the host role. 1 sec by default.
        * `fallback_master: bool` - Use connections from master if replicas are missing. False by default.
        * `master_as_replica_weight: float` - Probability of using the master as a replica (from 0. to 1.; 0. - master is not used as a replica; 1. - master can be used as a replica).
        * `balancer_policy: type` - Connection pool balancing policy (`pyscopg2.balancer_policy.GreedyBalancerPolicy`, `pyscopg2.balancer_policy.RandomWeightedBalancerPolicy` or `pyscopg2.balancer_policy.RoundRobinBalancerPolicy`).
        * `stopwatch_window_size: int` - Window size for calculating the median response time of each pool.
        * `pool_factory_kwargs: Optional[dict]` - Connection pool creation parameters that are passed to pool factory.

    * `get_pool_freesize(pool)`
        Getting the number of free connections in the connection pool. Returns number of free connections in the connection pool.
        * `pool` - Pool for which you to be getting the number of free connections.

    * coroutine async-with `acquire_from_pool(pool, **kwargs)`
        Acquire a connection from pool. Returns connection to the database.
        * `pool` - Pool from which you to be acquiring the connection.
        * `kwargs` - Arguments to be passed to the pool acquire() method.

    * coroutine `release_to_pool(connection, pool, **kwargs)`
        A coroutine that reverts connection conn to pool for future recycling.
        * `connection` - Connection to be released.
        * `pool` - Pool to which you are returning the connection.
        * `kwargs` - Arguments to be passed to the pool release() method.

    * `is_connection_closed(connection)`
        Returns True if connection is closed.

    * `get_last_response_time(pool)`
        Returns database host last response time (in seconds).

    * coroutine async-with `acquire(read_only, fallback_master, timeout, **kwargs)`
        Acquire a connection from free pool.
        * `readonly: bool` - True if need return connection to replica, False - to master. False by default.
        * `fallback_master: Optional[bool]` - Use connections from master if replicas are missing. If None, then the default value is used.
        * `master_as_replica_weight: float` - Probability of using the master as a replica (from 0. to 1.; 0. - master is not used as a replica; 1. - master can be used as a replica).
        * `timeout: Union[int, float]` - Timeout (in seconds) for connection operations.
        * `kwargs` - Arguments to be passed to the pool acquire() method.

    * coroutine async-with `acquire_master(timeout, **kwargs)`
        Acquire a connection from free master pool. Equivalent `acquire(read_only=False)`
        * `timeout: Union[int, float]` - Timeout (in seconds) for connection operations.
        * `kwargs` - Arguments to be passed to the pool acquire() method.

    * coroutine async-with `acquire_replica(fallback_master, timeout, **kwargs)`
        Acquire a connection from free master pool. Equivalent `acquire(read_only=True)`
        * `fallback_master: Optional[bool]` - Use connections from master if replicas are missing. If None, then the default value is used.
        * `master_as_replica_weight: float` - Probability of using the master as a replica (from 0. to 1.; 0. - master is not used as a replica; 1. - master can be used as a replica).
        * `timeout: Union[int, float]` - Timeout (in seconds) for connection operations.
        * `kwargs` - Arguments to be passed to the pool acquire() method.

    * coroutine `release(connection, **kwargs)`
        A coroutine that reverts connection conn to pool for future recycling.
        * `connection` - Connection to be released.
        * `kwargs` - Arguments to be passed to the pool release() method.

    * coroutine `close()`
        Close pool. Mark all pool connections to be closed on getting back to pool. Closed pool doesn’t allow to acquire new connections.

    * coroutine `terminate()`
        Terminate pool. Close pool with instantly closing all acquired connections also.

    * coroutine `wait_next_pool_check(timeout)`
        Waiting for the next step to update host roles.

    * coroutine `ready(masters_count, replicas_count, timeout)`
        Waiting for a connection to the database hosts. If masters_count is None and replicas_count is None, then connection to all hosts is expected.
        * `masters_count: Optional[int]` - Minimum number of master hosts. None by default.
        * `replicas_count: Optional[int]` - Minimum number of replica hosts. None by default.
        * `timeout: Union[int, float]` - Timeout for database connections. 10 secs by default.

    * coroutine `wait_all_ready()`
        Waiting to connect to all database hosts.

    * coroutine `wait_masters_ready(masters_count)`
        Waiting for connection to the specified number of database master servers.
        * `masters_count: int` - Minimum number of master hosts.

    * coroutine `wait_replicas_ready(replicas_count)`
        Waiting for connection to the specified number of database replica servers.
        * `replicas_count: int` - Minimum number of replica hosts.

    * coroutine `get_pool(read_only, fallback_master)`
        Returns connection pool with the maximum number of free connections.
        * `readonly: bool` - True if need return replica pool, False - master pool.
        * `fallback_master: Optional[bool]` - Returns master pool if replicas are missing. False by default.

    * coroutine `get_master_pools()`
        Returns a list of all master pools.

    * coroutine `get_replica_pools(fallback_master)`
        Returns a list of all replica pools.
        * `fallback_master: Optional[bool]` - Returns a list of all master pools if replicas are missing. False by default.

    * `pool_is_master(pool)`
        Returns True if connection is master.

    * `pool_is_replica(pool)`
        Returns True if connection is replica.

    * `register_connection(connection, pool)`
        Match connection with the pool from which it was taken. It is necessary for the release() method to work correctly.

* pyscopg2.aiopg.PoolManager

* pyscopg2.aiopg_sa.PoolManager

* pyscopg2.asyncpg.PoolManager

* pyscopg2.asyncpgsa.PoolManager

Balancer policies
=================

* pyscopg2.balancer_policy.GreedyBalancerPolicy

    Choice pool with the most free connections. If there are several such pools, a random one is taken.

* pyscopg2.balancer_policy.RandomWeightedBalancerPolicy

    Choice random pool according to their weights. The weight is inversely proportional to the response time of the database of the respective pool.

* pyscopg2.balancer_policy.RoundRobinBalancerPolicy

Publishing new release
=======================

See instructions for .pypirc configuration here https://wiki.yandex-team.ru/pypi/.

After that just run:

     make upload
