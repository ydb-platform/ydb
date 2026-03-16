ClickHouse Python Driver
========================

.. image:: https://img.shields.io/pypi/v/clickhouse-driver.svg
    :target: https://pypi.org/project/clickhouse-driver

.. image:: https://coveralls.io/repos/github/mymarilyn/clickhouse-driver/badge.svg?branch=master
    :target: https://coveralls.io/github/mymarilyn/clickhouse-driver?branch=master

.. image:: https://img.shields.io/pypi/l/clickhouse-driver.svg
    :target: https://pypi.org/project/clickhouse-driver

.. image:: https://img.shields.io/pypi/pyversions/clickhouse-driver.svg
    :target: https://pypi.org/project/clickhouse-driver

.. image:: https://img.shields.io/pypi/dm/clickhouse-driver.svg
    :target: https://pypi.org/project/clickhouse-driver

.. image:: https://travis-ci.org/mymarilyn/clickhouse-driver.svg?branch=master
   :target: https://travis-ci.org/mymarilyn/clickhouse-driver

ClickHouse Python Driver with native (TCP) interface support.

Asynchronous wrapper is available here: https://github.com/mymarilyn/aioch

Features
========

- External data for query processing.

- Query settings.

- Compression support.

- TLS support (since server version 1.1.54304).

- Types support:

  * Float32/64
  * [U]Int8/16/32/64
  * Date/DateTime('timezone')/DateTime64('timezone')
  * String/FixedString(N)
  * Enum8/16
  * Array(T)
  * Nullable(T)
  * UUID
  * Decimal
  * IPv4/IPv6
  * LowCardinality(T)
  * SimpleAggregateFunction(F, T)
  * Tuple(T1, T2, ...)
  * Nested

- Query progress information.

- Block by block results streaming.

- Reading query profile info.

- Receiving server logs.

- Multiple hosts support.

- Python DB API 2.0 specification support.

Documentation
=============

Documentation is available at https://clickhouse-driver.readthedocs.io.

Usage
=====

There are two ways to communicate with server:

- using pure Client;
- using DB API.

Pure Client example:

    .. code-block:: python

        >>> from clickhouse_driver import Client
        >>>
        >>> client = Client('localhost')
        >>>
        >>> client.execute('SHOW TABLES')
        [('test',)]
        >>> client.execute('DROP TABLE IF EXISTS test')
        []
        >>> client.execute('CREATE TABLE test (x Int32) ENGINE = Memory')
        []
        >>> client.execute(
        ...     'INSERT INTO test (x) VALUES',
        ...     [{'x': 100}]
        ... )
        1
        >>> client.execute('INSERT INTO test (x) VALUES', [[200]])
        1
        >>> client.execute(
        ...     'INSERT INTO test (x) '
        ...     'SELECT * FROM system.numbers LIMIT %(limit)s',
        ...     {'limit': 3}
        ... )
        []
        >>> client.execute('SELECT sum(x) FROM test')
        [(303,)]

DB API example:

    .. code-block:: python

        >>> from clickhouse_driver import connect
        >>>
        >>> conn = connect('clickhouse://localhost')
        >>> cursor = conn.cursor()
        >>>
        >>> cursor.execute('SHOW TABLES')
        >>> cursor.fetchall()
        [('test',)]
        >>> cursor.execute('DROP TABLE IF EXISTS test')
        >>> cursor.fetchall()
        []
        >>> cursor.execute('CREATE TABLE test (x Int32) ENGINE = Memory')
        >>> cursor.fetchall()
        []
        >>> cursor.executemany(
        ...     'INSERT INTO test (x) VALUES',
        ...     [{'x': 100}]
        ... )
        >>> cursor.rowcount
        1
        >>> cursor.executemany('INSERT INTO test (x) VALUES', [[200]])
        >>> cursor.rowcount
        1
        >>> cursor.execute(
        ...     'INSERT INTO test (x) '
        ...     'SELECT * FROM system.numbers LIMIT %(limit)s',
        ...     {'limit': 3}
        ... )
        >>> cursor.rowcount
        0
        >>> cursor.execute('SELECT sum(x) FROM test')
        >>> cursor.fetchall()
        [(303,)]

License
=======

ClickHouse Python Driver is distributed under the `MIT license
<http://www.opensource.org/licenses/mit-license.php>`_.
