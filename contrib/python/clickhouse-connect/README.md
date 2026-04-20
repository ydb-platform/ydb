## ClickHouse Connect

A high performance core database driver for connecting ClickHouse to Python, Pandas, and Superset

* Pandas DataFrames (numpy and arrow-backed). Pandas 2.x and above only, 1.x is deprecated and will be dropped in 1.0.
* Numpy Arrays
* PyArrow Tables
* Polars DataFrames
* Superset Connector
* SQLAlchemy Core (select, joins, lightweight deletes; limited feature set)

ClickHouse Connect currently uses the ClickHouse HTTP interface for maximum compatibility.

### Installation

```
pip install clickhouse-connect
```

ClickHouse Connect requires Python 3.9 or higher. We officially test against Python 3.10 through 3.14.
Python 3.9 is deprecated and support will be removed entirely in 1.0.

### Superset Connectivity

ClickHouse Connect is fully integrated with Apache Superset. Previous versions of ClickHouse Connect utilized a
dynamically loaded Superset Engine Spec, but as of Superset v2.1.0 the engine spec was incorporated into the main
Apache Superset project and removed from clickhouse-connect in v0.6.0. If you have issues connecting to earlier
versions of Superset, please use clickhouse-connect v0.5.25.

When creating a Superset Data Source, either use the provided connection dialog, or a SqlAlchemy DSN in the form
`clickhousedb://{username}:{password}@{host}:{port}`.

### SQLAlchemy Implementation

ClickHouse Connect includes a lightweight SQLAlchemy dialect implementation focused on compatibility with **Superset**
and **SQLAlchemy Core**. Both SQLAlchemy 1.4 and 2.x are supported. SQLAlchemy 1.4 compatibility is maintained
because Apache Superset currently requires `sqlalchemy>=1.4,<2`.

Supported features include:
- Basic query execution via SQLAlchemy Core
- `SELECT` queries with `JOIN`s (including ClickHouse-specific strictness, `USING`, and `GLOBAL` modifiers),
  `ARRAY JOIN` (single and multi-column), `FINAL`, and `SAMPLE`
- `VALUES` table function syntax
- Lightweight `DELETE` statements

A small number of features require SQLAlchemy 2.x: `Values.cte()` and certain literal-rendering behaviors.
All other dialect features, including those used by Superset, work on both 1.4 and 2.x.

Basic ORM usage works for insert-heavy, read-focused workloads: declarative model definitions, `CREATE TABLE`,
`session.add()`, `bulk_save_objects()`, and read queries all function correctly. However, full ORM support is not
provided. UPDATE compilation, foreign key/relationship reflection, autoincrement/RETURNING, and cascade operations
are not implemented. The dialect is best suited for SQLAlchemy Core usage and Superset connectivity.

### Asyncio Support

ClickHouse Connect provides an `AsyncClient` for use in `asyncio` environments.
See the [run_async example](./examples/run_async.py) for more details.

The current `AsyncClient` is a thread-pool executor wrapper around the synchronous client and is deprecated.
In 1.0.0 it will be replaced by a fully native async implementation. The API surface is the same,
with one difference: you will no longer be able to create a sync client first and pass it to the
`AsyncClient` constructor. Instead, use `clickhouse_connect.get_async_client()` directly.

### Complete Documentation

The documentation for ClickHouse Connect has moved to
[ClickHouse Docs](https://clickhouse.com/docs/integrations/python)
