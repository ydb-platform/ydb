# YDB Python DBAPI

## Introduction

Python DBAPI to `YDB`, which provides both sync and async drivers and complies with [PEP249](https://www.python.org/dev/peps/pep-0249/).

## Installation

```shell
pip install ydb-dbapi
```

## Usage

To establish a new DBAPI connection you should provide `host`, `port` and `database`:

```python
import ydb_dbapi

connection = ydb_dbapi.connect(
    host="localhost", port="2136", database="/local"
) # sync connection

async_connection = await ydb_dbapi.async_connect(
    host="localhost", port="2136", database="/local"
) # async connection
```

Usage of connection:

```python
with connection.cursor() as cursor:
    cursor.execute("SELECT id, val FROM table")

    row = cursor.fetchone()
    rows = cursor.fetchmany(size=5)
    rows = cursor.fetchall()
```

Usage of async connection:

```python
async with async_connection.cursor() as cursor:
    await cursor.execute("SELECT id, val FROM table")

    row = await cursor.fetchone()
    rows = await cursor.fetchmany(size=5)
    rows = await cursor.fetchall()
```
