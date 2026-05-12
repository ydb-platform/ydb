# SQLAlchemy

[SQLAlchemy](https://www.sqlalchemy.org/) is a popular Python library for working with databases, providing both ORM (Object-Relational Mapping) and Core API for executing SQL queries.

YDB supports integration with SQLAlchemy through a special `ydb-sqlalchemy` dialect, which provides full compatibility with SQLAlchemy 2.0 and partial support for SQLAlchemy 1.4.

## Installation

Install the `ydb-sqlalchemy` package using pip:

```bash
pip install ydb-sqlalchemy
```

## Quick Start

### Synchronous Connection

```python
import sqlalchemy as sa

# Create engine
engine = sa.create_engine("yql+ydb://localhost:2136/local")

# Execute query
with engine.connect() as conn:
    result = conn.execute(sa.text("SELECT 1 AS value"))
    print(result.fetchone())
```

### Asynchronous Connection

```python
import asyncio
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import create_async_engine

async def main():
    # Create async engine
    engine = create_async_engine("yql+ydb_async://localhost:2136/local")

    # Execute query
    async with engine.connect() as conn:
        result = await conn.execute(sa.text("SELECT 1 AS value"))
        print(await result.fetchone())

asyncio.run(main())
```

## Connection Configuration

### Authentication Methods

#### Anonymous Access

For local development or testing:

```python
import sqlalchemy as sa

engine = sa.create_engine("yql+ydb://localhost:2136/local")
```

#### Static Credentials

Use username and password authentication:

```python
engine = sa.create_engine(
    "yql+ydb://localhost:2136/local",
    connect_args={
        "credentials": {
            "username": "your_username",
            "password": "your_password"
        }
    }
)
```

#### Token Authentication

Use access token for authentication:

```python
engine = sa.create_engine(
    "yql+ydb://localhost:2136/local",
    connect_args={
        "credentials": {
            "token": "your_access_token"
        }
    }
)
```

#### Service Account Authentication

Use service account JSON key:

```python
import json

# Load from file
with open('service_account_key.json', 'r') as f:
    service_account_json = json.load(f)

engine = sa.create_engine(
    "yql+ydb://localhost:2136/local",
    connect_args={
        "credentials": {
            "service_account_json": service_account_json
        }
    }
)

# Or pass JSON directly
engine = sa.create_engine(
    "yql+ydb://localhost:2136/local",
    connect_args={
        "credentials": {
            "service_account_json": {
                "id": "your_key_id",
                "service_account_id": "your_service_account_id",
                "created_at": "2023-01-01T00:00:00Z",
                "key_algorithm": "RSA_2048",
                "public_key": "-----BEGIN PUBLIC KEY-----\n...\n-----END PUBLIC KEY-----",
                "private_key": "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----"
            }
        }
    }
)
```

#### Using YDB SDK Credentials

You can use any available authentication methods from YDB Python SDK:

```python
import ydb.iam

# Metadata service
engine = sa.create_engine(
    "yql+ydb://localhost:2136/local",
    connect_args={
        "credentials": ydb.iam.MetadataUrlCredentials()
    }
)

# OAuth token
engine = sa.create_engine(
    "yql+ydb://localhost:2136/local",
    connect_args={
        "credentials": ydb.iam.OAuthCredentials("your_oauth_token")
    }
)

# Static credentials
engine = sa.create_engine(
    "yql+ydb://localhost:2136/local",
    connect_args={
        "credentials": ydb.iam.StaticCredentials("username", "password")
    }
)
```

### TLS Configuration

For secure connections to YDB:

```python
engine = sa.create_engine(
    "yql+ydb://ydb.example.com:2135/prod",
    connect_args={
        "credentials": {"token": "your_token"},
        "protocol": "grpc",
        "root_certificates_path": "/path/to/ca-certificates.crt",
        # "root_certificates": crt_string,  # Alternative - certificate string
    }
)
```

## Supported Data Types

YDB SQLAlchemy provides comprehensive support for YDB data types through custom SQLAlchemy types. For detailed information about YDB data types, see the [YDB Data Types Documentation](https://ydb.tech/docs/en/concepts/datatypes).

### Type Mapping Summary

| YDB Type | YDB SQLAlchemy Type | Standard SQLAlchemy Type | Python Type | Notes |
|----------|-------------------|--------------------------|-------------|-------|
| `Bool` | `Boolean` | `Boolean` | `bool` | |
| `Int8` | `Int8` | | `int` | -2^7 to 2^7-1 |
| `Int16` | `Int16` | | `int` | -2^15 to 2^15-1 |
| `Int32` | `Int32` | | `int` | -2^31 to 2^31-1 |
| `Int64` | `Int64` | `Integer` | `int` | -2^63 to 2^63-1 |
| `Uint8` | `UInt8` | | `int` | 0 to 2^8-1 |
| `Uint16` | `UInt16` | | `int` | 0 to 2^16-1 |
| `Uint32` | `UInt32` | | `int` | 0 to 2^32-1 |
| `Uint64` | `UInt64` | | `int` | 0 to 2^64-1 |
| `Float` | `Float` | `Float` | `float` | |
| `Double` | `Double` | | `float` | Available in SQLAlchemy 2.0+ |
| `Decimal(p,s)` | `Decimal` | `DECIMAL` | `decimal.Decimal` | |
| `String` | | `BINARY` | `bytes` | |
| `Utf8` | | `String/Text` | `str` | |
| `Date` | `YqlDate` | `Date` | `datetime.date` | |
| `Date32` | `YqlDate32` | | `datetime.date` | Extended date range support |
| `Datetime` | `YqlDateTime` | `DATETIME` | `datetime.datetime` | |
| `Datetime64` | `YqlDateTime64` | | `datetime.datetime` | Extended range |
| `Timestamp` | `YqlTimestamp` | `TIMESTAMP` | `datetime.datetime` | |
| `Timestamp64` | `YqlTimestamp64` | | `datetime.datetime` | Extended range |
| `Json` | `YqlJSON` | `JSON` | `dict/list` | |
| `List<T>` | `ListType` | `ARRAY` | `list` | |
| `Struct<...>` | `StructType` | | `dict` | |
| `Optional<T>` | `nullable=True` | | `None + base type` | |

## Migrations with Alembic

### YDB-Specific Configuration

YDB requires special configuration in `env.py` due to its unique characteristics:

```python
# migrations/env.py
from logging.config import fileConfig
import sqlalchemy as sa
from sqlalchemy import engine_from_config, pool
from alembic import context
from alembic.ddl.impl import DefaultImpl

# Import your models
from myapp.models import Base

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata

# YDB-specific implementation
class YDBImpl(DefaultImpl):
    __dialect__ = "yql"

def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode."""
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()

def run_migrations_online() -> None:
    """Run migrations in 'online' mode."""
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata
        )

        with context.begin_transaction():
            context.run_migrations()

if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
```

## Useful Links

- [Examples on GitHub](https://github.com/ydb-platform/ydb-sqlalchemy/tree/main/examples)
- [PyPI Package](https://pypi.org/project/ydb-sqlalchemy)
