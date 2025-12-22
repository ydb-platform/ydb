# Django

[Django](https://www.djangoproject.com/) is a popular Python web framework with a powerful ORM for working with databases.

YDB supports integration with Django through a special `django-ydb-backend` backend, which provides full Django ORM support for working with YDB.

## Quick Start

### Installation

Install the `django-ydb-backend` package using pip:

```bash
pip install django-ydb-backend
```

### Connection Setup

Add YDB to your Django settings in `settings.py`:

```python
DATABASES = {
    "default": {
        "NAME": "ydb_db",
        "ENGINE": "ydb_backend.backend",
        "HOST": "localhost",
        "PORT": "2136",
        "DATABASE": "/local",
    }
}
```

## Configuration

### DATABASES

Required parameters:

- **NAME** (required): traditional Django database name
- **ENGINE** (required): must be set to `ydb_backend.backend`
- **HOST** (required): hostname or IP address of the YDB server (e.g., "localhost")
- **PORT** (required): gRPC port YDB is running on (default is 2136)
- **DATABASE** (required): full path to your YDB database (e.g., "/local" for local testing or "/my_production_db")

```python
DATABASES = {
    "default": {
        "NAME": "ydb_db",
        "ENGINE": "ydb_backend.backend",
        "HOST": "localhost",
        "PORT": "2136",
        "DATABASE": "/local",
    }
}
```

### Authentication Methods

#### Anonymous Authentication

To use anonymous authentication, you don't need to pass any additional parameters.

#### Static Authentication

To use static credentials, you should provide `username` and `password`:

```python
DATABASES = {
    "default": {
        "ENGINE": "ydb_backend.backend",
        "CREDENTIALS": {
            "username": "...",
            "password": "..."
        }
    }
}
```

#### Token Authentication

To use access token credentials, you should provide `token`:

```python
DATABASES = {
    "default": {
        "ENGINE": "ydb_backend.backend",
        "CREDENTIALS": {
            "token": "..."
        },
    }
}
```

#### Service Account Authentication

To use service account credentials, you should provide `service_account_json`:

```python
DATABASES = {
    "default": {
        "ENGINE": "ydb_backend.backend",
        "CREDENTIALS": {
            "service_account_json": {
                "id": "...",
                "service_account_id": "...",
                "created_at": "...",
                "key_algorithm": "...",
                "public_key": "...",
                "private_key": "..."
            }
        }
    }
}
```

## Django Fields

YDB backend supports Django builtin fields.

**Note:** ForeignKey, ManyToManyField or even OneToOneField could be used with YDB backend. However, it's important to note that these relationships won't enforce database-level constraints, which may lead to potential data consistency issues.

### Supported Django Fields

| Class | YDB Type | Python Type | Comments |
|-------|----------|-------------|----------|
| `SmallAutoField` | `Int16` | `int` | YDB type SmallSerial will generate value automatically. Range -32768 to 32767 |
| `AutoField` | `Int32` | `int` | YDB type Serial will generate value automatically. Range -2147483648 to 2147483647 |
| `BigAutoField` | `Int64` | `int` | YDB type BigSerial will generate value automatically. Range -9223372036854775808 to 9223372036854775807 |
| `CharField` | `UTF-8` | `str` | |
| `TextField` | `UTF-8` | `str` | |
| `BinaryField` | `String` | `bytes` | |
| `SlugField` | `UTF-8` | `str` | |
| `FileField` | `String` | `bytes` | |
| `FilePathField` | `UTF-8` | `str` | |
| `DateField` | `Date` | `datetime.date` | Range of values for all time types except Interval: From 00:00 01.01.1970 to 00:00 01.01.2106. Internal Date representation: Unsigned 16-bit integer |
| `DateTimeField` | `DateTime` | `datetime.datetime` | Internal representation: Unsigned 32-bit integer |
| `DurationField` | `Interval` | `int` | The range of values is from -136 years to +136 years. The internal representation is a 64–bit signed integer. Cannot be used in the primary key |
| `SmallIntegerField` | `Int16` | `int` | Range -32768 to 32767 |
| `IntegerField` | `Int32` | `int` | Range -2147483648 to 2147483647 |
| `BigIntegerField` | `Int64` | `int` | Range -9223372036854775808 to 9223372036854775807 |
| `PositiveSmallIntegerField` | `UInt16` | `int` | Range 0 to 65535 |
| `PositiveIntegerField` | `UInt32` | `int` | Range 0 to 4294967295 |
| `PositiveBigIntegerField` | `UInt64` | `int` | Range 0 to 18446744073709551615 |
| `FloatField` | `Float` | `float` | A real number with variable precision, 4 bytes in size. Can't be used in the primary key |
| `DecimalField` | `Decimal` | `Decimal` | Pythonic values are rounded to fit the scale of the database field. Supports only Decimal(22,9) |
| `UUIDField` | `UUID` | `uuid.UUID` | |
| `IPAddressField` | `UTF-8` | `str` | |
| `GenericIPAddressField` | `UTF-8` | `str` | |
| `BooleanField` | `Bool` | `boolean` | |
| `EmailField` | `UTF-8` | `str` | |

## Useful Links

- [Examples on GitHub](https://github.com/ydb-platform/django-ydb-backend/tree/main/examples)
- [PyPI Package](https://pypi.org/project/django-ydb-backend/)
