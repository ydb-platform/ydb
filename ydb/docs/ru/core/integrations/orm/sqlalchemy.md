# SQLAlchemy

[SQLAlchemy](https://www.sqlalchemy.org/) — это популярная Python-библиотека для работы с базами данных, предоставляющая как ORM (Object-Relational Mapping), так и Core API для выполнения SQL-запросов.

{{ ydb-short-name }} поддерживает интеграцию с SQLAlchemy через специальный диалект `ydb-sqlalchemy`, который обеспечивает полную совместимость с SQLAlchemy 2.0 и частичную поддержку SQLAlchemy 1.4.

## Установка

Установите пакет `ydb-sqlalchemy` с помощью pip:

```bash
pip install ydb-sqlalchemy
```

## Быстрый старт

### Строка подключения

Для подключения к {{ ydb-short-name }} через SQLAlchemy используйте следующую строку подключения:

```bash
yql+ydb://<cluster_url>:<port><db_path>
```

**Параметры строки подключения:**

- `<cluster_url>` — адрес сервера {{ ydb-short-name }} (например, `localhost`)
- `<port>` — порт gRPC-сервиса (по умолчанию `2136`)
- `<db_path>` — абсолютный путь к базе данных (например, `/local` или `/Root/db`)

Пример строки подключения:

```bash
yql+ydb://localhost:2136/local
```

### Подключение

{% list tabs %}

- Синхронное подключение

  ```python
  import sqlalchemy as sa

  # Пример настройки подключения к {{ ydb-short-name }} через SQLAlchemy (синхронный режим)
  engine = sa.create_engine("yql+ydb://localhost:2136/local")

  # Выполнение простого запроса
  with engine.connect() as conn:
      result = conn.execute(sa.text("SELECT 1 AS value"))
      print(result.fetchone())
  ```

- Асинхронное подключение

  ```python
  import asyncio
  import sqlalchemy as sa
  from sqlalchemy.ext.asyncio import create_async_engine

  async def main():
      # Создание асинхронного `engine`
      engine = create_async_engine("yql+ydb_async://localhost:2136/local")

      # Выполнение запроса
      async with engine.connect() as conn:
          result = await conn.execute(sa.text("SELECT 1 AS value"))
          print(result.fetchone())

  asyncio.run(main())
  ```

{% endlist %}

## Конфигурация подключения

### Методы аутентификации

{% list tabs %}

- Анонимная

  Для локальной разработки или тестирования:

  ```python
  import sqlalchemy as sa

  engine = sa.create_engine("yql+ydb://localhost:2136/local")
  ```

- С помощью логина и пароля

  Использование имени пользователя и пароля:

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

- С помощью токена

  Использование токена доступа:

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

- С помощью сервисного аккаунта

  Использование JSON-ключа сервисного аккаунта:

  ```python
  import json

  # Загрузка из файла
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

  # Или передача JSON напрямую
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

- С помощью SDK {{ ydb-short-name }}

  Можно использовать любые доступные методы аутентификации из Python SDK для {{ ydb-short-name }}:

  ```python
  import ydb.iam

  # Метаданные сервиса
  engine = sa.create_engine(
      "yql+ydb://localhost:2136/local",
      connect_args={
          "credentials": ydb.iam.MetadataUrlCredentials()
      }
  )

  # OAuth-токен
  engine = sa.create_engine(
      "yql+ydb://localhost:2136/local",
      connect_args={
          "credentials": ydb.iam.OAuthCredentials("your_oauth_token")
      }
  )

  # Статические учетные данные
  engine = sa.create_engine(
      "yql+ydb://localhost:2136/local",
      connect_args={
          "credentials": ydb.iam.StaticCredentials("username", "password")
      }
  )
  ```

{% endlist %}

### Использование шифрования

Если у вас развёрнут кластер {{ ydb-short-name }} с использованием шифрования, необходимо добавить в настройки подключения блок с указанием протокола и пути к сертификату.
Это обеспечит защищённое соединение с кластером {{ ydb-short-name }}.

Например:

```python
engine = sa.create_engine(
    "yql+ydb://ydb.example.com:2135/prod",
    connect_args={
        "credentials": {"token": "your_token"},
        "protocol": "grpcs",
        "root_certificates_path": "/path/to/ca-certificates.crt",
        # "root_certificates": crt_string,  # Альтернативно — строка с сертификатами
    }
)
```

## Поддерживаемые типы данных

{{ ydb-short-name }} SQLAlchemy предоставляет полную поддержку типов данных {{ ydb-short-name }} через пользовательские типы SQLAlchemy. Для получения подробной информации о системе типов {{ ydb-short-name }} см. [документацию по типам данных {{ ydb-short-name }}](https://ydb.tech/docs/ru/concepts/datatypes).

### Сводная таблица типов

| {{ ydb-short-name }} тип | {{ ydb-short-name }} SQLAlchemy тип | Стандартный SQLAlchemy тип | Python тип | Примечания |
|---------|-------------------|---------------------------|------------|------------|
| `Bool` | `Boolean` | `Boolean` | `bool` | |
| `Int8` | `Int8` | | `int` | -2^7 до 2^7-1 |
| `Int16` | `Int16` | | `int` | -2^15 до 2^15-1 |
| `Int32` | `Int32` | | `int` | -2^31 до 2^31-1 |
| `Int64` | `Int64` | `Integer` | `int` | -2^63 до 2^63-1 |
| `Uint8` | `UInt8` | | `int` | 0 до 2^8-1 |
| `Uint16` | `UInt16` | | `int` | 0 до 2^16-1 |
| `Uint32` | `UInt32` | | `int` | 0 до 2^32-1 |
| `Uint64` | `UInt64` | | `int` | 0 до 2^64-1 |
| `Float` | `Float` | `Float` | `float` | |
| `Double` | `Double` | | `float` | Доступно в SQLAlchemy 2.0+ |
| `Decimal(p,s)` | `Decimal` | `DECIMAL` | `decimal.Decimal` | |
| `String` | | `BINARY` | `bytes` | |
| `Utf8` | | `String/Text` | `str` | |
| `Date` | `YqlDate` | `Date` | `datetime.date` | |
| `Date32` | `YqlDate32` | | `datetime.date` | Расширенный диапазон дат |
| `Datetime` | `YqlDateTime` | `DATETIME` | `datetime.datetime` | |
| `Datetime64` | `YqlDateTime64` | | `datetime.datetime` | Расширенный диапазон |
| `Timestamp` | `YqlTimestamp` | `TIMESTAMP` | `datetime.datetime` | |
| `Timestamp64` | `YqlTimestamp64` | | `datetime.datetime` | Расширенный диапазон |
| `Json` | `YqlJSON` | `JSON` | `dict/list` | |
| `List<T>` | `ListType` | `ARRAY` | `list` | |
| `Struct<...>` | `StructType` | | `dict` | |
| `Optional<T>` | `nullable=True` | | `None + базовый тип` | |

## Миграции с Alembic

### Конфигурация, специфичная для {{ ydb-short-name }}

{{ ydb-short-name }} требует специальной конфигурации в `env.py` из-за своих уникальных характеристик:

```python
# migrations/env.py
from logging.config import fileConfig
import sqlalchemy as sa
from sqlalchemy import engine_from_config, pool
from alembic import context
from alembic.ddl.impl import DefaultImpl

# Импортируйте ваши модели
from myapp.models import Base

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata

# {{ ydb-short-name }}-специфичная реализация
class YDBImpl(DefaultImpl):
    __dialect__ = "yql"

def run_migrations_offline() -> None:
    """Запуск миграций в 'offline' режиме."""
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
    """Запуск миграций в 'online' режиме."""
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

## Полезные ссылки

- [Примеры использования на GitHub](https://github.com/ydb-platform/ydb-sqlalchemy/tree/main/examples)
- [PyPI пакет](https://pypi.org/project/ydb-sqlalchemy)
