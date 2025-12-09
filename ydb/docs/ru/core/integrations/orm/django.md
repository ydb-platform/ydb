# Django

[Django](https://www.djangoproject.com/) — это популярный Python веб-фреймворк с мощной ORM для работы с базами данных.

YDB поддерживает интеграцию с Django через специальный бэкенд `django-ydb-backend`, который обеспечивает полную поддержку Django ORM для работы с YDB.

## Быстрый старт

### Установка

Установите пакет `django-ydb-backend` с помощью pip:

```bash
pip install django-ydb-backend
```

### Настройка подключения

Добавьте YDB в настройки Django в файле `settings.py`:

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

## Конфигурация

### DATABASES

Обязательные параметры:

- **NAME** (обязательно): традиционное имя базы данных Django
- **ENGINE** (обязательно): должно быть установлено в `ydb_backend.backend`
- **HOST** (обязательно): имя хоста или IP-адрес YDB сервера (например, "localhost")
- **PORT** (обязательно): gRPC порт YDB (по умолчанию 2136)
- **DATABASE** (обязательно): полный путь к базе данных YDB (например, "/local" для локального тестирования или "/my_production_db")

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

### Методы аутентификации

#### Анонимная аутентификация

Для использования анонимной аутентификации не нужно передавать дополнительные параметры.

#### Статическая аутентификация

Для использования статических учетных данных нужно указать `username` и `password`:

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

#### Аутентификация по токену

Для использования токена доступа нужно указать `token`:

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

#### Аутентификация с помощью сервисного аккаунта

Для использования сервисного аккаунта нужно указать `service_account_json`:

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

## Поля Django

YDB backend поддерживает встроенные поля Django.

**Примечание:** ForeignKey, ManyToManyField и OneToOneField могут использоваться с YDB backend. Однако важно отметить, что эти связи не будут обеспечивать ограничения на уровне базы данных, что может привести к потенциальным проблемам согласованности данных.

### Поддерживаемые поля Django

| Класс | YDB тип | Python тип | Комментарии |
|-------|---------|------------|-------------|
| `SmallAutoField` | `Int16` | `int` | YDB тип SmallSerial будет генерировать значения автоматически. Диапазон -32768 до 32767 |
| `AutoField` | `Int32` | `int` | YDB тип Serial будет генерировать значения автоматически. Диапазон -2147483648 до 2147483647 |
| `BigAutoField` | `Int64` | `int` | YDB тип BigSerial будет генерировать значения автоматически. Диапазон -9223372036854775808 до 9223372036854775807 |
| `CharField` | `UTF-8` | `str` | |
| `TextField` | `UTF-8` | `str` | |
| `BinaryField` | `String` | `bytes` | |
| `SlugField` | `UTF-8` | `str` | |
| `FileField` | `String` | `bytes` | |
| `FilePathField` | `UTF-8` | `str` | |
| `DateField` | `Date` | `datetime.date` | Диапазон значений: с 00:00 01.01.1970 до 00:00 01.01.2106. Внутреннее представление: беззнаковое 16-битное целое |
| `DateTimeField` | `DateTime` | `datetime.datetime` | Внутреннее представление: беззнаковое 32-битное целое |
| `DurationField` | `Interval` | `int` | Диапазон значений от -136 лет до +136 лет. Внутреннее представление: знаковое 64-битное целое. Нельзя использовать в первичном ключе |
| `SmallIntegerField` | `Int16` | `int` | Диапазон -32768 до 32767 |
| `IntegerField` | `Int32` | `int` | Диапазон -2147483648 до 2147483647 |
| `BigIntegerField` | `Int64` | `int` | Диапазон -9223372036854775808 до 9223372036854775807 |
| `PositiveSmallIntegerField` | `UInt16` | `int` | Диапазон 0 до 65535 |
| `PositiveIntegerField` | `UInt32` | `int` | Диапазон 0 до 4294967295 |
| `PositiveBigIntegerField` | `UInt64` | `int` | Диапазон 0 до 18446744073709551615 |
| `FloatField` | `Float` | `float` | Вещественное число с переменной точностью, размер 4 байта. Нельзя использовать в первичном ключе |
| `DecimalField` | `Decimal` | `Decimal` | Python значения округляются до масштаба поля базы данных. Поддерживает только Decimal(22,9) |
| `UUIDField` | `UUID` | `uuid.UUID` | |
| `IPAddressField` | `UTF-8` | `str` | |
| `GenericIPAddressField` | `UTF-8` | `str` | |
| `BooleanField` | `Bool` | `boolean` | |
| `EmailField` | `UTF-8` | `str` | |


## Полезные ссылки

- [Примеры использования на GitHub](https://github.com/ydb-platform/django-ydb-backend/tree/main/examples)
- [PyPI пакет](https://pypi.org/project/django-ydb-backend/)
