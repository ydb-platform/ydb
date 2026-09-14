# Django

[Django](https://www.djangoproject.com/) — это популярный Python веб-фреймворк с мощной ORM для работы с базами данных.

{{ ydb-short-name }} поддерживает интеграцию с Django через специальный бэкенд `django-ydb-backend`, который обеспечивает полную поддержку Django ORM для работы с {{ ydb-short-name }}.

{% note alert %}

Интеграция с Django пока находится на этапе бета-тестирования. Обратите внимание, что сейчас поля ForeignKey, ManyToManyField и OneToOneField ещё не поддерживаются — их реализация находится в процессе разработки.

{% endnote %}

## Установка

Установите пакет django-ydb-backend с помощью pip:

```bash
pip install django-ydb-backend
```

## Конфигурация

### Настройки подключения

Для подключения к {{ ydb-short-name }} используйте стандартный механизм настройки баз данных Django через переменную `DATABASES` в файле `settings.py`. Подробнее о конфигурации баз данных в Django можно прочитать в официальной [документации](https://docs.djangoproject.com/en/stable/ref/settings/#databases).


Необходимые параметры конфигурации подключения:

- `NAME` — имя базы данных Django.
- `ENGINE` — используется значение `ydb_backend.backend`.
- `HOST` — адрес сервера {{ ydb-short-name }} (например, `localhost`).
- `PORT` — порт gRPC {{ ydb-short-name }} (по умолчанию 2136).
- `DATABASE` — абсолютный путь к базе данных {{ ydb-short-name }} (например, `/local` для локальных тестов или `/my_production_db` для production).

Пример настройки подключения:

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

{% list tabs %}

- Анонимная

  Для использования анонимной аутентификации не нужно передавать дополнительные параметры.

- С помощью логина и пароля

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

- С помощью токена

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

- С помощью сервисного аккаунта

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

{% endlist %}


### Использование шифрования

Если у вас развернут кластер {{ ydb-short-name }} с использованием шифрования, необходимо добавить в настройки базы данных блок `OPTIONS` с указанием протокола и пути к сертификату. Например:

Это обеспечит защищённое соединение с кластером {{ ydb-short-name }}.


```python
DATABASES = {
    "default": {
        ...
        "OPTIONS": {
            "protocol": "grpcs",
            "root_certificates_path": "path/to/cert.crt",
        }
    }
}
```

## Поля Django

{{ ydb-short-name }} backend поддерживает встроенные поля Django.

### Поддерживаемые поля Django

| Класс | {{ ydb-short-name }} тип | Python тип | Комментарии |
|-------|---------|------------|-------------|
| `SmallAutoField` | `Int16` | `int` | {{ ydb-short-name }} тип SmallSerial будет генерировать значения автоматически. Диапазон -32768 до 32767 |
| `AutoField` | `Int32` | `int` | {{ ydb-short-name }} тип Serial будет генерировать значения автоматически. Диапазон -2147483648 до 2147483647 |
| `BigAutoField` | `Int64` | `int` | {{ ydb-short-name }} тип BigSerial будет генерировать значения автоматически. Диапазон -9223372036854775808 до 9223372036854775807 |
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

## Быстрый старт

### Необходимые условия

Перед началом работы убедитесь, что:

1. Развернут кластер {{ ydb-short-name }} [локально](../../quickstart.md) или [используя Ansible](../../devops/deployment-options/ansible/initial-deployment/index.md)
2. Установлены Python-пакеты: `Django`, `django-ydb-backend`, `djangorestframework`
3. Склонирован репозиторий с [примером](https://github.com/ydb-platform/django-ydb-backend/tree/main/examples/bookstore)

### Перейдите в рабочую директорию

```bash
cd examples/bookstore
```

### Конфигурация подключения к {{ ydb-short-name }}

Для интеграции {{ ydb-short-name }} с Django укажите параметры подключения в файле `config/settings.py` проекта:

```python
DATABASES = {
    "default": {
        "NAME": "ydb_db",
        "ENGINE": "ydb_backend.backend",
        "HOST": "localhost",
        "PORT": "2136",
        "DATABASE": "/local",
        "OPTIONS": {
            "protocol": "grpcs",
            "root_certificates_path": "path/to/cert.crt",
        }
    }
}
```

### Запустите миграции

```bash
python manage.py migrate
```

### Запустите сервер

```bash
python manage.py runserver
```

### Проверьте работу приложения

После запуска приложение будет доступно по адресу `http://127.0.0.1:8000` (по умолчанию).

## Полезные ссылки

- [Примеры использования на GitHub](https://github.com/ydb-platform/django-ydb-backend/tree/main/examples)
- [PyPI пакет](https://pypi.org/project/django-ydb-backend/)
