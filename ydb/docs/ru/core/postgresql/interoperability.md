# Интероперабельность PostgreSQL и {{ydb-name}}

{% include [./_includes/alert.md](./_includes/alert_preview.md) %}

Поддержка исполнения запросов в {{ydb-name}} на синтаксисе PostgreSQL реализована с помощью слоя совместимости:
1. Программа отправляет запросы в YDB, где их обрабатывает компонент под названием `pgwire`. Pgwire реализует [сетевой протокол](https://postgrespro.ru/docs/postgresql/14/protocol) PostgreSQL и передает команды в query processor.
2. Query processor транслирует PostgreSQL запросы в YQL AST.
3. После обработки запросов результаты собираются и отправляются обратно в программу, отправившую запрос, по сетевому протоколу PostgreSQL. При обработке запроса он может распараллеливаться и исполняться на произвольном количестве узлов {{ydb-name}}.

Графически работу PostgreSQL совместимости можно представить так:
![Схема работы PostgreSQL совместимости](./_includes/ydb_pg_scheme.png)

Такая архитектура интеграции с PostgreSQL позволяет выполнять запросы на PostgreSQL над {{ydb-name}} типами данных и наоборот, выполнять YQL-запросы над типами данных PostgreSQL, обеспечивая интероперабельность работы с данными.

Проиллюстрируем это с помощью следующего сценария:
1. Создадим таблицу {{ydb-name}} с помощью YQL-синтаксиса
    ```sql
    CREATE TABLE `test_table`(col INT, PRIMARY KEY(col));
    ```
1. Добавим туда тестовые данные
    ```sql
    INSERT INTO test_table(col) VALUES(1)
    ```
1. Прочитаем эти данные с помощью PostgreSQL-синтаксиса
    ```
    psql -h <ydb_address> -d <database_name> -U <user_name> -c "SELECT * FROM test_table"
    col
    ---
    1
    (1 row)
    ```

    Где:
    - `ydb_address` - адрес кластера ydb, к которому выполняется подключение.
    - `database_name` - название базы данных в кластере. Может быть сложным именем, например, `mycluster/tenant1/database/`.
    - `user_name` - логин пользователя.


## Соответствие типов данных {#supported_types}

Система типов данных {{ydb-name}} и PostgreSQL похожи, но при этом не совпадают, различаясь на уровне представления данных и их обработки. Это означает, что даже простые типы данных из PostgreSQL, например, `int4` (32-битное знаковое целое число) отличаются от типа `int32` {{ydb-name}} (32-битное знаковое целое число), поэтому типы PostgreSQL в {{ydb-name}} имеют собственные названия, начинающиеся с `pg`. Например, тип `int4` PostgreSQL будет называться `pgint4` в {{ydb-name}}.


Таблица соответствия типов данных {{ydb-name}}, при их использовании в PostgreSQL запросах:

| {{ydb-name}} | PostgreSQL |
|---|---|
| `Bool` | `bool` |
| `Int8` | `int2` |
| `Uint8` | `int2` |
| `Int16` | `int2` |
| `Uint16` | `int4` |
| `Int32` | `int4` |
| `Uint32` | `int8` |
| `Int64` | `int8` |
| `Uint64` | `numeric` |
| `Float` | `float4` |
| `Double` | `float8` |
| `String` | `bytea` |
| `Utf8` | `text` |
| `Yson` | `bytea` |
| `Json` | `json` |
| `Uuid` | `uuid` |
| `JsonDocument` | `jsonb` |
| `Date` | `date` |
| `Datetime` | `timestamp` |
| `Timestamp` | `timestamp` |
| `Interval` | `interval` |
| `TzDate` | `text` |
| `TzDatetime` | `text` |
| `TzTimestamp` | `text` |
| `Date32` | `date` |
| `Datetime64` | `timestamp` |
| `Timestamp64` | `timestamp` |
| `Interval64`| `interval` |
| `TzDate32` | `text` |  |
| `TzDatetime64` | `text` |  |
| `TzTimestamp64` | `text` |  |
| `Decimal` | `numeric` |
| `DyNumber` | `numeric` |


Таблица соответствия типов данных PostgreSQL, при их использовании в {{ydb-name}} запросах:

| PostgreSQL | {{ydb-name}} |
|---|---|
| `bool` | `bool` |
| `int2` | `Int16` |
| `int4` | `Int32` |
| `int8` | `Int64` |
| `float4` | `Float` |
| `float8` | `Double` |
| `bytea` | `String` |
| `varchar` | `Utf8` |
| `text` | `Utf8` |
| `cstring` | `Utf8` |
| `uuid` | `Uuid` |
| `date` | `Date32` |
| `timestamp` | `Timestamp64` |

Преобразования типов, не указанные в вышепреведенных таблицах, не поддерживаются.
