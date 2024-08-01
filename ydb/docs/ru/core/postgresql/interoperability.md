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

## Соответствие типов данных {#supported_types}

Система типов данных {{ydb-name}} и PostgreSQL похожи, но при этом не совпадают, различаясь на уровне представления данных и их обработки. Это означает, что даже простые типы данных из PostgreSQL, например, `int4` (32-битное знаковое целое число) отличаются от типа `int32` {{ydb-name}} (32-битное знаковое целое число), поэтому типы PostgreSQL в {{ydb-name}} имеют собственные названия. Например, тип `int4` PostgreSQL будет называться `pgint4` в {{ydb-name}}.


Таблица соответствия типов данных PostgreSQL и {{ydb-name}} приведена ниже:

| Тип данных PostgreSQL | Тип данных {{ ydb-name }} | Тип данных PostgreSQL в {{ ydb-name }} | Примечания |
| :---: | :---: | :---: | :--- |
| `boolean` | `Bool` | `pgbool` ||
| `smallint` | `Int16` | `pgint16` ||
| `int2` | `Int16` | `pgint16` ||
| `integer` | `Int32` | `pgint4` ||
| `int` | `Int32` | `pgint4` ||
| `int4` | `Int32` | `pgint4` ||
| `serial` | `Int32` | `pgint4` ||
| `serial4` | `Int32` | `pgint4` ||
| `bigint` | `Int64` | `pgint8` ||
| `int8` | `Int64` | `pgint8` ||
| `bigserial` | `Int64` | `pgint8` ||
| `serial8` | `Int64` | `pgint8` ||
| `real` | `Float` | `pgfloat` ||
| `float4` | `Float` | `pgfloat` ||
| `double precision` | `Double` | `pgdouble` ||
| `float8` | `Double` | `pgdouble` ||
| `date` | `Date` | `pgdate` ||
| `timestamp` | `Timestamp` | `pgtimestamp` ||
| `bytea` | `String` | `pgbyte` ||
| `character` | `Utf8` | `pgchar`| [Правила сортировки](https://www.postgresql.org/docs/current/collation.html) по умолчанию, строка дополняется пробелами до требуемой длины. |
| `character varying` | `Utf8` | `pgchar`|[Правила сортировки](https://www.postgresql.org/docs/current/collation.html) по умолчанию. |
| `text` | `Utf8` | `pgchar`|[Правила сортировки](https://www.postgresql.org/docs/current/collation.html) по умолчанию. |
| `json` | `Json` | `pgjson`||
