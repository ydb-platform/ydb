# Работа с базами данных PostgreSQL

В этом разделе описана основная информация про работу с внешней базой данных [PostgreSQL](http://postgresql.org).

Для работы с внешней базой данных PostgreSQL необходимо выполнить следующие шаги:
1. Создать [секрет](../datamodel/secrets.md), содержащий пароль для подключения к базе данных.
    ```sql
    CREATE OBJECT postgresql_datasource_user_password (TYPE SECRET) WITH (value = "<password>");
    ```
1. Создать [внешний источник данных](../datamodel/external_data_source.md), описывающий определённую базу данных в составе кластера PostgreSQL. При чтении по умолчанию используется [пространство имен](https://www.postgresql.org/docs/current/catalog-pg-namespace.html) `public`, но это значение можно изменить с помощью опционального параметра `SCHEMA`. Сетевое подключение выполняется по стандартному ([Frontend/Backend Protocol](https://www.postgresql.org/docs/current/protocol.html)) по транспорту TCP (`PROTOCOL="NATIVE"`). Включить шифрование соединений к внешней базе данных можно с помощью параметра `USE_TLS="TRUE"`.
    ```sql
    CREATE EXTERNAL DATA SOURCE postgresql_datasource WITH (
        SOURCE_TYPE="PostgreSQL",
        LOCATION="<host>:<port>",
        DATABASE_NAME="<database>",
        AUTH_METHOD="BASIC",
        LOGIN="user",
        PASSWORD_SECRET_NAME="postgresql_datasource_user_password",
        PROTOCOL="NATIVE",
        USE_TLS="TRUE",
        SCHEMA="<schema>"
    );
    ```
1. {% include [!](_includes/connector_deployment.md) %}
1. [Выполнить запрос](#query) к базе данных.

## Синтаксис запросов { #query }
Для работы с PostgreSQL используется следующая форма SQL-запроса:

```sql
SELECT * FROM postgresql_datasource.<table_name>
```

где:
- `postgresql_datasource` - идентификатор внешнего источника данных;
- `<table_name>` - имя таблицы внутри внешнего источника данных.

## Ограничения

При работе с кластерами PostgreSQL существует ряд ограничений:

1. {% include [!](_includes/supported_requests.md) %}
1. {% include [!](_includes/datetime_limits.md) %}
1. {% include [!](_includes/predicate_pushdown.md) %}

## Поддерживаемые типы данных

В базе данных PostgreSQL признак опциональности значений колонки (разрешено или запрещено колонке содержать значения `NULL`) не является частью системы типов данных. Ограничение (constraint) `NOT NULL` для каждой колонки реализуется в виде атрибута `attnotnull` в системном каталоге [pg_attribute](https://www.postgresql.org/docs/current/catalog-pg-attribute.html), то есть на уровне метаданных таблицы. Следовательно, все базовые типы PostgreSQL по умолчанию могут содержать значения `NULL`, и в системе типов {{ ydb-full-name }} они должны отображаться в [опциональные](../../yql/reference/types/optional.md) типы.

Ниже приведена таблица соответствия типов PostgreSQL и {{ ydb-short-name }}. Все остальные типы данных, за исключением перечисленных, не поддерживаются.

|Тип данных PostgreSQL|Тип данных {{ ydb-full-name }}|Примечания|
|---|----|------|
|`boolean`|`Optional<Bool>`||
|`smallint`|`Optional<Int16>`||
|`int2`|`Optional<Int16>`||
|`integer`|`Optional<Int32>`||
|`int`|`Optional<Int32>`||
|`int4`|`Optional<Int32>`||
|`serial`|`Optional<Int32>`||
|`serial4`|`Optional<Int32>`||
|`bigint`|`Optional<Int64>`||
|`int8`|`Optional<Int64>`||
|`bigserial`|`Optional<Int64>`||
|`serial8`|`Optional<Int64>`||
|`real`|`Optional<Float>`||
|`float4`|`Optional<Float>`||
|`double precision`|`Optional<Double>`||
|`float8`|`Optional<Double>`||
|`date`|`Optional<Date>`|Допустимый диапазон дат с 1970-01-01 и до 2105-12-31. При выходе значения за границы диапазона возвращается `NULL`.|
|`timestamp`|`Optional<Timestamp>`|Допустимый диапазон времени с 1970-01-01 00:00:00 и до 2105-12-31 23:59:59. При выходе значения за границы диапазона возвращается значение `NULL`.|
|`bytea`|`Optional<String>`||
|`character`|`Optional<Utf8>`|[Правила сортировки](https://www.postgresql.org/docs/current/collation.html) по умолчанию, строка дополняется пробелами до требуемой длины.|
|`character varying`|`Optional<Utf8>`|[Правила сортировки](https://www.postgresql.org/docs/current/collation.html) по умолчанию.|
|`text`|`Optional<Utf8>`|[Правила сортировки](https://www.postgresql.org/docs/current/collation.html) по умолчанию.|
|`json`|`Optional<Json>`||
