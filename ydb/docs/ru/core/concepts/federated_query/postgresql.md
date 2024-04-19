# Работа с базами данных PostgreSQL

В этом разделе описана основная информация про работу с внешней базой данных [PostgreSQL](http://postgresql.org).

Для работы с внешней базой данных PostgreSQL необходимо выполнить следующие шаги:
1. Создать [секрет](../datamodel/secrets.md), содержащий пароль для подключения к базе данных.
    ```sql
    CREATE OBJECT postgresql_datasource_user_password (TYPE SECRET) WITH (value = "password");
    ```
1. Создать [внешний источник данных](../datamodel/external_data_source.md), описывающий определённую базу данных в составе кластера PostgreSQL. При чтении по умолчанию используется [пространство имен](https://www.postgresql.org/docs/current/catalog-pg-namespace.html) `public`, но это значение можно изменить с помощью опционального параметра `SCHEMA`. Сетевое подключение выполняется по стандартному ([Frontend/Backend Protocol](https://www.postgresql.org/docs/current/protocol.html)) по транспорту TCP (`PROTOCOL="NATIVE"`). При работе по защищенным TLS каналам связи используется системные сертификаты, расположенные на серверах {{ydb-full-name}}. 
    ```sql
    CREATE EXTERNAL DATA SOURCE postgresql_datasource WITH (
        SOURCE_TYPE="PostgreSQL",
        LOCATION="postgresql_cluster:5432",
        DATABASE_NAME="db",
        AUTH_METHOD="BASIC",
        LOGIN="user",
        PASSWORD_SECRET_NAME="postgresql_datasource_user_password",
        PROTOCOL="NATIVE",
        USE_TLS="TRUE",
        SCHEMA="public"
    );
    ```
1. Для корректного выполнения запроса необходимо [развернуть коннектор](../../deploy/manual/deploy-ydb-federated-query.md) и обеспечить сетевой доступ со всех хостов {{ydb-full-name}} к целевому кластеру PostgreSQL.
1. [Выполнить запрос](#query) к базе данных.

## Синтаксис запросов { #query }
Для работы с PostgreSQL используется следующая форма SQL-запроса:

```sql
SELECT * FROM postgresql_datasource.table_name
```

где:
- `postgresql_datasource` - идентификатор внешнего источника данных.
- `table_name` - имя таблицы внутри внешнего источника данных.

## Ограничения

При работе с кластерами ClickHouse существует ряд ограничений:

1. Поддерживаются только запросы чтения данных - `SELECT`, остальные виды запросов не поддерживаются.
1. {% include [!](_includes/datetime_limits.md) %}
1. {% include [!](_includes/pushdown_limits.md) %}

## Поддерживаемые типы данных

Ниже приведена таблица соответствия типов PostgreSQL и типов {{ydb-full-name}}.

|Тип данных PostgreSQL|Тип данных {{ydb-full-name}}|Примечания|
|---|----|------|
|`boolean`|`Bool`||
|`smallint`|`Int16`||
|`int2`|`Int16`||
|`integer`|`Int32`||
|`int`|`Int32`||
|`int4`|`Int32`||
|`serial`|`Int32`||
|`serial4`|`Int32`||
|`bigint`|`Int64`||
|`int8`|`Int64`||
|`bigserial`|`Int64`||
|`serial8`|`Int64`||
|`real`|`Float`||
|`float4`|`Float`||
|`double precision`|`Double`||
|`float8`|`Double`||
|`date`|`Date`|Допустимый диапазон дат с 1970-01-01 и до 2105-12-31|
|`timestamp`|`Timestamp`|Допустимый диапазон дат с 1970-01-01 00:00 и до 2105-12-31 23:59|
|`bytea`|`String`||
|`character`|`Utf8`|[Правила сортировки](https://www.postgresql.org/docs/current/collation.html) по умолчанию, строка дополняется пробелами до требуемой длины|
|`character varying`|`Utf8`|[Правила сортировки](https://www.postgresql.org/docs/current/collation.html) по умолчанию|
|`text`|`Utf8`|[Правила сортировки](https://www.postgresql.org/docs/current/collation.html) по умолчанию|

Остальные типы данных не поддерживаются.
