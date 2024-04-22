# Работа с базами данных ClickHouse

В этом разделе описана основная информация про работу с внешней базой данных [ClickHouse](https://clickhouse.com).

Для работы с внешней базой данных ClickHouse необходимо выполнить следующие шаги:
1. Создать [секрет](../datamodel/secrets.md), содержащий пароль для подключения к базе данных.
    ```sql
    CREATE OBJECT clickhouse_datasource_user_password (TYPE SECRET) WITH (value = "password");
    ```
1. Создать [внешний источник данных](../datamodel/external_data_source.md), описывающий целевую базу данных внутри кластера ClickHouse.  Для соединения с ClickHouse можно использовать либо [нативный TCP-протокол](https://clickhouse.com/docs/ru/interfaces/tcp) (`PROTOCOL="NATIVE"`), либо [протокол HTTP](https://clickhouse.com/docs/ru/interfaces/http) (`PROTOCOL="HTTP"`).
    ```sql
    CREATE EXTERNAL DATA SOURCE clickhouse_datasource WITH (
        SOURCE_TYPE="ClickHouse", 
        LOCATION="clickhouse_cluster:9440", 
        DATABASE_NAME="db",
        AUTH_METHOD="BASIC",
        LOGIN="user",
        PASSWORD_SECRET_NAME="clickhouse_datasource_user_password",
        PROTOCOL="NATIVE",
        USE_TLS="TRUE"
    );
    ```
1. Для корректного выполнения запроса необходимо [развернуть коннектор](../../deploy/manual/deploy-ydb-federated-query.md) и обеспечить сетевой доступ со всех хостов {{ydb-full-name}} к целевому кластеру ClickHouse.
1. [Выполнить запрос](#query) к базе данных.


## Синтаксис запросов { #query }
Для работы с ClickHouse используется следующая форма SQL-запроса:

```sql
SELECT * FROM clickhouse_datasource.table_name
```

где:
- `clickhouse_datasource` - идентификатор внешнего источника данных.
- `table_name` - имя таблицы внутри внешнего источника данных.

## Ограничения

При работе с кластерами ClickHouse существует ряд ограничений:

1. Поддерживаются только запросы чтения данных - `SELECT`, остальные виды запросов не поддерживаются.
1. {% include [!](_includes/datetime_limits.md) %}
1. {% include [!](_includes/predicate_pushdown.md) %}

## Поддерживаемые типы данных

Ниже приведена таблица соответствия типов ClickHouse и типов {{ydb-full-name}}.

|Тип данных ClickHouse|Тип данных {{ydb-full-name}}|Примечания|
|---|----|------|
|`Bool`|`Bool`||
|`Int8`|`Int8`||
|`UInt8`|`Uint8`||
|`Int16`|`Int16`||
|`UInt16`|`Uint16`||
|`Int32`|`Int32`||
|`UInt32`|`Uint32`||
|`Int64`|`Int64`||
|`UInt64`|`Uint64`||
|`Float32`|`Float`||
|`Float64`|`Double`||
|`Date`|`Date`||
|`Date32`|`Date`|Допустимый диапазон дат с 1970-01-01 00:00 и до 2105-12-31 23:59|
|`DateTime`|`DateTime`|Допустимый диапазон дат с 1970-01-01 00:00 и до 2105-12-31 23:59|
|`DateTime64`|`Timestamp`|Допустимый диапазон дат с 1970-01-01 00:00 и до 2105-12-31 23:59|
|`String`|`String`||
|`FixedString`|`String`|Нулевые байты `FixedString` переносятся в `String` без изменений|

Остальные типы данных не поддерживаются.
