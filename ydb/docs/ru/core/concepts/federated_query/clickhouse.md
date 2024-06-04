# Работа с базами данных ClickHouse

В этом разделе описана основная информация про работу с внешней базой данных [ClickHouse](https://clickhouse.com).

Для работы с внешней базой данных ClickHouse необходимо выполнить следующие шаги:
1. Создать [секрет](../datamodel/secrets.md), содержащий пароль для подключения к базе данных.
    ```sql
    CREATE OBJECT clickhouse_datasource_user_password (TYPE SECRET) WITH (value = "<password>");
    ```
1. Создать [внешний источник данных](../datamodel/external_data_source.md), описывающий целевую базу данных внутри кластера ClickHouse. Для соединения с ClickHouse можно использовать либо [нативный TCP-протокол](https://clickhouse.com/docs/ru/interfaces/tcp) (`PROTOCOL="NATIVE"`), либо [протокол HTTP](https://clickhouse.com/docs/ru/interfaces/http) (`PROTOCOL="HTTP"`). Включить шифрование соединений к внешней базе данных можно с помощью параметра `USE_TLS="TRUE"`.
    ```sql
    CREATE EXTERNAL DATA SOURCE clickhouse_datasource WITH (
        SOURCE_TYPE="ClickHouse", 
        LOCATION="<host>:<port>", 
        DATABASE_NAME="<database>",
        AUTH_METHOD="BASIC",
        LOGIN="<login>",
        PASSWORD_SECRET_NAME="clickhouse_datasource_user_password",
        PROTOCOL="NATIVE",
        USE_TLS="TRUE"
    );
    ```

1. {% include [!](_includes/connector_deployment.md) %}
1. [Выполнить запрос](#query) к базе данных.


## Синтаксис запросов { #query }
Для работы с ClickHouse используется следующая форма SQL-запроса:

```sql
SELECT * FROM clickhouse_datasource.<table_name>
```

где:
- `clickhouse_datasource` - идентификатор внешнего источника данных;
- `<table_name>` - имя таблицы внутри внешнего источника данных.

## Ограничения

При работе с кластерами ClickHouse существует ряд ограничений:

1. {% include [!](_includes/supported_requests.md) %}
1. {% include [!](_includes/datetime_limits.md) %}
1. {% include [!](_includes/predicate_pushdown.md) %}

## Поддерживаемые типы данных

По умолчанию в ClickHouse колонки физически не могут содержать значение `NULL`, однако пользователь имеет возможность создать таблицу с колонками опциональных, или [nullable](https://clickhouse.com/docs/ru/sql-reference/data-types/nullable) типов. Типы колонок, отображаемые {{ ydb-short-name }} при извлечении данных из внешней базы данных ClickHouse, будут зависеть от того, используются ли в таблице ClickHouse примитивные или опциональные типы. При этом в связи с рассмотренными выше ограничениями типов {{ ydb-short-name }}, использующихся для хранения дат и времени, все аналогичные типы ClickHouse отображаются в {{ ydb-short-name }} как [опциональные](../../yql/reference/types/optional.md). 

Ниже приведены таблицы соответствия типов ClickHouse и {{ ydb-short-name }}. Все остальные типы данных, за исключением перечисленных, не поддерживаются.

### Примитивные типы данных

|Тип данных ClickHouse|Тип данных {{ ydb-full-name }}|Примечания|
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
|`Date32`|`Optional<Date>`|Допустимый диапазон дат с 1970-01-01 и до 2105-12-31. При выходе значения за границы диапазона возвращается `NULL`.|
|`DateTime`|`Optional<DateTime>`|Допустимый диапазон значений времени с 1970-01-01 00:00:00 и до 2105-12-31 23:59:59. При выходе значения за границы диапазона возвращается значение `NULL`.|
|`DateTime64`|`Optional<Timestamp>`|Допустимый диапазон значений времени с 1970-01-01 00:00:00 и до 2105-12-31 23:59:59. При выходе значения за границы диапазона возвращается значение `NULL`.|
|`String`|`String`||
|`FixedString`|`String`|Нулевые байты `FixedString` переносятся в `String` без изменений.|

### Опциональные типы данных 

|Тип данных ClickHouse|Тип данных {{ ydb-full-name }}|Примечания|
|---|----|------|
|`Nullable(Bool)`|`Optional<Bool>`||
|`Nullable(Int8)`|`Optional<Int8>`||
|`Nullable(UInt8)`|`Optional<Uint8>`||
|`Nullable(Int16)`|`Optional<Int16>`||
|`Nullable(UInt16)`|`Optional<Uint16>`||
|`Nullable(Int32)`|`Optional<Int32>`||
|`Nullable(UInt32)`|`Optional<Uint32>`||
|`Nullable(Int64)`|`Optional<Int64>`||
|`Nullable(UInt64)`|`Optional<Uint64>`||
|`Nullable(Float32)`|`Optional<Float>`||
|`Nullable(Float64)`|`Optional<Double>`||
|`Nullable(Date)`|`Optional<Date>`||
|`Nullable(Date32)`|`Optional<Date>`|Допустимый диапазон дат с 1970-01-01 и до 2105-12-31. При выходе значения за границы диапазона возвращается `NULL`.|
|`Nullable(DateTime)`|`Optional<DateTime>`|Допустимый диапазон значений времени с 1970-01-01 00:00:00 и до 2105-12-31 23:59:59. При выходе значения за границы диапазона возвращается значение `NULL`.|
|`Nullable(DateTime64)`|`Optional<Timestamp>`|Допустимый диапазон значений времени с 1970-01-01 00:00:00 и до 2105-12-31 23:59:59. При выходе значения за границы диапазона возвращается значение `NULL`.|
|`Nullable(String)`|`Optional<String>`||
|`Nullable(FixedString)`|`Optional<String>`|Нулевые байты `FixedString` переносятся в `String` без изменений.|
