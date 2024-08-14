# Работа с базами данных Microsoft SQL Server

В этом разделе описана основная информация про работу с внешней базой данных [Microsoft SQL Server](https://learn.microsoft.com/ru-ru/sql/?view=sql-server-ver16).

Для работы с внешней базой данных Microsoft SQL Server необходимо выполнить следующие шаги:
1. Создать [секрет](../datamodel/secrets.md), содержащий пароль для подключения к базе данных.
    ```sql
    CREATE OBJECT ms_sql_server_datasource_user_password (TYPE SECRET) WITH (value = "<password>");
    ```
1. Создать [внешний источник данных](../datamodel/external_data_source.md), описывающий определённую базу данных Microsoft SQL Server. Параметр `LOCATION` содержит сетевой адрес экземпляра Microsoft SQL Server, к которому осуществляется подключение. В `DATABASE_NAME` указывается имя базы данных (например, `master`). Для аутентификации во внешнюю базу используются значения параметров `LOGIN` и `PASSWORD_SECRET_NAME`. Включить шифрование соединений к внешней базе данных можно с помощью параметра `USE_TLS="TRUE"`.
    ```sql
    CREATE EXTERNAL DATA SOURCE mysql_datasource WITH (
        SOURCE_TYPE="MsSQLServer",
        LOCATION="<host>:<port>",
        DATABASE_NAME="<database>",
        AUTH_METHOD="BASIC",
        LOGIN="user",
        PASSWORD_SECRET_NAME="ms_sql_server_datasource_user_password",
        USE_TLS="TRUE"
    );
    ```
1. {% include [!](_includes/connector_deployment.md) %}
1. [Выполнить запрос](#query) к базе данных.

## Синтаксис запросов { #query }
Для работы с Microsoft SQL Server используется следующая форма SQL-запроса:

```sql
SELECT * FROM ms_sql_server_datasource.<table_name>
```

где:
- `ms_sql_server_datasource` - идентификатор внешнего источника данных;
- `<table_name>` - имя таблицы внутри внешнего источника данных.

## Ограничения

При работе с кластерами Microsoft SQL Server существует ряд ограничений:

1. {% include [!](_includes/supported_requests.md) %}
1. {% include [!](_includes/datetime_limits.md) %}
1. {% include [!](_includes/predicate_pushdown.md) %}

## Поддерживаемые типы данных

В базе данных Microsoft SQL Server признак опциональности значений колонки (разрешено или запрещено колонке содержать значения `NULL`) не является частью системы типов данных. Ограничение (constraint) `NOT NULL` для любой колонки любой таблицы хранится в виде значения столбца `IS_NULLABLE` системной таблицы [INFORMATION_SCHEMA.COLUMNS](https://learn.microsoft.com/ru-ru/sql/relational-databases/system-information-schema-views/columns-transact-sql?view=sql-server-ver16), то есть на уровне метаданных таблицы. Следовательно, все базовые типы Microsoft SQL Server по умолчанию могут содержать значения `NULL`, и в системе типов {{ ydb-full-name }} они должны отображаться в [опциональные](https://ydb.tech/docs/ru/yql/reference/types/optional) типы.

Ниже приведена таблица соответствия типов Microsoft SQL Server и {{ ydb-short-name }}. Все остальные типы данных, за исключением перечисленных, не поддерживаются.

|Тип данных Microsoft SQL Server|Тип данных {{ ydb-full-name }}|Примечания|
|---|----|------|
|`bit`|`Optional<Bool>`||
|`tinyint`|`Optional<Int8>`||
|`smallint`|`Optional<Int16>`||
|`int`|`Optional<Int32>`||
|`bigint`|`Optional<Int64>`||
|`real`|`Optional<Float>`||
|`float`|`Optional<Double>`||
|`date`|`Optional<Date>`|Допустимый диапазон дат с 1970-01-01 и до 2105-12-31. При выходе значения за границы диапазона возвращается `NULL`.|
|`smalldatetime`|`Optional<Datetime>`|Допустимый диапазон времени с 1970-01-01 00:00:00 и до 2105-12-31 23:59:59. При выходе значения за границы диапазона возвращается значение `NULL`.|
|`datetime`|`Optional<Timestamp>`|Допустимый диапазон времени с 1970-01-01 00:00:00 и до 2105-12-31 23:59:59. При выходе значения за границы диапазона возвращается значение `NULL`.|
|`datetime2`|`Optional<Timestamp>`|Допустимый диапазон времени с 1970-01-01 00:00:00 и до 2105-12-31 23:59:59. При выходе значения за границы диапазона возвращается значение `NULL`.|
|`binary`|`Optional<String>`||
|`varbinary`|`Optional<String>`||
|`image`|`Optional<String>`||
|`char`|`Optional<Utf8>`||
|`varchar`|`Optional<Utf8>`||
|`text`|`Optional<Utf8>`||
|`nchar`|`Optional<Utf8>`||
|`nvarchar`|`Optional<Utf8>`||
|`ntext`|`Optional<Utf8>`||
