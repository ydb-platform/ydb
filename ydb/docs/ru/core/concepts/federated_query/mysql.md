# Работа с базами данных MySQL

В этом разделе описана основная информация про работу с внешней базой данных [MySQL](https://www.mysql.com/).

Для работы с внешней базой данных MySQL необходимо выполнить следующие шаги:
1. Создать [секрет](../datamodel/secrets.md), содержащий пароль для подключения к базе данных.
    ```sql
    CREATE OBJECT mysql_datasource_user_password (TYPE SECRET) WITH (value = "<password>");
    ```
1. Создать [внешний источник данных](../datamodel/external_data_source.md), описывающий определённую базу данных MySQL. Параметр `LOCATION` содержит сетевой адрес экземпляра MySQL, к которому осуществляется подключение. В `DATABASE_NAME` указывается имя базы данных (например, `mysql`). Для аутентификации во внешнюю базу используются значения параметров `LOGIN` и `PASSWORD_SECRET_NAME`. Включить шифрование соединений к внешней базе данных можно с помощью параметра `USE_TLS="TRUE"`.
    ```sql
    CREATE EXTERNAL DATA SOURCE mysql_datasource WITH (
        SOURCE_TYPE="MySQL",
        LOCATION="<host>:<port>",
        DATABASE_NAME="<database>",
        AUTH_METHOD="BASIC",
        LOGIN="user",
        PASSWORD_SECRET_NAME="mysql_datasource_user_password",
        USE_TLS="TRUE"
    );
    ```
1. {% include [!](_includes/connector_deployment.md) %}
1. [Выполнить запрос](#query) к базе данных.

## Синтаксис запросов { #query }
Для работы с MySQL используется следующая форма SQL-запроса:

```sql
SELECT * FROM mysql_datasource.<table_name>
```

где:
- `mysql_datasource` - идентификатор внешнего источника данных;
- `<table_name>` - имя таблицы внутри внешнего источника данных.

## Ограничения

При работе с кластерами MySQL существует ряд ограничений:

1. {% include [!](_includes/supported_requests.md) %}
1. {% include [!](_includes/datetime_limits.md) %}
1. {% include [!](_includes/predicate_pushdown.md) %}

## Поддерживаемые типы данных

В базе данных MySQL признак опциональности значений колонки (разрешено или запрещено колонке содержать значения `NULL`) не является частью системы типов данных. Ограничение (constraint) `NOT NULL` для любой колонки любой таблицы хранится в виде значения столбца `IS_NULLABLE` системной таблицы [INFORMATION_SCHEMA.COLUMNS](https://dev.mysql.com/doc/refman/8.4/en/information-schema-columns-table.html), то есть на уровне метаданных таблицы. Следовательно, все базовые типы MySQL по умолчанию могут содержать значения `NULL`, и в системе типов {{ ydb-full-name }} они должны отображаться в [опциональные](https://ydb.tech/docs/ru/yql/reference/types/optional) типы.

Ниже приведена таблица соответствия типов MySQL и {{ ydb-short-name }}. Все остальные типы данных, за исключением перечисленных, не поддерживаются.

|Тип данных MySQL|Тип данных {{ ydb-full-name }}|Примечания|
|---|----|------|
|`bool`|`Optional<Bool>`||
|`tinyint`|`Optional<Int8>`||
|`tinyint unsigned`|`Optional<Uint8>`||
|`smallint`|`Optional<Int16>`||
|`smallint unsigned`|`Optional<Uint16>`||
|`mediumint`|`Optional<Int32>`||
|`mediumint unsigned`|`Optional<Uint32>`||
|`int`|`Optional<Int32>`||
|`int unsigned`|`Optional<Uint32>`||
|`bigint`|`Optional<Int64>`||
|`bigint unsigned`|`Optional<Uint64>`||
|`float`|`Optional<Float>`||
|`real`|`Optional<Float>`||
|`double`|`Optional<Double>`||
|`date`|`Optional<Date>`|Допустимый диапазон дат с 1970-01-01 и до 2105-12-31. При выходе значения за границы диапазона возвращается `NULL`.|
|`datetime`|`Optional<Timestamp>`|Допустимый диапазон времени с 1970-01-01 00:00:00 и до 2105-12-31 23:59:59. При выходе значения за границы диапазона возвращается значение `NULL`.|
|`timestamp`|`Optional<Timestamp>`|Допустимый диапазон времени с 1970-01-01 00:00:00 и до 2105-12-31 23:59:59. При выходе значения за границы диапазона возвращается значение `NULL`.|
|`tinyblob`|`Optional<String>`||
|`blob`|`Optional<String>`||
|`mediumblob`|`Optional<String>`||
|`longblob`|`Optional<String>`||
|`tinytext`|`Optional<String>`||
|`text`|`Optional<String>`||
|`mediumtext`|`Optional<String>`||
|`longtext`|`Optional<String>`||
|`char`|`Optional<Utf8>`||
|`varchar`|`Optional<Utf8>`||
|`binary`|`Optional<String>`||
|`varbinary`|`Optional<String>`||
|`json`|`Optional<Json>`||
