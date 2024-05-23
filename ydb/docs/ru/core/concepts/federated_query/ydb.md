# Работа с базами данных YDB

{{ ydb-full-name }} может выступать в качестве внешнего источника данных для другой базы {{ ydb-full-name }}. В данном разделе рассматривается организация совместной работы двух независимых баз данных {{ ydb-short-name }} в режиме обработки федеративных запросов.

Для подключения к внешней базе {{ ydb-short-name }} со стороны другой базы {{ ydb-short-name }}, выступающей в роли движка обработки федеративных запросов, необходимо выполнить на последней следующие шаги:
1. Создать [секрет](../datamodel/secrets.md), содержащий пароль для подключения к внешней базе данных.
    ```sql
    CREATE OBJECT ydb_datasource_user_password (TYPE SECRET) WITH (value = "<password>");
    ```
1. Создать [внешний источник данных](../datamodel/external_data_source.md), описывающий определённую базу данных в составе внешнего кластера {{ ydb-short-name }}. Параметр `DATABASE_NAME` содержит имя базы данных, к которой выполняется соединение (например, `local`). Для аутентификации во внешнюю базу используются параметры `LOGIN` и `PASSWORD_SECRET_NAME`. Включить шифрование соединений к внешней базе данных можно с помощью параметра `USE_TLS="TRUE"`, при этом будут применяться системные корневые сертификаты, размещённые на серверах с динамическими узлами исходного кластера {{ ydb-short-name }}.
    ```sql
    CREATE EXTERNAL DATA SOURCE ydb_datasource WITH (
        SOURCE_TYPE="Ydb",
        LOCATION="<host>:<port>",
        DATABASE_NAME="<database>",
        AUTH_METHOD="BASIC",
        LOGIN="user",
        PASSWORD_SECRET_NAME="ydb_datasource_user_password",
        USE_TLS="TRUE"
    );
    ```
1. Для корректного выполнения запроса необходимо [развернуть коннектор](../../deploy/manual/deploy-ydb-federated-query.md) и обеспечить сетевой доступ с динамических узлов {{ ydb-full-name }} к кластеру {{ ydb-short-name }}, выступающему в качестве внешнего источника данных.
1. [Выполнить запрос](#query) к внешнему источнику данных.

## Синтаксис запросов { #query }
Для работы с внешним {{ ydb-short-name }} используется следующая форма SQL-запроса:

```sql
SELECT * FROM ydb_datasource.<table_name>
```

где:
- `ydb_datasource` - идентификатор внешнего источника данных;
- `<table_name>` - имя таблицы внутри внешнего источника данных.

## Ограничения

При работе с внешними источниками данных {{ ydb-short-name }} существует ряд ограничений:

1. Поддерживаются только запросы чтения данных - `SELECT`, остальные виды запросов не поддерживаются.
1. {% include [!](_includes/predicate_pushdown.md) %}

## Поддерживаемые типы данных

При выполнении федеративных запросов, извлекающих данные из таблиц внешней базы {{ ydb-short-name }}, пользователям доступен ограниченный набор типов данных. Все остальные типы, за исключением перечисленных ниже, не поддерживаются.

|Примитивный тип данных|Опциональный тип данных|
|----|----|
|`Bool`|`Optional<Bool>`|
|`Int8`|`Optional<Int8>`|
|`Int16`|`Optional<Int16>`|
|`Int32`|`Optional<Int32>`|
|`Int64`|`Optional<Int64>`|
|`Uint8`|`Optional<Int8>`|
|`Uint16`|`Optional<Int16>`|
|`Uint32`|`Optional<Int32>`|
|`Uint64`|`Optional<Int64>`|
|`Float`|`Optional<Float>`|
|`Double`|`Optional<Double>`|
|`String`|`Optional<String>`|
|`Utf8`|`Optional<Utf8>`|
|`Date`|`Optional<Date>`|
|`Datetime`|`Optional<Datetime>`|
|`Timestamp`|`Optional<Timestamp>`|
