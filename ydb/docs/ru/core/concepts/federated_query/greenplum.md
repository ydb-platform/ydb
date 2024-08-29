# Работа с базами данных Greenplum

В этом разделе описана основная информация про работу с внешней базой данных [Greenplum](https://greenplum.org). Поскольку Greenplum основан на [PostgreSQL](postgresql.md), интеграции с ними работают похожим образом, а некоторые ссылки ниже могут вести на документацию PostgreSQL.

Для работы с внешней базой данных Greenplum необходимо выполнить следующие шаги:
1. Создать [секрет](../datamodel/secrets.md), содержащий пароль для подключения к базе данных.
    ```sql
    CREATE OBJECT greenplum_datasource_user_password (TYPE SECRET) WITH (value = "<password>");
    ```
1. Создать [внешний источник данных](../datamodel/external_data_source.md), описывающий определённую базу данных в составе кластера Greenplum. В параметр `LOCATION` нужно передать сетевой адрес [мастер-ноды](https://greenplum.org/introduction-to-greenplum-architecture/) Greenplum. При чтении по умолчанию используется [пространство имен](https://docs.vmware.com/en/VMware-Greenplum/6/greenplum-database/ref_guide-system_catalogs-pg_namespace.html) `public`, но это значение можно изменить с помощью опционального параметра `SCHEMA`. Включить шифрование соединений к внешней базе данных можно с помощью параметра `USE_TLS="TRUE"`.
    ```sql
    CREATE EXTERNAL DATA SOURCE greenplum_datasource WITH (
        SOURCE_TYPE="Greenplum",
        LOCATION="<host>:<port>",
        DATABASE_NAME="<database>",
        AUTH_METHOD="BASIC",
        LOGIN="user",
        PASSWORD_SECRET_NAME="greenplum_datasource_user_password",
        USE_TLS="TRUE",
        SCHEMA="<schema>"
    );
    ```
1. {% include [!](_includes/connector_deployment.md) %}
1. [Выполнить запрос](#query) к базе данных.

## Синтаксис запросов { #query }
Для работы с Greenplum используется следующая форма SQL-запроса:

```sql
SELECT * FROM greenplum_datasource.<table_name>
```

где:
- `greenplum_datasource` - идентификатор внешнего источника данных;
- `<table_name>` - имя таблицы внутри внешнего источника данных.

## Ограничения

При работе с кластерами Greenplum существует ряд ограничений:

1. {% include [!](_includes/supported_requests.md) %}
1. {% include [!](_includes/datetime_limits.md) %}
1. {% include [!](_includes/predicate_pushdown.md) %}

## Поддерживаемые типы данных

В базе данных Greenplum признак опциональности значений колонки (разрешено или запрещено колонке содержать значения `NULL`) не является частью системы типов данных. Ограничение (constraint) `NOT NULL` для каждой колонки реализуется в виде атрибута `attnotnull` в системном каталоге [pg_attribute](https://docs.vmware.com/en/VMware-Greenplum/6/greenplum-database/ref_guide-system_catalogs-pg_attribute.html), то есть на уровне метаданных таблицы. Следовательно, все базовые типы Greenplum по умолчанию могут содержать значения `NULL`, и в системе типов {{ ydb-full-name }} они должны отображаться в [опциональные](../../yql/reference/types/optional.md) типы.

Ниже приведена таблица соответствия типов Greenplum и {{ ydb-short-name }}. Все остальные типы данных, за исключением перечисленных, не поддерживаются.

|Тип данных Greenplum|Тип данных {{ ydb-full-name }}|Примечания|
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
|`json`|`Optional<Json>`||
|`date`|`Optional<Date>`|Допустимый диапазон дат с 1970-01-01 и до 2105-12-31. При выходе значения за границы диапазона возвращается `NULL`.|
|`timestamp`|`Optional<Timestamp>`|Допустимый диапазон времени с 1970-01-01 00:00:00 и до 2105-12-31 23:59:59. При выходе значения за границы диапазона возвращается значение `NULL`.|
|`bytea`|`Optional<String>`||
|`character`|`Optional<Utf8>`|[Правила сортировки](https://www.postgresql.org/docs/current/collation.html) по умолчанию, строка дополняется пробелами до требуемой длины.|
|`character varying`|`Optional<Utf8>`|[Правила сортировки](https://www.postgresql.org/docs/current/collation.html) по умолчанию.|
|`text`|`Optional<Utf8>`|[Правила сортировки](https://www.postgresql.org/docs/current/collation.html) по умолчанию.|
