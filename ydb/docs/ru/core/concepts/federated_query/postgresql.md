# Работа с базами данных PostgreSQL

В этом разделе описана основная информация про работу с внешней базой данных [PostgreSQL](http://postgresql.org).

Для работы с внешней базой данных PostgreSQL необходимо выполнить следующие шаги:
1. Создать [секрет](../datamodel/secrets.md), содержащий пароль для подключения к базе данных.
1. Создать [внешний источник данных](../datamodel/external_data_source.md), ведущий на кластер PostgreSQL. Для корректной работы с внешними источниками необходимо иметь сетевые доступы со всех хостов {{ydb-full-name}} в целевую систему.
1. [Выполнить запрос](#query) к базе данных.

## Синтаксис запросов { #query }
Для работы с PostgreSQL используется следующая форма SQL-запроса:

```sql
SELECT * FROM test_datasource.`DB.TABLE`
```

где:
- `test_datasource` - название созданного подключения к БД.
- `DB` - имя базы данных PostgreSQL в кластере.
- `TABLE` - имя таблицы в базе данных.

## Ограничения

При работе с кластерами PostgreSQL существует ряд ограничений.

{% note warning %}

В настоящий момент независимо от указанных фильтров для чтения таблиц PostgreSQL, указанных в SQL-запросе, все данные из таблицы считываются в {{ydb-full-name}} и уже там происходит применение фильтров.

{% endnote %}

Ограничения:
1. Поддерживаются только запросы чтения данных - `SELECT`, остальные виды запросов не поддерживаются.
1. Максимальное поддерживаемое количество строк в таблице - 1000000. При превышении этого значения запрос завершается с ошибкой.
1. {% include [!](_includes/datetime_limits.md) %}
1. При чтении всегда используется [пространство имен](https://www.postgresql.org/docs/current/catalog-pg-namespace.html) `public`, указать другой нельзя.
1. В данный момент подключение к кластеру PostgreSQL всегда выполняется про стандартному ([Frontend/Backend Protocol](https://www.postgresql.org/docs/current/protocol.html)) по транспорту TCP. При работе по защищенным TLS каналам связи используется системные сертификаты, расположенные на серверах {{ydb-full-name}}.



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

