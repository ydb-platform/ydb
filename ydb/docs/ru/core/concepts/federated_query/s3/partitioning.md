# Партицирование данных в S3 ({{ objstorage-full-name }})

В S3 ({{ objstorage-full-name }}) можно хранить очень большие объемы данных. При этом запросы к этим данным могут затрагивать не все данные, а только их часть. Если описать правила разметки структуры хранения ваших данных в {{ydb-full-name}}, тогда данные, которые не нужны для запроса, можно даже не считывать из S3 ({{ objstorage-full-name }}). Это значительно ускоряет выполнение запросов без влияния на результат.

Например, данные хранятся в следующей структуре каталогов:

```text
year=2021
    month=01
    month=02
    month=03
year=2022
    month=01
```

Запрос ниже явно подразумевает, что обработать нужно только данные за февраль 2021-го года и другие данные не нужны.

```yql
SELECT
    *
FROM
    objectstorage.'/'
WITH
(
    FORMAT = "csv_with_names",
    SCHEMA =
    (
        data String,
        year Int32,
        month Int32
    )
)
WHERE
    year=2021
    AND month=02
```

Если схема партицирования данных не указана, то из S3 ({{ objstorage-full-name }}) будут считаны *все* хранимые данные, но в результате обработки данные за все другие даты будут отброшены.

Если описать структуру хранения явно, указав, что данные в S3 ({{ objstorage-full-name }}) размещены в каталогах по годам и месяцам

```yql
SELECT
    *
FROM
    objectstorage.'/'
WITH
(
    FORMAT = "csv_with_names",
    SCHEMA =
    (
        data String,
        year Int32 NOT NULL,
        month Int32 NOT NULL
    ),
    PARTITIONED_BY = (year, month)
)
WHERE
    year=2021
    AND month=02
```

то в процессе исполнения запроса из S3 ({{ objstorage-full-name }}) будут считаны не все данные, а только данные за февраль 2021-го года, что существенно сократит объем обрабатываемых данных и ускорит обработку, при этом результаты обоих запросов будут идентичны. Поддерживаемые типы данных для колонок патиционирования описаны [ниже](#supported-types).

{% note info %}

В примере выше показана работа с данными на уровне [внешних источников данных](../../datamodel/external_data_source.md). Такой пример выбран только для иллюстративных целей. Мы настоятельно рекомендуем для работы с данными использовать [внешние таблицы](../../datamodel/external_table.md) и не использовать прямую работу с внешними источниками.

{% endnote %}

## Синтаксис для внешних источников данных {#syntax-external-data-source}

При работе на уровне [внешних источников данных](../../datamodel/external_data_source.md) партицирование задается с помощью параметра `partitioned_by`, где в круглых скобках задается список колонок.

```yql
SELECT
    *
FROM
    <object_storage_external_datasource_name>.<path>
WITH
(
    SCHEMA =
    (
        <field1> <type1>,
        <field2> <type2> NOT NULL,
        <field3> <type3> NOT NULL
    ),
    PARTITIONED_BY = (<field2>, <field3>),
    <format_settings>
)
WHERE
    <filter>
```

В параметре `partitioned_by` перечисляются колонки схемы данных, по которым партицированы хранимые в S3 ({{ objstorage-full-name }}) данные. Порядок указания полей в параметре `partitioned_by` определяет вложенность каталогов S3 ({{objstorage-full-name}}) друг в друга. При этом запрещено указывать в схеме запроса только колонки партицирования.

Например, `PARTITIONED_BY=(year, month)` определяет структуру каталогов

```text
year=2021
    month=01
    month=02
    month=03
year=2022
    month=01
```

А `PARTITIONED_BY=(month, year)` определяет другую структуру каталогов

```text
month=01
    year=2021
    year=2022
month=02
    year=2021
month=03
    year=2021
```

## Синтаксис для внешних таблиц {#syntax-external-table}

Рекомендованным способом работы с партицированием данных является использование [внешних таблиц](../../datamodel/external_table.md). Для них в параметре `partitioned_by` указывается список колонок в формате JSON при создании таблицы.

```yql
CREATE EXTERNAL TABLE <external_table> (
    <field1> <type1>,
    <field2> <type2> NOT NULL,
    <field3> <type3> NOT NULL
) WITH (
    DATA_SOURCE = "<object_storage_external_datasource_name>",
    LOCATION = "<path>",
    PARTITIONED_BY = "['<field2>', '<field3>']",
    <format_settings>
);
```

Пример создания внешней таблицы с партицированием по колонкам `year` и `month`:

```yql
CREATE EXTERNAL TABLE `objectstorage_data` (
    data String,
    year Int32 NOT NULL,
    month Int32 NOT NULL
) WITH (
    DATA_SOURCE = "objectstorage",
    LOCATION = "/",
    FORMAT = "csv_with_names",
    PARTITIONED_BY = "['year', 'month']"
);
```

Далее вы можете читать из таблицы `objectstorage_data` с возможностью быстрой фильтрации по колонкам `year` и `month`:

```yql
SELECT
    *
FROM
    `objectstorage_data`
WHERE
    year=2021
    AND month=02
```

Запрещено создавать таблицу, схема которой состоит только из колонок партицирования.

## Поддерживаемые типы данных {#supported-types}

Партицирование возможно только по следующему набору типов данных YQL:

- `Uint32`, `Uint64`
- `Int32`, `Int64`
- `String`, `Utf8`
- `Date`, `Datetime`

При использовании других типов для указания партицирования возвращается ошибка. Колонки партицирования должны быть отмечены как `NOT NULL`.

## Поддерживаемые форматы путей хранения

Формат путей хранения, когда в имени каждого каталога явно указано название колонки, называется "[Hive-Metastore форматом](https://en.wikipedia.org/wiki/Apache_Hive)" или просто "Hive-форматом".

Такой формат выглядит следующим образом:

```text
month=01
    year=2021
    year=2022
month=02
    year=2021
month=03
    year=2021
```

{% note warning %}

Базовый режим партицирования в {{ydb-full-name}} поддерживает только Hive-формат.

{% endnote %}

Для указания произвольных путей хранения необходимо воспользоваться режимом [Расширенного партицирования данных](partition_projection.md).
