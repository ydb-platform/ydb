# Чтение данных из внешней таблицы, ведущей на S3 ({{ objstorage-name }})

Иногда одни и те же запросы к данным нужно выполнять регулярно. Чтобы не указывать все детали работы с этими данными при каждом вызове запроса, используйте режим с [внешними таблицами](../../datamodel/external_table.md). В этом случае запрос выглядит, как обычный запрос к таблицам {{ ydb-full-name }}.

Пример запроса для чтения данных:

```yql
SELECT
    *
FROM
    `s3_test_data`
WHERE
    version > 1
```

## Создание внешней таблицы, ведущей на бакет S3 ({{ objstorage-name }}) {#external-table-settings}

Чтобы создать внешнюю таблицу, описывающую бакет S3 ({{ objstorage-name }}), выполните следующий SQL-запрос. Запрос создает внешнюю таблицу с именем `s3_test_data`, в котором расположены файлы в формате `CSV` со строковыми полями `key` и `value`, находящиеся внутри бакета по пути `test_folder`, при этом для указания реквизитов подключения используется объект [внешний источник данных](../../datamodel/external_data_source.md) `bucket`:

```yql
CREATE EXTERNAL TABLE `s3_test_data` (
  key Utf8 NOT NULL,
  value Utf8 NOT NULL
) WITH (
  DATA_SOURCE="bucket",
  LOCATION="folder",
  FORMAT="csv_with_names",
  COMPRESSION="gzip"
);
```

Где:

- `key, value` - список колонок данных и их типов, список допустимых типов описан в разделе [{#T}](formats.md#types);
- `bucket` - имя [внешнего источника данных](../../datamodel/external_data_source.md) к S3 ({{ objstorage-name }});
- `folder` - путь внутри бакета с данными. Поддерживаются wildcards, подробнее [в разделе](external_data_source.md#path_format);
- `csv_with_names` - один из [допустимых типов хранения данных](formats.md);
- `gzip` - один из [допустимых алгоритмов сжатия](formats.md#compression).

Также при создании внешних таблиц поддерживаются [параметры форматирования](external_data_source.md#format_settings).

## Модель данных {#data-model}

Чтение данных с помощью внешних таблиц из S3 ({{ objstorage-name }}) выполняется с помощью обычных SQL-запросов, как к обычной таблице.

```yql
SELECT
    <expression>
FROM
    `s3_test_data`
WHERE
    <filter>;
```

## Ограничения

При работе с бакетами S3 ({{ objstorage-name }}) существует ряд ограничений.

Ограничения:

1. Поддерживаются только запросы чтения данных - `SELECT` и `INSERT`, остальные виды запросов не поддерживаются.
1. {% include [!](../_includes/datetime_limits.md)%}

## Импорт данных из S3 в таблицу {#import-from-s3}

{{ ydb-short-name }} поддерживает загрузку данных из S3 в таблицу {{ ydb-short-name }} с помощью команды [CREATE TABLE ... AS SELECT](../../../yql/reference/syntax/create_table/index.md). Параметры для создаваемой таблицы могут быть перечислены в секции `WITH`, подробнее см. в статье [{#T}](../../../yql/reference/syntax/create_table/with.md).

```yql
CREATE TABLE column_table (
    PRIMARY KEY (key)
)
WITH (
    STORE = COLUMN
)
AS SELECT * FROM s3_test_data
```

В результате будет создана [колоночная таблица](../../datamodel/table.md#column-oriented-tables) со схемой, соответствующей использованной для импорта [внешней таблицы](../../datamodel/external_table.md). Колонки, указанные в `PRIMARY KEY`, должны быть помечены как `NOT NULL` во внешней таблице.

Импорт данных из S3 в [строковые таблицы](../../datamodel/table.md#row-oriented-tables) с помощью внешних таблиц поддерживается только для данных размером до 1 ГБ. Импорт строковых таблиц без ограничения по размеру поддержан в виде фоновой операции, которую можно запустить через YDB CLI, подробнее см. в статье [{#T}](../../../reference/ydb-cli/export-import/import-s3.md).
