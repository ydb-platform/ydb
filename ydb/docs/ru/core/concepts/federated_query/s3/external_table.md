# Чтение данных из внешней таблицы, ведущей на S3 ({{ objstorage-name }})

Иногда одни и те же запросы к данным нужно выполнять регулярно. Чтобы не указывать все детали работы с этими данными при каждом вызове запроса, используйте режим с [внешними таблицами](../../datamodel/external_table.md). В этом случае запрос выглядит, как обычный запрос к таблицам {{ydb-full-name}}.

Пример запроса для чтения данных:

```sql
SELECT
    *
FROM
    `s3_test_data`
WHERE
    version > 1
```

## Создание внешней таблицы, ведущей на бакет S3 ({{ objstorage-name }}) {#external-table-settings}

Чтобы создать внешнюю таблицу, описывающую бакет S3 ({{ objstorage-name }}), выполните следующий SQL-запрос. Запрос создает внешнюю таблицу с именем `s3_test_data`, в котором расположены файлы в формате `CSV` со строковыми полями `key` и `value`, находящиеся внутри бакета по пути `test_folder`, при этом для указания реквизитов подключения используется объект [внешний источник данных](../../datamodel/external_data_source.md) `bucket`:

```sql
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
- `key, value` - список колонок данных и их типов;
- `bucket` - имя [внешнего источника данных](../../datamodel/external_data_source.md) к S3 ({{ objstorage-name }});
- `folder` - путь внутри бакета с данными;
- `csv_with_names` - один из [допустимых типов хранения данных](formats.md);
- `gzip` - один из [допустимых алгоритмов сжатия](formats.md#compression).


## Модель данных {#data-model}

Чтение данных с помощью внешних таблиц из S3 ({{ objstorage-name }}) выполняется с помощью обычных SQL-запросов, как к обычной таблице.

```sql
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
