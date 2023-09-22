# Работа с бакетами S3 ({{objstorage-full-name}})

При работе с {{ objstorage-full-name }} с помощью [внешних источников данных](../../datamodel/external_data_source.md) удобно выполнять прототипирование, первоначальную настройку подключений к данным.

Пример запроса для чтения данных:

```sql
SELECT
    *
FROM
    object_storage.`*.tsv`
WITH
(
    FORMAT = "tsv_with_names",
    SCHEMA =
    (
        ts Uint32,
        action Utf8
    )
);
```

Список поддерживаемых форматов и алгоритмов сжатия данных для чтения данных в S3 ({{objstorage-full-name}}), приведен в разделе [{#T}](formats.md).

## Модель данных {#data_model}

В {{ objstorage-full-name }} данные хранятся в файлах. Для чтения данных необходимо указать формат данных в файлах, сжатие, списки полей. Для этого используется следующее SQL-выражение:

```sql
SELECT
  <expression>
FROM
  <object_storage_connection_name>.`<file_path>`
WITH(
  FORMAT = "<file_format>",
  SCHEMA = (<schema_definition>),
  COMPRESSION = "<compression>")
WHERE
  <filter>;
```

Где:

* `object_storage_connection_name` — название [внешнего источника данных](#create_connection), ведущего на бакет  с S3 ({{ objstorage-full-name }}).
* `file_path` — путь к файлу или файлам внутри бакета. Поддерживаются wildcards `*`, подробнее [в разделе](#path_format).
* `file_format` — [формат данных](formats.md#formats) в файлах.
* `schema_definition` — [описание схемы хранимых данных](#schema) в файлах.
* `compression` — [формат сжатия](formats.md#compression_formats) файлов.

### Описание схемы данных { #schema }

Описание схемы данных состоит из набора полей:
- Названия поля.
- Типа поля.
- Признака обязательности данных.

Например, схема данных ниже описывает поле схемы с названием `Year` типа `Int32` и требованием наличия этого поля в данных:

```
Year Int32 NOT NULL
```

Если поле данных помечено, как обязательное, `NOT NULL`, но это поле отсутствует в обрабатываемом файле, то работа с таким файлом будет завершена с ошибкой. Если поле помечено как необязательное, `NULL`, то при отсутствии поля в обрабатываемом файле не будет возникать ошибки, но поле при этом примет значение `NULL`.

### Форматы путей к данным {#path_format}

В {{ ydb-full-name }} поддерживаются следующие пути к данным:

{% include [!](_includes/path_format.md) %}

## Пример {#read_example}

Пример запроса для чтения данных из S3 ({{ objstorage-full-name }}):

```sql
SELECT
    *
FROM
    connection.`folder/filename.csv`
WITH(
    FORMAT = "csv_with_names",
    SCHEMA =
    (
        Year Int32,
        Manufacturer Utf8,
        Model Utf8,
        Price Double
    )
);
```

Где:

* `connection` — название внешнего источника данных, ведущего на бакет S3 ({{ objstorage-full-name }}).
* `folder/filename.csv` — путь к файлу в бакете S3 ({{ objstorage-full-name }}).
* `SCHEMA` — описание схемы данных в файле.
