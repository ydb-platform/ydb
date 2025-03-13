# Работа с бакетами S3 ({{objstorage-full-name}})

Перед началом работы с S3 необходимо настроить подключение к хранилищу данных. Для этого существует DDL для настройки таких подключений. Далее рассмотрим SQL синтаксис и управление этими настройками.

Бакеты в S3 бывают двух видов: публичные и приватные. Для подключения к публичному бакету необходимо использовать `AUTH_METHOD="NONE"`, а для подключения к приватному — `AUTH_METHOD="AWS"`. Подробное описание `AWS` можно найти [здесь](https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_sigv-authentication-methods.html). `AUTH_METHOD="NONE"` означает, что аутентификация не требуется. В случае `AUTH_METHOD="AWS"` необходимо указать несколько дополнительных параметров:

- `AWS_ACCESS_KEY_ID_SECRET_NAME` — ссылка на имя [секрета](../../datamodel/secrets.md), в котором хранится `AWS_ACCESS_KEY_ID`.
- `AWS_SECRET_ACCESS_KEY_SECRET_NAME` — ссылка на имя [секрета](../../datamodel/secrets.md), в котором хранится `AWS_SECRET_ACCESS_KEY`.
- `AWS_REGION` — регион, из которого будет происходить чтение, например `ru-central-1`.

Для настройки соединения с публичным бакетом достаточно выполнить следующий SQL-запрос. Запрос создаст внешнее подключение с именем `object_storage`, которое будет указывать на конкретный S3-бакет с именем `bucket`.

```yql
CREATE EXTERNAL DATA SOURCE object_storage WITH (
  SOURCE_TYPE="ObjectStorage",
  LOCATION="https://object_storage_domain/bucket/",
  AUTH_METHOD="NONE"
);
```

Для настройки соединения с приватным бакетом необходимо выполнить несколько SQL-запросов. Сначала нужно создать [секреты](../../datamodel/secrets.md), содержащие `AWS_ACCESS_KEY_ID` и `AWS_SECRET_ACCESS_KEY`.

```yql
    CREATE OBJECT aws_access_id (TYPE SECRET) WITH (value=`<id>`);
    CREATE OBJECT aws_access_key (TYPE SECRET) WITH (value=`<key>`);
```

Следующим шагом создаётся внешнее подключение с именем `object_storage`, которое будет указывать на конкретный S3-бакет с именем `bucket`, а также использовать `AUTH_METHOD="AWS"`, для которого заполняются параметры `AWS_ACCESS_KEY_ID_SECRET_NAME`, `AWS_SECRET_ACCESS_KEY_SECRET_NAME`, `AWS_REGION`. Значения этих параметров описаны выше.

```yql
CREATE EXTERNAL DATA SOURCE object_storage WITH (
  SOURCE_TYPE="ObjectStorage",
  LOCATION="https://object_storage_domain/bucket/",
  AUTH_METHOD="AWS",
  AWS_ACCESS_KEY_ID_SECRET_NAME="aws_access_id",
  AWS_SECRET_ACCESS_KEY_SECRET_NAME="aws_access_key",
  AWS_REGION="ru-central-1"
);
```

## Использование внешнего подключения к S3-бакету {#external-data-source-settings}

При работе с {{ objstorage-full-name }} с помощью [внешних источников данных](../../datamodel/external_data_source.md) удобно выполнять прототипирование, первоначальную настройку подключений к данным.

Пример запроса для чтения данных:

```yql
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

```yql
SELECT
  <expression>
FROM
  <object_storage_connection_name>.`<file_path>`
WITH(
  FORMAT = "<file_format>",
  COMPRESSION = "<compression>",
  SCHEMA = (<schema_definition>))
WHERE
  <filter>;
```

Где:

* `object_storage_connection_name` — название внешнего источника данных, ведущего на бакет с S3 ({{ objstorage-full-name }}).
* `file_path` — путь к файлу или файлам внутри бакета. Поддерживаются wildcards `*`, подробнее [в разделе](#path_format).
* `file_format` — [формат данных](formats.md#formats) в файлах.
* `compression` — [формат сжатия](formats.md#compression_formats) файлов.
* `schema_definition` — [описание схемы хранимых данных](#schema) в файлах.

### Описание схемы данных {#schema}

Описание схемы данных состоит из набора полей:

- Названия поля.
- Типа поля.
- Признака обязательности данных.

Например, схема данных ниже описывает поле схемы с названием `Year` типа `Int32` и требованием наличия этого поля в данных:

```text
Year Int32 NOT NULL
```

Если поле данных помечено как обязательное, `NOT NULL`, но это поле отсутствует в обрабатываемом файле, то работа с таким файлом будет завершена с ошибкой. Если поле помечено как необязательное, `NULL`, то при отсутствии поля в обрабатываемом файле ошибки не возникнет, но поле примет значение `NULL`. Ключевое слово `NULL` в необязательных полях является опциональным.

### Автоматический вывод схемы данных {#inference}

{{ ydb-short-name }} может автоматически определять схему данных в файлах бакета, чтобы вам не пришлось указывать все поля схемы вручную.

{% note info %}

Автоматический вывод схемы доступен для всех [форматов данных](formats.md#formats), кроме `raw` и `json_as_string`. Для этих форматов придётся [описывать схему данных вручную](#schema).

{% endnote %}

Чтобы включить автоматический вывод схемы, используйте параметр `WITH_INFER`:

```yql
SELECT
  <expression>
FROM
  <object_storage_connection_name>.`<file_path>`
WITH(
  FORMAT = "<file_format>",
  COMPRESSION = "<compression>",
  WITH_INFER = "true")
WHERE
  <filter>;
```

Где:

* `object_storage_connection_name` — название внешнего источника данных, ведущего на S3 бакет ({{ objstorage-full-name }}).
* `file_path` — путь к файлу или файлам внутри бакета. Поддерживаются wildcards `*`, подробнее [ниже](#path_format).
* `file_format` — [формат данных](formats.md#formats) в файлах. Поддерживаются все форматы, кроме `raw` и `json_as_string`.
* `compression` — [формат сжатия](formats.md#compression_formats) файлов.

В результате выполнения такого запроса будут автоматически выведены названия и типы полей.

### Форматы путей к данным {#path_format}

В {{ ydb-full-name }} поддерживаются следующие пути к данным:

{% include [!](_includes/path_format.md) %}

## Пример {#read_example}

Пример запроса для чтения данных из S3 ({{ objstorage-full-name }}):

```yql
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
