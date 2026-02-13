# Чтение из бакетов S3 через внешние источники данных

Перед началом работы с S3 необходимо настроить подключение к хранилищу данных. Для этого существует DDL для настройки таких подключений. Далее рассмотрим SQL синтаксис и управление этими настройками.

Бакеты в S3 бывают двух видов: публичные и приватные. Для подключения к публичному бакету необходимо использовать `AUTH_METHOD="NONE"`, а для подключения к приватному — `AUTH_METHOD="AWS"`. Подробное описание `AWS` можно найти [здесь](https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_sigv-authentication-methods.html). `AUTH_METHOD="NONE"` означает, что аутентификация не требуется. В случае `AUTH_METHOD="AWS"` необходимо указать несколько дополнительных параметров:

- `AWS_ACCESS_KEY_ID_SECRET_PATH` — [секрет](../../../datamodel/secrets.md), в котором хранится `AWS_ACCESS_KEY_ID`.
- `AWS_SECRET_ACCESS_KEY_SECRET_PATH` — [секрет](../../../datamodel/secrets.md), в котором хранится `AWS_SECRET_ACCESS_KEY`.
- `AWS_REGION` — регион, из которого будет происходить чтение, например `ru-central-1`.

Для настройки соединения с публичным бакетом достаточно выполнить следующий SQL-запрос. Запрос создаст внешний источник данных с именем `s3_data_source`, который будет указывать на конкретный S3-бакет с именем `bucket`.

```yql
CREATE EXTERNAL DATA SOURCE s3_data_source WITH (
  SOURCE_TYPE="ObjectStorage",
  LOCATION="https://s3_storage_domain/bucket/",
  AUTH_METHOD="NONE"
);
```

Для настройки соединения с приватным бакетом необходимо выполнить несколько SQL-запросов. Сначала нужно создать [секреты](../../../datamodel/secrets.md), содержащие `AWS_ACCESS_KEY_ID` и `AWS_SECRET_ACCESS_KEY`.

```yql
CREATE SECRET aws_access_id WITH (value=<id>);
CREATE SECRET aws_access_key WITH (value=<key>);
```

Следующим шагом создаётся внешний источник данных с именем `s3_data_source`, который будет указывать на конкретный S3-бакет с именем `bucket`, а также использовать `AUTH_METHOD="AWS"`, для которого задаются параметры `AWS_ACCESS_KEY_ID_SECRET_PATH`, `AWS_SECRET_ACCESS_KEY_SECRET_PATH`, `AWS_REGION`. Значения этих параметров описаны выше.

```yql
CREATE EXTERNAL DATA SOURCE s3_data_source WITH (
  SOURCE_TYPE="ObjectStorage",
  LOCATION="https://s3_storage_domain/bucket/",
  AUTH_METHOD="AWS",
  AWS_ACCESS_KEY_ID_SECRET_PATH="aws_access_id",
  AWS_SECRET_ACCESS_KEY_SECRET_PATH="aws_access_key",
  AWS_REGION="ru-central-1"
);
```

## Использование внешнего источника данных для S3-бакета {#external-data-source-settings}

При работе с S3-совместимым хранилищем данных с помощью [внешних источников данных](../../../datamodel/external_data_source.md) можно выяснить свойства файлов в бакете S3 до создания [внешних таблиц](../../../datamodel/external_table.md):

- Быстро просмотреть файлы
- Проверить права доступа
- Проверить пути и параметры хранения данных

Пример запроса для чтения данных:

```yql
SELECT
  *
FROM
  s3_data_source.`*.tsv`
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

Список поддерживаемых форматов и алгоритмов сжатия данных для чтения из S3-совместимого хранилища данных приведен в разделе [{#T}](formats.md).

## Модель данных {#data_model}

В S3 данные хранятся в файлах. Для чтения данных необходимо указать формат данных, сжатие, списки полей. Для этого используется следующее SQL-выражение:

```yql
SELECT
  <expression>
FROM
  <s3_external_datasource_name>.<file_path>
WITH
(
  FORMAT = "<file_format>",
  COMPRESSION = "<compression>",
  SCHEMA = (<schema_definition>),
  <format_settings>
)
WHERE
  <filter>;
```

Где:

* `s3_external_datasource_name` — название внешнего источника данных, ведущего на бакет с S3-совместимым хранилищем данных.
* `file_path` — путь к файлу или файлам внутри бакета. Поддерживаются подстановочные знаки `*`, `?`, `{ ... }`; подробнее [в разделе](#path_format).
* `file_format` — [формат данных](formats.md#formats) в файлах, обязательно.
* `compression` — [формат сжатия](formats.md#compression_formats) файлов, опционально.
* `schema_definition` — [описание схемы хранимых данных](#schema) в файлах, обязательно.
* `format_settings` — [параметры форматирования](#format_settings), опционально.

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

Список поддерживаемых типов данных, которые можно указать в схеме в зависимости от формата данных, приведён в разделе [{#T}](formats.md#types).

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
  <s3_external_datasource_name>.<file_path>
WITH
(
  FORMAT = "<file_format>",
  COMPRESSION = "<compression>",
  WITH_INFER = "true"
)
WHERE
  <filter>;
```

Где:

* `s3_external_datasource_name` — название внешнего источника данных, ведущего на S3 бакет.
* `file_path` — путь к файлу или файлам внутри бакета. Поддерживаются подстановочные знаки `*`, `?`, `{ ... }`; подробнее [ниже](#path_format).
* `file_format` — [формат данных](formats.md#formats) в файлах. Поддерживаются все форматы, кроме `raw` и `json_as_string`.
* `compression` — опциональный [формат сжатия](formats.md#compression_formats) файлов.

В результате выполнения такого запроса будут автоматически выведены названия и типы полей.

Ограничения для автоматического вывода схемы:

* Вывод схемы делается по данным только из одного произвольного непустого файла.
* Для форматов данных `csv_with_names`, `tsv_with_names`, `json_list`, `json_each_row` вывод схемы выполняется по первым 10 МБ данных из файла.
* Вывод схемы для файлов с форматом `parquet` возможен только в случае, если размер метаданных файла не превышает 10 МБ.
* Если файлы имеют разную схему, то запрос завершится с ошибкой парсинга данных в случае несовпадения типов колонок или пропуска не опциональных колонок.

### Форматы путей к данным {#path_format}

В {{ ydb-full-name }} поддерживаются следующие пути к данным:

{% include [!](_includes/path_format.md) %}

Такие же значения можно использовать для параметра `file_pattern`.

### Параметры форматирования {#format_settings}

В {{ ydb-full-name }} поддерживаются следующие параметры форматирования:

{% include [!](_includes/format_settings.md) %}

Подробное описание поддерживаемых подстановочных знаков для параметра `file_pattern` приведено [выше](#path_format). Параметр `file_pattern` можно использовать только в том случае, если `file_path` — путь к каталогу.

В строках форматирования для даты и времени можно использовать любые шаблонные переменные, поддерживаемые функцией [`strftime` (C99)](https://en.cppreference.com/w/c/chrono/strftime). В {{ ydb-full-name }} поддерживаются следующие форматы типов `Datetime` и `Timestamp`:

{% include [!](_includes/date_formats.md) %}

## Пример {#read_example}

Пример запроса для чтения данных из S3-совместимого хранилища данных:

```yql
SELECT
  *
FROM
  external_source.`folder/`
WITH(
  FORMAT = "csv_with_names",
  COMPRESSION = "gzip"
  SCHEMA =
  (
    Id Int32 NOT NULL,
    UserId Int32 NOT NULL,
    TripDate Date NOT NULL,
    TripDistance Double NOT NULL,
    UserComment Utf8
  ),
  FILE_PATTERN = "*.csv.gz",
  `DATA.DATE.FORMAT` = "%Y-%m-%d",
  CSV_DELIMITER = "/"
);
```

Где:

* `external_source` — название внешнего источника данных, ведущего на бакет S3-совместимого хранилища данных.
* `folder/` — путь к папке с данными в бакете S3.
* `SCHEMA` — описание схемы данных в файле.
* `*.csv.gz` — шаблон имени файлов с данными.
* `%Y-%m-%d` — формат записи данных типа `Date` в S3.
