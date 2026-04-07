# CREATE TABLE

{% if feature_bulk_tables %}

Таблица создается автоматически при первом [INSERT INTO](../insert_into.md){% if feature_mapreduce %}, в заданной оператором [USE](../use.md) базе данных{% endif %}. Схема при этом определяется автоматически.

{% else %}

Вызов `CREATE TABLE` создает {% if concept_table %}[таблицу]({{ concept_table }}){% else %}таблицу{% endif %} с указанной схемой данных{% if feature_map_tables %}  и ключевыми колонками (`PRIMARY KEY`){% endif %}.{% if feature_secondary_index == true %} Позволяет определить вторичные индексы на создаваемой таблице.

{% endif %}

{% endif %}

```yql
CREATE TABLE [IF NOT EXISTS] <table_name> (
  [<column_name> <column_data_type>] [FAMILY <family_name>] [NULL | NOT NULL] [DEFAULT <default_value>]
  [, ...],
    INDEX <index_name>
      [GLOBAL]
      [UNIQUE]
      [SYNC|ASYNC]
      [USING <index_type>]
      ON ( <index_columns> )
      [COVER ( <cover_columns> )]
      [WITH ( <parameter_name> = <parameter_value>[, ...])]
    [, ...]
  PRIMARY KEY ( <column>[, ...]),
  [FAMILY <column_family> ( family_options[, ...])]
)
[PARTITION BY HASH ( <column>[, ...])]
[WITH (<setting_name> = <setting_value>[, ...])]

[AS SELECT ...]
```

{% if oss == true and backend_name == "YDB" %}

## Параметры запроса

### table_name

Путь создаваемой таблицы.

При выборе имени для таблицы учитывайте общие [правила именования схемных объектов](../../../../concepts/datamodel/cluster-namespace.md#object-naming-rules).

### IF NOT EXISTS

Если таблица с указанным именем уже существует, выполнение оператора полностью пропускается — не происходит никаких проверок или сопоставления схемы, и никакой ошибки не возникает. Обратите внимание, что существующая таблица может отличаться по структуре от той, которую вы хотели бы создать этим запросом — сравнение или проверка эквивалентности не производится.

### column_name

Имя колонки, создаваемой в новой таблице.

При выборе имени для колонки учитывайте общие [правила именования колонок](../../../../concepts/datamodel/table.md#column-naming-rules).

### column_data_type

Тип данных колонки. Полный список типов данных, которые поддерживает {{ ydb-short-name }} доступен в разделе [{#T}](../../types/index.md).

{% include [column_option_list.md](../_includes/column_option_list.md) %}

### INDEX

Определение индекса на таблице. Поддерживаются [вторичные индексы](secondary_index.md) и [векторные индексы](vector_index.md).

### PRIMARY KEY

Определение первичного ключа таблицы. Указывает колонки, которые составляют первичный ключ в порядке перечисления. Подробнее о выборе первичного ключа в разделе [{#T}](../../../../dev/primary-key/index.md).

### PARTITION BY HASH

Определение ключей партиционирования для **колоночных** таблиц. Указывает колонки, по хэшу которых выполняется [партиционирование](../../../../concepts/glossary.md#partition) данных. Колонки должны быть частью первичного ключа. При этом колонки не обязательно должны быть префиксом или суффиксом -- требование быть частью первичного ключа.

Если параметр не будет указан, таблица будет разбита на партиции по тем же колонкам, которые входят в первичный ключ. Как правильно выбирать ключи для партиционирования в колоночных таблицах, читайте в статье [{#T}](../../../../dev/primary-key/column-oriented.md).

Подробнее о партиционировании колоночных таблиц читайте в разделе [{#T}](../../../../concepts/datamodel/table.md#olap-tables-partitioning).

### FAMILY <column_family> (настройка группы колонок)

Определение группы колонок с заданными параметрами. Подробнее в разделе [{#T}](family.md).

### WITH

Дополнительные параметры создания таблицы. Подробнее в разделе [{#T}](with.md).

{ % note info % }

{{ ydb-short-name }} поддерживает два типа таблиц:

* [Строковые](../../../../concepts/datamodel/table.md#row-oriented-tables).
* [Колоночные](../../../../concepts/datamodel/table.md#column-oriented-tables).

Тип таблицы при создании задается параметром `STORE` в блоке `WITH`, где `ROW` означает [строковую таблицу](../../../../concepts/datamodel/table.md#row-oriented-tables), а `COLUMN` — [колоночную](../../../../concepts/datamodel/table.md#column-oriented-tables):

```yql
CREATE <table_name> (
  columns
  ...
)

WITH (
  STORE = COLUMN -- Default value ROW
)
```

По умолчанию, если параметр `STORE` не указан, создается строковая таблица.

{% endnote %}

{% note info %}

При выборе имени для таблицы учитывайте общие [правила именования схемных объектов](../../../../concepts/datamodel/cluster-namespace.md#object-naming-rules).

{% endnote %}

### AS SELECT

Создание и заполнение таблицы на основе результатов запроса `SELECT`. Подробнее в разделе [{#T}](as_select.md).

## Примеры создания таблиц

{% list tabs %}

- Создание строковой таблицы

  {% if feature_column_container_type %}

  ```yql
    CREATE TABLE <table_name> (
      a Uint64,
      b Uint64,
      c Float,
      d "List<List<Int32>>"
      PRIMARY KEY (a, b)
    );
    ```

  {% else %}

  ```yql
    CREATE TABLE <table_name> (
      a Uint64,
      b Uint64,
      c Float,
      PRIMARY KEY (a, b)
    );
    ```

  {% endif %}

  Пример создания таблицы с использованием значения по умолчанию (DEFAULT):

  ```yql
    CREATE TABLE table_with_default (
    id Uint64,
    name String DEFAULT "unknown",
    score Double NOT NULL DEFAULT 0.0,
    PRIMARY KEY (id)
  );
  ```

  {% if feature_column_container_type == true %}

  Для неключевых колонок допускаются любые типы данных{% if feature_serial %} , кроме [серийных](../../types/serial.md) {% endif %}, для ключевых - только [примитивные](../../types/primitive.md){% if feature_serial %} и [серийные](../../types/serial.md){% endif %}. При указании сложных типов (например, `List<String>`) тип заключается в двойные кавычки.

  {% else %}

  {% if feature_serial %}

  Для ключевых колонок допускаются только [примитивные](../../types/primitive.md) и [серийные](../../types/serial.md) типы данных, для неключевых колонок допускаются только [примитивные](../../types/primitive.md).

  {% else %}

  Для ключевых и неключевых колонок допускаются только [примитивные](../../types/primitive.md) типы данных.

  {% endif %}

  {% endif %}

  {% if feature_not_null == true %}

  Без дополнительных модификаторов колонка приобретает [опциональный тип](../../types/optional.md), и допускает запись `NULL` в качестве значений. Для получения неопционального типа необходимо использовать `NOT NULL`.

  {% else %}

  {% if feature_not_null_for_pk %}

  По умолчанию все колонки [опциональные](../../types/optional.md) и могут иметь значение NULL. Ограничение `NOT NULL` можно указать только для колонок, входящих в первичный ключ.

  {% else %}

  Все колонки допускают запись `NULL` в качестве значений, то есть являются [опциональными](../../types/optional.md).

  {% endif %}

  {% endif %}

  {% if feature_map_tables %}

  Обязательно указание `PRIMARY KEY` с непустым списком колонок. Эти колонки становятся частью ключа в порядке перечисления.

  {% endif %}

  Пример создания строковой таблицы с использованием опций партиционирования:

  ```yql
  CREATE TABLE <table_name> (
    a Uint64,
    b Uint64,
    c Float,
    PRIMARY KEY (a, b)
  )
  WITH (
    AUTO_PARTITIONING_BY_SIZE = ENABLED,
    AUTO_PARTITIONING_PARTITION_SIZE_MB = 512
  );
  ```

  Такой код создаст строковую таблицу с включенным автоматическим партиционированием по размеру партиции (`AUTO_PARTITIONING_BY_SIZE`) и предпочитаемым размером каждой партиции (`AUTO_PARTITIONING_PARTITION_SIZE_MB`) в 512 мегабайт. Полный список опций партиционирования строковой таблицы находится в разделе [Партиционирование строковой таблицы](../../../../concepts/datamodel/table.md#partitioning_row_table) статьи [{#T}](../../../../concepts/datamodel/table.md).

- Создание колоночной таблицы

  ```yql
  CREATE TABLE table_name (
    a Uint64 NOT NULL,
    b Timestamp NOT NULL,
    c Float,
    PRIMARY KEY (a, b)
  )
  PARTITION BY HASH(b)
  WITH (
    STORE = COLUMN
  );
  ```

  Для колоночных таблиц можно явно указать, по каким колонкам будет происходить партиционирование, с помощью конструкции `PARTITION BY HASH`. Обычно для этого выбирают колонки первичного ключа с большим числом уникальных значений, например, `Timestamp`. Если `PARTITION BY HASH` не указать, партиционирование произойдёт автоматически по всем колонкам, входящим в первичный ключ. Подробнее о выборе и работе ключей партиционирования в колоночных таблицах читайте в статье [{#T}](../../../../dev/primary-key/column-oriented.md).

  В настоящий момент колоночные таблицы не поддерживают автоматического репартицирования, поэтому важно указывать правильное число партиций при создании таблицы с помощью параметра `AUTO_PARTITIONING_MIN_PARTITIONS_COUNT`:

  ```yql
  CREATE TABLE table_name (
    a Uint64 NOT NULL,
    b Timestamp NOT NULL,
    c Float,
    PRIMARY KEY (a, b)
  )
  PARTITION BY HASH(b)
  WITH (
    STORE = COLUMN,
    AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 10
  );
  ```

  Такой код создаст колоночную таблицу с 10-ю партициями. С полным списком опций партиционирования колоночных таблиц можно ознакомиться в разделе [{#T}](../../../../concepts/datamodel/table.md#olap-tables-partitioning) статьи [{#T}](../../../../concepts/datamodel/table.md).


{% endlist %}

{% else %}

{% if feature_column_container_type == true %}

Для неключевых колонок допускаются любые типы данных, для ключевых - только [примитивные](../../types/primitive.md). При указании сложных типов (например, `List<String>`) тип заключается в двойные кавычки.

{% else %}

Для ключевых и неключевых колонок допускаются только [примитивные](../../types/primitive.md) типы данных.

{% endif %}

{% if feature_not_null == true %}

Без дополнительных модификаторов колонка приобретает [опциональный тип](../../types/optional.md), и допускает запись `NULL` в качестве значений. Для получения неопционального типа необходимо использовать `NOT NULL`.

{% else %}

{% if feature_not_null_for_pk %}

По умолчанию все колонки [опциональные](../../types/optional.md) и могут иметь значение NULL. Ограничение `NOT NULL` можно указать только для колонок, входящих в первичный ключ.

{% else %}

Все колонки допускают запись `NULL` в качестве значений, то есть являются [опциональными](../../types/optional.md).

{% endif %}

{% endif %}

{% if feature_map_tables %}

Обязательно указание `PRIMARY KEY` с непустым списком колонок. Эти колонки становятся частью ключа в порядке перечисления.

{% endif %}

Пример:

```yql
CREATE TABLE <table_name> (
  a Uint64,
  b Uint64,
  c Float,
  PRIMARY KEY (a, b)
);
```

{% endif %}

{% if backend_name == "YDB" and oss == true %}

При создании строковых таблиц возможно задать:

* [Вторичный индекс](secondary_index.md).
* [Векторный индекс](vector_index.md).
* [Группы колонок](family.md).
* [Дополнительные параметры](with.md).
* [Создание и заполнение таблицы на основе результатов запроса](as_select.md).

Для колоночных таблиц при их создании возможно задать:

* [Группы колонок](family.md).
* [Дополнительные параметры](with.md).
* [Создание и заполнение таблицы на основе результатов запроса](as_select.md).

{% endif %}