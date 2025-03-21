# CREATE TABLE

## Синтаксис CREATE TABLE

{% if feature_bulk_tables %}

Таблица создается автоматически при первом [INSERT INTO](../insert_into.md){% if feature_mapreduce %}, в заданной оператором [USE](../use.md) базе данных{% endif %}. Схема при этом определяется автоматически.

{% else %}

Вызов `CREATE TABLE` создает {% if concept_table %}[таблицу]({{ concept_table }}){% else %}таблицу{% endif %} с указанной схемой данных{% if feature_map_tables %}  и ключевыми колонками (`PRIMARY KEY`){% endif %}.{% if feature_secondary_index == true %} Позволяет определить вторичные индексы на создаваемой таблице.

{% endif %}

{% endif %}

    CREATE [TEMP | TEMPORARY] TABLE table_name (
        column1 type1,
{% if feature_not_null == true %}        column2 type2 NOT NULL,{% else %}        column2 type2,{% endif %}
        ...
        columnN typeN,
{% if feature_secondary_index == true %}
        INDEX index1_name GLOBAL ON ( column ),
        INDEX index2_name GLOBAL ON ( column1, column2, ... ),
{% endif %}
{% if feature_map_tables %}
        PRIMARY KEY ( column, ... ),
        FAMILY column_family ( family_options, ... )
{% else %}
        ...
{% endif %}
    )
{% if feature_map_tables %}
    WITH ( key = value, ... )
{% endif %}

{% if oss == true and backend_name == "YDB" %}

{% if feature_olap_tables %}

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

{% endif %}

{% include [table naming rules](../../../../concepts/datamodel/_includes/object-naming-rules.md) %}

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

  Такой код создаст строковую таблицу с включенным автоматическим партиционированием по размеру партиции (`AUTO_PARTITIONING_BY_SIZE`) и предпочитаемым размером каждой партиции (`AUTO_PARTITIONING_PARTITION_SIZE_MB`) в 512 мегабайт. Полный список опций партиционирования строковой таблицы находится в разделе [{#T}](../../../../concepts/datamodel/table.md#partitioning_row_table) статьи [{#T}](../../../../concepts/datamodel/table.md).

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

  При создании колоночных таблиц обязательно нужно использовать конструкцию `PARTITION BY HASH` с указанием первичных ключей, которые имеют высококардинальный тип данных (например, `Timestamp`), так как колоночные таблицы партиционируют данные не по первичным ключам, а по специально выделенным ключам — ключам партицирования. Подробно про ключи партиционирования колоночных таблиц изложено в статье [{#T}](../../../../dev/primary-key/column-oriented.md).

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
* [Группы колонок](family.md).
* [Дополнительные параметры](with.md).

Для колоночных таблиц при их создании возможно задать:

* [Группы колонок](family.md).
* [Дополнительные параметры](with.md).

{% endif %}
