# CREATE TABLE

{% if feature_olap_tables %}

{{ ydb-short-name }} поддерживает два типа таблиц:

* [строковые](../../../../concepts/datamodel/table.md);
* [колоночные](../../../../concepts/datamodel/table.md#column-tables).

Тип таблицы при создании задается параметром `STORE`, где `ROW` означает [строковую таблицу](#row), а `COLUMN` — [колоночную](#olap-tables). По умолчанию, если параметр `STORE` не указан, создается строковая таблица.

{%endif%}

{% if feature_olap_tables %}

## Строковые таблицы {#row}

{%endif%}

{% if feature_bulk_tables %}

Таблица создается автоматически при первом [INSERT INTO](insert_into.md){% if feature_mapreduce %}, в заданной оператором [USE](../use.md) базе данных{% endif %}. Схема при этом определяется автоматически.

{% else %}

Вызов `CREATE TABLE` создает {% if concept_table %}[таблицу]({{ concept_table }}){% else %}таблицу{% endif %} с указанной схемой данных{% if feature_map_tables %}  и ключевыми колонками (`PRIMARY KEY`){% endif %}. {% if feature_secondary_index == true %}Позволяет определить вторичные индексы на создаваемой таблице.{% endif %}

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

{% if feature_olap_tables %}#{%endif%}## Колонки {#row-columns}

{% if feature_column_container_type == true %}
Для неключевых колонок допускаются любые типы данных, для ключевых - только [примитивные](../../types/primitive.md). При указании сложных типов (например, `List<String>`) тип заключается в двойные кавычки.
{% else %}
Для ключевых и неключевых колонок допускаются только [примитивные](../../types/primitive.md) типы данных.
{% endif %}

{% if feature_not_null == true %}
Без дополнительных модификаторов колонка приобретает [опциональный тип](../../types/optional.md) тип, и допускает запись `NULL` в качестве значений. Для получения неопционального типа необходимо использовать `NOT NULL`.
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

**Пример**

    CREATE TABLE my_table (
{% if feature_not_null_for_pk %}        a Uint64 NOT NULL,{% else %}        a Uint64,{% endif %}
        b Bool,
{% if feature_not_null %}        c Float NOT NULL,{% else %}        c Float,{% endif %}
{% if feature_column_container_type %}         d "List<List<Int32>>"{% endif %}
{% if feature_map_tables %}
        PRIMARY KEY (b, a)
{% endif %}
    )



{% if feature_secondary_index %}
{% if feature_olap_tables %}#{%endif%}## Вторичные индексы {#secondary_index}

Конструкция INDEX используется для определения {% if concept_secondary_index %}[вторичного индекса]({{ concept_secondary_index }}){% else %}вторичного индекса{% endif %} на таблице:

```sql
CREATE TABLE table_name (
    ...
    INDEX <index_name> GLOBAL [SYNC|ASYNC] ON ( <index_columns> ) COVER ( <cover_columns> ),
    ...
)
```

где:
* **index_name** — уникальное имя индекса, по которому будет возможно обращение к данным.
* **SYNC/ASYNC** — синхронная или асинхронная запись в индекс, если не указано — синхронная.
* **index_columns** — имена колонок создаваемой таблицы через запятую, по которым возможен поиск в индексе.
* **cover_columns** — имена колонок создаваемой таблицы через запятую, которые будет сохранены в индексе дополнительно к колонкам поиска, давая возможность получить дополнительные данные без обращения за ними в таблицу.

**Пример**

```sql
CREATE TABLE my_table (
    a Uint64,
    b Bool,
    c Utf8,
    d Date,
    INDEX idx_d GLOBAL ON (d),
    INDEX idx_ba GLOBAL ASYNC ON (b, a) COVER (c),
    PRIMARY KEY (a)
)
```
{% endif %}

{% if feature_temp_tables %}
{% if feature_olap_tables %}#{%endif%}## Создание временных таблиц {#temporary_tables}
```sql
CREATE TEMPORARY TABLE table_name (
    ...
);
```
{% include [temp-table-description.md](../../../../_includes/temp-table-description.md) %}

{% endif %}

{% if feature_map_tables and concept_table %}
{% if feature_olap_tables %}#{%endif%}## Дополнительные параметры {#row-additional}

Для таблицы может быть указан ряд специфичных для {{ backend_name }} параметров. При создании таблицы такие параметры перечисляются в блоке ```WITH```:

```sql
CREATE TABLE table_name (...)
WITH (
    key1 = value1,
    key2 = value2,
    ...
)
```

Здесь key — это название параметра, а value — его значение.

Перечень допустимых имен параметров и их значений приведен на странице [описания таблицы {{ backend_name }}]({{ concept_table }}).

Например, такой код создаст таблицу с включенным автоматическим партиционированием по размеру партиции и предпочитаемым размером каждой партиции 512 мегабайт:

<small>Листинг 4</small>

```sql
CREATE TABLE my_table (
    id Uint64,
    title Utf8,
    PRIMARY KEY (id)
)
WITH (
    AUTO_PARTITIONING_BY_SIZE = ENABLED,
    AUTO_PARTITIONING_PARTITION_SIZE_MB = 512
);
```

{% if feature_olap_tables %}#{%endif%}## Группы колонок {#column-family}

Колонки одной таблицы можно объединять в группы, для того чтобы задать следующие параметры:

* `DATA` — тип устройства хранения для данных колонок этой группы. Допустимые значения: ```"ssd"```, ```"rot"```.
* `COMPRESSION` — кодек сжатия данных. Допустимые значения: ```"off"```, ```"lz4"```.

По умолчанию все колонки находятся в одной группе с именем ```default```.  При желании, параметры этой группы тоже можно переопределить.

В примере ниже для создаваемой таблицы добавляется группа колонок ```family_large``` и устанавливается для колонки ```series_info```, а также переопределяются параметры для группы ```default```, которая по умолчанию установлена для всех остальных колонок.

```sql
CREATE TABLE series_with_families (
    series_id Uint64,
    title Utf8,
    series_info Utf8 FAMILY family_large,
    release_date Uint64,
    PRIMARY KEY (series_id),
    FAMILY default (
        DATA = "ssd",
        COMPRESSION = "off"
    ),
    FAMILY family_large (
        DATA = "rot",
        COMPRESSION = "lz4"
    )
);
```

{% note info %}

Доступные типы устройств хранения зависят от конфигурации кластера {{ ydb-short-name }}.

{% endnote %}

{% endif %}

{% endif %}

{% if feature_olap_tables %}

## Колоночные таблицы {#olap-tables}

{% note warning %}

Колоночные таблицы {{ ydb-short-name }} доступны в режиме Preview.

{% endnote %}

Вызов `CREATE TABLE` создает [колоночную таблицу](../../../../concepts/datamodel/table.md#column-tables) с указанной схемой данных и ключевыми колонками (`PRIMARY KEY`).

```sql
CREATE TABLE table_name (
    column1 type1,
    column2 type2 NOT NULL,
    column2 type2,
    ...
    columnN typeN,
    PRIMARY KEY ( column, ... ),
    ...
)
PARTITION BY HASH(column1, column2, ...)
WITH (
    STORE = COLUMN,
    key = value,
    ...
)
```

### Колонки {#olap-columns}

Поддерживаемые типы данных в колоночных таблицах и ограничение на использование типов в первичных ключах или колонках данных описаны в разделе [колоночные таблицы](../../../../concepts/datamodel/table.md#column-tables).

Обязательно указание `PRIMARY KEY` и `PARTITION BY` с непустым списком колонок.

Без дополнительных модификаторов колонка приобретает [опциональный](../../types/optional.md) тип и допускает запись `NULL` в качестве значений. Для получения неопционального типа необходимо использовать `NOT NULL`.

**Пример**

```sql
CREATE TABLE my_table (
    a Uint64 NOT NULL,
    b String,
    c Float,
    PRIMARY KEY (b, a)
)
PARTITION BY HASH(b)
WITH (
STORE = COLUMN
)
```

### Дополнительные параметры {#olap-additional}

Для таблицы может быть указан ряд специфичных для {{ backend_name }} параметров. При создании таблицы такие параметры перечисляются в блоке ```WITH```:

```sql
CREATE TABLE table_name (...)
WITH (
    key1 = value1,
    key2 = value2,
    ...
)
```

Здесь `key` — это название параметра, а `value` — его значение.

Поддерживаемые параметры колоночных таблиц:

* `AUTO_PARTITIONING_MIN_PARTITIONS_COUNT` — определяет минимальное физическое количество партиций для хранения данных (см. [{#T}](../../../../concepts/datamodel/table.md#olap-tables-partitioning)).

Например, следующий код создает колоночную таблицу с 10-ю партициями:

```sql
CREATE TABLE my_table (
    id Uint64,
    title Utf8,
    PRIMARY KEY (id)
)
PARTITION BY HASH(id)
WITH (
    AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 10
);
```

{%endif%}
