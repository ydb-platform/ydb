# CREATE TABLE

{% if feature_olap_tables %}

{{ ydb-short-name }} поддерживает два типа таблиц:

* [строковые](../../../../concepts/datamodel/table.md);
* [колоночные](../../../../concepts/datamodel/table.md#column-tables).

Тип таблицы при создании задается параметром `STORE` в блоке `WITH`, где `ROW` означает [строковую таблицу](../../../../concepts/datamodel/table.md), а `COLUMN` — [колоночную](../../../../concepts/datamodel/table.md#column-tables). По умолчанию, если параметр `STORE` не указан, создается строковая таблица:

```sql
CREATE table_name (
  columns 
  ...
)

WITH (
  STORE = COLUMN -- Default value ROW
)
```

{%endif%}

## Синтаксис CREATE TABLE

{% if feature_bulk_tables %}

Таблица создается автоматически при первом [INSERT INTO](insert_into.md){% if feature_mapreduce %}, в заданной оператором [USE](../use.md) базе данных{% endif %}. Схема при этом определяется автоматически.

{% else %}

Вызов `CREATE TABLE` создает {% if concept_table %}[таблицу]({{ concept_table }}){% else %}таблицу{% endif %} с указанной схемой данных{% if feature_map_tables %}  и ключевыми колонками (`PRIMARY KEY`){% endif %}. {% if feature_secondary_index == true %}Позволяет определить вторичные индексы на создаваемой таблице.

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

### Примеры создания таблиц

{% list tabs %}

- Создание строковой таблицы


    CREATE TABLE table_name (
      a Uint64,
      b Uint64,
      c Float,
      {% if feature_column_container_type %}    d "List<List<Int32>>" {% endif %}
      PRIMARY KEY (a, b)
    );
   

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

- Создание колоночной таблицы

  ```sql
  CREATE TABLE table_name (
    a Uint64 NOT NULL,
    b Uint64 NOT NULL,
    c Float,
    PRIMARY KEY (a, b)
  )
  WITH (
    STORE = COLUMN
  );
  ```  

{% endlist %}

При создании строковых и колоночных таблиц возможно задать:
* [Вторичный индекс](secondary_index.md).
* [Группы колонок](family.md) (поддерживается только в строковых таблицах).
* [Дополнительные параметры](with.md):
  + Время жизни записи в таблице (TTL).
  + Шардирование таблицы и размер шарда.