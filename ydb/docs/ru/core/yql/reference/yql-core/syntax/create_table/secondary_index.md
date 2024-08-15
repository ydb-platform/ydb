# INDEX

{% include [not_allow_for_olap](../../../../_includes/not_allow_for_olap_note.md) %}

Конструкция `INDEX` используется для определения {% if concept_secondary_index %}[вторичного индекса]({{ concept_secondary_index }}){% else %}вторичного индекса{% endif %} {% if backend_name == "YDB" %}для [строковых](../../../../concepts/datamodel/table.md#row-oriented-tables) таблиц{% else %}на таблице{% endif %}:

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

{% if backend_name == "YDB" %}
## Примеры создания таблиц со вторичным индексом {#secondary-index-tables-example}

{% list tabs %}

- Строковая таблица cо вторичным индексом

    ```sql
    CREATE TABLE my_table (
        a Uint64,
        b Uint64,
        c Utf8,
        d Date,
        INDEX idx_d GLOBAL ON (d),
        INDEX idx_ba GLOBAL ASYNC ON (b, a) COVER (c),
        PRIMARY KEY (a)
    )
    ```

- Колоночная таблица cо вторичным индексом

    ```sql
    CREATE TABLE my_table (
    a Uint64 NOT NULL,
    b Uint64,
    c Utf8,
    d Date,
    INDEX idx_d GLOBAL ON (d),
    INDEX idx_ba GLOBAL ASYNC ON (b, a) COVER (c),
    PRIMARY KEY (a)
    )
    WITH (
        STORE = COLUMN
    );
    ```

{% endlist %}
{% else %}
**Пример**
```sql
CREATE TABLE my_table (
    a Uint64,
    b Uint64,
    c Utf8,
    d Date,
    INDEX idx_d GLOBAL ON (d),
    INDEX idx_ba GLOBAL ASYNC ON (b, a) COVER (c),
    PRIMARY KEY (a)
)
```
{% endif %}
