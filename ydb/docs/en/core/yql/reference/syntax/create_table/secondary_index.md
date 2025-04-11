# INDEX

{% include [not_allow_for_olap](../../../../_includes/not_allow_for_olap_note.md) %}

The INDEX construct is used to define a {% if concept_secondary_index %}[secondary index]({{ concept_secondary_index }}){% else %}secondary index{% endif %} in a [row-oriented](../../../../concepts/datamodel/table.md#row-oriented-tables) table:

```yql
CREATE TABLE table_name (
    ...
    INDEX <index_name> GLOBAL [UNIQUE] [SYNC|ASYNC] [USING <index_type>] ON ( <index_columns> ) COVER ( <cover_columns> ),
    ...
)
```

Where:

* **Index_name** is the unique name of the index to be used to access data.
* **SYNC/ASYNC** indicates synchronous/asynchronous data writes to the index. If not specified, synchronous.
* **Index_type** is the index type. Only `secondary` is supported now.
* **UNIQUE** indicates that the index should guarantee the uniqueness of the indexed column set, thereby enforcing the unique constraint.
* **Index_columns** is a list of comma-separated names of columns in the created table to be used for a search in the index.
* **Cover_columns** is a list of comma-separated names of columns in the created table, which will be stored in the index in addition to the search columns, making it possible to fetch additional data without accessing the table for it.

## Example

```yql
CREATE TABLE my_table (
    a Uint64,
    b Bool,
    c Utf8,
    d Date,
    INDEX idx_d GLOBAL ON (d),
    INDEX idx_ba GLOBAL ASYNC ON (b, a) COVER (c),
    INDEX idx_bc GLOBAL UNIQUE SYNC ON (b, c),
    PRIMARY KEY (a)
)
```
