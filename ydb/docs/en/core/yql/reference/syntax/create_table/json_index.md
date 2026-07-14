# JSON index

{% if backend_name == 'YDB' %} [JSON indexes](../../../../dev/json-indexes.md){% else %}JSON indexes{% endif %} in {% if backend_name == 'YDB' %}[string](../../../../concepts/datamodel/table.md#row-oriented-tables){% else %}string{% endif %} tables are created using the same syntax as [secondary indexes](secondary_index.md), when specifying `json` as the index type. A subset of the syntax available for JSON indexes:


```yql
CREATE TABLE `<table_name>` (
    ...
    INDEX `<index_name>`
        GLOBAL
        [SYNC]
        USING json
        ON ( <json_column> )
    [,   ...]
)
```


Where:

* `<index_name>`: unique index name for data access.
* `SYNC`: indicates synchronous index update. This is the only mode available for JSON indexes; it does not need to be explicitly specified.
* `<json_column>`: table column of type `Json` or `JsonDocument`. A JSON index is built on a single column only.

A JSON index does not support the `COVER` expression — attempting to specify it will result in an error.

{% include [not_allow_for_olap](../../../../_includes/not_allow_for_olap_note.md) %}

## Example


```yql
CREATE TABLE documents (
    id Uint64 NOT NULL,
    payload JsonDocument NOT NULL,
    INDEX json_idx GLOBAL USING json ON (payload),
    PRIMARY KEY (id)
)
```


In this example, a table `documents` is created with a JSON index `json_idx` on column `payload`. The index will be used by queries whose `WHERE` predicate contains calls to `JSON_EXISTS` or `JSON_VALUE` on column `payload`.
