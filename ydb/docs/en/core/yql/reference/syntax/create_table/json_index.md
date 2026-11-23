# JSON-index

{% if backend_name == 'YDB' %} [JSON indexes](../../../../dev/json-indexes.md){% else %}JSON indexes{% endif %} in {% if backend_name == 'YDB' %}[row](../../../../concepts/datamodel/table.md#row-oriented-tables){% else %}row{% endif %} tables are created using the same syntax as [secondary indexes](secondary_index.md), by specifying `json` as the index type. A subset of the syntax available for JSON indexes:


```yql
CREATE TABLE `<table_name>` (
    ...
    INDEX `<index_name>`
        GLOBAL
        [SYNC]
        USING json
        ON ( [<prefix_columns>,] <json_column> )
    [,   ...]
)
```


Where:

* `<index_name>` — unique index name for data access.
* `SYNC` — specifies synchronous index update. This is the only mode available for JSON indexes; explicit specification is not required.
* `<prefix_columns>` — optional filter columns for a [filtered JSON index](../../../../dev/json-indexes.md#filtered). Their types must be allowed in a primary key; together, they must not include all columns of the table's primary key.
* `<json_column>` — a single column of type `Json` or `JsonDocument`, specified last in `ON`.

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

## Example with a filter column {#filtered}

To search within a particular user's documents, specify `user_id` before the JSON column:

```yql
CREATE TABLE documents (
    id Uint64 NOT NULL,
    user_id Uint64,
    payload JsonDocument,
    INDEX json_idx GLOBAL USING json ON (user_id, payload),
    PRIMARY KEY (id)
);
```

Queries using this index must specify `user_id = <value>` together with a JSON condition. See the [query example](../select/json_index.md#filtered).
