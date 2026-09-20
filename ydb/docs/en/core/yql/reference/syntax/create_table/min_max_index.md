# min_max index

{% if backend_name == 'YDB' %} [min-max index](../../../../dev/min_max-skip-index.md){% else %}min-max index{% endif %} is a [local index](../../../../concepts/glossary.md#local-index) that can only be specified with the `LOCAL` keyword. When creating a table, the `min_max` type is used in the `INDEX` section (similar to a [secondary index](secondary_index.md), but with a mandatory `LOCAL` and corresponding `USING`). See also [local indexes](../../../../concepts/query_execution/local_indexes.md).


```yql
CREATE TABLE `<table_name>` (
    ...
    INDEX `<index_name>`
        LOCAL
        USING min_max
        ON ( <index_column> )
    [,   ...]
)
```


Where:

* `<index_name>` — index name.
* `LOCAL` — required keyword for the min_max index.
* `<index_column>` — the column on which the index is built. You must specify exactly one column.
* For the min_max index, covering columns (`COVER (...)`) and additional data columns are not supported.

`WITH (...)` parameters:

{% include [min_max_index_parameters.md](../_includes/min_max_index_parameters.md) %}

Creating a min_max index for an existing table is described in the [`ALTER TABLE ADD INDEX`](../alter_table/indexes.md#local-min-max) section.

## Example {#example}


```yql
CREATE TABLE events (
    id Uint64,
    created_at Timestamp,
    level Int32,
    PRIMARY KEY (id),
    INDEX idx_created_at LOCAL USING min_max
        ON (created_at),
    INDEX idx_level LOCAL USING min_max
        ON (level)
)
WITH (
    STORE = COLUMN
);
```
