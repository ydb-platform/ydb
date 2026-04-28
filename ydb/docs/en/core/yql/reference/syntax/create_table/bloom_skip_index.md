# Bloom skip index

{% if backend_name == 'YDB' %}[Bloom skip indexes](../../../../dev/bloom-skip-indexes.md){% else %}Bloom skip indexes{% endif %} are defined only as local (`LOCAL`) indexes using `bloom_filter` or `bloom_ngram_filter` in the `INDEX` clause when you create a table (similar to a [secondary index](secondary_index.md), but with `LOCAL` and the matching `USING` type).

```yql
CREATE TABLE `<table_name>` (
    ...
    INDEX `<index_name>`
        LOCAL
        USING bloom_filter | bloom_ngram_filter
        ON ( <index_columns> )
        [WITH ( <parameter_name> = <parameter_value>[, ...])]
    [,   ...]
)
```

Where:

* `<index_name>`: Index name.
* `LOCAL`: Required for Bloom skip indexes.
* `<index_columns>`: One or more columns in `ON (...)`, depending on the table type and index type (see [limitations](../../../../dev/bloom-skip-indexes.md#limitations)).
* `COVER (...)` and data columns are **not supported** for Bloom skip indexes.

`WITH (...)` parameters:

{% include [bloom_skip_index_parameters.md](../_includes/bloom_skip_index_parameters.md) %}

Creating and altering these indexes on an existing table is described in [`ALTER TABLE ADD INDEX`](../alter_table/indexes.md#local-bloom).

## Examples

### `bloom_filter` index

```yql
CREATE TABLE events (
    id Uint64,
    resource_id Utf8,
    PRIMARY KEY (id),
    INDEX idx_bloom LOCAL USING bloom_filter
        ON (resource_id)
        WITH (false_positive_probability = 0.01)
);
```

### `bloom_ngram_filter` index

```yql
CREATE TABLE logs (
    id Uint64,
    message Utf8,
    PRIMARY KEY (id),
    INDEX idx_ngram LOCAL USING bloom_ngram_filter
        ON (message)
        WITH (
            ngram_size = 3,
            false_positive_probability = 0.01,
            case_sensitive = true
        )
);
```
