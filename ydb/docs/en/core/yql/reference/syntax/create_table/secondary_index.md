# INDEX

The INDEX construct is used to define a {% if concept_secondary_index %}[secondary index]({{ concept_secondary_index }}){% else %}secondary index{% endif %} in a [row-oriented](../../../../concepts/datamodel/table.md#row-oriented-tables) table:

```yql
CREATE TABLE `<table_name>` (
  ...
    INDEX `<index_name>`
    [GLOBAL|LOCAL]
    [UNIQUE]
    [SYNC|ASYNC]
    [USING <index_type>]
    ON ( <index_columns> )
    [COVER ( <cover_columns> )]
    [WITH ( <parameter_name> = <parameter_value>[, ...])]
  [,   ...]
)
```

where:

{% include [index_grammar_explanation.md](../_includes/index_grammar_explanation.md) %}

{% include [not_allow_for_olap](../../../../_includes/not_allow_for_olap_note.md) %}

For [column-oriented tables](../../../../concepts/datamodel/table.md#column-oriented-tables), you can define **local Bloom skip indexes** in `CREATE TABLE` using `INDEX ... LOCAL USING bloom_filter` or `INDEX ... LOCAL USING bloom_ngram_filter`. See [ALTER TABLE ADD INDEX — Local Bloom skip indexes](../../../alter_table/indexes.md#local-bloom-column) for parameters and index types.

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

### Column-oriented table with local Bloom skip index

```yql
CREATE TABLE events (
    ts Timestamp NOT NULL,
    user_id Uint64 NOT NULL,
    resource_id Utf8,
    PRIMARY KEY (ts, user_id),
    INDEX idx_bloom LOCAL USING bloom_filter
        ON (resource_id)
        WITH (false_positive_probability = 0.01)
)
WITH (STORE = COLUMN)
PARTITION BY HASH(ts, user_id);
```
