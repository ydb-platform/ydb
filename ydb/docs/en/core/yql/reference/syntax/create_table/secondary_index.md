# INDEX

{% include [not_allow_for_olap](../../../../_includes/not_allow_for_olap_note.md) %}

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
