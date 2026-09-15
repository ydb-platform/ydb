### FAMILY <family_name> (column setting)

{% if oss == true and backend_name == "YDB" %}

{% include [OLTP_only_allow_note](../../../../_includes/only_allow_for_oltp_note.md) %}

{% endif %}

Specifies that this column belongs to the specified column group. For more information, see [{#T}](../create_table/family.md).

### DEFAULT <default_value>

{% note warning %}

The `DEFAULT` option is supported:

* Only for [row](../../../../concepts/datamodel/table.md#row-oriented-tables) tables.
* Only with literal values.

{% endnote %}

Sets a default value for the column. If no value is specified for this column when inserting a row, the specified default value is used. The default value must match the column's data type.

### NOT NULL

`DROP NOT NULL` removes the `NOT NULL` constraint from a non-key column, allowing `NULL` values. This operation is supported for both [row-oriented](../../../../concepts/datamodel/table.md#row-oriented-tables) and [column-oriented](../../../../concepts/datamodel/table.md#column-oriented-tables) tables.

```yql
ALTER TABLE table_name ALTER COLUMN column_name DROP NOT NULL;
```

### COMPRESSION([algorithm=<algorithm_name>[, level=<value>]]) {#compression}

{% if oss == true and backend_name == "YDB" %}

{% include [OLAP_only_allow_note](../../../../_includes/only_allow_for_olap_note.md) %}

{% endif %}

The following compression parameters can be set for columns:

* `algorithm` — data compression algorithm. Valid values: `off` (disable compression), `lz4`, `zstd`.
* `level` — compression level, supported only for the `zstd` algorithm (valid values from 0 to 22).

If `COMPRESSION()` is specified without parameters, the default compression is used for the column. Currently this is `lz4`; future versions will allow configuring default compression at the cluster or table level.
