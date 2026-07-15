### FAMILY <family_name> (column setting)

Specifies that this column belongs to the specified column group. For more information, see [{#T}](../create_table/family.md).

### DEFAULT <default_value>

{% note warning %}

The `DEFAULT` option is supported:

* Only for [row-oriented](../../../../concepts/datamodel/table.md#row-oriented-tables) tables.
* Only with literal values.  

{% endnote %}

Allows you to set a default value for a column. If no value is specified for this column when inserting a row, the specified default value will be used. The default value must match the column's data type.

### NOT NULL

{% note warning %}

Currently only `DROP NOT NULL` is supported. 

{% endnote %}

Removes the `NOT NULL` constraint from the column, allowing `NULL` values again.

### COMPRESSION([algorithm=<algorithm_name>[, level=<value>]])

{% if oss == true and backend_name == "YDB" %}

{% include [OLAP_only_allow_note](../../../../_includes/only_allow_for_olap_note.md) %}

{% endif %}

You can set the following compression parameters for columns:

* `algorithm` — compression algorithm. Allowed values: `off` (disable compression), `lz4`, `zstd`.

* `level` — compression level; supported only for `zstd` (allowed values are 0 through 22).

If `COMPRESSION()` is specified without parameters, the column uses the default compression. Currently that is `lz4`; future versions will let you configure default compression at the cluster or table level.

### ENCODING([OFF|DICT])

{% if oss == true and backend_name == "YDB" %}

{% include [OLAP_only_allow_note](../../../../_includes/only_allow_for_olap_note.md) %}

{% endif %}

Sets the encoding for a column's data.

Available options:

* `ENCODING(DICT)` — enables dictionary encoding. Repeated values are replaced by small integer identifiers stored in a dictionary. Effective for low-cardinality columns (few unique values). Supported only for comparable types such as `String`, `Timestamp`, `UInt64`. Using `ENCODING(DICT)` on non-comparable types (`Json`, `JsonDocument`, `Yson`) returns an error.

* `ENCODING(OFF)` — disables special encoding. Data is stored in the standard format without additional encoding.

If `ENCODING()` is specified without parameters, the column uses the default encoding. Currently that is `OFF`; future versions will let you configure the default encoding at the database or table level.
