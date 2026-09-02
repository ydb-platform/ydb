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

### COMPRESSION([algorithm=<algorithm_name>[, level=<value>]]) {#compression}

{% if oss == true and backend_name == "YDB" %}

{% include [OLAP_only_allow_note](../../../../_includes/only_allow_for_olap_note.md) %}

{% endif %}

The following compression parameters can be set for columns:

* `algorithm` — data compression algorithm. Valid values: `off` (disable compression), `lz4`, `zstd`.
* `level` — compression level, supported only for the `zstd` algorithm (valid values from 0 to 22).

If `COMPRESSION()` is specified without parameters, the default compression is used for the column. Currently this is `lz4`; future versions will allow configuring default compression at the cluster or table level.

### ENCODING([OFF|DICT]) {#encoding}

{% if oss == true and backend_name == "YDB" %}

{% include [OLAP_only_allow_note](../../../../_includes/only_allow_for_olap_note.md) %}

{% endif %}

Sets the encoding method for the column data.

Available options:

* `ENCODING(DICT)`: enables dictionary encoding. Duplicate values are replaced with small integer identifiers, and the values themselves are stored in a dictionary. Dictionary encoding is effective for columns with low cardinality (a small number of unique values). It reduces the amount of stored data and speeds up some operations. Supported only for comparable data types, such as `String`, `Timestamp`, `UInt64`, and others. For non-comparable types, such as `Json`, `JsonDocument`, or `Yson`, using `ENCODING(DICT)` will result in an error.
* `ENCODING(OFF)`: disables special encoding. Data will be stored in standard format without additional encoding.

If `ENCODING()` is specified without parameters, the default encoding will be used for the column. Currently it is `OFF`; future versions will allow configuring the default encoding at the database or table level.
