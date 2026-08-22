### FAMILY <family_name> (column setting)

<<<<<<< HEAD
Specifies that this column belongs to the specified column group. For more information, see [{#T}](../create_table/family.md).
=======
{% if oss == true and backend_name == "YDB" %}

{% include [OLTP_only_allow_note](../../../../_includes/only_allow_for_oltp_note.md) %}

{% endif %}

Specifies the belonging of this column to the specified group of columns. For more details, see the section [{#T}](../create_table/family.md).
>>>>>>> 3cc83c6fefe (Clarifications regarding column groups (#45849))

### DEFAULT <default_value>

{% note warning %}

The `DEFAULT` option is supported:

* Only for [row-oriented](../../../../concepts/datamodel/table.md#row-oriented-tables) tables. Support for [column-oriented](../../../../concepts/datamodel/table.md#column-oriented-tables) tables is under development.
* Only with literal values. Support for computed expressions is under development.

{% endnote %}

Allows you to set a default value for a column. If no value is specified for this column when inserting a row, the specified default value will be used. The default value must match the column's data type.

The `DEFAULT false NOT NULL` construct is invalid due to ambiguity in interpretation. In this case, use a comma-separated list or change the order of options.

### NULL

This column can contain `NULL` values (default).

### NOT NULL

This column does not accept `NULL` values.

### COMPRESSION([algorithm=<algorithm_name>[, level=<value>]]) {#compression}

{% if oss == true and backend_name == "YDB" %}

{% include [OLAP_only_allow_note](../../../../_includes/only_allow_for_olap_note.md) %}

{% endif %}

You can set the following compression parameters for columns:

* `algorithm` — compression algorithm. Allowed values: `off` (disable compression), `lz4`, `zstd`.

* `level` — compression level; supported only for `zstd` (allowed values are 0 through 22).

<<<<<<< HEAD
If `COMPRESSION()` is specified without parameters, the column uses the default compression. Currently that is `lz4`; future versions will let you configure default compression at the cluster or table level.
=======
### ENCODING([OFF|DICT]) {#encoding}

{% if oss == true and backend_name == "YDB" %}

{% include [OLAP_only_allow_note](../../../../_includes/only_allow_for_olap_note.md) %}

{% endif %}

Allows you to set the data encoding method for the column.

Available options:

* `ENCODING(DICT)` — enables dictionary encoding. Repeating values are replaced with small integer identifiers, and the values themselves are stored in a dictionary. Dictionary encoding is effective for columns with low cardinality (a small number of unique values). It reduces the amount of stored data and speeds up some operations. It is supported only for comparable data types, such as `String`, `Timestamp`, `UInt64`, and others. Using `ENCODING(DICT)` for incomparable types, such as `Json`, `JsonDocument`, or `Yson`, will result in an error.
* `ENCODING(OFF)` — disables special encoding. Data will be stored in the standard format without additional encoding.

If `ENCODING()` is set without parameters, the default encoding will be used for the column. Currently, it is `OFF`; in future versions, it will be possible to configure the default encoding at the database or table level.
>>>>>>> 3cc83c6fefe (Clarifications regarding column groups (#45849))
