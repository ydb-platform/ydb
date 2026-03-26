### FAMILY <family_name> (column setting)

Specifies that this column belongs to the specified column group. For more information, see [{#T}](../create_table/family.md).

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
