# UNIQUE DISTINCT hints

Directly after `SELECT`, it is possible to add [SQL hints](../lexer.md#sql-hints) `unique` or `distinct`, which declare that this projection generates data containing unique values in the specified set of columns of a row-based or columnar table. This can be used to optimize subsequent subqueries executed on this projection, or for writing to table meta-attributes during `INSERT` (currently not supported for columnar tables).

* Columns are specified in the hint values, separated by spaces.
* If the set of columns is not specified, uniqueness applies to the entire set of columns in this projection.
* `unique` - indicates unique or `null` values. According to the SQL standard, each null is unique: NULL = NULL -> NULL.
* `distinct` - indicates completely unique values including null: NULL IS DISTINCT FROM NULL -> FALSE.
* Multiple sets of columns can be specified in several hints for a single projection.
* If the hint contains a column that is not in the projection, it will be ignored.

