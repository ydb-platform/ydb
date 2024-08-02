## SELECT execution procedure {#selectexec}

The `SELECT` query result is calculated as follows:

* Determine the set of input tables by evaluating the [FROM](../../select/from.md) clauses.
* Apply [SAMPLE](../../select/sample.md)/[TABLESAMPLE](../../select/sample.md) to input tables.
* Execute [FLATTEN COLUMNS](../../flatten.md#flatten-columns) or [FLATTEN BY](../../flatten.md); aliases set in `FLATTEN BY` become visible after this point.
{% if feature_join %}* Execute every [JOIN](../../join.md).{% endif %}
* Add to (or replace in) the data the columns listed in [GROUP BY ... AS ...](../../group_by.md).
* Execute [WHERE](../../select/where.md) &mdash; Discard all the data mismatching the predicate.
* Execute [GROUP BY](../../group_by.md), evaluate aggregate functions.
* Apply the filter [HAVING](../../group_by.md#having).
{% if feature_window_functions %} * Evaluate [window functions](../../window.md);{% endif %}
* Evaluate expressions in `SELECT`.
* Assign names set by aliases to expressions in `SELECT`.
* Apply top-level [DISTINCT](../../select/distinct.md) to the resulting columns.
* Execute similarly every subquery inside [UNION ALL](../../select/union.md#union_all), combine them (see [PRAGMA AnsiOrderByLimitInUnionAll](../../pragma.md#pragmas)).
* Perform sorting with [ORDER BY](../../select/order_by.md).
* Apply [OFFSET and LIMIT](../../select/limit_offset.md) to the result.

