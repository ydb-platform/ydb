# Classic SQL constructs not supported yet

## Correlated EXISTS and NOT EXISTS {#not-exists}

`EXISTS` and `NOT EXISTS` can be used with uncorrelated subqueries. Correlated
subqueries are not supported. To select rows based on the presence or absence
of matching rows, use `LEFT SEMI JOIN` or `LEFT ONLY JOIN`. For details, see
[Correlated subqueries, EXISTS, and NOT EXISTS](correlated-subqueries.md).

## INTERSECT and EXCEPT {#intersect-except}

YQL does not support `INTERSECT` or `EXCEPT`.

## NATURAL JOIN {#natural-join}

An alternative is to explicitly list the matching columns on both sides.

## NOW() / CURRENT_TIME() {#now}

An alternative is to use the functions [CurrentUtcDate, CurrentUtcDatetime and CurrentUtcTimestamp](../builtins/basic.md#current-utc).
