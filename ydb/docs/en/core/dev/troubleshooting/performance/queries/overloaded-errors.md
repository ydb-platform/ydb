# Overloaded errors

{{ ydb-short-name }} returns `OVERLOADED` errors in the following cases:

* Overloaded table partitions with over 10000 operations in their queue.

* The number of sessions with a {{ ydb-short-name }} node has reached the limit of 1000.

## Diagnostics

{% include notitle [#](_includes/overloaded-errors.md) %}

## Recommendations

If a YQL query returns an `OVERLOADED` error, retry the query using a randomized exponential back-off strategy.

Exceeding the limit of open sessions per node may indicate a problem in the application logic.