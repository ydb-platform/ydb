# Overloaded errors

{{ ydb-short-name }} returns `OVERLOADED` errors in the following cases:

* Overloaded table partitions with over 10k operations in their queue

* The number of sessions with a {{ ydb-short-name }} node has reached the limit of 1000

## Diagnostics

{% include notitle [#](_includes/overloaded-errors.md) %}

## Recommendations

If a YQL query returns an *OVERLOADED* error, we recommend retrying the query with a back-off strategy.

The exceeded limit of open sessions per node may indicate a problem in the application logic.