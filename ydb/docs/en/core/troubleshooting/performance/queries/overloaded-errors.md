# Overloaded errors

{{ ydb-short-name }} returns `OVERLOADED` errors in the following cases:

* Overloaded table partitions with over 15000 queries in their queue.

* The outbound [CDC](../../../concepts/glossary.md#cdc) queue exceeds the limit of 10000 elements or 125 MB.

* Table partitions in states other than normal, for example partitions in the process of splitting or merging.

* The number of sessions with a {{ ydb-short-name }} node has reached the limit of 1000.

## Diagnostics

<!-- The include is added to allow partial overrides in overlays  -->
{% include notitle [#](_includes/overloaded-errors.md) %}

## Recommendations

If a YQL query returns an `OVERLOADED` error, retry the query using a randomized exponential back-off strategy. The YDB SDK provides a built-in mechanism for handling temporary failures. For more information, see [{#T}](../../../reference/ydb-sdk/error_handling.md).

Exceeding the limit of open sessions per node may indicate a problem in the application logic.
